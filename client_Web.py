# =============================================================================
# client_web.py - Client P2P con interfaccia web (chat + invio file)
#
# Un browser NON può usare socket UDP, quindi l'hole punching resta in Python.
# Questo programma contiene la stessa logica di client.py e in più avvia un
# piccolo server web SOLO su questo computer (127.0.0.1) che serve index.html:
# la pagina parla con Python tramite HTTP (invio) e Server-Sent Events (ricezione).
#
#   browser (index.html)  <--HTTP/SSE-->  client_web.py  <--UDP P2P-->  altro peer
#
# Uso (index.html deve stare nella stessa cartella di questo file):
#   python client_web.py                               -> inserisci tutto nella pagina web
#   python client_web.py <IP_SERVER> <stanza>          -> si collega subito (reti diverse)
#   python client_web.py <IP_SERVER> <stanza> lan      -> si collega subito (stessa LAN)
# Opzioni: --porta-web 8080   --no-browser
# =============================================================================
import argparse
import json
import math
import os
import random
import re
import socket
import struct
import sys
import threading
import time
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import unquote, urlparse

# --- Percorsi (funzionano sia da script sia da .exe creato con PyInstaller) ---
FROZEN = getattr(sys, "frozen", False)
# cartella dove salvare file ricevuti, temporanei e impostazioni: accanto all'exe (o allo script)
APP_DIR = os.path.dirname(sys.executable if FROZEN else os.path.abspath(__file__))
# cartella dove si trova index.html: dentro l'exe (_MEIPASS) oppure accanto allo script
RES_DIR = getattr(sys, "_MEIPASS", APP_DIR)


def die(msg):
    # Con un .exe aperto con doppio clic la finestra si chiuderebbe subito: aspetta un tasto
    print(msg)
    if FROZEN:
        try:
            input("Premi Invio per chiudere...")
        except EOFError:
            pass
    sys.exit(1)


# --- Argomenti da riga di comando -------------------------------------------
ap = argparse.ArgumentParser(description="Client P2P con interfaccia web")
ap.add_argument("server", nargs="?", help="IP del server di rendezvous")
ap.add_argument("room", nargs="?", help="nome della stanza")
ap.add_argument("mode", nargs="?", choices=["lan"], help="'lan' se peer e server sono sulla stessa rete")
ap.add_argument("--porta-web", type=int, default=8080, help="porta dell'interfaccia web (default 8080)")
ap.add_argument("--no-browser", action="store_true", help="non aprire il browser automaticamente")
args = ap.parse_args()

# Parametri di connessione: si impostano quando si preme "Connetti" nella pagina
SERVER = None
ROOM = None
LAN_MODE = False
CONFIG_PATH = os.path.join(APP_DIR, "config.json")  # ultimi valori usati, per precompilare il modulo

INDEX_FILE = os.path.join(RES_DIR, "index.html")
OUT_DIR = os.path.join(APP_DIR, "ricevuti")    # file ricevuti dal peer
TMP_DIR = os.path.join(APP_DIR, "invio_tmp")   # copie temporanee dei file caricati dal browser

# --- Formato dei pacchetti P2P (identico a client.py) -----------------------
T_PUNCH, T_MSG, T_FILE_HDR, T_FILE_DATA, T_ACK = range(5)
HDR = struct.Struct("!BII")  # tipo (1 byte), id trasferimento (4), sequenza (4)
CHUNK = 1100                 # byte di file per pacchetto
WINDOW = 64                  # pacchetti non confermati ammessi in volo
RTO = 0.5                    # secondi prima di ritrasmettere

# IMPORTANTE: stesso socket per server, STUN e peer, così il NAT riusa lo stesso mapping
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.bind(("0.0.0.0", 0))
s.settimeout(1)

peer = None  # indirizzo (ip, porta) del peer, impostato dopo l'hole punching

# --- Stato condiviso con l'interfaccia web ----------------------------------
events = []                     # log di eventi (messaggi, file, stato) che la pagina mostra
ev_cond = threading.Condition()  # avvisa i client web quando arrivano nuovi eventi
def load_config():
    try:
        with open(CONFIG_PATH) as f:
            cfg = json.load(f)
        return {"server": str(cfg.get("server", "")), "room": str(cfg.get("room", "")),
                "lan": bool(cfg.get("lan", False))}
    except (OSError, ValueError, AttributeError):
        return {"server": "", "room": "", "lan": False}


# phase: "setup" (mostra il modulo) -> "connecting" (registrazione/punching) -> "connected"
state = {"phase": "setup", "peer": None, "config": load_config()}
phase_lock = threading.Lock()
cancel_ev = threading.Event()  # impostato dal pulsante "Annulla"
transfers = {}                  # trasferimenti in corso (per le barre di avanzamento)
tr_lock = threading.Lock()
acks = {}                       # tid -> seq confermate dal peer (lato mittente)
incoming = {}                   # tid -> {"meta": ..., "chunks": {...}} (lato ricevente)
done = set()                    # tid già completati


def emit(kind, **data):
    # Aggiunge un evento al log e sveglia i client web in ascolto
    with ev_cond:
        events.append({"id": len(events), "kind": kind, "t": time.strftime("%H:%M:%S"), **data})
        ev_cond.notify_all()


def set_transfer(tid, name, direction, done_, total):
    with tr_lock:
        transfers[tid] = {"id": tid, "name": name, "dir": direction, "done": done_, "total": total}


def drop_transfer(tid):
    with tr_lock:
        transfers.pop(tid, None)


def pkt(t, tid=0, seq=0, payload=b""):
    # Costruisce un pacchetto: intestazione + dati
    return HDR.pack(t, tid, seq) + payload


def local_ip():
    # Scopre l'IP della nostra interfaccia di rete verso il server
    t = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    t.connect(SERVER)  # non invia nulla, serve solo a scegliere l'interfaccia
    ip = t.getsockname()[0]
    t.close()
    return ip


STUN_SERVERS = [("stun.l.google.com", 19302), ("stun.cloudflare.com", 3478)]


def stun_public_addr():
    """Chiede a un server STUN il proprio indirizzo pubblico (stesso socket del P2P)."""
    for host, port in STUN_SERVERS:
        try:
            dst = socket.getaddrinfo(host, port, socket.AF_INET, socket.SOCK_DGRAM)[0][4]
        except OSError:
            continue
        tid = os.urandom(12)
        req = struct.pack("!HHI", 1, 0, 0x2112A442) + tid  # Binding Request
        for _ in range(2):
            s.sendto(req, dst)
            end = time.time() + 1
            while time.time() < end:
                try:
                    data, addr = s.recvfrom(2048)
                except socket.timeout:
                    break
                except ConnectionResetError:  # Windows: ICMP "porta irraggiungibile", si ignora
                    continue
                if addr[0] != dst[0] or len(data) < 20 or data[8:20] != tid:
                    continue
                i, mapped = 20, None
                while i + 4 <= len(data):
                    atype, alen = struct.unpack_from("!HH", data, i)
                    val = data[i + 4:i + 4 + alen]
                    if atype == 0x0020 and alen >= 8 and val[1] == 1:  # XOR-MAPPED-ADDRESS
                        prt = struct.unpack("!H", val[2:4])[0] ^ 0x2112
                        ip = socket.inet_ntoa(bytes(
                            a ^ b for a, b in zip(val[4:8], struct.pack("!I", 0x2112A442))))
                        return ip, prt
                    if atype == 0x0001 and alen >= 8 and val[1] == 1:  # MAPPED-ADDRESS
                        mapped = (socket.inet_ntoa(val[4:8]), struct.unpack("!H", val[2:4])[0])
                    i += 4 + alen + (-alen % 4)
                if mapped:
                    return mapped
    return None


# ---------------------------------------------------------------------------
# Connessione al peer (gira in background, così la pagina è sempre disponibile)
# ---------------------------------------------------------------------------
def back_to_setup(msg=None, is_error=False):
    # Torna al modulo di inserimento dati (errore, annullamento) per poter riprovare
    if msg:
        emit("error" if is_error else "status", text=msg)
    state["phase"] = "setup"


def start_connect(server, room, lan):
    global SERVER, ROOM, LAN_MODE
    with phase_lock:
        if state["phase"] != "setup":
            return False
        state["phase"] = "connecting"
    SERVER, ROOM, LAN_MODE = (server, 9999), room, lan
    cancel_ev.clear()
    state["config"] = {"server": server, "room": room, "lan": lan}
    try:
        with open(CONFIG_PATH, "w") as f:
            json.dump(state["config"], f)
    except OSError:
        pass  # non è un problema se non si riesce a salvare
    threading.Thread(target=connect, daemon=True).start()
    return True


def connect():
    global peer
    try:
        lan_ip, lan_port = local_ip(), s.getsockname()[1]
    except OSError as e:
        return back_to_setup(f"Server non raggiungibile: {e}", True)

    # Indirizzo pubblico via STUN (serve se siamo sulla stessa LAN del server ma il peer è fuori)
    pub = None
    if not LAN_MODE:
        emit("status", text="Scopro il mio indirizzo pubblico (STUN)...")
        pub = stun_public_addr()
    if LAN_MODE:
        emit("status", text="Modalità LAN: uso gli indirizzi locali")
    else:
        emit("status", text=f"Indirizzo pubblico: {pub[0]}:{pub[1]}" if pub
             else "STUN non disponibile: uso l'indirizzo visto dal server")
    reg = f"REGISTER {ROOM} {lan_ip} {lan_port}" + (f" {pub[0]} {pub[1]}" if pub else "")

    # Fase 1: registrazione, reinvia REGISTER finché il server risponde con PEER
    emit("status", text=f"Registrato nella stanza '{ROOM}', in attesa dell'altro peer...")
    candidates = None
    while candidates is None and not cancel_ev.is_set():
        try:
            s.sendto(reg.encode(), SERVER)
            data, addr = s.recvfrom(1024)
            p = data.decode(errors="ignore").split()
            if addr == SERVER and p and p[0] == "PEER":
                if LAN_MODE:
                    candidates = [(p[3], int(p[4]))]
                else:
                    candidates = [(p[1], int(p[2])), (p[3], int(p[4]))]
        except socket.timeout:
            pass
        except ConnectionResetError:  # Windows: ICMP "porta irraggiungibile" (server spento?)
            time.sleep(1)
        except OSError as e:
            return back_to_setup(f"Server non raggiungibile: {e}", True)
    if cancel_ev.is_set():
        return back_to_setup("Collegamento annullato")
    emit("status", text="Peer trovato, avvio hole punching...")

    # Fase 2: hole punching su tutti i candidati
    found = None
    for _ in range(30):
        if cancel_ev.is_set():
            return back_to_setup("Collegamento annullato")
        for c in candidates:
            s.sendto(pkt(T_PUNCH), c)
        try:
            data, addr = s.recvfrom(2048)
            if addr in candidates:
                found = addr
                for _ in range(3):
                    s.sendto(pkt(T_PUNCH), found)
                break
        except (socket.timeout, ConnectionResetError):
            pass

    if found is None:
        return back_to_setup("Hole punching fallito (probabile NAT simmetrico/CGNAT).", True)

    peer = found
    s.settimeout(None)  # da ora recvfrom è bloccante (lo usa il thread receiver)
    state["peer"] = f"{peer[0]}:{peer[1]}"
    state["phase"] = "connected"
    emit("status", text=f"Connesso a {state['peer']}")
    threading.Thread(target=receiver, daemon=True).start()
    threading.Thread(target=keepalive, daemon=True).start()


def unique_path(name):
    # Percorso in ricevuti/ senza sovrascrivere file esistenti (aggiunge _1, _2, ...)
    os.makedirs(OUT_DIR, exist_ok=True)
    name = os.path.basename(name) or "file"  # basename: evita percorsi malevoli tipo ../../x
    base, ext = os.path.splitext(name)
    path, i = os.path.join(OUT_DIR, name), 1
    while os.path.exists(path):
        path = os.path.join(OUT_DIR, f"{base}_{i}{ext}")
        i += 1
    return path


def try_finish(tid):
    # Se sono arrivati intestazione e tutti i pezzi, ricompone e salva il file
    st = incoming[tid]
    meta = st["meta"]
    if meta is None or len(st["chunks"]) < meta["n"]:
        return
    path = unique_path(meta["name"])
    with open(path, "wb") as f:
        for i in range(1, meta["n"] + 1):
            f.write(st["chunks"][i])
    done.add(tid)
    del incoming[tid]
    drop_transfer(tid)
    emit("file_in", name=meta["name"], file=os.path.basename(path), size=meta["size"])


def receiver():
    # Thread che riceve tutti i pacchetti dal peer e li smista per tipo
    while True:
        try:
            data, addr = s.recvfrom(4096)
        except ConnectionResetError:  # Windows: ICMP "porta irraggiungibile", si ignora
            continue
        if addr != peer or len(data) < HDR.size:
            continue
        t, tid, seq = HDR.unpack_from(data)
        payload = data[HDR.size:]
        if t == T_MSG:
            emit("msg", who="peer", text=payload.decode(errors="replace"))
        elif t == T_ACK:
            if tid in acks:
                acks[tid].add(seq)
        elif t in (T_FILE_HDR, T_FILE_DATA):
            s.sendto(pkt(T_ACK, tid, seq), peer)  # conferma sempre, anche i duplicati
            if tid in done:
                continue
            st = incoming.setdefault(tid, {"meta": None, "chunks": {}})
            if t == T_FILE_HDR:
                try:
                    st["meta"] = json.loads(payload)
                except ValueError:
                    continue
            else:
                st["chunks"][seq] = payload
            meta = st["meta"]
            set_transfer(tid, meta["name"] if meta else "file in arrivo", "in",
                         len(st["chunks"]), meta["n"] if meta else 0)
            try_finish(tid)


def keepalive():
    # Ogni 15 s manda un pacchetto per non far chiudere al NAT il buco aperto
    while True:
        time.sleep(15)
        s.sendto(pkt(T_PUNCH), peer)


def send_file(path, name, cleanup=False):
    # Invio affidabile su UDP: finestra scorrevole + ack + ritrasmissione
    try:
        size = os.path.getsize(path)
        f = open(path, "rb")
    except OSError as e:
        emit("error", text=f"Impossibile leggere il file: {e}")
        return
    n = math.ceil(size / CHUNK)
    total = n + 1  # seq 0 = intestazione, 1..n = dati
    tid = random.getrandbits(32)
    acked = acks[tid] = set()
    header = json.dumps({"name": name, "size": size, "n": n}).encode()
    set_transfer(tid, name, "out", 0, total)

    def build(q):
        if q == 0:
            return pkt(T_FILE_HDR, tid, 0, header)
        f.seek((q - 1) * CHUNK)
        return pkt(T_FILE_DATA, tid, q, f.read(CHUNK))

    ok = False
    try:
        sent, base, last_progress, last_count = {}, 0, time.time(), 0
        while len(acked) < total:
            while base in acked:
                base += 1
            now, inflight = time.time(), 0
            for q in range(base, total):
                if q in acked:
                    continue
                ts = sent.get(q)
                if ts is not None and now - ts < RTO:
                    inflight += 1
                    continue
                if inflight >= WINDOW:
                    break
                s.sendto(build(q), peer)
                sent[q] = now
                inflight += 1
            set_transfer(tid, name, "out", len(acked), total)
            if len(acked) != last_count:
                last_count, last_progress = len(acked), now
            elif now - last_progress > 20:
                break  # nessun ack da 20 s: il peer non risponde più
            time.sleep(0.003)
        else:
            ok = True
    finally:
        f.close()
        del acks[tid]
        drop_transfer(tid)
        if cleanup:
            try:
                os.remove(path)
            except OSError:
                pass
    if ok:
        emit("file_out", name=name, size=size)
    else:
        emit("error", text=f"Invio di '{name}' interrotto: il peer non risponde.")


# ---------------------------------------------------------------------------
# Server web locale (solo 127.0.0.1)
# ---------------------------------------------------------------------------
class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):  # niente log di ogni richiesta nel terminale
        pass

    def allowed(self):
        # Difesa da altri siti aperti nel browser: accetta solo Host/Origin locali
        host = (self.headers.get("Host") or "").rsplit(":", 1)[0]
        if host not in ("localhost", "127.0.0.1"):
            return False
        origin = self.headers.get("Origin")
        return not origin or urlparse(origin).hostname in ("localhost", "127.0.0.1")

    def reply(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if not self.allowed():
            return self.reply(403, {"error": "vietato"})
        path = urlparse(self.path).path
        if path in ("/", "/index.html"):
            try:
                with open(INDEX_FILE, "rb") as f:
                    body = f.read()
            except OSError:
                return self.reply(500, {"error": "index.html non trovato accanto a client_web.py"})
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif path == "/events":
            self.sse()
        elif path.startswith("/files/"):
            self.download(os.path.basename(unquote(path[len("/files/"):])))
        else:
            self.reply(404, {"error": "non trovato"})

    def sse(self):
        # Server-Sent Events: la pagina riceve in tempo reale eventi e stato dei trasferimenti
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        idx, last, last_write = 0, None, time.time()
        try:
            while True:
                with ev_cond:
                    if idx >= len(events):
                        ev_cond.wait(0.3)
                    new = events[idx:]
                    idx += len(new)
                out = "".join(f"event: log\ndata: {json.dumps(e)}\n\n" for e in new)
                with tr_lock:
                    trs = list(transfers.values())
                snap = json.dumps({"phase": state["phase"], "peer": state["peer"],
                                   "config": state["config"], "transfers": trs})
                if snap != last:
                    out += f"event: snapshot\ndata: {snap}\n\n"
                    last = snap
                if not out and time.time() - last_write > 10:
                    out = ": ping\n\n"  # tiene viva la connessione e rileva la chiusura della pagina
                if out:
                    self.wfile.write(out.encode())
                    self.wfile.flush()
                    last_write = time.time()
        except OSError:
            pass  # pagina chiusa

    def download(self, name):
        full = os.path.join(OUT_DIR, name)
        if not name or not os.path.isfile(full):
            return self.reply(404, {"error": "file non trovato"})
        self.send_response(200)
        # octet-stream + attachment: un file ricevuto non viene mai "eseguito" dal browser
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Disposition", "attachment")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Content-Length", str(os.path.getsize(full)))
        self.end_headers()
        with open(full, "rb") as f:
            while True:
                block = f.read(1 << 20)
                if not block:
                    break
                self.wfile.write(block)

    def do_POST(self):
        if not self.allowed():
            return self.reply(403, {"error": "vietato"})
        path = urlparse(self.path).path
        length = int(self.headers.get("Content-Length") or 0)

        if path == "/api/connect":
            try:
                body = json.loads(self.rfile.read(length))
                server = str(body.get("server", "")).strip()
                room = str(body.get("room", "")).strip()
                lan = bool(body.get("lan", False))
            except (ValueError, AttributeError):
                return self.reply(400, {"error": "richiesta non valida"})
            if not re.fullmatch(r"[A-Za-z0-9.-]{1,253}", server):
                return self.reply(400, {"error": "Indirizzo del server non valido (es. 192.168.1.10)"})
            if not re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", room):
                return self.reply(400, {"error": "Nome stanza non valido: usa lettere, numeri, punto, trattino o underscore"})
            if not start_connect(server, room, lan):
                return self.reply(409, {"error": "Collegamento già in corso"})
            return self.reply(200, {"ok": True})
        if path == "/api/cancel":
            cancel_ev.set()
            return self.reply(200, {"ok": True})

        if state["phase"] != "connected":
            return self.reply(409, {"error": "non sei ancora connesso al peer"})

        if path == "/api/msg":
            try:
                text = str(json.loads(self.rfile.read(length)).get("text", "")).strip()
            except ValueError:
                return self.reply(400, {"error": "richiesta non valida"})
            if not text:
                return self.reply(400, {"error": "messaggio vuoto"})
            s.sendto(pkt(T_MSG, payload=text.encode()), peer)
            emit("msg", who="me", text=text)
            self.reply(200, {"ok": True})

        elif path == "/api/send_file":
            # il browser manda il file come corpo della richiesta; lo salviamo e lo inviamo via UDP
            name = os.path.basename(unquote(self.headers.get("X-Filename", ""))) or "file"
            os.makedirs(TMP_DIR, exist_ok=True)
            tmp = os.path.join(TMP_DIR, f"{random.getrandbits(32):08x}_{name}")
            remaining = length
            try:
                with open(tmp, "wb") as f:
                    while remaining > 0:
                        block = self.rfile.read(min(1 << 20, remaining))
                        if not block:
                            raise OSError("upload interrotto")
                        f.write(block)
                        remaining -= len(block)
            except OSError:
                if os.path.exists(tmp):
                    os.remove(tmp)
                return self.reply(400, {"error": "upload interrotto"})
            threading.Thread(target=send_file, args=(tmp, name, True), daemon=True).start()
            self.reply(200, {"ok": True})
        else:
            self.reply(404, {"error": "non trovato"})


if not os.path.isfile(INDEX_FILE):
    die("Manca index.html: mettilo nella stessa cartella del programma.")

try:
    httpd = ThreadingHTTPServer(("127.0.0.1", args.porta_web), Handler)
except OSError as e:
    die(f"Impossibile usare la porta web {args.porta_web}: {e} (prova --porta-web 8081)")
httpd.daemon_threads = True
url = f"http://localhost:{args.porta_web}"
print(f"Interfaccia web su {url}  (Ctrl+C per uscire)")
if args.server and args.room:
    start_connect(args.server, args.room, args.mode == "lan")  # dati dalla riga di comando: parte subito
if not args.no_browser:
    webbrowser.open(url)
try:
    httpd.serve_forever()
except KeyboardInterrupt:
    pass