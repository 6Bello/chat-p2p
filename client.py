# =============================================================================
# client.py - Client P2P: chat e invio file con UDP hole punching
#
# Uso:
#   python client.py <IP_SERVER> <stanza>        -> reti diverse (o server in LAN, peer fuori)
#   python client.py <IP_SERVER> <stanza> lan    -> entrambi i peer sulla stessa LAN del server
#
# Fasi:
#   1) scopre il proprio indirizzo pubblico (STUN) e si registra sul server
#   2) riceve dal server gli indirizzi del peer
#   3) hole punching: i due peer si mandano pacchetti insieme per aprire il NAT
#   4) chat (testo) e invio file (/send) direttamente tra i due peer
# =============================================================================
import json
import math
import os
import random
import socket
import struct
import sys
import threading
import time

# --- Argomenti da riga di comando -------------------------------------------
if len(sys.argv) not in (3, 4) or (len(sys.argv) == 4 and sys.argv[3] != "lan"):
    sys.exit("Uso: python client.py <IP_SERVER> <nome_stanza> [lan]")

SERVER = (sys.argv[1], 9999)
ROOM = sys.argv[2]
LAN_MODE = len(sys.argv) == 4  # "lan": peer e server sulla stessa rete locale

# --- Formato dei pacchetti P2P ----------------------------------------------
# Ogni pacchetto = intestazione (tipo, id trasferimento, numero di sequenza) + dati
T_PUNCH, T_MSG, T_FILE_HDR, T_FILE_DATA, T_ACK = range(5)
HDR = struct.Struct("!BII")  # tipo (1 byte), id trasferimento (4), sequenza (4)
CHUNK = 1100                 # byte di file per pacchetto (sotto l'MTU, evita la frammentazione)
WINDOW = 64                  # pacchetti non ancora confermati che possono essere in volo
RTO = 0.5                    # secondi di attesa prima di ritrasmettere un pacchetto
OUT_DIR = "ricevuti"         # cartella dove vengono salvati i file ricevuti

# IMPORTANTE: stesso socket per server, STUN e peer, così il NAT riusa lo stesso mapping
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.bind(("0.0.0.0", 0))  # porta locale scelta dal sistema
s.settimeout(1)


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
            continue  # nome non risolvibile: prova il prossimo server
        tid = os.urandom(12)  # identificativo casuale della richiesta
        req = struct.pack("!HHI", 1, 0, 0x2112A442) + tid  # Binding Request
        for _ in range(2):  # due tentativi per server
            s.sendto(req, dst)
            end = time.time() + 1
            while time.time() < end:
                try:
                    data, addr = s.recvfrom(2048)
                except socket.timeout:
                    break
                # ignora pacchetti che non sono la risposta alla nostra richiesta
                if addr[0] != dst[0] or len(data) < 20 or data[8:20] != tid:
                    continue
                # Scorre gli attributi della risposta cercando l'indirizzo pubblico
                i, mapped = 20, None
                while i + 4 <= len(data):
                    atype, alen = struct.unpack_from("!HH", data, i)
                    val = data[i + 4:i + 4 + alen]
                    if atype == 0x0020 and alen >= 8 and val[1] == 1:  # XOR-MAPPED-ADDRESS
                        # porta e IP sono "mascherati" con XOR col magic cookie
                        prt = struct.unpack("!H", val[2:4])[0] ^ 0x2112
                        ip = socket.inet_ntoa(bytes(
                            a ^ b for a, b in zip(val[4:8], struct.pack("!I", 0x2112A442))))
                        return ip, prt
                    if atype == 0x0001 and alen >= 8 and val[1] == 1:  # MAPPED-ADDRESS (vecchio formato)
                        mapped = (socket.inet_ntoa(val[4:8]), struct.unpack("!H", val[2:4])[0])
                    i += 4 + alen + (-alen % 4)  # gli attributi sono allineati a 4 byte
                if mapped:
                    return mapped
    return None  # nessun server STUN ha risposto


LAN_IP, LAN_PORT = local_ip(), s.getsockname()[1]

# Indirizzo pubblico via STUN (serve se siamo sulla stessa LAN del server ma il peer è fuori)
PUB = None if LAN_MODE else stun_public_addr()
print(f"Indirizzo pubblico (STUN): {PUB[0]}:{PUB[1]}" if PUB else "STUN non disponibile: uso l'indirizzo visto dal server")
REG = f"REGISTER {ROOM} {LAN_IP} {LAN_PORT}" + (f" {PUB[0]} {PUB[1]}" if PUB else "")

# --- Fase 1: registrazione e attesa degli indirizzi del peer ----------------
# Reinvia REGISTER ogni secondo finché il server risponde con PEER
candidates = None
print("Registrazione sul server...")
while candidates is None:
    s.sendto(REG.encode(), SERVER)
    try:
        data, addr = s.recvfrom(1024)
        p = data.decode(errors="ignore").split()
        if addr == SERVER and p and p[0] == "PEER":
            if LAN_MODE:
                candidates = [(p[3], int(p[4]))]  # solo indirizzo locale, niente NAT
            else:
                candidates = [(p[1], int(p[2])), (p[3], int(p[4]))]  # pubblico, locale
    except socket.timeout:
        pass
print(f"Peer trovato, candidati: {candidates}. Avvio hole punching...")

# --- Fase 2: hole punching ---------------------------------------------------
# Entrambi i peer mandano pacchetti a tutti i candidati nello stesso momento:
# l'uscita crea il "buco" nel NAT e le risposte dell'altro possono entrare.
# Si usa l'indirizzo da cui arriva la prima risposta.
peer = None
for _ in range(30):
    for c in candidates:
        s.sendto(pkt(T_PUNCH), c)
    try:
        data, addr = s.recvfrom(2048)
        if addr in candidates:
            peer = addr
            for _ in range(3):  # conferma, così anche l'altro riceve
                s.sendto(pkt(T_PUNCH), peer)
            break
    except socket.timeout:
        pass

if peer is None:
    sys.exit("Hole punching fallito (probabile NAT simmetrico/CGNAT).")

print(f"Connesso a {peer}!")
print("Scrivi un messaggio, oppure /send <percorso_file> per inviare un file. Ctrl+C per uscire.\n")
s.settimeout(None)  # da ora recvfrom è bloccante (lo usa il thread receiver)

# --- Stato dei trasferimenti -------------------------------------------------
acks = {}      # tid -> insieme di seq confermate dal peer (lato mittente)
incoming = {}  # tid -> {"meta": info file, "chunks": {seq: dati}} (lato ricevente)
done = set()   # tid già completati: continuiamo ad ack-are i duplicati


def say(text):
    # Stampa un messaggio senza rovinare la riga di input "> "
    print(f"\r{text}\n> ", end="", flush=True)


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
        for i in range(1, meta["n"] + 1):  # i pezzi vanno scritti in ordine
            f.write(st["chunks"][i])
    done.add(tid)
    del incoming[tid]
    say(f"[file ricevuto: {path} ({meta['size']} byte)]")


def receiver():
    # Thread che riceve tutti i pacchetti dal peer e li smista per tipo
    while True:
        data, addr = s.recvfrom(4096)
        if addr != peer or len(data) < HDR.size:
            continue  # ignora pacchetti da altri o troppo corti
        t, tid, seq = HDR.unpack_from(data)
        payload = data[HDR.size:]
        if t == T_MSG:
            say(f"Peer: {payload.decode(errors='replace')}")
        elif t == T_ACK:
            # il peer conferma di aver ricevuto il pacchetto seq del trasferimento tid
            if tid in acks:
                acks[tid].add(seq)
        elif t in (T_FILE_HDR, T_FILE_DATA):
            s.sendto(pkt(T_ACK, tid, seq), peer)  # conferma sempre, anche i duplicati
            if tid in done:
                continue
            st = incoming.setdefault(tid, {"meta": None, "chunks": {}})
            if t == T_FILE_HDR:
                try:
                    st["meta"] = json.loads(payload)  # nome, dimensione, numero pezzi
                except ValueError:
                    continue
            else:
                st["chunks"][seq] = payload  # i pezzi possono arrivare in disordine
            try_finish(tid)


def keepalive():
    # Ogni 15 s manda un pacchetto per non far chiudere al NAT il buco aperto
    while True:
        time.sleep(15)
        s.sendto(pkt(T_PUNCH), peer)


def send_file(path):
    # Invio affidabile su UDP: finestra scorrevole + ack + ritrasmissione
    try:
        size = os.path.getsize(path)
        f = open(path, "rb")
    except OSError as e:
        say(f"[errore: {e}]")
        return
    n = math.ceil(size / CHUNK)
    total = n + 1  # seq 0 = intestazione, 1..n = dati
    tid = random.getrandbits(32)  # identifica questo trasferimento
    acked = acks[tid] = set()
    header = json.dumps({"name": os.path.basename(path), "size": size, "n": n}).encode()

    def build(q):
        # Costruisce il pacchetto numero q (leggendo il pezzo dal file solo quando serve)
        if q == 0:
            return pkt(T_FILE_HDR, tid, 0, header)
        f.seek((q - 1) * CHUNK)
        return pkt(T_FILE_DATA, tid, q, f.read(CHUNK))

    say(f"[invio {path}: {size} byte]")
    # sent: quando è stato inviato l'ultima volta ogni pacchetto
    # base: primo pacchetto non ancora confermato
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
                inflight += 1  # inviato da poco: aspettiamo ancora l'ack
                continue
            if inflight >= WINDOW:
                break  # finestra piena: aspetta gli ack
            s.sendto(build(q), peer)  # prima volta o ritrasmissione dopo RTO
            sent[q] = now
            inflight += 1
        # Se per 20 s non arriva nessun nuovo ack, il peer non risponde più
        if len(acked) != last_count:
            last_count, last_progress = len(acked), now
        elif now - last_progress > 20:
            say("[invio interrotto: il peer non risponde]")
            break
        time.sleep(0.003)
    else:
        say(f"[file inviato: {os.path.basename(path)}]")
    f.close()
    del acks[tid]


# --- Avvio dei thread e ciclo di input --------------------------------------
threading.Thread(target=receiver, daemon=True).start()
threading.Thread(target=keepalive, daemon=True).start()

try:
    while True:
        line = input("> ").strip()
        if line.startswith("/send "):
            # l'invio gira in un thread, così si può continuare a chattare
            threading.Thread(target=send_file, args=(line[6:].strip().strip('"'),), daemon=True).start()
        elif line:
            s.sendto(pkt(T_MSG, payload=line.encode()), peer)
except (KeyboardInterrupt, EOFError):
    pass