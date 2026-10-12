import json
import math
import os
import random
import socket
import struct
import sys
import threading
import time

if len(sys.argv) not in (3, 4) or (len(sys.argv) == 4 and sys.argv[3] != "lan"):
    sys.exit("Uso: python client.py <IP_SERVER> <nome_stanza> [lan]")

SERVER = (sys.argv[1], 9999)
ROOM = sys.argv[2]
LAN_MODE = len(sys.argv) == 4  # "lan": peer e server sulla stessa rete locale

# Tipi di pacchetto
T_PUNCH, T_MSG, T_FILE_HDR, T_FILE_DATA, T_ACK = range(5)
HDR = struct.Struct("!BII")  # tipo, id trasferimento, numero di sequenza
CHUNK = 1100                 # byte di file per pacchetto (sotto l'MTU)
WINDOW = 64                  # pacchetti non ancora confermati in volo
RTO = 0.5                    # secondi prima di ritrasmettere
OUT_DIR = "ricevuti"

# IMPORTANTE: stesso socket per server e peer, così il NAT riusa lo stesso mapping
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.bind(("0.0.0.0", 0))
s.settimeout(1)


def pkt(t, tid=0, seq=0, payload=b""):
    return HDR.pack(t, tid, seq) + payload


def local_ip():
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


LAN_IP, LAN_PORT = local_ip(), s.getsockname()[1]

# Indirizzo pubblico via STUN (serve se siamo sulla stessa LAN del server ma il peer è fuori)
PUB = None if LAN_MODE else stun_public_addr()
print(f"Indirizzo pubblico (STUN): {PUB[0]}:{PUB[1]}" if PUB else "STUN non disponibile: uso l'indirizzo visto dal server")
REG = f"REGISTER {ROOM} {LAN_IP} {LAN_PORT}" + (f" {PUB[0]} {PUB[1]}" if PUB else "")

# 1) Registrazione e attesa degli indirizzi del peer (pubblico, locale)
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

# 2) Hole punching su entrambi i candidati: usa quello che risponde
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
s.settimeout(None)

# ---------------- stato trasferimenti ----------------
acks = {}      # tid -> set di seq confermate (lato mittente)
incoming = {}  # tid -> {"meta": dict|None, "chunks": {seq: bytes}}
done = set()   # tid già completati (continuiamo ad ack-are i duplicati)


def say(text):
    print(f"\r{text}\n> ", end="", flush=True)


def unique_path(name):
    os.makedirs(OUT_DIR, exist_ok=True)
    name = os.path.basename(name) or "file"
    base, ext = os.path.splitext(name)
    path, i = os.path.join(OUT_DIR, name), 1
    while os.path.exists(path):
        path = os.path.join(OUT_DIR, f"{base}_{i}{ext}")
        i += 1
    return path


def try_finish(tid):
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
    say(f"[file ricevuto: {path} ({meta['size']} byte)]")


def receiver():
    while True:
        data, addr = s.recvfrom(4096)
        if addr != peer or len(data) < HDR.size:
            continue
        t, tid, seq = HDR.unpack_from(data)
        payload = data[HDR.size:]
        if t == T_MSG:
            say(f"Peer: {payload.decode(errors='replace')}")
        elif t == T_ACK:
            if tid in acks:
                acks[tid].add(seq)
        elif t in (T_FILE_HDR, T_FILE_DATA):
            s.sendto(pkt(T_ACK, tid, seq), peer)
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
            try_finish(tid)


def keepalive():
    while True:
        time.sleep(15)
        s.sendto(pkt(T_PUNCH), peer)  # tiene aperto il mapping sul NAT


def send_file(path):
    try:
        size = os.path.getsize(path)
        f = open(path, "rb")
    except OSError as e:
        say(f"[errore: {e}]")
        return
    n = math.ceil(size / CHUNK)
    total = n + 1  # seq 0 = intestazione, 1..n = dati
    tid = random.getrandbits(32)
    acked = acks[tid] = set()
    header = json.dumps({"name": os.path.basename(path), "size": size, "n": n}).encode()

    def build(q):
        if q == 0:
            return pkt(T_FILE_HDR, tid, 0, header)
        f.seek((q - 1) * CHUNK)
        return pkt(T_FILE_DATA, tid, q, f.read(CHUNK))

    say(f"[invio {path}: {size} byte]")
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


threading.Thread(target=receiver, daemon=True).start()
threading.Thread(target=keepalive, daemon=True).start()

try:
    while True:
        line = input("> ").strip()
        if line.startswith("/send "):
            threading.Thread(target=send_file, args=(line[6:].strip().strip('"'),), daemon=True).start()
        elif line:
            s.sendto(pkt(T_MSG, payload=line.encode()), peer)
except (KeyboardInterrupt, EOFError):
    pass