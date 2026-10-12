import ipaddress
import socket
import time

PORT = 9999
# room -> {addr_osservato: (ultimo_contatto, (ip_lan, porta_lan), (ip_pub, porta_pub))}
rooms = {}


def is_private(ip):
    try:
        return ipaddress.ip_address(ip).is_private
    except ValueError:
        return False


s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.bind(("0.0.0.0", PORT))
print(f"Rendezvous in ascolto su UDP {PORT}")

while True:
    data, addr = s.recvfrom(1024)
    parts = data.decode(errors="ignore").split()
    if len(parts) not in (4, 6) or parts[0] != "REGISTER":
        continue

    room, lan = parts[1], (parts[2], parts[3])

    # Indirizzo pubblico del client: quello che vede il server...
    pub = addr
    # ...ma se il server lo vede con IP privato (client sulla stessa LAN del server)
    # quell'indirizzo è inutile per chi sta fuori: usiamo quello scoperto dal client via STUN.
    if len(parts) == 6 and is_private(addr[0]):
        try:
            ipaddress.ip_address(parts[4])
            pub = (parts[4], int(parts[5]))
        except ValueError:
            pass

    now = time.time()
    peers = rooms.setdefault(room, {})
    peers[addr] = (now, lan, pub)
    for a in [a for a, v in peers.items() if now - v[0] > 30]:
        del peers[a]

    if len(peers) >= 2:
        (a, (_, la, pa)), (b, (_, lb, pb)) = list(peers.items())[:2]
        # ognuno riceve indirizzo pubblico + locale dell'altro
        s.sendto(f"PEER {pb[0]} {pb[1]} {lb[0]} {lb[1]}".encode(), a)
        s.sendto(f"PEER {pa[0]} {pa[1]} {la[0]} {la[1]}".encode(), b)
        print(f"Stanza '{room}': {a} (pub {pa}) <-> {b} (pub {pb})")
        del rooms[room]