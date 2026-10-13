# =============================================================================
# server.py - Server di rendezvous per UDP hole punching
#
# Non inoltra nessun messaggio: serve solo a far "incontrare" due client.
# Ogni client si registra in una "stanza"; quando nella stanza ce ne sono due,
# il server manda a ciascuno l'indirizzo dell'altro. Poi i due client
# comunicano direttamente tra loro (peer-to-peer).
#
# Protocollo (testo, via UDP):
#   client -> server:  REGISTER <stanza> <ip_lan> <porta_lan> [<ip_pub> <porta_pub>]
#   server -> client:  PEER <ip_pub_altro> <porta_pub_altro> <ip_lan_altro> <porta_lan_altro>
# =============================================================================
import ipaddress
import socket
import time

PORT = 9999  # porta UDP su cui il server ascolta (da aprire/inoltrare sul router)

# Stanze in attesa di un secondo peer:
# room -> {addr_osservato: (ultimo_contatto, (ip_lan, porta_lan), (ip_pub, porta_pub))}
rooms = {}


def is_private(ip):
    # True se l'IP è privato (192.168.x.x, 10.x.x.x, 172.16-31.x.x, 127.x.x.x...)
    try:
        return ipaddress.ip_address(ip).is_private
    except ValueError:
        return False


# Socket UDP in ascolto su tutte le interfacce
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.bind(("0.0.0.0", PORT))
print(f"Rendezvous in ascolto su UDP {PORT}")

while True:
    # Aspetta un pacchetto; addr = (ip, porta) di chi l'ha mandato, come lo vede il server
    data, addr = s.recvfrom(1024)
    parts = data.decode(errors="ignore").split()

    # Accetta solo REGISTER con 4 campi (senza STUN) o 6 campi (con indirizzo STUN)
    if len(parts) not in (4, 6) or parts[0] != "REGISTER":
        continue

    room, lan = parts[1], (parts[2], parts[3])

    # Indirizzo pubblico del client: quello che vede il server...
    pub = addr
    # ...ma se il server lo vede con IP privato (client sulla stessa LAN del server)
    # quell'indirizzo è inutile per chi sta fuori: usiamo quello scoperto dal client via STUN.
    # (Se l'IP osservato è pubblico ci fidiamo di quello, che il client non può falsificare.)
    if len(parts) == 6 and is_private(addr[0]):
        try:
            ipaddress.ip_address(parts[4])
            pub = (parts[4], int(parts[5]))
        except ValueError:
            pass  # dati malformati: teniamo l'indirizzo osservato

    # Registra (o aggiorna) il client nella sua stanza
    now = time.time()
    peers = rooms.setdefault(room, {})
    peers[addr] = (now, lan, pub)

    # Elimina i client che non si fanno sentire da più di 30 secondi
    for a in [a for a, v in peers.items() if now - v[0] > 30]:
        del peers[a]

    # Quando ci sono due client nella stanza, falli incontrare
    if len(peers) >= 2:
        (a, (_, la, pa)), (b, (_, lb, pb)) = list(peers.items())[:2]
        # ognuno riceve indirizzo pubblico + locale dell'altro
        # (si risponde all'indirizzo osservato, che è quello da cui il client ascolta)
        s.sendto(f"PEER {pb[0]} {pb[1]} {lb[0]} {lb[1]}".encode(), a)
        s.sendto(f"PEER {pa[0]} {pa[1]} {la[0]} {la[1]}".encode(), b)
        print(f"Stanza '{room}': {a} (pub {pa}) <-> {b} (pub {pb})")
        del rooms[room]  # la stanza è di nuovo libera