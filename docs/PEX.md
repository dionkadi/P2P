## PeerExchange (PEX) - BEP 11   
Peer Exchange (PEX) is a peer protocol extension defined in BEP 11 for the BitTorrent protocol. It allows peers in a swarm to exchange information about other peers they know, thereby accelerating peer discovery and improving the robustness of the swarm, especially in the absence of or as a supplement to a tracker.   

---

### Background   
In a typical BitTorrent swarm, peers discover each other via:   
- HTTP/HTTPS trackers (centralized)
- UDP trackers
- Distributed Hash Table (DHT), a decentralized network (BEP 5)
- Peer Exchange (PEX), direct peer-to-peer information exchange

While trackers and DHT provide inital peer lists, they may become outdated, slow or unavailable. PEX allows peers to continuously exchange fresh peer lists to each other directly, having advantages:
- Faster propagation of new peers
- Reduced load on trackers
- Better connectivity in censored or trackerless environments
- Improved swarm health (more resilient to churn)

---

### Protocol   
PEX uses the BitTorrent Extension Protocol (BEP 10) to exchange messages. The extension handshake includes a flag indicating support for PEX. Once both peers agree, they can exchange PEX messages containing lists of peers:
- Extended Protocol (BEP 10): PEX messages are sent as extended messages with a negotiated (in handshake) message id.
- ut_pex: The name used for the PEX extension.
- Delta exchange: Peers typically send only changes since last exchange, i.e. added peers and dropped peers.
- Incremental updates: PEX messages are sent periorically or when significant changes occur

To inform peers of supporting PEX, peers should specify it in extended handshake message, particularly in `m` dictionary:
```
{
    "m": {
        "ut_pex": <message_id>
    }
}
```

A PEX message sent by peers should look like `<20><message_id><payload>`, where `20` is the extended message ID specified by protocol, `message_id` is the ID peers agree upon (and in extended handshake), and `payload` is what PEX message actually holds.

Accordding to BEP 11, `payload` should be a bencoded dictionary, which contains:
- `added`: a string of compact `IP:port` information for peers that have been added since last exchange.
- `added6`: for IPv6.
- `added.f`: a string of flags corresponding to each peer in `added`.
- `dropped`: a string of compact `IP:port` information for peers that has been removed.
- `dropped6`: for IPv6.

The compact `IP:port` consists of:
- For IPv4: 4 bytes for IPv4 address (network byte order), 2 bytes for the port (network byte order).
- For IPv6: 16 bytes for IPv6 adress, 2 bytes for the port.

The `added` string concatenates all these 6-byte records.

The flags `added.f` are a string of bytes, one byte per peer in the added list, in the same order. The bits in each byte indicate properties of the peer:

| Bit | Meaning |
|-----|---------|
|0x01 | Prefers encryption (obsolete) |
|0x02 | Seed/Upload-only |
|0x04 | Support uTP (UDP Transport) |
|0x08 | Support holepunch (BEP 55) |
|0x10 | Outgoing connection (the peer we are sending to connected to us) |

---

### Details   
#### When to send PEX Messages?
- Initially: After the extension handshake, a peer may send a full list of its current peers (as `added`) to the other peer.
- Periodically: Every minute or so, a peer sends an incremental update containing only the peers that have been added or dropped since the last message.
- On significant changes: Some implementations also send immediately when a new peer connects or an existing one disconnects, but this is less common to avoid flooding.

#### Peer Selection for Exchange
Not all known peers are necessarily sent. Typically:
- Exclude the peer you are sending to.
- Exclude peers that are already known to the receiver.
- (Possibly) limit the number of peers sent to keep messages small.

#### Handling Received PEX Message
Upon receiving a PEX message, the peer:
- Decodes the `added` list and corresponding flags.
- For each new peer, if it's not present in internal list, it may attempt to connect to it, where flags may influence decisions.
- For each peer in `dropped`, it removes that peer from its own list if present. Note that this is just a hint.

#### Interacting with Other Discovery Mechanisms
PEX complements DHT and trackers. For example:
- A peer learns about others via trackers, DHT and PEX.
- It advertises its own peer list to connected peers.
- Over time, the swarm becomes well-connected even if trackers go down.

---

### Example
Suppose a peer wants to inform another about two new IPv4 peers:
- Peer A: 192.168.1.10, port 6881 (seed, supports uTP)
- Peer B: 10.0.0.5, port 51413 (not a seed, no uTP)
- And it wants to drop one peer: 203.0.113.7, port 6882

The `added` string would be:
- 192.168.1.10 → 4 bytes: c0 a8 01 0a (hex)
- port 6881 → 2 bytes: 1a e1 (6881 decimal = 0x1AE1)
- 10.0.0.5 → 4 bytes: 0a 00 00 05
- port 51413 → 2 bytes: c8 d5 (51413 decimal = 0xC8D5)

Concatenated: `c0 a8 01 0a 1a e1 0a 00 00 05 c8 d5` (12 bytes)

Flags for the two added peers (one byte each):
- Peer A: seed (0x02) + uTP (0x04) = 0x06
- Peer B: no flags = 0x00

So `added.f = 06 00`

`dropped` string for the dropped peer:
- 203.0.113.7 → cb 00 71 07
- port 6882 → 1a e2

Concatenated: `cb 00 71 07 1a e2` (6 bytes)

The complete bencoded dictionary:
```
d
5:added12: c0 a8 01 0a 1a e1 0a 00 00 05 c8 d5
7:added.f2: 06 00
7:dropped6: cb 00 71 07 1a e2
e
```

This is sent as the payload of an extended message with ID corresponding to ut_pex.