# BEP-5: DHT Protocol — Implementation

## Overview

This document describes the BEP-5 (DHT Protocol) implementation for the P2PFileSharing
project. The implementation follows the [BEP-5 specification](https://www.bittorrent.org/beps/bep_0005.html)
using Kademlia-based Distributed Hash Table over UDP.

**Source**: `src/Kademlia.hpp` (header-only, ~1330 lines)

---

## Architecture

### Components

| Component | Description |
|---|---|
| `DHTNode` | Top-level node: owns socket, routing table, peer/token stores, background loops |
| `RouteTable` | 160 Kademlia k-buckets covering the 160-bit ID space |
| `KBucket` | Single k-bucket holding up to K=8 `BucketEntry` nodes, LRU-ordered |
| `BucketEntry` | Node ID, UDP endpoint, status (Good/Questionable/Bad), timestamps |
| `PeerInfo` | TCP endpoint + expiry for stored peer contact info |
| `AnnounceToken` | Opaque token + expiry for `announce_peer` authorization |
| `PendingQuery` | Outstanding KRPC query: completion handler, remote, expiry |

### Constants

| Constant | Value | Purpose |
|---|---|---|
| `K` | 8 | K-bucket capacity (max nodes per bucket) |
| `ALPHA` | 3 | Parallelism factor for iterative lookups |
| `BUCKET_REFRESH_INTERVAL` | 15 min | Refresh idle k-buckets |
| `ANNOUNCE_TOKEN_LIFETIME` | 10 min | Token validity for announce_peer |
| `PEER_STORAGE_LIFETIME` | 30 min | How long announced peers are kept |
| `KRPC_QUERY_TIMEOUT` | 10 s | Per-query timeout |

---

## KRPC Protocol

All messages are bencoded dictionaries sent over UDP. Every message has:

| Key | Value |
|---|---|
| `t` | Transaction ID (short binary string) |
| `y` | Message type: `q` (query), `r` (response), `e` (error) |
| `v` | Client version (optional) |

### Queries

Queries (`y: "q"`) add:

| Key | Value |
|---|---|
| `q` | Method name: `ping`, `find_node`, `get_peers`, `announce_peer` |
| `a` | Arguments dictionary (always includes `id` = sender's node ID) |

### Responses

Responses (`y: "r"`) add:

| Key | Value |
|---|---|
| `r` | Return values dictionary (always includes `id` = responder's node ID) |

### Errors

Errors (`y: "e"`) add:

| Key | Value |
|---|---|
| `e` | `[error_code: int, error_message: string]` |

**Error codes**: 201 (generic), 202 (server), 203 (protocol/bad token), 204 (method unknown).

---

## Message Flows

### DHT Queries

#### `ping`

```
→ {"t":"aa", "y":"q", "q":"ping", "a":{"id":"<nid>"}}
← {"t":"aa", "y":"r", "r":{"id":"<nid>"}}
```

**Server**: `on_ping_query()` — responds with own node ID, inserts sender into routing table.

**Client**: `send_ping(endpoint)` — sends ping with timeout, inserts responder on success, marks bad on timeout.

#### `find_node`

```
→ {"t":"aa", "y":"q", "q":"find_node", "a":{"id":"<nid>", "target":"<target_nid>"}}
← {"t":"aa", "y":"r", "r":{"id":"<nid>", "nodes":"<compact_node_info>"}}
```

**Server**: `on_find_node_query()` — returns K closest nodes from routing table as compact node info.

**Client**: `find_nodes(target_id, count)` — iterative lookup:
1. Query ALPHA closest known nodes in parallel
2. Collect returned nodes, keep closest K
3. Repeat up to 3 iterations or until no closer nodes found

#### `get_peers`

```
→ {"t":"aa", "y":"q", "q":"get_peers", "a":{"id":"<nid>", "info_hash":"<20byte>"}}
← {"t":"aa", "y":"r", "r":{"id":"<nid>", "token":"<token>", "values":["<compact_peer>", ...]}}
  OR
← {"t":"aa", "y":"r", "r":{"id":"<nid>", "token":"<token>", "nodes":"<compact_node_info>"}}
```

**Server**: `on_get_peers_query()`:
- Returns stored peers for `info_hash` with a token if available
- Otherwise returns K closest nodes + token
- Token = SHA256(remote_IP + rotating_secret)[:8]

**Client**: `get_peers(info_hash, count)` — iterative lookup (same pattern as find_nodes):
- First checks local peer store
- Queries ALPHA closest nodes in parallel
- Collects both peers and closer nodes
- Repeats until K peers found or iterations exhausted

#### `announce_peer`

```
→ {"t":"aa", "y":"q", "q":"announce_peer", "a":{"id":"<nid>", "implied_port":<0|1>, "info_hash":"<20byte>", "port":<port>, "token":"<token>"}}
← {"t":"aa", "y":"r", "r":{"id":"<nid>"}}
```

**Server**: `on_announce_peer_query()`:
1. Validates required arguments
2. Checks `implied_port` flag (uses UDP source port if set)
3. Validates token against `generate_token(remote_endpoint)`
4. Stores `(info_hash → {TCP endpoint, expiry})` in `known_peers_`

**Client**: `announce_peer(info_hash, client_port)`:
1. Finds K closest nodes to `info_hash` from routing table
2. For each: sends `get_peers` to obtain a token
3. For each with a token: sends `announce_peer` with the TCP client port

---

## Routing Table

### K-Buckets

- 160 buckets, each covering a range of the 160-bit ID space
- Bucket index = `floor(log2(distance(my_id, other_id)))`
- Each bucket holds up to K=8 entries, ordered by most-recently-seen (MRU at back)

### Insertion (`KBucket::add`)

1. If node already in bucket → move to MRU, update timestamp
2. If bucket not full → append at MRU position
3. If bucket full → look for a `Bad` node to replace
4. If all nodes Good/Questionable → return failure (trigger asynchronous ping of oldest)

### Node Status

| Status | Condition |
|---|---|
| `Good` | Responded to query within 15 min or sent a query within 15 min |
| `Questionable` | No response seen in 15 min |
| `Bad` | Failed to respond to multiple queries |

### Refresh

`routing_table_refresh_loop()` runs every 15 minutes and checks each bucket:
- If `last_changed` > 15 min ago → spawn `find_nodes` with a random ID in the bucket's range
- Automatically reschedules via `refresh_timer_.async_wait()`

---

## Compact Encoding

### Node Info (26 bytes)

```
[20 bytes NodeID | 4 bytes IPv4 | 2 bytes port]
```

All values in network byte order.

### Peer Info (6 bytes)

```
[4 bytes IPv4 | 2 bytes port]
```

---

## Token System

Tokens provide proof that a node recently performed a `get_peers` query.

**Generation** (`generate_token`):
```
token = SHA256(remote_ip + "DHT-TOKEN-SECRET-" + time_window)[:8]
```
Where `time_window` changes every `ANNOUNCE_TOKEN_LIFETIME` (10 min).

**Validation** (`on_announce_peer_query`):
Recomputes the expected token and compares. Rejects mismatches with error 203.

---

## Bootstrap

`bootstrap(bootstrap_nodes_addrs)`:

1. Pings each bootstrap node address (e.g. `router.bittorrent.com:6881`)
2. On ping response: inserts responder into routing table
3. Performs iterative `find_nodes` for own node ID
4. Populates routing table with discovered nodes

**Well-known bootstrap nodes** (example):
- `router.bittorrent.com:6881`
- `dht.libtorrent.org:25401`
- `dht.transmissionbt.com:6881`

---

## Background Maintenance

### Cleanup Loop (`cleanup_loop`)

Runs every 5 minutes:
1. **Peer store**: Removes expired `PeerInfo` entries from `known_peers_`
2. **Token store**: Removes expired `AnnounceToken` entries from `tokens_issued_`
3. **Pending queries**: Removes timed-out queries from `pending_queries_`

---

## Usage

```cpp
// Create and start a DHT node
auto dht = std::make_shared<DHTNode>(io_context, /*port*/ 6881);
dht->start();

// Bootstrap from known nodes
co_await dht->bootstrap({"router.bittorrent.com:6881", "dht.libtorrent.org:25401"});

// Find peers for a torrent
auto peers = co_await dht->get_peers(info_hash, 50);
for (const auto& peer : peers) {
    // Connect to peer via BitTorrent protocol
}

// Announce as a peer for a torrent
co_await dht->announce_peer(info_hash, /*tcp_port*/ 6882);

// Graceful shutdown
dht->stop();
```

---

## Integration Points

### TorrentSession Integration

The DHT node is integrated into `TorrentSession` as follows:

| File | Change |
|---|---|
| `TorrentSession.hpp` | Added `std::shared_ptr<DHTNode> dht_node_` member, `asio::steady_timer dht_announce_timer_`, `dht_announce_loop()` method |
| `TorrentSession.cpp` | Constructs `DHTNode` on the same `peer_port` (UDP, coexists with TCP peer server), starts it in `run()`, spawns announce loop, handles graceful shutdown in `stop()` |

**Integration flow**:

```
TorrentSession::run()
  ├── dht_node_->start()                      // Start UDP listen loop
  ├── dht_node_->bootstrap(seed_nodes)         // Discover initial DHT peers
  └── asio::co_spawn(dht_announce_loop())      // Periodic DHT operations

dht_announce_loop()
  ├── if Seeder/Complete: dht_node_->announce_peer(info_hash, port)
  ├── dht_node_->get_peers(info_hash, 50)       // Discover peers via DHT
  ├── peer_manager_->add_discovered_peer(ep)   // Feed into existing PEX loop
  └── sleep(30 min)                             // Repeat

TorrentSession::stop()
  ├── dht_node_->stop()                         // Close socket, cancel timers
  └── dht_announce_timer_.cancel()
```

Discovered peers from the DHT are fed into `peer_manager_->add_discovered_peer()`, which is the same mechanism used by PEX. The existing `discovered_peers_loop()` picks them up and initiates TCP connections.

**Seed nodes** (default):
- `router.bittorrent.com:6881`
- `dht.libtorrent.org:25401`
- `dht.transmissionbt.com:6881`

### PeerConnection

DHT-enabled peers exchange UDP port via PORT message (bit 7 of reserved bytes in handshake). The extended handshake mechanism in `PeerConnection` already supports this.

### TorrentFile

Trackerless torrents use `nodes` key instead of `announce` for initial DHT contacts. Currently, the project requires at least one tracker URL (the constructor throws if `tracker_clients_by_tier_` is empty). Full DHT-only support requires relaxing this constraint.

---

## Tests

### Test File: `test/dht_test.cpp`

**Unit Tests** (`KBucketTest`):

| Test | Description |
|---|---|
| `AddNodeToEmptyBucket` | Verifies the first node is accepted |
| `AddNodeToFullBucketReplacesBad` | Full bucket with a Bad node — new node replaces it |
| `TouchMovesNodeToBack` | Touching a node moves it to LRU tail |

**Unit Tests** (`RouteTableTest`):

| Test | Description |
|---|---|
| `InsertAndFindClosest` | Inserts 5 nodes at various XOR distances, verifies `find_closest_nodes` returns them sorted by distance |
| `DontInsertSelf` | Ensures own node ID is never inserted into the routing table |
| `UpdateStatus` | Changes a node's status and verifies the change is reflected |

**Integration Tests** (`DHTIntegrationTest`):

| Test | Description |
|---|---|
| `PingBetweenTwoNodes` | Two `DHTNode` instances on different UDP ports; A pings B, verifies success |
| `FindNode` | A pings B, then A does `find_nodes` for a random target — confirms B is in results |
| `GetPeersAndAnnouncePeer` | Full cycle: bootstrap, announce_peer on A, get_peers on B, verifies peer propagation |
| `GetPeersWithStoredPeers` | Three-step: get_peers (obtain token), announce_peer (with token), get_peers (verify stored) |
| `TokenValidation` | Validates the test infrastructure for token-based operations |

All tests use 2-worker io_context thread pool and ephemeral-range UDP ports.

---

## Known Limitations

1. **IPv6**: Only IPv4 is supported. Compact encoding assumes IPv4 addresses.
2. **Routing table persistence**: The routing table is not saved/restored between sessions.
3. **Bucket splitting**: The routing table uses fixed 160 buckets rather than dynamic Kademlia tree splitting. The current implementation pre-allocates all 160 buckets. Nodes that would fall into a bucket whose range includes the local node ID are handled via the `needs_refresh` mechanism rather than bucket splitting.
4. **Ping on full bucket**: When a bucket is full and all nodes are Good, the new node is discarded. A full implementation would asynchronously ping the oldest node and potentially replace it.
5. **`from_compact_node_info`**: This function parses compact node info without auto-inserting into the routing table. The caller must insert nodes as needed.
6. **Tracker requirement**: The `TorrentSession` constructor currently throws if no tracker URLs are configured. Full DHT/trackerless support requires making tracker clients optional.
