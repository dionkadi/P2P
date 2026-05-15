# BEP-9 / BEP-10: Magnet Links & Metadata Exchange

## Overview

This document describes the BEP-9 (Magnet URIs) and BEP-10 (Extension Protocol: Metadata Exchange)
implementation for the P2PFileSharing project.

Magnet links enable content exchange without a `.torrent` file. The flow is:

1. User provides a `magnet:` URI containing the torrent's `info_hash`
2. The client finds peers via DHT/trackers using just the info_hash
3. Connected peers exchange the full `.torrent` metadata via the `ut_metadata` extension
4. Once metadata arrives, the session proceeds as if a `.torrent` file was loaded

---

## Sources

| File | Role |
|---|---|
| `src/MagnetUri.hpp` | Magnet URI parser, base32/hex info_hash decoding, roundtrip serialization |
| `src/PeerConnection.hpp/cpp` | `ut_metadata` extension ID tracking, `send_metadata_request()` |
| `src/SessionState.hpp` | Magnet-link constructor (info_hash + trackers, no metadata yet) |
| `src/TorrentSession.hpp/cpp` | `create_from_magnet()` factory, metadata download coordination |
| `src/TorrentFile.hpp` | `get_info_bencoded()` / `set_info_bencoded()` for serving metadata |
| `src/IPeerEvents.hpp` | Extended message dispatch for `ut_metadata` |
| `test/magnet_test.cpp` | URI parsing, decoding, roundtrip, metadata lifecycle tests |
| `docs/BEP9.md` | This document |

---

## Magnet URI Format (BEP-9)

```
magnet:?xt=urn:btih:<info_hash>&dn=<name>&tr=<tracker>&tr=<tracker>&xs=<source>
```

| Parameter | Description |
|---|---|
| `xt` | Exact topic: `urn:btih:<hash>` where hash is 40 hex chars or 32 base32 chars |
| `dn` | Display name (URL-encoded) |
| `tr` | Tracker URL (multiple allowed) |
| `xs` | Exact source URL |

### Info Hash Encoding

**Hex**: 40 lowercase hex characters → 20 bytes SHA-1 hash.

**Base32**: 32 characters per RFC 4648 → 20 bytes. Each group of 8 base32 characters
encodes 5 bytes (40 bits). Used by some clients for shorter URIs.

### Parsing Implementation

`parse_magnet_uri()` in `MagnetUri.hpp`:
1. Validates `magnet:?` prefix
2. Splits query string on `&`
3. URL-decodes each value
4. For `xt=urn:btih:`:
   - 40-char → `decode_hex_info_hash()`
   - 32-char → `decode_base32_info_hash()`
5. Collects `dn`, `tr`, `xs` parameters
6. Throws if no valid info_hash found

---

## Metadata Exchange (BEP-10)

### Extended Handshake

The extended handshake (sent after BitTorrent handshake if the peer supports extensions)
includes:

```json
{
  "m": {
    "ut_metadata": 3,
    "ut_pex": 1
  },
  "v": "My C++ Client 1.0",
  "metadata_size": 37500
}
```

Key fields:

| Field | Description |
|---|---|
| `m.ut_metadata` | Extension ID for ut_metadata messages (0 = not supported) |
| `metadata_size` | Total size of the bencoded info dictionary in bytes |

The local client always advertises `ut_metadata` with ID 3.

### Metadata Messages

Three message types, sent as extended messages with the negotiated extension ID:

**Request (msg_type=0)**: Request a metadata piece
```json
{"msg_type": 0, "piece": 0}
```

**Data (msg_type=1)**: Response with a metadata piece
```json
{"msg_type": 1, "piece": 0, "total_size": 37500}
<raw binary metadata bytes for this piece>
```

**Reject (msg_type=2)**: Piece not available
```json
{"msg_type": 2, "piece": 5}
```

The metadata is split into pieces of 16 KiB (`METADATA_PIECE_SIZE = 16384`).
The data message payload is: `bencoded_dict + raw_metadata_bytes`.

### Download Flow

1. Peer connects and sends extended handshake with `metadata_size` and `ut_metadata` ID
2. `TorrentSession::on_extended_message(Handshake)` detects the peer supports metadata
3. If session is in magnet mode (`metadata_download_active_`), spawns `request_metadata_from_peer()`
4. For each piece from 0 to `total_size / 16384`, a request is sent
5. Peer responds with data messages; `TorrentSession::on_extended_message(ut_metadata)` stores each piece
6. When all pieces arrive, `on_metadata_complete()` is called

### Upload Flow

When the local client has metadata (from a `.torrent` file or completed download):

1. Peer sends a request for piece N
2. `TorrentSession::on_extended_message(ut_metadata)` with `msg_type=0` serves the piece
3. The metadata is read from `state_->info().get_info_bencoded()`
4. Response includes the bencoded dict header + raw bytes for the requested piece

---

## Session Lifecycle

### Magnet Mode Creation

```cpp
auto session = TorrentSession::create_from_magnet(
    io_context, my_peer_id,
    "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567"
    "&tr=http://tracker.example.com/announce",
    save_path, peer_port, Mode::Leech
);
```

### State Transitions

```
create_from_magnet()
  └─ parse_magnet_uri() → MagnetLink
  └─ SessionState(info_hash, tracker_tiers)   → num_pieces = 0
  └─ metadata_download_active_ = true
  └─ TorrentSession::run()
       ├─ init() → skipped (no metadata yet)
       ├─ dht_node_->start()
       └─ dht_announce_loop()

Peer connects, extended handshake with metadata_size
  └─ request_metadata_from_peer()
       └─ sends ut_metadata requests for all pieces

Metadata piece arrives → stored in metadata_buffer_
  └─ metadata_pieces_received_++

All pieces received → on_metadata_complete()
  ├─ Decode info dict from metadata_buffer_
  ├─ Populate state_->torrent_info()
  ├─ init_pieces(num_pieces)
  ├─ Initialize file_manager_, piece_manager_
  ├─ Start choke_loop, downloader, pex_loop
  └─ Transition to normal download/seed mode
```

---

## Integration Points

| Component | Change |
|---|---|
| `PeerConnection` | Tracks `metadata_ext_id_` and `metadata_size_` from extended handshake |
| `PeerConnection` | New `send_metadata_request(ext_id, piece)` method |
| `TorrentSession` | `create_from_magnet()` factory method |
| `TorrentSession` | ut_metadata message handler (request/data/reject) |
| `TorrentSession` | `request_metadata_from_peer()`, `on_metadata_complete()` |
| `SessionState` | New constructor from InfoHash + tracker tiers |
| `SessionState` | `init_pieces()` for post-metadata initialization |
| `MetaInfo` | Stores bencoded info dict for serving metadata requests |

---

## Tests

**File**: `test/magnet_test.cpp`

| Test Suite | Tests | Description |
|---|---|---|
| `MagnetUriTest` | 10 | Hex parsing, base32 parsing, display name, trackers, roundtrip, error cases |
| `UrlDecodeTest` | 2 | URL-encoding, percent decoding |
| `SessionStateMagnetTest` | 2 | Constructor from info_hash, init_pieces after metadata |

**Edge cases covered**:
- Hex-encoded (40 char) and base32-encoded (32 char) info hashes
- All-zeros info_hash (valid base32 input)
- Missing info_hash → exception
- Invalid hex characters → exception
- URL-encoded display names (spaces, special chars)
- Multiple tracker URLs
- `xs` source URLs
- Roundtrip: parse → serialize → parse
- SessionState with 0 pieces before metadata arrives
- `init_pieces()` after metadata populates piece_status

---

## Known Limitations

1. **Base32 decoding**: Uses a simple linear scan for each character. For the 32-char info_hash
   case this is negligible (32 iterations × 32 comparisons).

2. **Concurrent metadata downloads**: Metadata is downloaded from one peer at a time.
   A more robust implementation would request different pieces from different peers.

3. **No metadata retry**: If metadata download fails (all peers reject or disconnect),
   the session remains in magnet mode indefinitely. Future work should add retry logic.

4. **Seed-only magnet**: Creating a magnet session in `Mode::Seed` will start in metadata-download
   mode — it needs to fetch metadata before it can serve data. This is intentional but may be
   surprising.

5. **No metadata persistence**: Received metadata is held in memory only and is not saved to a
   `.torrent` file. Restarting a magnet-downloaded torrent requires the magnet URI again.
