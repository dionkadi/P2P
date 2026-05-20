# Learnings & Conventions

## Project Conventions
- `namespace asio = boost::asio;`
- Coroutine style: `asio::awaitable<void>`, `co_await`, `co_spawn`
- Shared ownership: `std::shared_ptr` + `std::enable_shared_from_this`
- Thread safety: `asio::strand` + `std::mutex` + `std::lock_guard`
- Strings: `std::string` not `std::string_view` in public interfaces
- Copy deleted; move defaulted
- `std::ranges` and `std::views` throughout
- `using namespace std::chrono_literals` in Utils.hpp (global scope)
- Macros: `LOGINFO`, `LOGDBG`, `LOGERR`, `LOGWARN`, `LOGCRITICAL` (spdlog wrappers)
- Headers: `#pragma once`
- Block size: `16384` bytes (`BLOCK_SIZE`)

## Build System
- CMake 4.0, C++23, `-Werror` in debug
- vcpkg manages: spdlog, fmt, boost, gtest, openssl
- `compile_commands.json` in `build/`

## Task 2.4: Per-Peer Rate Limiting (2026-05-20)

### Changes Made
- **src/PeerConnection.hpp**: Added `#include "AsyncRateLimiter.hpp"`, `set_upload_rate()`/`set_download_rate()` public methods, `upload_limiter_`/`download_limiter_` shared_ptr members
- **src/PeerConnection.cpp**: Limiters created in `create()` with 10 MB/s default; `co_await upload_limiter_->await_tokens()` in `send_piece()`; `co_await download_limiter_->await_tokens()` in Piece message handler before `on_piece_block`
- **src/AsyncRateLimiter.hpp**: Added `set_rate()` method for runtime rate changes (made rate/capacity non-const)
- **test/protocol_test.cpp**: 3 new tests (UploadRateLimiterCreatedAndSettable, UploadRateLimitingAppliesBackpressure, DownloadRateLimitingAppliesBackpressure)

### Test Results
- 126 tests pass (123 existing + 3 new per-peer rate limit tests)
- Backpressure tests take ~15s each (expected - they test rate limiting at 100/200 bps)

## Task 2.x: FileManager Disk Cache (2026-05-20)

### Changes Made
- **src/FileManager.hpp**:
  - Added `DISK_CACHE_SIZE` constant (32 MB = 32 * 1024 * 1024)
  - Added cache types: `CacheKey` = `std::pair<size_t, uint32_t>`, `CacheValue` = `std::vector<std::byte>`
  - Added LRU tracking: `std::list<CacheKey>` for access order, `std::unordered_map` + custom hash for O(1) lookup
  - Added `std::set<CacheKey> dirty_blocks_` for write-back tracking
  - Added `std::mutex cache_mutex_` for thread safety
  - Added `std::unique_ptr<asio::steady_timer>` for periodic flush (5s)
  - Added cache helper methods: `touch_cache`, `evict_if_needed`, `sync_write_block`, `sync_flush_all_dirty`, `flush_all_dirty`, `periodic_flush`
  - Made destructor non-default (virtual) for shutdown flush

- **src/FileManager.cpp**:
  - `read_block()`: Check cache first; on miss read from disk and populate cache
  - `write_piece()`: Split piece into BLOCK_SIZE blocks, cache each, mark dirty; lazy-initialize periodic flush timer
  - `touch_cache()`: Move accessed key to front of LRU list
  - `evict_if_needed()`: Evict LRU entries when `cache_current_size_ > DISK_CACHE_SIZE`; write back dirty blocks synchronously before eviction
  - `sync_write_block()`: Synchronous file write for dirty block flush (handles piece-file overlap logic)
  - `sync_flush_all_dirty()`: Flush all dirty blocks (used in destructor)
  - `flush_all_dirty()`: Async flush through `async_write_to_file()` (used by periodic flush)
  - `periodic_flush()`: Timer-based loop at 5s intervals; exits on `shutting_down_` flag or `operation_aborted`
  - Destructor: Sets `shutting_down_`, cancels flush timer, calls `sync_flush_all_dirty()`

- **test/filemanager_test.cpp**:
  - `CacheHitOnReRead`: Read block from disk (populates cache), modify disk data, re-read returns cached (original) data
  - `WriteThenReadFromCache`: Write piece to cache, verify disk unchanged, read back from cache
  - `LruEvictionUnderLoad`: Write 2500 pieces (~39 MB) exceeding 32 MB cache, read all back — validates eviction + write-back round-trip

### Cache Design Notes
- Cache key = (piece_index, block_offset) — block granularity matches BLOCK_SIZE (16384)
- Write-back semantics: writes go to cache immediately, flushed to disk on eviction (if dirty), periodic timer (5s), or shutdown
- `sync_write_block()` in `evict_if_needed()` holds `cache_mutex_` — acceptable because 16 KB sync writes are fast
- LRU eviction evicts from back of `lru_order_` list; dirty blocks are written back synchronously before eviction
- Periodic flush timer is lazily created on first `write_piece()` call, using `co_await asio::this_coro::executor`
- The spawned `periodic_flush()` coroutine is detached; destructor cancels the timer and sets `shutting_down_` to signal exit

### Test Results
- 129 tests pass (126 existing + 3 new disk cache tests)
- LruEvictionUnderLoad takes ~1.2s (writes/reads ~39 MB through cache with eviction)

## Task 3.1: Disk Cache in FileManager (2026-05-20)

### Changes Made
- **src/FileManager.hpp**: Added `DISK_CACHE_SIZE` constant (32 MB), cache types (`CacheKey`, `CacheKeyHash`, `LRUList`), data members (`cache_`, `lru_order_`, `lru_index_`, `dirty_blocks_`, `cache_mutex_`, `cache_current_size_`, `flush_timer_`), helper methods (`touch_cache`, `evict_if_needed`, `sync_write_block`, `sync_flush_all_dirty`, `flush_all_dirty`, `periodic_flush`), and `shutting_down_` atomic
- **src/FileManager.cpp**: Cache-aware `read_block` (check cache first), `write_piece` (write to cache, mark dirty), destructor (cancel timer + sync flush), and all cache helper implementations
- **test/filemanager_test.cpp**: 3 new tests (CacheHitOnReRead, WriteThenReadFromCache, LruEvictionUnderLoad)

### Key Design Decisions
- Cache key: `std::pair<size_t, uint32_t>` (piece_index, offset)
- LRU tracking: `std::list<CacheKey>` + `std::unordered_map` for O(1) lookup
- Dirty tracking: `std::set<CacheKey>` — blocks written but not flushed
- Eviction write-back: Synchronous 16KB writes while holding cache mutex
- Periodic flush: Timer-based every 5 seconds, lazy-started on first `write_piece`
- Shutdown flush: Destructor cancels timer, calls sync_flush_all_dirty()
- Thread safety: `cache_mutex_` for all cache operations

### Test Results
- 129 tests pass (126 existing + 3 new disk cache tests)
- LruEvictionUnderLoad: 2500 pieces (~39 MB), LRU eviction with dirty write-back

## Task 4.x: BEP-6 Fast Extension (2026-05-20)

### Changes Made
- **src/Utils.hpp**:
  - Added `Reject = 16`, `HaveNone = 17`, `HaveAll = 18`, `AllowedFast = 19` to `MessageType` enum
  - Added `bool fast_extension{false}` to `Handshake` struct
  - In `Handshake::serialize()`: set `reserved[7] |= 0x04` when `fast_extension` is true (same byte as DHT but different bit)
  - In `Handshake::deserialize()`: read fast extension flag from `reserved[7] & 0x04`
  - Added `RejectPayload` struct (same wire format as `RequestPayload`)

- **src/PeerConnection.hpp**:
  - Added `bool fast_extension_supported_` member + getter/setter
  - Added `send_reject()` method declaration

- **src/PeerConnection.cpp**:
  - Set `fast_extension = true` in outgoing handshake
  - After receiving peer handshake, detect and store `fast_extension_supported_` flag
  - After handshake completes, send `have-none` (when 0 pieces) or `have-all` (when all pieces) or normal bitfield based on state
  - `message_loop()`: Added `MessageType::Reject` case that parses payload and calls `events_->on_piece_rejected()`
  - Implemented `send_reject()` (same wire format as send_request but with Reject type)

- **src/IPeerEvents.hpp**:
  - Added `on_piece_rejected()` virtual method for BEP-6 reject handling

- **src/TorrentSession.hpp**:
  - Added `on_piece_rejected()` override

- **src/TorrentSession.cpp**:
  - `on_block_request()`: When choked and peer supports fast extension, sends `reject` instead of silently ignoring
  - `on_piece_rejected()`: Removes peer from outstanding requests, re-requests block from another unchoked peer
  - Removed bitfield sending from `handle_new_connection()` (now done in `perform_handshake()` which also handles have-none/have-all)

### Key Design Decisions
- BEP-6 reserved bit: `reserved[7]` bit `0x04` — same byte as DHT (`0x01`) but different bit; both can be set simultaneously
- `RejectPayload` reuses `RequestPayload` serialization since wire format is identical (4B index + 4B begin + 4B length)
- Have-none replaces empty bitfield when peer has no pieces (leecher with no data)
- Have-all replaces full bitfield when peer is a seeder (all pieces complete)
- On receiving Reject: immediately re-request from another peer (do NOT wait for timeout), similar to timeout handling
- Bitfield/have-all/have-none sending moved from `TorrentSession::handle_new_connection` to `PeerConnection::perform_handshake` so all connection paths are covered

### Test Results
- 138 tests pass (129 existing + 9 new BEP-6 tests)
- New tests cover: message type values, handshake serialization/deserialization with fast extension flag, reject payload serialize/deserialize, reject wire format matches request payload

## Task 3.2: Fast Extension BEP 6 (2026-05-20)

### Changes Made
- **src/Utils.hpp**: Added Reject=16, HaveNone=17, HaveAll=18, AllowedFast=19 to MessageType enum; added `fast_extension` bool to Handshake struct with serialize/deserialize (reserved[7] |= 0x04); added RejectPayload struct
- **src/IPeerEvents.hpp**: Added `on_piece_rejected()` virtual method
- **src/PeerConnection.hpp/cpp**: Added `fast_extension_supported_` member, detection in handshake, have-none/have-all/bitfield sending in handshake based on piece state, Reject handling in message_loop, send_reject() implementation
- **src/TorrentSession.hpp/cpp**: on_block_request sends Reject when choked (if peer supports fast ext); on_piece_rejected removes from outstanding and re-requests from another peer
- **test/protocol_test.cpp**: 9 new BEP6FastExtensionTest tests covering flag, payloads, deserialization

### Key Design
- have-none sent when metadata unavailable or 0 completed pieces
- have-all sent when all pieces completed (seeder)
- Normal bitfield sent for partial state
- Reject payload uses same wire format as RequestPayload (12 bytes: index+begin+length)
- Re-request skips the rejecting peer and picks another non-choking peer with the piece

### Test Results
- 138 tests pass (129 existing + 9 new BEP-6 tests)

## Task 5.x: Local Peer Discovery BEP 14 (2026-05-20)

### Changes Made
- **src/LsdDiscovery.hpp**: New header-only file implementing `LsdDiscovery` class with:
  - `LsdCrypto::base32_encode()` / `base32_decode()` (RFC 4648, no padding) — constexpr reverse lookup table for decoding
  - UDP multicast socket setup: `reuse_address(true)`, bind to port 6771, `join_group(239.192.152.143)`, `hops(1)`
  - `start()`: Opens/binds UDP socket, joins multicast group, spawns `announce_loop()` and `listen_loop()` on strand
  - `stop()`: Leaves multicast group, closes socket, cancels timer
  - `announce_loop()`: Sends `BT-SEARCH * HTTP/1.1` announcement every 5 minutes via `co_await socket_.async_send_to()`
  - `listen_loop()`: Receives announcements via `co_await socket_.async_receive_from()`, parses HTTP-like header, extracts Infohash (base32) and Port, matches against local info_hash, calls `peer_manager_->add_discovered_peer()`
  - Static `build_announce_message()` and `parse_announcement()` exposed for unit testing
  - Follows existing header-only pattern from `Kademlia.hpp` (all inline, `enable_shared_from_this`, strand)

- **src/TorrentSession.hpp**: Added `#include "LsdDiscovery.hpp"`, `std::shared_ptr<LsdDiscovery> lsd_discovery_` member

- **src/TorrentSession.cpp**: Both constructors initialize `lsd_discovery_` with `io_context`, `peer_port_`, `peer_manager_`, `state_`; `run()` calls `lsd_discovery_->start()`; `stop()` calls `lsd_discovery_->stop()`

- **test/protocol_test.cpp**: 10 new LSD tests covering:
  - Base32 encode known SHA1 value
  - Base32 empty input
  - Base32 roundtrip encode/decode
  - Announce message format (BT-SEARCH header, Host, Port, Infohash fields)
  - Parse valid announcement
  - Parse missing BT-SEARCH
  - Parse missing Infohash
  - Parse missing Port
  - Parse non-numeric port
  - Parse different port value

### Key Design Decisions
- `start()` is a void method (not coroutine) matching DHTNode pattern — it internally spawns the announce and listen coroutines
- Uses `strand_` for both loops to avoid data races with `running_` flag and peer_manager_ access
- Self-announcement filtering: skips if sender IP is 127.0.0.1 and port matches listening_port_
- Only processes announcements for info_hashes matching the local session (filters by base32 comparison)
- `std::tolower` is NOT constexpr in C++23/clang — using separate `upper`/`lower` string literals for reverse lookup table instead

### Test Results
- 148 tests pass (138 existing + 10 new LSD tests)

## Task 3.3: Local Peer Discovery BEP 14 (2026-05-20)

### Changes Made
- **src/LsdDiscovery.hpp** (new): Header-only LsdDiscovery class with LsdCrypto::base32_encode/decode, UDP multicast socket (239.192.152.143:6771), periodic announce every 5min, listen loop for incoming announcements
- **src/TorrentSession.hpp**: Added LsdDiscovery include + member
- **src/TorrentSession.cpp**: Initialize lsd_discovery_ in constructors; start() in run(); stop() on shutdown
- **test/protocol_test.cpp**: 10 LsdDiscoveryTest tests covering base32, announce format, parsing

### Key Design
- Multicast group: 239.192.152.143:6771, link-local hops(1)
- Self-announcements filtered (127.0.0.1 + matching port)
- Base32 encoding for infohash per BEP 14 spec
- Announce format: `BT-SEARCH * HTTP/1.1\r\n...`
- Parsing handles missing fields, non-numeric port gracefully

### Test Results
- 148 tests pass (138 existing + 10 new LSD tests)

## Task 6.x: Client Configuration System (2026-05-20)

### Changes Made
- **src/ClientConfig.hpp** (new): Header-only `ClientConfig` struct with:
  - All configurable parameters (peer_port, upload/download rate limits, max connections, per-IP limit, half-open limit, block timeout, ban threshold/duration, download dir, DHT/LSD/PEX enable flags, DHT bootstrap nodes)
  - `to_dict()` / `from_dict()` for Bencode serialization
  - `load(path)`: Load from Bencoded config file with fallback chain (`--config` path → `./p2p.conf` → `~/.config/p2p/p2p.conf`); returns defaults if file not found
  - `save(path)`: Save to Bencoded config file, creates parent directories automatically
  - `from_cli(argc, argv)`: Full pipeline (defaults → config file → CLI overrides); handles `--port`, `--upload-rate`, `--download-rate`, `--max-connections`, `--max-connections-per-ip`, `--max-half-open`, `--block-timeout`, `--download-dir`, `--no-dht`, `--no-lsd`, `--no-pex`, `--config`
  - `find_config_path()`: Search order for config file location

- **src/main_client.cpp**:
  - Integrated `ClientConfig::from_cli()` for full config pipeline at startup
  - Added `--save-config` support: saves current merged config and exits
  - Added `collect_positional()` helper to extract non-flag args (allows flags anywhere in argv)
  - Seed/download commands now pass `cfg.upload_rate_limit` and `cfg.download_rate_limit` to TorrentSession constructor
  - Positional peer_port (argv[4]) still overrides `--port` flag for backward compatibility
  - Extended `print_usage()` with config flag documentation

- **test/config_test.cpp** (new): 20 tests covering:
  - Default values verification (all fields)
  - Serialize/deserialize roundtrip (both explicit values and defaults)
  - Every CLI flag individually (`--port`, `--upload-rate`, `--download-rate`, `--max-connections`, `--max-connections-per-ip`, `--max-half-open`, `--block-timeout`, `--download-dir`)
  - Boolean flags (`--no-dht`, `--no-lsd`, `--no-pex`)
  - Multiple flags combined
  - Defaults preserved when CLI only overrides some fields
  - Config file save & load roundtrip
  - Bencode validity verification of saved config
  - Nonexistent file returns defaults
  - Parent directory creation on save

- **CMakeLists.txt**: Added `test/config_test.cpp` to build

### Key Design Decisions
- `from_cli()` handles the entire pipeline internally: starts with defaults, loads config file (using `--config` path or search), applies CLI overrides
- `--save-config` is handled in `main()` rather than `from_cli()` — keeps the function focused on config construction
- CLI flags are position-independent: `collect_positional()` extracts them from anywhere in argv, so flags can appear before or after the command verb
- `std::vector<std::byte>` file reading uses explicit `static_cast<std::byte>(char)` loop to avoid C++23 `construct_at` issue (cannot construct `std::byte` from `char` implicitly)
- Config file load never throws — silently returns defaults on any error (missing file, malformed Bencode, etc.)

### Test Results
- 168 tests pass (148 existing + 20 new config tests)
- All 20 config tests complete in ~3ms total
