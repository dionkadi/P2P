# AGENTS.md

P2P is a C++23 BitTorrent client implementation with a tracker server.

## Build

```bash
cmake -B build && cmake --build build
```

Build outputs in `build/`:
- `client` - BitTorrent client (create/seed/download)
- `tracker` - BitTorrent tracker server
- `p2p_test` - Test runner
- `libp2p_common.a` - Static library

**Build constraints:**
- C++23 required
- Strict warnings: `-Wall -Wextra -Werror` (any warning fails build)
- Debug builds include AddressSanitizer
- `compile_commands.json` generated in `build/`

## Test

```bash
./build/p2p_test                    # Run all tests
./build/p2p_test --gtest_filter=BencodeTest.*  # Run specific suite
```

Tests use GTest. Integration tests spin up an in-process tracker.

## Dependencies

- Boost (asio, url, headers)
- OpenSSL (SSL, Crypto)
- spdlog
- GTest

## Client Usage

```bash
./build/client create <file> <output.torrent> <tracker_url>
./build/client seed <torrent> <content_dir> [--port 6881]
./build/client download <torrent> <save_path> [--port 6881]
```

Config flags: `--port`, `--upload-rate`, `--download-rate`, `--max-connections`, `--no-dht`, `--no-lsd`, `--no-pex`, `--config <path>`, `--save-config`.

Config file: `p2p.conf` (bencode format), searched in `./`, `~/.config/p2p/`.

## Tracker Usage

```bash
./build/tracker --http-port 3333 --udp-port 3333 --data-dir ./tracker_data
```

## Architecture

Header-heavy design. Most logic lives in `.hpp` files under `src/`.

Core components:
- `TorrentSession` - Per-torrent session (uses coroutines)
- `PeerManager` - Connection management, choking/unchoking
- `PieceManager` - Piece selection, block requests
- `FileManager` - Disk I/O, caching
- `Tracker` - HTTP/UDP tracker server
- `Kademlia` - DHT (BEP 5)
- `LsdDiscovery` - Local peer discovery (BEP 14)

Async pattern: `asio::awaitable<T>` coroutines throughout.

## Style Notes

- `asio` namespace alias used throughout
- Logging via `LOGINFO`, `LOGWARN`, `LOGCRITICAL` macros (spdlog)
- Types: `PeerId`, `InfoHash` = `std::array<std::byte, 20>`

## Recent Work

Completed production push plan documented in `.omo/plans/push-to-real-world-bittorrent.md`. All features implemented.
