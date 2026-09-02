# P2P

A C++23 BitTorrent client and tracker built around Boost.Asio coroutines. The
project provides two executables:

- `client` — downloads and seeds torrents, creates `.torrent` files, and
  discovers peers through trackers, DHT, local peer discovery, and PEX.
- `tracker` — a lightweight HTTP and UDP tracker with a live terminal status
  display and persisted peer state.

> **Status:** experimental and under active development. The current client is
> primarily intended for Linux/POSIX environments and IPv4 peer paths.

## Features

- `.torrent` creation for single files and multi-file directories.
- BitTorrent peer wire protocol over TCP, including piece/block verification.
- Concurrent torrent sessions managed by one client process.
- Magnet links with BEP-9/BEP-10 metadata exchange.
- HTTP, HTTPS, and UDP tracker clients.
- Embedded HTTP and UDP tracker server.
- Peer discovery through:
  - DHT (BEP 5)
  - Local Service Discovery (BEP 14)
  - Peer Exchange (BEP 11)
  - Tracker announces
- BitTorrent protocol encryption (MSE), enabled by default.
- Rarest-first piece management, request pipelining, choking/unchoking, and
  end-game handling.
- Upload/download rate limits, connection limits, retry backoff, and peer
  banning for repeated misbehavior.
- Resume data and client state persistence.
- Live terminal UI with progress, peer/tracker counts, transfer speeds, and
  interactive torrent management.
- AddressSanitizer-enabled Debug builds and a broad GoogleTest suite.

## Requirements

The project currently requires:

- A C++23 compiler with support for the standard library features used by the
  project, including `std::format` and coroutines.
- CMake 4.0 or newer.
- Ninja (required by the provided CMake presets).
- Boost with Asio, headers, and the `url` component.
- OpenSSL.
- spdlog.
- oneTBB.
- GoogleTest.
- POSIX threads and a POSIX-compatible terminal for the interactive client.

The exact package names vary by distribution. CMake reports any missing
packages during configuration.

## Build

A regular Debug build can be configured and compiled with:

```sh
cmake -B build
cmake --build build
```

Debug builds use strict warnings (`-Wall -Wextra -Werror`) and AddressSanitizer.
For the preset-based workflow:

```sh
# Debug: build/debug/client, build/debug/tracker, build/debug/p2p_test
cmake --preset debug
cmake --build --preset debug-build

# Release: build/release/client, build/release/tracker, build/release/p2p_test
cmake --preset release
cmake --build --preset release-build
```

The main targets are:

| Target | Description |
| --- | --- |
| `client` | BitTorrent client and terminal UI |
| `tracker` | HTTP/UDP tracker server |
| `p2p_common` | Shared protocol, networking, storage, and session library |
| `p2p_test` | Unit and integration tests |

Run commands from the repository root so relative paths such as `./downloads`,
`./logs`, and `./tracker_data` resolve as expected.

## Client

### Create a torrent

```sh
./build/client create <file-or-directory> <output.torrent> <tracker-url[,tracker-url...]>
```

For example:

```sh
./build/client create ./shared ./shared.torrent http://127.0.0.1:3333/announce
```

The creator walks directories in sorted order, hashes the content into pieces,
and writes the tracker announce URL(s) into the generated metainfo. The current
CLI uses a 256 KiB piece size.

### Run a torrent

```sh
./build/client run [torrent-file] [destination] [options]
```

Examples:

```sh
# Download or seed a torrent into ./downloads
./build/client run ./shared.torrent ./downloads

# Listen for peers on a different port and disable the TUI command input
./build/client run ./shared.torrent ./downloads \
  --port 6882 --non-interactive

# Use a custom default download directory
./build/client run --download-dir /srv/downloads
```

A `.torrent` path is accepted as the positional `torrent-file`. Magnets are
added from the interactive `m` command rather than as the positional argument;
see [Interactive commands](#interactive-commands). If no torrent is supplied,
the client attempts to restore torrents from its saved client state.

### Client options

| Option | Default | Description |
| --- | ---: | --- |
| `--port PORT` | `6881` | TCP peer listening port |
| `--upload-rate BYTES` | `0` | Upload limit in bytes/second; `0` means unlimited |
| `--download-rate BYTES` | `0` | Download limit in bytes/second; `0` means unlimited |
| `--max-connections N` | `500` | Maximum total peer connections |
| `--max-connections-per-ip N` | `4` | Maximum connections from one IP address |
| `--max-half-open N` | `500` | Maximum concurrent outgoing connection attempts |
| `--block-timeout SECONDS` | `30` | Timeout for a requested block |
| `--download-dir PATH` | `./downloads` | Default destination for added torrents |
| `--no-dht` | off | Disable DHT peer discovery |
| `--no-lsd` | off | Disable local peer discovery |
| `--no-pex` | off | Disable peer exchange |
| `--no-encryption` | off | Disable MSE protocol encryption |
| `--non-interactive` | off | Disable interactive command input |
| `--profile` | off | Print CTRACK profiling results on exit |

`--config PATH` and `--save-config` are also supported as global client
options. They are scanned before normal command parsing:

```sh
# Write the default configuration to ./p2p.conf
./build/client --save-config

# Write or use a specific configuration file
./build/client --config ./p2p.conf --save-config
./build/client --config ./p2p.conf run ./shared.torrent
```

## Interactive commands

Interactive input is enabled by default. The client starts a live display and
accepts one command per line. Torrent indexes are zero-based and follow the
order in which they appear in the client.

| Command | Description |
| --- | --- |
| `a <torrent> [dest]` | Add a `.torrent` file |
| `m <magnet> [dest]` | Add a magnet link and download its metadata |
| `d <path>` | Change the default destination for subsequently added torrents |
| `t <url>` | Add a tracker to all active torrents and future torrents |
| `f <url>` | Fetch a tracker list from a URL in the background |
| `s <index>` | Stop a torrent |
| `p <index>` | Resume a stopped torrent |
| `r <index>` | Remove a torrent and persist the removal |
| `h` | Show the command help |
| `q` | Stop all torrents and quit |

Quote magnet links and paths when the shell or terminal input would otherwise
interpret special characters or spaces:

```text
m "magnet:?xt=urn:btih:<info-hash>&dn=Example"
a "/path/with spaces/example.torrent" "/path/with spaces/downloads"
```

The display also shows whether each session is downloading, seeding, or
stopped, along with progress, peers, trackers, current rates, and cumulative
bytes transferred. Under `--non-interactive`, the live display remains enabled
but command input is disabled.

## Tracker server

Start the embedded tracker with:

```sh
./build/tracker [options]
```

By default it listens for both HTTP and UDP announces on port `3333` and stores
its state under `./tracker_data`.

```sh
# HTTP and UDP on the same port
./build/tracker --port 3333 --data-dir ./tracker_data

# Use separate HTTP and UDP ports
./build/tracker --http-port 3333 --udp-port 3334
```

Tracker options:

| Option | Default | Description |
| --- | ---: | --- |
| `--http-port PORT` | `3333` | HTTP listen port |
| `--udp-port PORT` | `3333` | UDP listen port |
| `--port PORT` | unset | Set both HTTP and UDP ports |
| `--data-dir PATH` | `./tracker_data` | Directory for persisted tracker state |

The HTTP announce endpoint is `/announce`; the root path is also accepted by
the embedded tracker. A typical local announce URL is:

```text
http://127.0.0.1:3333/announce
```

Stop the tracker with `Ctrl-C` or `SIGTERM`. Its terminal display reports the
configured endpoints, swarm count, peer count, and announce count.

## Persistence and runtime files

The client and tracker use local files for configuration, resume data, and
operational logs:

| Path | Purpose |
| --- | --- |
| `./p2p.conf` | Preferred local client configuration file |
| `~/.config/p2p/p2p.conf` | Fallback client configuration file |
| `~/.config/p2p/client_state.bencode` | List of torrents restored when no positional torrent is supplied |
| `~/.config/p2p/dht_state.bencode` | Persisted DHT node identity and routing state |
| `downloads/.<infohash>.resume` | Per-torrent resume data |
| `./tracker_data/tracker_state.bencode` | Tracker peer state |
| `./logs/` | Rotating client/tracker logs and `speed.txt` |

Configuration and state files use the project’s bencode implementation. The
client prefers `./p2p.conf`; if it is absent, it checks the per-user path.
Command-line run settings are applied to the loaded client configuration.

## Architecture

The shared `p2p_common` library is organized around a torrent session:

```text
client / tracker
       |
   p2p_common
       |
   ClientApp
       |
   TorrentSession
    /    |       \
PeerManager  PieceManager  FileManager
    |
PeerConnection / TrackerClient / DHT / LSD
```

Important components include:

- `ClientApp` — owns multiple sessions, shared DHT state, persistence, and
  shutdown coordination.
- `TorrentSession` — coordinates metadata, trackers, peer discovery, piece
  selection, disk I/O, rate limits, and completion.
- `PeerManager` and `PeerConnection` — manage TCP peers, wire messages,
  choking/unchoking, PEX, retries, and bans.
- `PieceManager` and `SessionState` — track piece availability, block requests,
  verification, completion, and resume state.
- `FileManager` — maps torrent pieces to single-file or multi-file content.
- `Kademlia` and `LsdDiscovery` — provide DHT and LAN peer discovery.
- `TrackerClient` and `Tracker` — implement tracker announces and compact peer
  responses over HTTP/HTTPS and UDP.
- `Bencode`, `TorrentFile`, and `MagnetUri` — parse and produce torrent and
  magnet metadata.

Protocol notes and implementation details are available in:

- [`docs/BEP5.md`](docs/BEP5.md) — DHT implementation
- [`docs/BEP9.md`](docs/BEP9.md) — magnet links and metadata exchange
- [`docs/BEP11.md`](docs/BEP11.md) — peer exchange
- [`docs/PeerManager.md`](docs/PeerManager.md) — peer management
- [`docs/plan.md`](docs/plan.md) — planned improvements and known follow-up work

## Testing

Build the project, then run the complete test executable:

```sh
./build/p2p_test
```

Run a focused suite with GoogleTest filters:

```sh
./build/p2p_test --gtest_filter=BencodeTest.*
./build/p2p_test --gtest_filter=ProtocolTest.*
./build/p2p_test --gtest_filter=IntegrationTest.BasicDownload
```

The integration tests start an in-process tracker and exercise real asynchronous
sessions. Some integration cases intentionally take longer than unit tests;
run an individual case first when investigating a failure.

## Development notes

- Debug builds treat compiler warnings as errors and enable AddressSanitizer.
- The code uses Boost.Asio coroutines and shared `io_context` instances for
  asynchronous networking.
- Client profiling is opt-in via `client run --profile`; the tracker prints
  its CTRACK report on exit.
- There is currently no install target or CI workflow. Build and test locally.
- IPv4 is the safest choice for peer connectivity today; some peer-response
  paths do not yet consume IPv6 peers.

Use this software only to share and retrieve content you are authorized to
handle.
