# AGENTS.md

P2P is a C++23 BitTorrent client (`build/client`) + tracker server (`build/tracker`). README is empty; trust `CMakeLists.txt` and `src/main_client.cpp`/`src/main_tracker.cpp` as sources of truth.

## Build

- `cmake -B build && cmake --build build`
- CMake 4.0 required; Debug by default with `-Wall -Wextra -Werror` + AddressSanitizer; warnings are errors.
- System deps: Boost (`url`, headers/asio), OpenSSL, spdlog, GTest, TBB, Threads. Vendored: `include/{argparse.hpp,ctrack.hpp,progress_bar.hpp}`.
- Presets: `cmake --preset debug && cmake --build --preset debug-build` (binaries `build/debug/`, test at `build/debug/p2p_test`).
- Library target `p2p_common`; `client`, `tracker`, `p2p_test` link it (test also links GTest).
- No CI workflow exists — build and test locally.

## Profiling

- App is instrumented via CTRACK (`include/ctrack.hpp`): `CTRACK` (sync RAII) on hot paths, `CTRACK_ASYNC` on coroutines.
- Client profiling is opt-in: only `client run --profile` enables it and prints the report at exit; without the flag it's a runtime no-op (`ctrack::set_profiling_enabled(false)` in `main_client.cpp`). Tracker always prints at exit.
- Compile-time disable with `-DCTRACK_DISABLE`.

## Verify

- All tests: `./build/p2p_test`
- One suite: `./build/p2p_test --gtest_filter=BencodeTest.*`
- One integration case: `./build/p2p_test --gtest_filter=IntegrationTest.BasicDownload`
- Unit suites: `BencodeTest`, `BufferWriterTest`/`BufferReaderTest`, `CryptoTest`, `BackoffTest`, `MagnetUriTest`, `ClientConfigTest`, `TorrentFileTest`, `SessionStateTest`, `AsyncRateLimiterTest`, `ProtocolTest` (many sub-suites), `FileManagerTest`, `DHTNetworkTest`, `TrackerDirectTest`, `BanUnitTest`, `ClientAppShutdownTest`.
- Integration quirks: `IntegrationTest` shares one `asio::io_context` fixture across all cases — state can leak between tests. Rerun individually before chasing flakiness. Known timeouts: `TrackerFailover` (180s), `LargeTorrentManyPieces` (600s), `ChokingAlgorithm` (sleeps 15s). Full suite >90s even when passing.
- Integration tests spin up an in-process tracker on ports 6880/6880; `#include` `src/` headers directly.
- Test helpers in `test/helper.hpp`: `RunAsync`, `RunAsyncFor`.
- `test/output.txt` is a tracked LeakSanitizer crash-log artifact — ignore it.

## CLI Gotchas

- Client subcommands: only `create` and `run`.
- `client run <torrent> [dest]` accepts a `.torrent` path. Magnets go through interactive `m <magnet> [dest]` or saved-state restore, not the positional arg.
- Interactive TUI on by default; `h` shows: `a` (add .torrent), `m` (add magnet), `d` (download dir), `t` (add tracker), `f` (fetch trackers), `s`/`p`/`r` (stop/resume/remove by index), `q` (quit). `--non-interactive` disables it.
- `--config` and `--save-config` are pre-scanned manually (`main_client.cpp:290-297`) before argparse — argparse never registers them.
- Without a positional torrent, client reloads `~/.config/p2p/client_state.bencode`.
- Config lookup: `./p2p.conf` first, then `~/.config/p2p/p2p.conf`.
- Tracker: `--port` sets HTTP+UDP to the same value; `--http-port`/`--udp-port` splits them.

## Layout

- `src/main_client.cpp` — CLI, raw-terminal input loop, config/state restore, `ClientApp` startup.
- `src/main_tracker.cpp` — HTTP+UDP tracker listeners, live stats (`LiveDisplay` from vendored `progress_bar.hpp`).
- `ClientApp` owns torrent sessions; `TorrentSession` owns per-torrent subsystems (`PieceManager`, `PeerManager`, `FileManager`, DHT node, LSD discovery, tracker clients).
- `src/` has 21 headers + 8 library `.cpp` + 2 mains; template/coroutine/inline-heavy. `include/` has 3 vendored headers.
- `build/`, `downloads/`, `data/`, `logs/`, `coredump/`, `.cache/`, `.omo/`, `*.torrent` are gitignored. Resume state lives in `downloads/.{infohash}.resume`; root `*.torrent` files (bl.torrent etc.) are untracked local test data, and `coredump/core.client.txt` is a crash artifact.
- `docs/` has BEP references (BEP5, BEP9, BEP11) + `PeerManager.md`/`plan.md` — useful for protocol work.
- `PRINCIPLES.md` is generic software engineering philosophy — skip it for project-specific decisions.

## Conventions

- `Utils.hpp` is the central shared header (aliases, logging, crypto, backoff, constants, protocol structs). Don't add helpers there unless truly cross-cutting.
- `namespace asio = boost::asio;` in `Utils.hpp` but copy-pasted into several other headers.
- Logging is file-only: `Logger::init(...)` creates rotating logs under `logs/`; `LOG*` macros before `init()` silently go to a null sink.
- Peer wire transport uses `AsyncSocket` (wrapper over `asio::ip::tcp::socket`); Beast is used for tracker HTTP/HTTPS (`HttpServer.hpp`, `TrackerClient.hpp`).
- `DHTNode::stop()`: move `pending_queries_` out under `queries_mutex_`, then invoke completions after releasing the lock — avoids reentrant deadlock.
- `test/integration_test.cpp` `SessionHandle` captures raw `this` in coroutine callbacks and waits up to 10s in its destructor for `stop()`.
