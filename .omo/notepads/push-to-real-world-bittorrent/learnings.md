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
