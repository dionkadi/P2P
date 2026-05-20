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
