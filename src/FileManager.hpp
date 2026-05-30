#pragma once

#include <atomic>
#include <cassert>
#include <cstddef>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <unordered_map>
#include <filesystem>
#include <vector>
#include <boost/asio.hpp>

#include "SessionState.hpp"
#include "Utils.hpp"

namespace asio = boost::asio;

class FileLocker {
public:
    std::mutex& get_lock(const std::filesystem::path& path) {
        std::lock_guard lock(global_mutex_);
        return file_locks_[path.string()];
    }
    
private:
    std::mutex global_mutex_;
    std::map<std::string, std::mutex> file_locks_;
};

class FileManager {
public:
    /// Disk cache size constant: 32 MB
    static constexpr size_t DISK_CACHE_SIZE = 32 * 1024 * 1024;

    explicit FileManager(std::shared_ptr<SessionState> state);
    virtual ~FileManager();

    FileManager(const FileManager&) = delete;
    FileManager& operator=(const FileManager&) = delete;
    FileManager(FileManager&&) noexcept = delete;
    FileManager& operator=(FileManager&&) noexcept = delete;

    asio::awaitable<std::vector<std::byte>> read_block(size_t piece_index, uint32_t begin, uint32_t length);
    asio::awaitable<void> write_piece(size_t piece_index, std::span<const std::byte> piece_data);
    asio::awaitable<bool> verify_piece(size_t piece_index);
    asio::awaitable<void> verify_pieces();
    asio::awaitable<bool> verify_seed_data();
    asio::awaitable<void> save_resume_data(std::span<const std::byte> data);
    asio::awaitable<std::optional<std::vector<std::byte>>> load_resume_data();
    asio::awaitable<bool> preallocate_files();
    
    std::filesystem::path get_full_path_for_file(const FileInfo& file_info) const;
    std::filesystem::path get_resume_file_path() const;

    std::shared_ptr<const std::vector<size_t>> get_file_to_pieces_map(size_t file_idx) const noexcept { 
        assert(file_idx < file_to_pieces_map_.size());
        std::lock_guard lock(m_);
        return file_to_pieces_map_.at(file_idx); 
    }
    std::shared_ptr<const std::vector<PieceFileOverlap>> get_piece_to_files_map(size_t piece_idx) const noexcept { 
        assert(piece_idx < piece_to_files_map_.size());
        std::lock_guard lock(m_);
        return piece_to_files_map_.at(piece_idx); 
    }
    asio::awaitable<std::map<std::string, int64_t>> async_get_file_mtimes();

    // Async flush — safe to call while the io_context is running.
    asio::awaitable<void> flush();

    // Sync flush — safe to call during shutdown (does not dispatch
    // through the io_context, so it won't deadlock when the io_context
    // is being stopped).
    void sync_flush_all_dirty();

    // Signal shutdown and cancel the periodic flush timer.
    // Called from TorrentSession::stop() to allow periodic_flush() to exit
    // cleanly so the io_context can drain.
    void signal_shutdown() noexcept {
        shutting_down_.store(true);
        io_cancelled_->store(true);
        if (flush_timer_) {
            flush_timer_->cancel();
        }
    }

protected:
    virtual asio::awaitable<int> async_prompt(const std::string& question);
    bool auto_approve_all_{true};

private:
    asio::awaitable<void> async_write_to_file(const std::filesystem::path& path, uint64_t offset, std::span<const std::byte> data);
    asio::awaitable<std::vector<std::byte>> async_read_from_file(const std::filesystem::path& path, uint64_t offset, uint32_t size = 0);

    void build_maps();

    std::shared_ptr<SessionState> state_;
    // -- Disk cache types --
    using CacheKey = std::pair<size_t, uint32_t>;
    struct CacheKeyHash {
        size_t operator()(const CacheKey& p) const noexcept {
            auto h1 = std::hash<size_t>{}(p.first);
            auto h2 = std::hash<uint32_t>{}(p.second);
            return h1 ^ (h2 << 1);
        }
    };
    using LRUList = std::list<CacheKey>;

    ThreadPool file_io_pool_;
    std::shared_ptr<FileLocker> file_locker_;
    std::vector<std::shared_ptr<std::vector<PieceFileOverlap>>> piece_to_files_map_;
    std::vector<std::shared_ptr<std::vector<size_t>>> file_to_pieces_map_;
    mutable std::mutex m_;

    // -- Disk cache (write-back) --
    std::map<CacheKey, std::vector<std::byte>> cache_;
    LRUList lru_order_;
    std::unordered_map<CacheKey, LRUList::iterator, CacheKeyHash> lru_index_;
    std::set<CacheKey> dirty_blocks_;
    mutable std::mutex cache_mutex_;
    size_t cache_current_size_ = 0;
    std::unique_ptr<asio::steady_timer> flush_timer_;
    bool flush_timer_started_ = false;
    std::atomic<bool> shutting_down_{false};
    std::shared_ptr<std::atomic<bool>> io_cancelled_{std::make_shared<std::atomic<bool>>(false)};

    // -- Cache helpers --
    void touch_cache(const CacheKey& key);
    void evict_if_needed();
    void sync_write_block(const CacheKey& key, const std::vector<std::byte>& data);
    asio::awaitable<void> flush_all_dirty();
    asio::awaitable<void> periodic_flush();
};
