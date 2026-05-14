#pragma once

#include <cassert>
#include <cstddef>
#include <memory>
#include <mutex>
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
    explicit FileManager(std::shared_ptr<SessionState> state);
    virtual ~FileManager() = default;

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

protected:
    virtual asio::awaitable<int> async_prompt(const std::string& question);

private:
    static ThreadPool& get_file_io_pool();
    
    asio::awaitable<void> async_write_to_file(const std::filesystem::path& path, uint64_t offset, std::span<const std::byte> data);
    asio::awaitable<std::vector<std::byte>> async_read_from_file(const std::filesystem::path& path, uint64_t offset, uint32_t size = 0);

    void build_maps();

    std::shared_ptr<SessionState> state_;
    ThreadPool& file_io_pool_;
    std::shared_ptr<FileLocker> file_locker_;
    std::vector<std::shared_ptr<std::vector<PieceFileOverlap>>> piece_to_files_map_;
    std::vector<std::shared_ptr<std::vector<size_t>>> file_to_pieces_map_;
    mutable std::mutex m_;
};