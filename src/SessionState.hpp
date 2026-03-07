#pragma once

#include "TorrentFile.hpp"

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <format>
#include <mutex>
#include <ranges>
#include <vector>

class SessionState {
public:
    explicit SessionState(const std::filesystem::path& torrent_path, const std::filesystem::path& save_path)
        : data_file_path_(save_path)
    {
        if (!meta_info_.load_from_file(torrent_path, tracker_tiers_)) {
            throw std::runtime_error("Could not load torrent file: " + torrent_path.string());
        }

        if (meta_info_.get_info_hash().size() != HASH_SIZE) {
            throw std::runtime_error("Invalid info hash size.");
        }

        num_pieces_ = meta_info_.get_torrent_info().pieces.size() / 20;
        piece_status_.resize(num_pieces_, PieceStatus::Needed);
    }

    const MetaInfo& info() const noexcept { return meta_info_; }
    const TorrentInfo& torrent_info() const noexcept { return meta_info_.get_torrent_info(); }
    TorrentInfo& torrent_info() noexcept { return meta_info_.get_torrent_info(); }
    const std::vector<std::byte>& info_hash() const noexcept { return meta_info_.get_info_hash(); }
    const std::vector<std::vector<std::string>>& tracker_tiers() const noexcept { return tracker_tiers_; }

    PieceStatus piece_status(size_t piece_index) const noexcept { 
        assert(piece_index < num_pieces_);
        std::lock_guard lock(m_);
        return piece_status_[piece_index]; 
    }

    uint64_t total_bytes_downloaded() const noexcept { return total_bytes_downloaded_.load(std::memory_order_relaxed); }
    uint64_t total_bytes_uploaded() const noexcept { return total_bytes_uploaded_.load(std::memory_order_relaxed); }
    size_t completed_pieces() const noexcept { return completed_pieces_.load(std::memory_order_relaxed); }
    size_t num_pieces() const noexcept { return num_pieces_; }
    bool is_download_complete() const noexcept { return is_download_complete_.load(std::memory_order_relaxed); }
    bool is_in_endgame_mode() const noexcept { return is_in_endgame_mode_.load(std::memory_order_relaxed); }
    const std::filesystem::path& save_path() const noexcept { return data_file_path_; }

    void piece_status(size_t piece_index, PieceStatus status) noexcept { 
        std::lock_guard lock(m_);
        piece_status_[piece_index] = status; 
    }
    void add_total_bytes_downloaded(uint64_t val) noexcept { total_bytes_downloaded_.fetch_add(val, std::memory_order_relaxed); }
    void add_total_bytes_uploaded(uint64_t val) noexcept { total_bytes_uploaded_.fetch_add(val, std::memory_order_relaxed); }
    void completed_pieces(size_t val) noexcept { completed_pieces_.store(val, std::memory_order_relaxed); }
    void add_completed_pieces(size_t val) noexcept { completed_pieces_.fetch_add(val, std::memory_order_relaxed); }
    void is_download_complete(bool val) noexcept { is_download_complete_.store(val, std::memory_order_relaxed); }
    void is_in_endgame_mode(bool val) noexcept { is_in_endgame_mode_.store(val, std::memory_order_relaxed); }

    std::string progress() const {
        auto completed = completed_pieces_.load();
        return std::format("{:.2f}% ({}/{})", (static_cast<float>(completed) / num_pieces_) * 100.0f, completed, num_pieces_); 
    }

    std::string get_have_bitfield_str() const {
        std::lock_guard lock(m_);
        std::string bitfield((num_pieces_ + 7) / 8, 0);
        std::ranges::for_each(std::views::iota(0UL, num_pieces_)
                                | std::views::filter([this](size_t i) {
                                    return piece_status_[i] == PieceStatus::Have;
                                }),
                                [&](size_t i) {
                                    bitfield[i / 8] |= (1 << (7 - (i % 8)));
                                });
        return bitfield;
    }

    bool is_multi_file() const { return torrent_info().files.size() > 1;}
    void update_file_stat(size_t file_idx, bool val) {
        std::lock_guard lock(m_);
        auto& file = meta_info_.get_torrent_info().files.at(file_idx);
        file.download = val;
    }

    size_t needed_pieces() const noexcept {
        std::lock_guard lock(m_);
        return std::ranges::count_if(piece_status_, 
                                    [](PieceStatus status) { 
                                        return status == PieceStatus::Needed; 
                                    });
    }
    template<typename Func>
    size_t status_count(Func f) const {
        std::lock_guard lock(m_);
        return std::ranges::count_if(piece_status_, f);
    }

private:
    MetaInfo meta_info_;
    std::vector<PieceStatus> piece_status_;
    std::vector<std::vector<std::string>> tracker_tiers_;
    std::filesystem::path data_file_path_;
    std::atomic_uint64_t total_bytes_downloaded_{0};
    std::atomic_uint64_t total_bytes_uploaded_{0};
    std::atomic_size_t completed_pieces_{0};
    std::atomic_bool is_download_complete_{false};
    std::atomic_bool is_in_endgame_mode_{false};
    size_t num_pieces_;
    mutable std::mutex m_;
};