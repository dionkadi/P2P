#pragma once

#include "TorrentFile.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <format>
#include <mutex>

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

    const MetaInfo& info() const { return meta_info_; }
    const TorrentInfo& torrent_info() const { return meta_info_.get_torrent_info(); }
    TorrentInfo& torrent_info() { return meta_info_.get_torrent_info(); }
    const std::vector<std::byte>& info_hash() const { return meta_info_.get_info_hash(); }
    const std::vector<PieceStatus>& piece_status() const { return piece_status_; }
    std::vector<PieceStatus>& piece_status() { return piece_status_; }
    PieceStatus piece_status(size_t piece_index) const { return piece_status_[piece_index]; }
    const std::vector<std::vector<std::string>>& tracker_tiers() const { return tracker_tiers_; }
    uint64_t total_bytes_downloaded() const { return total_bytes_downloaded_.load(); }
    uint64_t total_bytes_uploaded() const { return total_bytes_uploaded_.load(); }
    size_t completed_pieces() const { return completed_pieces_.load(); }
    size_t num_pieces() const { return num_pieces_; }
    bool is_download_complete() const { return is_download_complete_.load(); }
    bool is_in_endgame_mode() const { return is_in_endgame_mode_.load(); }
    const std::filesystem::path& save_path() const { return data_file_path_; }

    void piece_status(size_t piece_index, PieceStatus status) { piece_status_[piece_index] = status; }
    void add_total_bytes_downloaded(uint64_t val) { total_bytes_downloaded_ += val; }
    void add_total_bytes_uploaded(uint64_t val) { total_bytes_uploaded_ += val; }
    void completed_pieces(size_t val) { completed_pieces_ = val; }
    void add_completed_pieces(size_t val) { completed_pieces_ += val; }
    void is_download_complete(bool val) { is_download_complete_.store(val); }
    void is_in_endgame_mode(bool val) { is_in_endgame_mode_.store(val); }

    std::string progress() const { return std::format("{:.2f}% ({}/{})", (static_cast<float>(completed_pieces_) / num_pieces_) * 100.0f, completed_pieces_.load(), num_pieces_); }

    std::string get_have_bitfield_str() const {
        std::lock_guard lock(m_);
        std::string bitfield((num_pieces_ + 7) / 8, 0);
        for (size_t i = 0; i < num_pieces_; ++i) {
            if (piece_status_[i] == PieceStatus::Have) {
                bitfield[i / 8] |= (1 << (7 - (i % 8)));
            }
        }
        return bitfield;
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