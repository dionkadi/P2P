#pragma once

#include "Utils/AsyncSocket.hpp"
#include "Utils/ThreadPool.hpp"
#include "Utils/AsyncRateLimiter.hpp"
#include "Protocols/MetaInfo.hpp"
#include "Protocols/Protocol.hpp"
#include "Peers/TrackerClient.hpp"
#include <atomic>
#include <boost/asio/awaitable.hpp>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <unordered_set>
#include <map>
#include <memory>
#include <string>
#include <vector>

enum class PieceStatus { 
    Needed, 
    InProgress, 
    Have 
};

struct PeerConnection {
    PeerConnection(AsyncSocket other_socket): socket(std::move(other_socket)) {}

    bool has_piece(size_t index) const {
        if (bitfield.empty() || index / 8 >= bitfield.size()) {
            return false;
        }
        return (bitfield[index / 8] >> (7 - (index % 8))) & 1;
    }

    void set_has_piece(size_t index) {
        if (bitfield.empty() || index / 8 >= bitfield.size()) {
            return ;
        }
        bitfield[index / 8] |= (1 << (7 - (index % 8)));
    }

    AsyncSocket socket;
    PeerId peer_id;
    std::string peer_addr;
    std::vector<uint8_t> bitfield;

    bool am_choking = true;
    bool peer_is_choking = true;
    bool am_interested = false;
    bool peer_is_interested = false;

    std::atomic<uint64_t> bytes_downloaded {0};
    std::atomic<uint64_t> bytes_uploaded {0};
};

struct InProgressPiece {
    std::vector<std::byte> data;
    std::vector<bool> blocks_received;
    uint32_t received_count = 0;
    uint32_t total_blocks = 0;

    std::vector<std::vector<PeerId>> outstanding_requests;

    InProgressPiece(uint64_t piece_size): data(piece_size) {
        total_blocks = (piece_size + BLOCK_SIZE - 1) / BLOCK_SIZE;
        blocks_received.resize(total_blocks, false);
        outstanding_requests.resize(total_blocks);
    }
};

class PeerLogic: public std::enable_shared_from_this<PeerLogic> {
public:
    PeerLogic(asio::io_context& io_context, PeerId peer_id, const std::filesystem::path& torrent_path,
        int peer_port,
        uint64_t upload_rate_bps, uint64_t download_rate_bps
    );

    virtual ~PeerLogic() = default;

protected:
    asio::awaitable<void> handle_new_connection(AsyncSocket socket, std::string peer_addr);
    asio::awaitable<void> message_loop(std::shared_ptr<PeerConnection> conn);

    asio::awaitable<void> choke_loop();
    asio::awaitable<void> downloader_loop();
    asio::awaitable<void> request_one_piece_loop();
    asio::awaitable<void> keep_alive_loop(std::shared_ptr<PeerConnection> conn);

    asio::awaitable<void> send_simple_message(PeerConnection& conn, MessageType type);
    asio::awaitable<void> send_have_message_to_all(uint32_t piece_index);
    asio::awaitable<void> send_request_message(PeerConnection& conn, uint32_t index, uint32_t begin, uint32_t length);
    asio::awaitable<void> send_cancel_for_block(uint32_t piece_index, uint32_t block_index, const PeerId& exclude_peer_id);
    asio::awaitable<void> send_cancel_message(PeerConnection& conn, uint32_t index, uint32_t begin, uint32_t length);
    
    asio::awaitable<void> await_upload_tokens(size_t amount);
    asio::awaitable<void> await_download_tokens(size_t amount);
    
    asio::awaitable<void> check_and_enter_endgame();
    asio::awaitable<void> broadcast_outstanding_requests();

    asio::awaitable<void> handle_piece_message(std::shared_ptr<PeerConnection> conn, std::span<const std::byte> payload);
    asio::awaitable<void> handle_completed_piece(int piece_index, const std::vector<std::byte>& piece_data);
    asio::awaitable<void> return_piece_to_queue(int piece_index);
    asio::awaitable<void> handle_request_message(std::shared_ptr<PeerConnection> conn, std::span<const std::byte> payload);
    
    asio::awaitable<void> verify_existing_file();
    asio::awaitable<bool> verify_seed_data(); 
    asio::awaitable<void> save_progress();
    asio::awaitable<void> load_progress();

    asio::awaitable<void> tracker_announce_loop();

    std::filesystem::path get_full_path_for_file(const FileInfo& file_info) const;
    void update_piece_rarity(int piece_index, uint32_t old_rarity, uint32_t new_rarity);

    asio::io_context& io_context_;
    MetaInfo meta_info_;
    PeerId my_peer_id_;
    std::vector<std::byte> info_hash_bytes_;

    asio::strand<asio::io_context::executor_type> strand_;
    std::filesystem::path data_file_path_;
    
    std::vector<PieceStatus> piece_status_;
    std::vector<uint32_t> piece_availability_;
    std::map<uint32_t, std::unordered_set<int>> pieces_by_rarity_;
    std::map<PeerId, std::shared_ptr<PeerConnection>> active_connections_;
    std::map<int, InProgressPiece> in_progress_pieces_;
    std::atomic<uint32_t> pieces_done_count_{0};
    std::atomic<bool> is_download_complete_{false};
    std::atomic<bool> is_in_endgame_mode_{false};

    const uint64_t UPLOAD_RATE_BPS_;
    const uint64_t DOWNLOAD_RATE_BPS_;
    const uint64_t TOKEN_BUCKET_CAPACITY_FACTOR = 2;

    int choke_loop_counter_{0};

    asio::steady_timer completion_timer_;
    asio::steady_timer piece_request_trigger_;

    ThreadPool& file_io_pool_;

    std::vector<std::vector<std::shared_ptr<ITrackerClient>>> tracker_clients_by_tier_;
    int peer_port_;

private:
    static ThreadPool& get_file_io_pool();

    asio::awaitable<void> async_write_to_file(std::filesystem::path path, uint64_t offset, const std::vector<std::byte>& data);
    asio::awaitable<std::vector<std::byte>> async_read_from_file(std::filesystem::path path, uint64_t offset, uint32_t size);
    asio::awaitable<void> connect_to_peer(std::string peer_addr);

    bool is_seeder_() const;

    AsyncRateLimiter upload_limiter_;
    AsyncRateLimiter download_limiter_;
};

class Seeder: public PeerLogic {
    struct private_key {}; 
    asio::awaitable<bool> async_init();

public:
    Seeder(private_key, asio::io_context& io_context, PeerId peer_id, const std::filesystem::path& torrent_path, const std::filesystem::path& content_dir, 
        int peer_port, uint64_t upload_rate_bps
    );
        
    static asio::awaitable<std::shared_ptr<Seeder>> create(
        asio::io_context& io_context, 
        PeerId peer_id, 
        const std::filesystem::path& torrent_path, 
        const std::filesystem::path& content_dir, 
        int peer_port, uint64_t upload_rate_bps = 512 * 1024
    );

    asio::awaitable<void> run();

private:   
    int peer_port_;
};



class Leecher: public PeerLogic {
    struct private_key {}; 
    asio::awaitable<bool> async_init();
    asio::awaitable<void> save();

public:
    Leecher(private_key, asio::io_context& io_context, PeerId peer_id, const std::filesystem::path& torrent_path, const std::filesystem::path& save_path,
        int peer_port,
        uint64_t upload_rate_bps, uint64_t download_rate_bps
    );

    static asio::awaitable<std::shared_ptr<Leecher>> create(
        asio::io_context& io_context, 
        PeerId peer_id, 
        const std::filesystem::path& torrent_path, 
        const std::filesystem::path& save_path,
        int peer_port,
        uint64_t upload_rate_bps = 512 * 1024, 
        uint64_t download_rate_bps = 2048 * 1024
    );

    asio::awaitable<bool> run();
};

