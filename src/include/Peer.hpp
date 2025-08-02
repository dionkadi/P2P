#pragma once

#include "AsyncSocket.hpp"
#include "MetaInfo.hpp"
#include "Protocol.hpp"
#include "ThreadPool.hpp"
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <string>
#include <string_view>
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
    std::string peer_id;
    std::vector<uint8_t> bitfield;

    bool am_choking = true;
    bool peer_is_choking = true;
    bool am_interested = false;
    bool peer_is_interested = false;

    std::atomic<uint64_t> bytes_downloaded {0};
    std::atomic<uint64_t> bytes_uploaded {0};
};

struct InProgressPiece {
    std::vector<char> data;
    std::vector<bool> blocks_received;
    uint32_t received_count = 0;
    uint32_t total_blocks = 0;

    InProgressPiece(uint64_t piece_size): data(piece_size) {
        total_blocks = (piece_size + BLOCK_SIZE - 1) / BLOCK_SIZE;
        blocks_received.resize(total_blocks, false);
    }
};

class PeerLogic: public std::enable_shared_from_this<PeerLogic> {
public:
    PeerLogic(asio::io_context& io_context, std::string peer_id, const std::filesystem::path& torrent_path);
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

    asio::awaitable<void> handle_piece_message(std::shared_ptr<PeerConnection> conn, std::span<const char> payload);
    asio::awaitable<void> handle_completed_piece(int piece_index);
    asio::awaitable<void> return_piece_to_queue(int piece_index);
    asio::awaitable<void> handle_request_message(std::shared_ptr<PeerConnection> conn, std::span<const char> payload);

    asio::io_context& io_context_;
    MetaInfo meta_info_;
    std::string my_peer_id_;
    std::string info_hash_bytes_;

    asio::strand<asio::io_context::executor_type> strand_;
    std::filesystem::path data_file_path_;
    
    std::vector<PieceStatus> piece_status_;
    std::vector<uint32_t> piece_availability_;
    std::map<std::string, std::shared_ptr<PeerConnection>> active_connections_;
    std::map<int, InProgressPiece> in_progress_pieces_;
    std::atomic<uint32_t> pieces_done_count_{0};
    std::atomic<bool> is_download_complete_{false};

    asio::steady_timer completion_timer_;

    ThreadPool& file_io_pool_;

private:
    static ThreadPool& get_file_io_pool();

    asio::awaitable<void> async_write_to_file(std::filesystem::path path, uint64_t offset, const std::vector<char>& data);
    asio::awaitable<std::vector<char>> async_read_from_file(std::filesystem::path path, uint64_t offset, uint32_t size);
};

class Seeder: public PeerLogic {
public:
    Seeder(asio::io_context& io_context, std::string peer_id, const std::filesystem::path& torrent_path, const std::filesystem::path& content_dir, int peer_port);
    
    asio::awaitable<void> run(const std::string& tracker_host, int tracker_port);

private:   
    int peer_port_;
};



class Leecher: public PeerLogic {
public:
    Leecher(asio::io_context& io_context, std::string peer_id, const std::filesystem::path& torrent_path, const std::filesystem::path& save_path);

    asio::awaitable<bool> run(const std::string& tracker_host, int tracker_port);

private:
    asio::awaitable<void> connect_to_peer(const std::string& peer_addr);
};

