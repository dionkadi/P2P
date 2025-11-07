#include "Peers/Peer.hpp"
#include "Utils/Crypto.hpp"
#include "Utils/Logger.hpp"
#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <filesystem>
#include <fstream>
#include <ios>
#include <netinet/in.h>
#include <random>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>
#include <vector>
#include <memory>


PeerLogic::PeerLogic(asio::io_context& io_context, std::string peer_id, const std::filesystem::path& torrent_path,
                    int peer_port, 
                    uint64_t upload_rate_bps, uint64_t download_rate_bps)
    : io_context_(io_context),
      my_peer_id_(std::move(peer_id)),
      strand_(asio::make_strand(io_context)),
      UPLOAD_RATE_BPS_(upload_rate_bps),
      DOWNLOAD_RATE_BPS_(download_rate_bps),
      completion_timer_(io_context),
      piece_request_trigger_(io_context),
      file_io_pool_(get_file_io_pool()),
      peer_port_(peer_port)
{

    std::vector<std::vector<std::string>> tracker_tiers;
    if (!meta_info_.load_from_file(torrent_path, tracker_tiers)) {
        throw std::runtime_error("Could not load torrent file: " + torrent_path.string());
    }

    for (const auto& tier : tracker_tiers) {
        std::vector<std::shared_ptr<ITrackerClient>> client_tier;
        for (const auto& url : tier) {
            try {
                client_tier.push_back(create_tracker_client(io_context, url));
            } catch (const std::exception& e) {
                LOGWARN("Failed to create tracker client for URL '{}': {}", url, e.what());
            }
        }
        if (!client_tier.empty()) {
            tracker_clients_by_tier_.push_back(std::move(client_tier));
        }
    }

    if (tracker_clients_by_tier_.empty()) {
        throw std::runtime_error("No valid tracker clients could be created from the torrent file.");
    }

    info_hash_bytes_ = meta_info_.get_info_hash();
    if (info_hash_bytes_.size() != HASH_SIZE) {
        throw std::runtime_error("Invalid info hash size after conversion.");
    }

    const size_t num_pieces = meta_info_.get_torrent_info().pieces.length() / 20;
    piece_availability_.resize(num_pieces, 0);

    completion_timer_.expires_at(asio::steady_timer::time_point::max());
    piece_request_trigger_.expires_at(asio::steady_timer::time_point::max());

    upload_tokens_ = UPLOAD_RATE_BPS_ * TOKEN_BUCKET_CAPACITY_FACTOR;
    download_tokens_ = DOWNLOAD_RATE_BPS_ * TOKEN_BUCKET_CAPACITY_FACTOR;
}

asio::awaitable<void> PeerLogic::handle_new_connection(AsyncSocket socket, std::string peer_addr) {
    auto conn = std::make_shared<PeerConnection>(std::move(socket));
    conn->peer_addr = peer_addr;
    try {
        LOGDBG("Starting handshake with peer {}", peer_addr);
        Handshake my_handshake {info_hash_bytes_, my_peer_id_};
        co_await conn->socket.send_raw(my_handshake.serialize());

        std::vector<char> handshake_buffer = co_await conn->socket.receive_raw(HANDSHAKE_BASE_LEN);
        Handshake peer_handshake = Handshake::deserialize(handshake_buffer);

        conn->peer_id = peer_handshake.peer_id_bytes;
        LOGINFO("Handshake successful with peer ID: {}", conn->peer_id);

        if (conn->peer_id == my_peer_id_) {
            LOGWARN("Connected to self. Dropping connection.");
            co_return;
        }

        if (peer_handshake.info_hash_bytes != info_hash_bytes_) {
            throw std::runtime_error("Info hash mismatch during handshake. Closing connection.");
        }

        co_await asio::dispatch(strand_, asio::use_awaitable);
        if (active_connections_.count(conn->peer_id)) {
            if (my_peer_id_ < conn->peer_id) {
                LOGWARN("Duplicate connection to {}. Dropping this one.", conn->peer_id);
                co_return;
            } else {
                LOGWARN("Duplicate connection to {}. Closing the other one.", conn->peer_id);
                active_connections_[conn->peer_id]->socket.close();
                active_connections_.erase(conn->peer_id);
            }
        }
        active_connections_[conn->peer_id] = conn;

        if (pieces_done_count_ > 0) {
            std::vector<char> my_bitfield_data((piece_status_.size() + 7) / 8, 0);
            for (size_t i = 0; i < piece_status_.size(); ++i) {
                if (piece_status_[i] == PieceStatus::Have) {
                    my_bitfield_data[i/8] |= (1 << (7 - (i % 8)));
                }
            }
            
            std::vector<char> bitfield_msg_body;
            bitfield_msg_body.push_back(static_cast<char>(MessageType::Bitfield));
            bitfield_msg_body.insert(bitfield_msg_body.end(), my_bitfield_data.begin(), my_bitfield_data.end());
            co_await conn->socket.send_message(bitfield_msg_body);
        }


        co_await send_simple_message(*conn, MessageType::Unchoke);
        conn->am_choking = false;

        asio::co_spawn(io_context_, message_loop(conn), asio::detached);
        asio::co_spawn(io_context_, keep_alive_loop(conn), asio::detached);

    } catch (const boost::system::system_error& e) {
        if (e.code() != asio::error::eof && e.code() != asio::error::connection_reset) {
            LOGERR("Network error with {}: {}", peer_addr, e.what());
        }
    } catch (const std::exception& e) {
        LOGERR("Exception with {}: {}", peer_addr, e.what());
    }   
}

asio::awaitable<void> PeerLogic::message_loop(std::shared_ptr<PeerConnection> conn) {
    auto self = shared_from_this();
    try {
        while (true) {
            std::vector<char> msg = co_await conn->socket.receive_message();
            if (msg.empty()) {
                // LOGDBG("Received keep-alive from {}", conn->peer_id);
                continue ;
            }


            // LOGDBG("Received raw message of size {} from {}. First byte is: {:#04x}",
            //          msg.size(), conn->peer_id, static_cast<uint8_t>(msg[0]));

            MessageType type = static_cast<MessageType>(msg[0]);
            
            if (type == MessageType::Request) {
                // LOGDBG("Received REQUEST from {}", conn->peer_id);
                asio::co_spawn(io_context_, 
                    handle_request_message(conn, {msg.data() + 1, msg.size() - 1}), 
                    asio::detached);
                continue;
            }
            
            std::span<const char> payload(msg.data() + 1, msg.size() - 1);

            co_await asio::dispatch(strand_, asio::use_awaitable);

            switch (type) {
                case MessageType::Choke:
                    // LOGDBG("Received CHOKE from {}", conn->peer_id);
                    conn->peer_is_choking = true;
                    break;
                case MessageType::Unchoke:
                    // LOGDBG("Received UNCHOKE from {}", conn->peer_id);    
                    conn->peer_is_choking =false;
                    piece_request_trigger_.cancel_one();
                    break;
                case MessageType::Interested:
                    // LOGDBG("Received INTERESTED from {}", conn->peer_id);    
                    conn->peer_is_interested = true;
                    break;
                case MessageType::NotInterested:
                    // LOGDBG("Received NOT INTERESTED from {}", conn->peer_id);    
                    conn->peer_is_interested =false;
                    break;
                case MessageType::Bitfield: {
                    // LOGDBG("Received BITFIELD message from {}", conn->peer_id);
                    
                    size_t expected_bitfield_size = (piece_status_.size() + 7) / 8;
                    if (payload.size() != expected_bitfield_size) {
                        LOGWARN("Received bitfield of incorrect size. Expected {}, got {}. Dropping connection.",
                                expected_bitfield_size, payload.size());
                        conn->socket.close();
                        co_return;
                    }

                    conn->bitfield.assign(payload.begin(), payload.end());

                    for (size_t i = 0; i < piece_status_.size(); ++i) {
                        if (conn->has_piece(i)) {
                            ++piece_availability_[i];
                        }
                    }
                    
                    bool should_be_interested = false;
                    for (size_t i = 0; i < piece_status_.size(); ++i) {
                        if (piece_status_[i] == PieceStatus::Needed && conn->has_piece(i)) {
                            should_be_interested = true;
                            break;
                        }
                    }

                    if (should_be_interested && !conn->am_interested) {
                        conn->am_interested = true;
                        asio::co_spawn(io_context_, [this, conn] () -> asio::awaitable<void> {
                            co_await send_simple_message(*conn, MessageType::Interested);
                        }, asio::detached);
                    }

                    piece_request_trigger_.cancel_one();

                    break;
                }
                case MessageType::Have: {
                    if (payload.size() < 4) {
                        break ;
                    } 
                    BufferReader reader(payload);
                    uint32_t index = asio::detail::socket_ops::network_to_host_long(reader.read<uint32_t>());

                    // LOGDBG("Received HAVE for piece {} from {}", index, conn->peer_id);

                    co_await asio::dispatch(strand_, asio::use_awaitable);

                    if (index < conn->bitfield.size()) {
                        conn->set_has_piece(index);
                        ++piece_availability_[index];
                    }

                    if (piece_status_[index] == PieceStatus::Needed && !conn->am_interested) {
                        conn->am_interested = true;
                        asio::co_spawn(io_context_, [this, conn] () -> asio::awaitable<void> {
                            co_await send_simple_message(*conn, MessageType::Interested);
                        }, asio::detached);
                    }

                    piece_request_trigger_.cancel_one();

                    break;
                }
                case MessageType::Piece: {
                    co_await handle_piece_message(conn, payload);
                    break;
                }
                case MessageType::Cancel: {
                    // LOGDBG("Received CANCEL from {}", conn->peer_id);
                    break;
                }
                default:
                    LOGWARN("Received unhandled message type: {}", static_cast<int>(type));
            }
        }
    } catch (const std::exception& e) {
        LOGINFO("Connection to {} lost or closed: {}", conn->peer_id, e.what());
    }


    co_await asio::dispatch(strand_, asio::use_awaitable);
    if (active_connections_.erase(conn->peer_id)) {
        LOGINFO("Cleaned up connection to {}. Updating piece availability.", conn->peer_id);
        for (size_t i = 0; i < piece_status_.size(); ++i) {
            if (conn->has_piece(i)) {
                if (piece_availability_[i] > 0) {
                    --piece_availability_[i];
                }
            }
        }
    }  
}

asio::awaitable<void> PeerLogic::choke_loop() {
    auto self = shared_from_this();
    asio::steady_timer timer(io_context_);
    std::mt19937 rng(std::chrono::high_resolution_clock::now().time_since_epoch().count());

    std::shared_ptr<PeerConnection> optimistically_unchoked_peer = nullptr;

    while (!is_download_complete_) {
        timer.expires_after(std::chrono::seconds(10));
        co_await timer.async_wait(asio::use_awaitable);

        co_await asio::dispatch(strand_, asio::use_awaitable);

        ++choke_loop_counter_;

        std::vector<std::shared_ptr<PeerConnection>> interested_peers;
        for (auto const& [id, conn] : active_connections_) {
            if (conn->peer_is_interested) {
                interested_peers.push_back(conn);
            }
        }

        std::sort(interested_peers.begin(), interested_peers.end(), 
            [] (const auto& a, const auto& b) {
                return a->bytes_uploaded.load() > b->bytes_uploaded.load();
            }
        );

        const int unchoke_slots = 4;
        std::vector<std::shared_ptr<PeerConnection>> unchoked_this_round;

        for (size_t i = 0; i < interested_peers.size() && unchoked_this_round.size() < unchoke_slots - 1; ++i) {
            auto& conn = interested_peers[i];
            if (conn->am_choking) {
                LOGDBG("Unchoking fast peer {} (uploaded {} bytes)", conn->peer_id, conn->bytes_uploaded.load());
                co_await send_simple_message(*conn, MessageType::Unchoke);
                conn->am_choking = false;
            }
            unchoked_this_round.push_back(conn);
        }

        if (choke_loop_counter_ % 3 == 0) {
            if (optimistically_unchoked_peer && optimistically_unchoked_peer->am_choking == false) {
                bool is_top_peer = false;
                for (const auto& top_peer : unchoked_this_round) {
                    if (top_peer->peer_id == optimistically_unchoked_peer->peer_id) {
                        is_top_peer = true;
                        break ;
                    }
                }
                if (!is_top_peer) {
                    LOGINFO("Re-choking previous optimistic peer {}", optimistically_unchoked_peer->peer_id);
                    co_await send_simple_message(*optimistically_unchoked_peer, MessageType::Choke);
                    optimistically_unchoked_peer->am_choking = true;
                }
            }

            std::vector<std::shared_ptr<PeerConnection>> candidates;
            for (const auto& peer: interested_peers) {
                if (peer->am_choking) {
                    candidates.push_back(peer);
                }
            }

            if (!candidates.empty()) {
                std::uniform_int_distribution<size_t> dist(0, candidates.size() - 1);
                auto& new_optimistic_peer = candidates[dist(rng)];

                LOGINFO("Optimistically unchoking peer {}", new_optimistic_peer->peer_id);
                co_await send_simple_message(*new_optimistic_peer, MessageType::Unchoke);
                new_optimistic_peer->am_choking = false;

                optimistically_unchoked_peer = new_optimistic_peer;
                unchoked_this_round.push_back(new_optimistic_peer);
            }
        } else if (optimistically_unchoked_peer) {
            unchoked_this_round.push_back(optimistically_unchoked_peer);
        }

        for (const auto& peer : interested_peers) {
            bool should_be_unchoked = false;
            for (const auto& unchoked_peer : unchoked_this_round) {
                if (unchoked_peer->peer_id == peer->peer_id) {
                    should_be_unchoked = true;
                    break ;
                }
            }
            if (!should_be_unchoked && !peer->am_choking) {
                LOGDBG("Choking slow/non-optimistic peer {}", peer->peer_id);
                co_await send_simple_message(*peer, MessageType::Choke);
                peer->am_choking = true;
            }
        }

        for (auto const& [id, conn] : active_connections_) {
            conn->bytes_uploaded = 0;
        }
    }
}

asio::awaitable<void> PeerLogic::downloader_loop() {
    auto self = shared_from_this();
    asio::steady_timer timer(io_context_);

    const int max_in_progress_pieces = 5;

    while (!is_download_complete_) {
        int slots_to_fill = 0;
        int needed_pieces_count = 0;

        co_await asio::dispatch(strand_, asio::use_awaitable);
        {
            for (const auto& status : piece_status_) {
                if (status == PieceStatus::Needed) {
                    needed_pieces_count++;
                }
            }

            int current_in_progress = in_progress_pieces_.size();
            if (current_in_progress < max_in_progress_pieces) {
                slots_to_fill = std::min(max_in_progress_pieces - current_in_progress, needed_pieces_count);
            }
        }

        if (slots_to_fill > 0) {
            // LOGDBG("Downloader has {} slots to fill. Spawning {} request tasks.", slots_to_fill, slots_to_fill);
            for (int i = 0; i < slots_to_fill; ++i) {
                asio::co_spawn(io_context_, request_one_piece_loop(), asio::detached);
            }
        }

        piece_request_trigger_.expires_at(asio::steady_timer::time_point::max());
        try {
            co_await piece_request_trigger_.async_wait(asio::use_awaitable);
        } catch (const boost::system::system_error& e) {
            if (e.code() != asio::error::operation_aborted) {
                throw ;
            }
        }
    }
}

asio::awaitable<void> PeerLogic::request_one_piece_loop() {
    auto self = shared_from_this();

    int piece_index = -1;
    std::vector<std::shared_ptr<PeerConnection>> available_peers;
    // uint32_t min_availability = UINT32_MAX;

    const auto& t_info = meta_info_.get_torrent_info();
    const size_t num_pieces = t_info.pieces.length() / 20;

    struct CandidatePiece {
        int index;
        uint32_t availability;
    };
    std::vector<CandidatePiece> candidates;

    co_await asio::dispatch(strand_, asio::use_awaitable);
    {
        for (size_t i = 0; i < piece_status_.size(); ++i) {
            if (piece_status_[i] == PieceStatus::Needed) {
                bool is_available = false;
                for (const auto& [id, conn] : active_connections_) {
                    if (!conn->peer_is_choking && conn->has_piece(i) && conn->am_interested) {
                        is_available = true;
                        break;
                    }
                }
                if (is_available) {
                    candidates.push_back({static_cast<int>(i), piece_availability_[i]});
                }
            }
        }

        if (candidates.empty()) {
            LOGDBG("No available pieces to download from any connected peer right now.");
            co_return ;
        }

        std::sort(candidates.begin(), candidates.end(), [] (const auto& a, const auto& b) {
            return a.availability < b.availability;
        });

        piece_index = candidates.front().index;
        uint64_t piece_size;
        if (static_cast<uint64_t>(piece_index) == num_pieces - 1) {
            uint64_t last_piece_size = t_info.total_size % t_info.piece_size;
             if (last_piece_size == 0) { // If total size is a multiple of piece size
                piece_size = t_info.piece_size;
            } else {
                piece_size = last_piece_size;
            }
        } else {
            piece_size = t_info.piece_size;
        }

        in_progress_pieces_.emplace(piece_index, InProgressPiece(piece_size));
        piece_status_[piece_index] = PieceStatus::InProgress;

        // LOGDBG("Selected rarest piece {} (size: {}, availability: {}). Starting download.", 
        //     piece_index, piece_size, candidates.front().availability);

        
        for (const auto& [id, conn] : active_connections_) {
            if (!conn->peer_is_choking && conn->has_piece(piece_index)) {
                available_peers.push_back(conn);
            }
        }

        if (available_peers.empty()) {
            LOGWARN("No available peers for piece {}. Returning to queue.", piece_index);
            piece_status_[piece_index] = PieceStatus::Needed;
            in_progress_pieces_.erase(piece_index);
            co_return ;
        }
    }

    auto& piece_progress = in_progress_pieces_.at(piece_index);
    uint32_t num_blocks = (piece_progress.data.size() + BLOCK_SIZE - 1) / BLOCK_SIZE;
    for (uint32_t block_idx = 0; block_idx < num_blocks; ++block_idx) {
        uint32_t offset = block_idx * BLOCK_SIZE;
        uint32_t length = (block_idx == num_blocks - 1) 
            ? (piece_progress.data.size() - offset)
            : BLOCK_SIZE;
        auto& peer_conn = available_peers[block_idx % available_peers.size()];
        co_await send_request_message(*peer_conn, piece_index, offset, length);
    
        piece_progress.outstanding_requests[block_idx].push_back(peer_conn->peer_id);
    }
    
    co_await check_and_enter_endgame();
}

asio::awaitable<void> PeerLogic::handle_piece_message(std::shared_ptr<PeerConnection> conn, std::span<const char> payload) {
    if (payload.size() < 8) {
        co_return ;
    }

    BufferReader reader(payload);
    uint32_t index = asio::detail::socket_ops::network_to_host_long(reader.read<uint32_t>());
    uint32_t begin = asio::detail::socket_ops::network_to_host_long(reader.read<uint32_t>());
    auto block = reader.read_bytes(reader.remaining());

    co_await await_download_tokens(block.size());

    // LOGDBG("Received block for piece {}, offset {}, from peer {}", index, begin, conn->peer_id);

    conn->bytes_downloaded += block.size();

    co_await asio::dispatch(strand_, asio::use_awaitable);

    if (piece_status_[index] == PieceStatus::Have) {
        LOGINFO("Received block for piece {} which is already complete. Sending CANCEL.", index);
        co_await send_cancel_message(*conn, index, begin, block.size());
        co_return;
    }

    auto it = in_progress_pieces_.find(index);
    if (it != in_progress_pieces_.end()) {
        uint32_t block_index = begin / BLOCK_SIZE;
        
        if (block_index < it->second.blocks_received.size() && !it->second.blocks_received[block_index]) {
            if (is_in_endgame_mode_.load()) {
                co_await send_cancel_for_block(index, block_index, conn->peer_id);
            }
            
            it = in_progress_pieces_.find(index);
            if (it == in_progress_pieces_.end() || it->second.blocks_received[block_index]) {
                co_return ;
            }

            auto& p = it->second;
            std::copy(block.begin(), block.end(), p.data.data() + begin);
            p.blocks_received[block_index] = true;
            ++p.received_count;

            if (p.received_count == p.total_blocks) {
                std::string received_hash_bytes = Crypto::calculate_sha1_hash_data(p.data);
                std::string expected_hash_bytes = meta_info_.get_torrent_info().pieces.substr(index * 20, 20);

                if (received_hash_bytes == expected_hash_bytes) {
                    co_await handle_completed_piece(index, p.data);
                } else {
                    LOGERR("Hash mismatch for piece {}. Returning to queue.", index);
                    co_await return_piece_to_queue(index);
                }
                in_progress_pieces_.erase(it);
            }
        }
    }
}

asio::awaitable<void> PeerLogic::handle_completed_piece(int piece_index, const std::vector<char>& piece_data) {
    const auto& info = meta_info_.get_torrent_info();
    uint64_t offset = static_cast<uint64_t>(piece_index) * info.piece_size;
    const size_t num_pieces = info.pieces.length() / 20;

    uint64_t current_file_offset = 0;
    uint32_t written_from_piece = 0;

    for (const auto& file_info : info.files) {
        if (offset < current_file_offset + file_info.size && current_file_offset < offset + piece_data.size()) {
            uint64_t write_pos_in_file = 0;
            if (offset > current_file_offset) {
                write_pos_in_file = offset - current_file_offset;
            }

            uint32_t bytes_to_write_to_this_file = std::min(static_cast<uint64_t>(piece_data.size() - written_from_piece), file_info.size - write_pos_in_file);

            std::vector<char> sub_data(bytes_to_write_to_this_file);
            std::copy(piece_data.begin() + written_from_piece, piece_data.begin() + written_from_piece + bytes_to_write_to_this_file, sub_data.begin());

            std::filesystem::path full_file_path = get_full_path_for_file(file_info);

            try {
                co_await async_write_to_file(full_file_path, write_pos_in_file, sub_data);
            } catch (const std::exception& e) {
                LOGCRITICAL("Failed to write piece {} to disk (file {}): {}", piece_index, full_file_path.string(), e.what());
                co_return; 
            }

            written_from_piece += bytes_to_write_to_this_file;
        }
        current_file_offset += file_info.size;
        if (written_from_piece == piece_data.size()) {
            break ;
        }
    }

    co_await asio::dispatch(strand_, asio::use_awaitable);
    piece_status_[piece_index] = PieceStatus::Have;
    ++pieces_done_count_;

    piece_request_trigger_.cancel_one();

    float progress = (static_cast<float>(pieces_done_count_) / num_pieces) * 100.0f;
    LOGINFO("Piece {} downloaded and verified. Progress: {:.2f}% ({}/{})", 
            piece_index, progress, pieces_done_count_.load(), num_pieces);

    co_await send_have_message_to_all(piece_index);

    if (pieces_done_count_ == num_pieces) {
        is_download_complete_ = true;
        LOGINFO("🎉 Download complete! File saved to {}", data_file_path_.string());
        LOGINFO("Closing all peer connections...");
        co_await asio::dispatch(strand_, asio::use_awaitable);
        for (const auto& [id, conn] : active_connections_) {
            conn->socket.close();
        }
        completion_timer_.cancel();
    }
}

asio::awaitable<void> PeerLogic::return_piece_to_queue(int piece_index) {
    piece_status_[piece_index] = PieceStatus::Needed;
    piece_request_trigger_.cancel_one();
    co_return ;
}

asio::awaitable<void> PeerLogic::handle_request_message(std::shared_ptr<PeerConnection> conn, std::span<const char> payload) {
    auto self = shared_from_this();

    const auto& info = meta_info_.get_torrent_info();

    RequestPayload req;
    try {
        req = RequestPayload::deserialize(payload);
    } catch (const std::exception& e) {
        LOGERR("Failed to deserialize REQUEST payload: {}", e.what());
        co_return;
    }
 
    bool should_respond = false;
    co_await asio::dispatch(strand_, asio::use_awaitable);
    if (!conn->am_choking && conn->peer_is_interested) {
        should_respond = true;
    }
 
    if (!should_respond) {
        LOGWARN("Ignoring REQUEST from peer {} because state is not met (am_choking: {}, peer_is_interested: {})",
                conn->peer_id, conn->am_choking, conn->peer_is_interested);
        co_return;
    }
    

    // LOGINFO("Peer {} requested piece {}, offset {}, length {}", conn->peer_id, req.index, req.begin, req.length);

    std::vector<char> block_data;
    try {
        block_data.resize(req.length);
        uint64_t file_offset = static_cast<uint64_t>(req.index) * info.piece_size + req.begin;

        uint64_t current_file_offset = 0;
        uint32_t read_for_block = 0;

        for (const auto& file_info : info.files) {
            if (file_offset + read_for_block < current_file_offset + file_info.size && current_file_offset < file_offset + req.length) {
                uint64_t read_pos_in_file = 0;
                if (file_offset + read_for_block > current_file_offset) {
                    read_pos_in_file = (file_offset + read_for_block) - current_file_offset;
                }

                uint32_t bytes_to_read_from_this_file = std::min(static_cast<uint64_t>(req.length - read_for_block), file_info.size - read_pos_in_file);

                std::filesystem::path full_file_path = get_full_path_for_file(file_info);
                auto file_data_part = co_await async_read_from_file(full_file_path, read_pos_in_file, bytes_to_read_from_this_file);
                std::copy(file_data_part.begin(), file_data_part.end(), block_data.begin() + read_for_block);
                read_for_block += bytes_to_read_from_this_file;
            }
            current_file_offset += file_info.size;
            if (read_for_block == req.length) {
                break ;
            }
        }
    } catch (const std::exception& e) {
        LOGERR("File read error for request: {}", e.what());
        co_return;
    }

    co_await await_upload_tokens(block_data.size());
    
    std::vector<char> piece_msg;
    piece_msg.push_back(static_cast<char>(MessageType::Piece));
    BufferWriter writer(piece_msg);
    writer.write(asio::detail::socket_ops::host_to_network_long(req.index));
    writer.write(asio::detail::socket_ops::host_to_network_long(req.begin));
    writer.write_bytes(block_data.data(), block_data.size());
 
    try {
        co_await conn->socket.send_message(piece_msg);
    } catch (const std::exception& e) {
        LOGWARN("Failed to send PIECE to {}: {}", conn->peer_id, e.what());
        co_return;
    }
 
    co_await asio::dispatch(strand_, asio::use_awaitable);
    conn->bytes_uploaded += req.length;
}

asio::awaitable<void> PeerLogic::keep_alive_loop(std::shared_ptr<PeerConnection> conn) {
    auto self = shared_from_this();
    asio::steady_timer timer(io_context_);

    while (true) {
        timer.expires_after(std::chrono::minutes(2));
        auto [ec] = co_await timer.async_wait(asio::as_tuple(asio::use_awaitable));
        if (ec) {
            co_return ;
        }

        co_await asio::dispatch(strand_, asio::use_awaitable);
        if (!active_connections_.count(conn->peer_id)) {
            co_return ;
        }

        try {
            co_await conn->socket.send_message({});
        } catch(...) {
            LOGWARN("Failed to send keep-alive to peer {}, closing connection.", conn->peer_id);
            conn->socket.close(); 
            co_return;
        }
    }
}

asio::awaitable<void> PeerLogic::send_have_message_to_all(uint32_t piece_index) {
    std::vector<char> have_msg;
    have_msg.push_back(static_cast<char>(MessageType::Have));
    BufferWriter writer(have_msg);
    writer.write(asio::detail::socket_ops::host_to_network_long(piece_index));

    for (const auto& [id, conn] : active_connections_) {
        co_await conn->socket.send_message(have_msg);
    }
}

asio::awaitable<void> PeerLogic::send_request_message(PeerConnection& conn, uint32_t index, uint32_t begin, uint32_t length) {
    std::vector<char> msg_body;
    msg_body.push_back(static_cast<char>(MessageType::Request));
    auto payload = RequestPayload::serialize(index, begin, length);
    msg_body.insert(msg_body.end(), payload.begin(), payload.end());

    // LOGDBG("REQUEST msg[0] raw byte value: {:#04x}", static_cast<uint8_t>(msg_body[0]));

    co_await conn.socket.send_message(msg_body);
}

asio::awaitable<void> PeerLogic::send_cancel_message(PeerConnection& conn, uint32_t index, uint32_t begin, uint32_t length) {
    std::vector<char> msg_body;
    msg_body.push_back(static_cast<char>(MessageType::Cancel));
    auto payload = RequestPayload::serialize(index, begin, length);
    msg_body.insert(msg_body.end(), payload.begin(), payload.end());
    
    // LOGDBG("Sending CANCEL for piece {}, offset {} to {}", index, begin, conn.peer_id);
    co_await conn.socket.send_message(msg_body);
}

asio::awaitable<void> PeerLogic::send_simple_message(PeerConnection& conn, MessageType type) {
    std::vector<char> msg_body;
    msg_body.push_back(static_cast<char>(type));

    // LOGDBG("Sending simple message of type {}. Raw byte value: {:#04x}", 
    //          static_cast<int>(type), static_cast<uint8_t>(msg_body[0]));

    co_await conn.socket.send_message(msg_body);
}

ThreadPool& PeerLogic::get_file_io_pool() {
    static ThreadPool instance(4);
    return instance;
}

asio::awaitable<void> PeerLogic::async_write_to_file(std::filesystem::path path, uint64_t offset, const std::vector<char>& data) {
    auto token = asio::use_awaitable;
    co_await asio::async_initiate<void(std::error_code)>(
        [this, path = std::move(path), offset, &data] (auto&& completion_handler) {
            file_io_pool_.enqueue([
                path = std::move(path),
                offset,
                &data,
                handler = std::move(completion_handler)
            ] () mutable {
                try {
                    std::fstream output_file(path, std::ios::in | std::ios::out | std::ios::binary);
                    if (!output_file) {
                        throw std::runtime_error("Failed to open file for writing: " + path.string());
                    }
                    output_file.seekp(offset);
                    output_file.write(data.data(), data.size());
                    output_file.close();
                    
                    std::move(handler)(std::error_code{});
                } catch (const std::exception& e) {
                    LOGERR("File write error: {}", e.what());
                    std::move(handler)(make_error_code(std::errc::io_error));
                }
            });
        },
        token
    );
}

asio::awaitable<std::vector<char>> PeerLogic::async_read_from_file(std::filesystem::path path, uint64_t offset, uint32_t size) {
    auto token = asio::use_awaitable;
    // 1. co_await the operation and capture its result (the tuple)
    auto [ec, block_data] = co_await asio::async_initiate<void(std::error_code, std::vector<char>)>(
        [this, path = std::move(path), offset, size](auto&& completion_handler) {
            file_io_pool_.enqueue([
                path = std::move(path),
                offset,
                size,
                handler = std::move(completion_handler)
            ] () mutable {
                try {
                    std::ifstream data_file(path, std::ios::binary);
                    if (!data_file) {
                        throw std::runtime_error("Failed to open file for reading: " + path.string());
                    }
                    std::vector<char> buffer(size);
                    data_file.seekg(offset);
                    data_file.read(buffer.data(), size);
                    if (static_cast<std::size_t>(data_file.gcount()) != size) {
                        throw std::runtime_error("Incomplete read from file: " + path.string());
                    }
                    
                    std::move(handler)(std::error_code{}, std::move(buffer));
                } catch (const std::exception& e) {
                    LOGERR("File read error: {}", e.what());
                    std::move(handler)(make_error_code(std::errc::io_error), std::vector<char>{});
                }
            });
        },
        token
    );
    // 2. Check the error code from the tuple. If it's set, throw an exception.
    if (ec) {
        throw boost::system::system_error(ec, "async_read_from_file");
    }
    // 3. On success, co_return ONLY the value part of the tuple.
    // This correctly satisfies the function's awaitable<std::vector<char>> return type.
    co_return block_data;
}


asio::awaitable<void> PeerLogic::verify_existing_file() {
    const auto& info = meta_info_.get_torrent_info();
    const size_t num_pieces = info.pieces.length() / 20;

    LOGINFO("Verifying existing file: {}", data_file_path_.string());

    for (size_t i = 0; i < num_pieces; ++i) {
        uint64_t offset = static_cast<uint64_t>(i) * info.piece_size;
        uint32_t piece_size_to_read = info.piece_size;

        if (i == num_pieces - 1) {
            uint64_t last_piece_size = info.total_size % info.piece_size;
            if (last_piece_size != 0) {
                piece_size_to_read = last_piece_size;
            } else {
                piece_size_to_read = info.piece_size;
            }
        }

        std::vector<char> piece_data(piece_size_to_read);
        try {
            uint64_t current_file_offset = 0;
            uint32_t read_for_piece = 0;

            for (const auto& file_info : info.files) {
                if (offset + read_for_piece < current_file_offset + file_info.size && current_file_offset < offset + piece_size_to_read) {
                    uint64_t read_pos_in_file = 0;
                    if (offset + read_for_piece > current_file_offset) {
                        read_pos_in_file = (offset + read_for_piece) - current_file_offset;
                    }

                    uint32_t bytes_to_read_from_this_file = std::min(static_cast<uint64_t>(piece_size_to_read - read_for_piece), file_info.size - read_pos_in_file);

                    std::filesystem::path full_file_path = get_full_path_for_file(file_info);
                    if (!std::filesystem::exists(full_file_path)) {
                        break ;
                    }

                    auto file_data_part = co_await async_read_from_file(full_file_path, read_pos_in_file, bytes_to_read_from_this_file);
                    std::copy(file_data_part.begin(), file_data_part.end(), piece_data.begin() + read_for_piece);
                    read_for_piece += bytes_to_read_from_this_file;
                }
                current_file_offset += file_info.size;
                if (read_for_piece == piece_size_to_read) {
                    break ;
                }
            }

            if (read_for_piece != piece_size_to_read) {
                continue ;
            }

            std::string actual_hash_bytes = Crypto::calculate_sha1_hash_data(piece_data);
            std::string expected_hash_bytes = info.pieces.substr(i * 20, 20);

            if (actual_hash_bytes == expected_hash_bytes) {
                co_await asio::dispatch(strand_, asio::use_awaitable);
                piece_status_[i] = PieceStatus::Have;
                ++pieces_done_count_;
            }

        } catch (const std::exception& e) {
            LOGWARN("Could not verify piece {}: {}. Will download it.", i, e.what());
        }
    }

    co_await asio::dispatch(strand_, asio::use_awaitable);
    if (pieces_done_count_ > 0) {
        float progress = (static_cast<float>(pieces_done_count_) / num_pieces) * 100.0f;
        LOGINFO("Verification complete. Found {}/{} valid pieces ({:.2f}% progress).", 
                pieces_done_count_.load(), num_pieces, progress);
    }
}


asio::awaitable<bool> PeerLogic::verify_seed_data() {
    const auto& info = meta_info_.get_torrent_info();
    LOGINFO("Verifying seeder data for '{}'...", info.name);

    uint64_t discovered_total_size = 0;
    for (const auto& file_info : info.files) {
        std::filesystem::path full_file_path = get_full_path_for_file(file_info);
        if (!std::filesystem::exists(full_file_path)) {
            LOGCRITICAL("Seeder file missing: {}", full_file_path.string());
            co_return false;
        }

        discovered_total_size += std::filesystem::file_size(full_file_path);
    }

    if (discovered_total_size != info.total_size) {
        LOGCRITICAL("Seeder file size mismatch. Expected {}, found {}.", 
                    info.total_size, discovered_total_size);
        co_return false;
    }

    const size_t num_pieces = info.pieces.length() / 20;

    for (size_t i = 0; i < num_pieces; ++i) {
        uint64_t offset = static_cast<uint64_t>(i) * info.piece_size;
        uint32_t piece_size_to_read = info.piece_size;

        if (i == num_pieces - 1) {
            uint64_t last_piece_size = info.total_size % info.piece_size;
            if (last_piece_size != 0) {
                piece_size_to_read = last_piece_size;
            } else {
                piece_size_to_read = info.piece_size;
            }
        }

        std::vector<char> piece_data(piece_size_to_read);
        try {
            uint64_t current_file_offset = 0;
            uint32_t read_for_piece = 0;

            for (const auto& file_info : info.files) {
                if (offset + read_for_piece < current_file_offset + file_info.size && current_file_offset < offset + piece_size_to_read) {
                    uint64_t read_pos_in_file = 0;
                    if (offset + read_for_piece > current_file_offset) {
                        read_pos_in_file = (offset + read_for_piece) - current_file_offset;
                    }

                    uint32_t bytes_to_read_from_this_file = std::min(static_cast<uint64_t>(piece_size_to_read - read_for_piece), file_info.size - read_pos_in_file);

                    std::filesystem::path full_file_path = get_full_path_for_file(file_info);
                    if (!std::filesystem::exists(full_file_path)) {
                        LOGCRITICAL("File missing: {}", full_file_path.string());
                        co_return false;
                    }

                    auto file_data_part = co_await async_read_from_file(full_file_path, read_pos_in_file, bytes_to_read_from_this_file);
                    std::copy(file_data_part.begin(), file_data_part.end(), piece_data.begin() + read_for_piece);
                    read_for_piece += bytes_to_read_from_this_file;
                }
                current_file_offset += file_info.size;
                if (read_for_piece == piece_size_to_read) {
                    break ;
                }
            }

            if (read_for_piece != piece_size_to_read) {
                LOGCRITICAL("File missing or piece incomplete");
                co_return false;
            }

            std::string actual_hash_bytes = Crypto::calculate_sha1_hash_data(piece_data);
            std::string expected_hash_bytes = info.pieces.substr(i * 20, 20);

            if (actual_hash_bytes != expected_hash_bytes) {
                LOGCRITICAL("Hash mismatch for piece {}! File is corrupt. Aborting.", i);
                co_return false;
            }
        } catch (const std::exception& e) {
            LOGCRITICAL("Error when verifying data: {}", e.what());
            co_return false;
        }
    }    

    LOGINFO("Seeder data verified successfully.");
    co_return true;
}

asio::awaitable<void> PeerLogic::token_refill_loop() {
    auto self = shared_from_this();
    asio::steady_timer timer(io_context_);
    const auto refill_interval = std::chrono::milliseconds(100);

    while (true) {
        timer.expires_after(refill_interval);
        co_await timer.async_wait(asio::use_awaitable);

        if (is_download_complete_ && pieces_done_count_ > 0) {
            if (active_connections_.empty()) {
                continue ;
            }
        }

        int64_t upload_refill = UPLOAD_RATE_BPS_ * refill_interval.count() / 1000;
        int64_t download_refill = DOWNLOAD_RATE_BPS_ * refill_interval.count() / 1000;

        upload_tokens_.store(std::min(upload_tokens_.load() + upload_refill, UPLOAD_RATE_BPS_ * TOKEN_BUCKET_CAPACITY_FACTOR));
        download_tokens_.store(std::min(download_tokens_.load() + download_refill, DOWNLOAD_RATE_BPS_ * TOKEN_BUCKET_CAPACITY_FACTOR));
    }
}


asio::awaitable<void> PeerLogic::await_upload_tokens(size_t amount) {
    if (UPLOAD_RATE_BPS_ == 0) {
        co_return ;
    }

    asio::steady_timer timer(io_context_);
    while (upload_tokens_.load() < amount) {
        timer.expires_after(std::chrono::milliseconds(50));
        co_await timer.async_wait(asio::use_awaitable);
    }

    upload_tokens_ -= amount;
}

asio::awaitable<void> PeerLogic::await_download_tokens(size_t amount) {
    if (DOWNLOAD_RATE_BPS_ == 0) co_return; // Unlimited
    asio::steady_timer timer(io_context_);
    while (download_tokens_.load() < amount) {
        timer.expires_after(std::chrono::milliseconds(50));
        co_await timer.async_wait(asio::use_awaitable);
    }
    download_tokens_ -= amount;
}

asio::awaitable<void> PeerLogic::check_and_enter_endgame() {
    co_await asio::dispatch(strand_, asio::use_awaitable);

    if (is_in_endgame_mode_.load()) {
        co_return ;
    }

    for (size_t i = 0; i < piece_status_.size(); ++i) {
        if (piece_status_[i] == PieceStatus::Needed) {
            co_return ;
        }
    }

    LOGINFO("🎉 All pieces requested. Entering ENDGAME MODE. 🎉");
    is_in_endgame_mode_ = true;
    asio::co_spawn(io_context_, broadcast_outstanding_requests(), asio::detached);
}

asio::awaitable<void> PeerLogic::broadcast_outstanding_requests() {
    co_await asio::dispatch(strand_, asio::use_awaitable);

    LOGINFO("Endgame: Re-requesting all outstanding blocks from all unchoked peers.");

    const auto& info = meta_info_.get_torrent_info();
    const size_t num_pieces = info.pieces.length() / 20;

    for (auto& [piece_idx, piece_progress] : in_progress_pieces_) {
        for (uint32_t block_idx = 0; block_idx < piece_progress.total_blocks; ++block_idx) {
            if (!piece_progress.blocks_received[block_idx]) {
                uint32_t offset = block_idx * BLOCK_SIZE;
                uint64_t current_piece_size;
                 if (static_cast<uint64_t>(piece_idx) == num_pieces - 1) {
                    current_piece_size = info.total_size - (static_cast<uint64_t>(piece_idx) * info.piece_size);
                } else {
                    current_piece_size = info.piece_size;
                }
 
                uint32_t length = (offset + BLOCK_SIZE > current_piece_size)
                                ? (current_piece_size - offset)
                                : BLOCK_SIZE;
                
                for (const auto& [peer_id, conn] : active_connections_) {
                    if (!conn->peer_is_choking && conn->has_piece(piece_idx)) {
                        co_await send_request_message(*conn, piece_idx, offset, length);
                        piece_progress.outstanding_requests[block_idx].push_back(conn->peer_id);
                    }
                }
            }
        }
    }
}


asio::awaitable<void> PeerLogic::send_cancel_for_block(uint32_t piece_index, uint32_t block_index, const std::string& exclude_peer_id) {
    co_await asio::dispatch(strand_, asio::use_awaitable);

    auto it = in_progress_pieces_.find(piece_index);
    if (it == in_progress_pieces_.end() || block_index >= it->second.outstanding_requests.size()) {
        co_return ;
    }

    auto& requests_for_block = it->second.outstanding_requests[block_index];
    std::vector<std::string> peer_ids_to_cancel = requests_for_block;
    requests_for_block.clear();

    const auto& info = meta_info_.get_torrent_info();
    const size_t num_pieces = info.pieces.length() / 20;

    uint32_t offset = block_index * BLOCK_SIZE;
    uint64_t current_piece_size;
    if (static_cast<uint64_t>(piece_index) == num_pieces - 1) {
        current_piece_size = info.total_size - (static_cast<uint64_t>(piece_index) * info.piece_size);
    } else {
        current_piece_size = info.piece_size;
    }
 
    uint32_t length = (offset + BLOCK_SIZE > current_piece_size)
                    ? (current_piece_size - offset)
                    : BLOCK_SIZE;
    
    for (const auto& peer_id : peer_ids_to_cancel) {
        if (peer_id != exclude_peer_id) {
            auto conn_it = active_connections_.find(peer_id);
            if (conn_it != active_connections_.end()) {
                co_await send_cancel_message(*conn_it->second, piece_index, offset, length);
            }
        }
    }

}

asio::awaitable<void> PeerLogic::tracker_announce_loop() {
    auto self = shared_from_this();
    asio::steady_timer timer(io_context_);
    std::string event = "started";
    bool completed_event_sent = false;

    std::random_device rd;
    std::mt19937 g(rd());

    while (true) {
        if (is_download_complete_ && !is_seeder_()) {
            LOGINFO("Download complete. Stopping tracker announcements.");
            co_return;
        }

        const auto& info = meta_info_.get_torrent_info();
        const size_t num_pieces = info.pieces.length() / 20;

        uint64_t downloaded_bytes = pieces_done_count_.load() * info.piece_size;
        uint64_t left = is_seeder_() ? 0 : (num_pieces - pieces_done_count_) * info.piece_size;

        if (is_download_complete_ && !completed_event_sent && !is_seeder_()) {
            event = "completed";
            completed_event_sent = true;
        }

        AnnounceRequestParams params {
            .info_hash_bytes = info_hash_bytes_,
            .peer_id = my_peer_id_,
            .port = static_cast<uint16_t>(peer_port_),
            .uploaded = downloaded_bytes,
            .downloaded = pieces_done_count_ * info.piece_size,
            .left = left,
            .event = event
        };

        int interval = 1800;
        bool announce_successful = false;
        for (auto& tier : tracker_clients_by_tier_) {
            std::shuffle(tier.begin(), tier.end(), g);
            
            for (const auto& tracker_client : tier) {
                try {
                    LOGINFO("Announcing to tracker {} (event: '{}')...", tracker_client->get_url(), event);
                    auto result = co_await tracker_client->announce(params);
                    interval = result.interval_seconds;
                    announce_successful = true;
                    
                    if (!is_download_complete_) {
                        for (const auto& peer_addr : result.peers) {
                            bool already_connnected = false;
                            co_await asio::dispatch(strand_, asio::use_awaitable);
                            for (const auto& [id, conn] : active_connections_) {
                                if (conn->peer_addr == peer_addr) {
                                    already_connnected = true;
                                    break;
                                }
                            }
                            if (!already_connnected) {
                                asio::co_spawn(io_context_, connect_to_peer(peer_addr), asio::detached);
                            }
                        }
                    }

                    break;
                } catch (const std::exception& e) {
                    LOGERR("Failed to announce to tracker: {}. Retrying in {} seconds.", e.what(), interval);
                }
            }

            if (announce_successful) {
                break ;
            }
        }

        if (!announce_successful) {
            LOGERR("All trackers failed to respond. Retrying in {} seconds.", interval);
        }

        event = "";
 
        timer.expires_after(std::chrono::seconds(interval));
        co_await timer.async_wait(asio::use_awaitable);
    }
}


bool PeerLogic::is_seeder_() const {
    return dynamic_cast<const Seeder*>(this) != nullptr;
}

asio::awaitable<void> PeerLogic::connect_to_peer(std::string peer_addr) {
    try {
        size_t colon_pos = peer_addr.find(':');
        if (colon_pos == std::string::npos) {
            LOGWARN("Invalid peer address format: {}", peer_addr);
            co_return;
        }
        std::string ip = peer_addr.substr(0, colon_pos);
        int peer_port = std::stoi(peer_addr.substr(colon_pos+1));
 
        AsyncSocket peer_socket(asio::ip::tcp::socket{io_context_});
        co_await peer_socket.connect(ip, peer_port);
        LOGINFO("Successfully connected to peer {}", peer_addr);
 
        co_await handle_new_connection(std::move(peer_socket), peer_addr);
        
    } catch (const std::exception& e) {
        LOGWARN("Failed to connect or lost connection to peer {}: {}", peer_addr, e.what());
    }
}

std::filesystem::path PeerLogic::get_full_path_for_file(const FileInfo& file_info) const {
    const auto& info = meta_info_.get_torrent_info();
    std::filesystem::path full_path = data_file_path_;
    // Handle single-file torrents.
    // The data_file_path_ IS the full path to the file.
    if (info.files.size() == 1) {
        return full_path;
    }
    // Handle multi-file torrents.
    // The torrent's 'name' field is the root directory.
    // data_file_path_ is the directory WHERE this root directory is located.
    full_path /= info.name;
    full_path /= file_info.path;
    return full_path;
}

/*
#####################################################
            Seeder implementation
#####################################################
*/

Seeder::Seeder(asio::io_context& io_context, std::string peer_id, const std::filesystem::path& torrent_path, const std::filesystem::path& content_dir, 
                int peer_port, uint64_t upload_rate_bps)
    : PeerLogic(io_context, std::move(peer_id), torrent_path, peer_port, upload_rate_bps, 0), peer_port_(peer_port) {

    const auto& info = meta_info_.get_torrent_info();
    const size_t num_pieces = info.pieces.length() / 20;

    piece_status_.assign(num_pieces, PieceStatus::Have);
    pieces_done_count_ = num_pieces;
    is_download_complete_ = true;

    data_file_path_ = content_dir;

    if (info.files.size() > 1 && !std::filesystem::is_directory(data_file_path_)) {
        throw std::runtime_error("Content path for multi-file torrent must be a directory: " + data_file_path_.string());
    } else if (info.files.size() == 1 && !std::filesystem::is_regular_file(data_file_path_ / info.files[0].path)) {
        data_file_path_ /= info.files[0].path;
        if (!std::filesystem::exists(data_file_path_)) {
            throw std::runtime_error("Data file not found: " + data_file_path_.string());
        }
    }

    LOGINFO("Seeder initialized for '{}'", info.name);
}

asio::awaitable<void> Seeder::run() {

    bool is_ok = co_await verify_seed_data();
    if (!is_ok) {
        throw std::runtime_error("Seeder data verification failed. File may be corrupt.");
    }

    asio::co_spawn(io_context_, token_refill_loop(), asio::detached);
    asio::co_spawn(io_context_, tracker_announce_loop(), asio::detached);

    try {
        AsyncServerSocket seeder_server(io_context_, peer_port_);
        LOGINFO("Seeder ready and listening on port {}", peer_port_);
        while (true) {
            AsyncSocket leecher_socket = co_await seeder_server.accept();
            auto endpoint = leecher_socket.get_socket().remote_endpoint();
            std::string addr = endpoint.address().to_string() + ":" + std::to_string(endpoint.port());
            asio::co_spawn(io_context_, handle_new_connection(std::move(leecher_socket), addr), asio::detached);
        }
    } catch (const std::exception& e) {
        LOGCRITICAL("Seeder listening loop failed: {}", e.what());
    }
}


/*
#####################################################
            Leecher implementation
#####################################################
*/

Leecher::Leecher(private_key, asio::io_context& io_context, std::string peer_id, const std::filesystem::path& torrent_path, const std::filesystem::path& save_path,
                int peer_port,
                uint64_t upload_rate_bps, uint64_t download_rate_bps)
    : PeerLogic(io_context, std::move(peer_id), torrent_path, peer_port, upload_rate_bps, download_rate_bps) { 

    data_file_path_ = save_path;

    const auto& info = meta_info_.get_torrent_info();
    const size_t num_pieces = info.pieces.length() / 20;

    piece_status_.resize(num_pieces, PieceStatus::Needed);

    LOGINFO("Leecher initialized for '{}', {} pieces to download.", info.name, num_pieces);
}


asio::awaitable<bool> Leecher::async_init() {
    const auto& info = meta_info_.get_torrent_info();
    const size_t num_pieces = info.pieces.length() / 20;
    try {
        std::filesystem::path base_save_path = data_file_path_;
        
        // For a multi-file torrent, data_file_path_ is the parent directory.
        // We create a subdirectory named after the torrent.
        if (info.files.size() > 1) {
            // Check if the user gave a file path instead of a directory
            if (base_save_path.has_extension() || !std::filesystem::is_directory(base_save_path.parent_path())) {
                base_save_path = base_save_path.parent_path();
            }
        }

        for (const auto& file : info.files) {
            std::filesystem::path full_path = get_full_path_for_file(file);
            // Only create/truncate the file if it DOES NOT exist
            if (!std::filesystem::exists(full_path)) {
                LOGINFO("File {} does not exist. Pre-allocating...", full_path.string());
                if (full_path.has_parent_path()) {
                    std::filesystem::create_directories(full_path.parent_path());
                }
                // Now, truncating is safe because the file is new
                std::ofstream output_file(full_path, std::ios::binary | std::ios::trunc);
                if (file.size > 0) {
                    output_file.seekp(file.size - 1);
                    output_file.write("", 1);
                }
            }
        }

        co_await verify_existing_file();
        if (pieces_done_count_ == num_pieces) {
            is_download_complete_ = true;
            LOGINFO("File is already complete and verified. Nothing to download.");
            completion_timer_.cancel();
        }

    } catch (const std::exception& e) {
        LOGCRITICAL("Failed during async init (file creation/verification): {}", e.what());
        co_return false; // Signal failure
    }
    co_return true; // Signal success
}

asio::awaitable<std::shared_ptr<Leecher>> Leecher::create(
    asio::io_context& io_context, 
    std::string peer_id, 
    const std::filesystem::path& torrent_path, 
    const std::filesystem::path& save_path,
    int peer_port,
    uint64_t upload_rate_bps, 
    uint64_t download_rate_bps)
{
    auto leecher = std::make_shared<Leecher>(
        private_key{}, io_context, std::move(peer_id), torrent_path, 
        save_path, peer_port, upload_rate_bps, download_rate_bps
    );

    bool success = co_await leecher->async_init();
    if (!success) {
        // Return nullptr or throw to indicate failure
        co_return nullptr; 
    }

    co_return leecher;
}

asio::awaitable<bool> Leecher::run() {
    if (is_download_complete_) {
        co_return true;
    }

    asio::co_spawn(io_context_, token_refill_loop(), asio::detached);
    asio::co_spawn(io_context_, choke_loop(), asio::detached);
    asio::co_spawn(io_context_, downloader_loop(), asio::detached);
    asio::co_spawn(io_context_, tracker_announce_loop(), asio::detached);

    try {
        co_await completion_timer_.async_wait(asio::use_awaitable);
    } catch (const boost::system::system_error& e) {
        if (e.code() != asio::error::operation_aborted) {
            LOGERR("Completion timer failed with unexpected error: {}", e.what());
        }
    }

    co_return is_download_complete_;
}


