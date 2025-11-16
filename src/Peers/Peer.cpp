#include "Peers/Peer.hpp"
#include "Utils/Crypto.hpp"
#include "Utils/Logger.hpp"
#include "Utils/Bencode.hpp"
#include <algorithm>
#include <array>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/read_until.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <filesystem>
#include <format>
#include <fstream>
#include <ios>
#include <netinet/in.h>
#include <random>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>
#include <variant>
#include <vector>
#include <memory>

using namespace boost::asio::experimental::awaitable_operators;

PeerLogic::PeerLogic(asio::io_context& io_context, PeerId peer_id, const std::filesystem::path& torrent_path,
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
      peer_port_(peer_port),
      file_lock_manager_(std::make_shared<FileLockManager>()),
      upload_limiter_(io_context, upload_rate_bps),
      download_limiter_(io_context, download_rate_bps)
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

    const size_t num_pieces = meta_info_.get_torrent_info().pieces.size() / 20;
    piece_availability_.resize(num_pieces, 0);

    std::unordered_set<int> zero_rarity_pieces;
    for (size_t i = 0; i < num_pieces; ++i) {
        zero_rarity_pieces.insert(i);
    }
    if (!zero_rarity_pieces.empty()) {
        pieces_by_rarity_[0] = std::move(zero_rarity_pieces);
    }

    build_pieces_files_map();

    completion_timer_.expires_at(asio::steady_timer::time_point::max());
    piece_request_trigger_.expires_at(asio::steady_timer::time_point::max());

}

asio::awaitable<void> PeerLogic::handle_new_connection(AsyncSocket socket, std::string peer_addr) {
    auto conn = std::make_shared<PeerConnection>(std::move(socket));
    conn->peer_addr = peer_addr;
    try {
        LOGDBG("Starting handshake with peer {}", peer_addr);
        Handshake my_handshake {
            .info_hash_bytes = info_hash_bytes_, 
            .peer_id_bytes = my_peer_id_,
            .extended = true
        };
        co_await conn->socket.send_raw(my_handshake.serialize());

        std::vector<std::byte> handshake_buffer = co_await conn->socket.receive_raw(HANDSHAKE_BASE_LEN);
        Handshake peer_handshake = Handshake::deserialize(handshake_buffer);

        conn->peer_id = peer_handshake.peer_id_bytes;
        
        if (conn->peer_id == my_peer_id_) {
            LOGWARN("Connected to self. Dropping connection.");
            co_return;
        }
        
        if (peer_handshake.info_hash_bytes != info_hash_bytes_) {
            throw std::runtime_error("Info hash mismatch during handshake. Closing connection.");
        }
        
        LOGINFO("Handshake successful with peer ID: {}", conn->peer_id);

        if (peer_handshake.extended) {
            LOGINFO("Peer {} supports extended plugins", conn->peer_id);

            std::vector<std::byte> message;
            message.push_back(static_cast<std::byte>(MessageType::ExtendedMessage));
            message.push_back(static_cast<std::byte>(ExtendedMessageType::Handshake));
            
            Dict m_dict;
            m_dict["ut_pex"] = Value(static_cast<Integer>(1));
            m_dict["ut_metadata"] = Value(static_cast<Integer>(3));
            Dict ehs_dict;
            ehs_dict["m"] = Value(std::move(m_dict));
            ehs_dict["v"] = Value("My C++ Client 1.0");

            auto encoded_payload = encode(Value(ehs_dict));
            message.insert(message.end(), encoded_payload.begin(), encoded_payload.end());
            
            co_await conn->socket.send_message(message);
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
            std::vector<uint8_t> my_bitfield_data((piece_status_.size() + 7) / 8, 0);
            for (size_t i = 0; i < piece_status_.size(); ++i) {
                if (piece_status_[i] == PieceStatus::Have) {
                    my_bitfield_data[i/8] |= (1 << (7 - (i % 8)));
                }
            }
            
            std::vector<std::byte> bitfield_msg_body;
            bitfield_msg_body.push_back(static_cast<std::byte>(MessageType::Bitfield));
            bitfield_msg_body.insert(bitfield_msg_body.end(), reinterpret_cast<std::byte *>(my_bitfield_data.data()), reinterpret_cast<std::byte *>(my_bitfield_data.data()) + my_bitfield_data.size());
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
            std::vector<std::byte> msg = co_await conn->socket.receive_message();
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
            
            std::span<const std::byte> payload(msg.data() + 1, msg.size() - 1);

            co_await asio::dispatch(strand_, asio::use_awaitable);

            switch (type) {
                case MessageType::Choke:
                    // LOGDBG("Received CHOKE from {}", conn->peer_id);
                    conn->peer_is_choking = true;
                    break;
                case MessageType::Unchoke: {
                    // LOGDBG("Received UNCHOKE from {}", conn->peer_id);    
                    conn->peer_is_choking =false;

                    // If we're not interested but we need pieces from this peer, send INTERESTED
                    if (!conn->am_interested) {
                        bool has_needed_pieces = false;
                        for (size_t i = 0; i < piece_status_.size(); ++i) {
                            if ((piece_status_[i] == PieceStatus::Needed || piece_status_[i] == PieceStatus::InProgress) && 
                                conn->has_piece(i)) {
                                has_needed_pieces = true;
                                break;
                            }
                        }
                        
                        if (has_needed_pieces) {
                            conn->am_interested = true;
                            co_await send_simple_message(*conn, MessageType::Interested);
                        }
                    }

                    piece_request_trigger_.cancel_one();
                    break;
                }
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

                    conn->bitfield.assign(reinterpret_cast<const uint8_t *>(payload.data()), reinterpret_cast<const uint8_t *>(payload.data()) + payload.size());

                    for (size_t i = 0; i < piece_status_.size(); ++i) {
                        if (conn->has_piece(i)) {
                            if (piece_status_[i] == PieceStatus::Have) {
                                piece_availability_[i]++;
                                continue;
                            }

                            uint32_t old_rarity = piece_availability_[i];
                            uint32_t new_rarity = ++piece_availability_[i];
                            update_piece_rarity(i, old_rarity, new_rarity);
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
                        if (piece_status_[index] == PieceStatus::Have) {
                            piece_availability_[index]++;
                            continue;
                        }
                        uint32_t old_rarity = piece_availability_[index];
                        uint32_t new_rarity = ++piece_availability_[index];
                        update_piece_rarity(index, old_rarity, new_rarity);
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
                case MessageType::ExtendedMessage: {
                    // LOGDBG("Received ExtendedMessage from {}", conn->peer_id);
                    co_await handle_extended_message(conn, payload);
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
                    uint32_t old_rarity = piece_availability_[i];
                    uint32_t new_rarity = --piece_availability_[i];
                    update_piece_rarity(i, old_rarity, new_rarity);
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
    co_await asio::dispatch(strand_, asio::use_awaitable);

    // LOGDBG("Rarity map state:");
    // for (const auto& [rarity, piece_set] : pieces_by_rarity_) {
    //     LOGDBG("  Rarity {}: {} pieces", rarity, piece_set.size());
    // }

    // LOGDBG("Active connections: {}", active_connections_.size());
    // for (const auto& [id, conn] : active_connections_) {
    //     LOGDBG("  Peer {}: choking={}, interested={}, has_bitfield={}", 
    //            conn->peer_id, conn->peer_is_choking, conn->peer_is_interested, 
    //            !conn->bitfield.empty());
    // }

    // int needed_count = 0;
    // for (const auto& status : piece_status_) {
    //     if (status == PieceStatus::Needed) needed_count++;
    // }
    // LOGDBG("Total needed pieces: {}", needed_count);
    
    for (const auto& [rarity, piece_set] : pieces_by_rarity_) {
        if (rarity == 0) continue;  // We only care about pieces that are actually available (rarity > 0)

        // Create a shuffled list of candidates at this rarity level
        // Shuffling prevents multiple clients from requesting the same piece simultaneously
        std::vector<int> candidates(piece_set.begin(), piece_set.end());
        std::shuffle(
            candidates.begin(), candidates.end(), 
            std::mt19937{std::random_device{}()}
        );

        for (size_t piece_index : candidates) {
            if (try_piece_download(piece_index)) {
                co_return ;
            }
        }
    }

    LOGDBG("No available pieces to download, trying any needed pieces...");

    std::vector<size_t> all_needed_pieces;
    for (size_t i = 0; i < piece_status_.size(); ++i) {
        if (piece_status_[i] == PieceStatus::Needed) {
            all_needed_pieces.push_back(i);
        }
    }
    
    if (!all_needed_pieces.empty()) {
        std::shuffle(
            all_needed_pieces.begin(), all_needed_pieces.end(),
            std::mt19937{std::random_device{}()}
        );
        
        for (size_t piece_index : all_needed_pieces) {
            if (try_piece_download(piece_index)) {
                co_return;
            }
        }
    }

    LOGDBG("No available and needed pieces to download from any unchoked peer right now.");
}

bool PeerLogic::try_piece_download(size_t piece_index) {
    if (piece_status_[piece_index] != PieceStatus::Needed) {
        // LOGDBG("Piece {} is not needed (status: {})", piece_index, static_cast<int>(piece_status_[piece_index]));
        return false;
    }
    
    // Find an unchoked peer that has this piece
    std::vector<std::shared_ptr<PeerConnection>> available_peers;
    for (const auto& [id, conn] : active_connections_) {
        if (!conn->peer_is_choking && conn->has_piece(piece_index)) {
            available_peers.push_back(conn);
        }
        // if (!conn->peer_is_choking) {
        //     if (conn->has_piece(piece_index)) {
        //         available_peers.push_back(conn);
        //     } else {
        //         LOGDBG("Peer {} is unchoked but doesn't have piece {}", conn->peer_id, piece_index);
        //     }
        // } else {
        //     LOGDBG("Peer {} is choking us", conn->peer_id);
        // }
    }
    
    if (available_peers.empty()) {
        // LOGDBG("No available peers for piece {}. Total connections: {}", piece_index, active_connections_.size());
        return false;
    }
    
    // Start downloading this piece
    const auto& t_info = meta_info_.get_torrent_info();
    const size_t num_pieces = t_info.pieces.size() / 20;
    uint64_t piece_size = (piece_index == num_pieces - 1) 
        ? (t_info.total_size % t_info.piece_size ?: t_info.piece_size)
        : t_info.piece_size;
    
    piece_status_[piece_index] = PieceStatus::InProgress;
    
    // Update rarity map - remove from current rarity
    uint32_t current_rarity = piece_availability_[piece_index];
    update_piece_rarity(piece_index, current_rarity, -1);
    
    in_progress_pieces_.emplace(piece_index, InProgressPiece(piece_size));
    auto& piece_progress = in_progress_pieces_.at(piece_index);
    uint32_t num_blocks = piece_progress.total_blocks;
    
    // Request all blocks for this piece
    for (uint32_t block_idx = 0; block_idx < num_blocks; ++block_idx) {
        uint32_t offset = block_idx * BLOCK_SIZE;
        uint32_t length = (block_idx == num_blocks - 1) 
            ? (piece_progress.data.size() - offset)
            : BLOCK_SIZE;
        
        auto& peer_conn = available_peers[block_idx % available_peers.size()];
        asio::co_spawn(io_context_, 
            [this, peer_conn, piece_index, offset, length]() -> asio::awaitable<void> {
                co_await send_request_message(*peer_conn, piece_index, offset, length);
            }, 
            asio::detached
        );
        
        piece_progress.outstanding_requests[block_idx].push_back(peer_conn->peer_id);
    }
    
    asio::co_spawn(io_context_, check_and_enter_endgame(), asio::detached);
    return true;
}

asio::awaitable<void> PeerLogic::handle_piece_message(std::shared_ptr<PeerConnection> conn, std::span<const std::byte> payload) {
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

            total_bytes_downloaded_ += block.size();

            if (p.received_count == p.total_blocks) {
                auto received_hash_bytes = Crypto::calculate_sha1_hash_data(p.data);
                auto start = meta_info_.get_torrent_info().pieces.begin() + index * 20;
                std::vector<std::byte> expected_hash_bytes(start, start + 20);

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

asio::awaitable<void> PeerLogic::handle_completed_piece(int piece_index, const std::vector<std::byte>& piece_data) {
    const auto& info = meta_info_.get_torrent_info();
    uint64_t offset = static_cast<uint64_t>(piece_index) * info.piece_size;
    const size_t num_pieces = info.pieces.size() / 20;

    uint64_t current_file_offset = 0;
    uint32_t written_from_piece = 0;

    for (const auto& file_info : info.files) {
        if (!file_info.download) {
            current_file_offset += file_info.size;
            continue;
        }

        if (offset < current_file_offset + file_info.size && current_file_offset < offset + piece_data.size()) {
            uint64_t write_pos_in_file = 0;
            if (offset > current_file_offset) {
                write_pos_in_file = offset - current_file_offset;
            }

            uint32_t bytes_to_write_to_this_file = std::min(static_cast<uint64_t>(piece_data.size() - written_from_piece), file_info.size - write_pos_in_file);

            std::vector<std::byte> sub_data(bytes_to_write_to_this_file);
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
    // Before this coroutine, the piece would have been removed from in_progress_pieces_
    // Now we must also update its status and put it back in the rarity map.
    piece_status_[piece_index] = PieceStatus::Needed;
    
    // Put it back into the rarity map with its correct, current rarity.
    // The dummy group is -1, its actual rarity is stored in piece_availability_
    update_piece_rarity(piece_index, -1, piece_availability_[piece_index]);
    piece_request_trigger_.cancel_one();
    co_return;
}

asio::awaitable<void> PeerLogic::handle_request_message(std::shared_ptr<PeerConnection> conn, std::span<const std::byte> payload) {
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

    std::vector<std::byte> block_data;
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
    
    std::vector<std::byte> piece_msg;
    piece_msg.push_back(static_cast<std::byte>(MessageType::Piece));
    BufferWriter writer(piece_msg);
    writer.write(asio::detail::socket_ops::host_to_network_long(req.index));
    writer.write(asio::detail::socket_ops::host_to_network_long(req.begin));
    writer.write_bytes(block_data);
 
    try {
        co_await conn->socket.send_message(piece_msg);
    } catch (const boost::system::system_error& e) { // Be more specific
        // These errors are expected when the peer disconnects abruptly.
        if (e.code() == asio::error::eof ||
            e.code() == asio::error::connection_reset ||
            e.code() == asio::error::broken_pipe)
        {
            // Log at a debug level if you want, but it's not a warning.
            LOGDBG("Failed to send PIECE to {}: Peer disconnected.", conn->peer_id);
        } else {
            // Log other, unexpected errors as warnings or errors.
            LOGWARN("Failed to send PIECE to {}: {}", conn->peer_id, e.what());
        }
        co_return;
    } catch (const std::exception& e) { // Keep this for other exceptions
        LOGWARN("Failed to send PIECE to {}: {}", conn->peer_id, e.what());
        co_return;
    }
 
    co_await asio::dispatch(strand_, asio::use_awaitable);
    conn->bytes_uploaded += req.length;
    total_bytes_uploaded_ += req.length;
}

asio::awaitable<void> PeerLogic::handle_extended_message(std::shared_ptr<PeerConnection> conn, std::span<const std::byte> payload) {
    conn->remote_extension_map[0] = ExtendedMessageType::Handshake;
    auto remote_id = static_cast<uint8_t>(payload[0]);
    auto it = conn->remote_extension_map.find(remote_id);
    if (it == conn->remote_extension_map.end()) {
        LOGWARN("Peer {} sent unknown extended message ID {}", conn->peer_id, remote_id);
        co_return;
    }

    ExtendedMessageType message_type = it->second;
    std::span<const std::byte> extended_payload(payload.data() + 1, payload.size() - 1);

    switch (message_type) {
        case ExtendedMessageType::Handshake: {
            LOGDBG("Received extended handshake message");

            auto decoded_payload = decode(extended_payload);
            const auto *ehs_dict = std::get_if<std::unique_ptr<Dict>>(&decoded_payload.get_variant());
            if (!ehs_dict || !ehs_dict->get()->count("m")) {
                throw std::runtime_error("Invalid extended handshake message");
            }

            const auto *m_dict = std::get_if<std::unique_ptr<Dict>>(&(ehs_dict->get()->at("m").get_variant()));
            if (m_dict && !m_dict->get()->empty()) {
                LOGDBG("Peer {} supports:", conn->peer_id);
                for (auto &[k, v] : **m_dict) {
                    LOGDBG("\t{}", k);
                    uint8_t index = std::get<Integer>(v.get_variant());
                    conn->remote_extension_map[index] = to_extended_type(k);
                }
            }

            if (ehs_dict->get()->count("v")) {
                auto version = std::get<String>(ehs_dict->get()->at("v").get_variant());
                LOGDBG("Version: {}", version);
            }

            // ...
            break;
        }
        case ExtendedMessageType::ut_pex: {
            LOGDBG("Unimplemented yet");
            break;
        }
        case ExtendedMessageType::ut_metadata: {
            LOGDBG("Unimplemented yet");
            break;
        }
    }
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
        } catch (const boost::system::system_error& e) { // Be more specific
            if (e.code() == asio::error::eof ||
                e.code() == asio::error::connection_reset ||
                e.code() == asio::error::broken_pipe)
            {
                LOGDBG("Failed to send keep-alive to peer {}, it has disconnected.", conn->peer_id);
            } else {
                LOGWARN("Failed to send keep-alive to peer {}, closing connection: {}", conn->peer_id, e.what());
            }
            conn->socket.close();
            co_return;
        } catch (...) { // Fallback
            LOGWARN("Failed to send keep-alive to peer {}, closing connection.", conn->peer_id);
            conn->socket.close();
            co_return;
        }
    }
}

asio::awaitable<void> PeerLogic::send_have_message_to_all(uint32_t piece_index) {
    std::vector<std::byte> have_msg;
    have_msg.push_back(static_cast<std::byte>(MessageType::Have));
    BufferWriter writer(have_msg);
    writer.write(asio::detail::socket_ops::host_to_network_long(piece_index));

    for (const auto& [id, conn] : active_connections_) {
        co_await conn->socket.send_message(have_msg);
    }
}

asio::awaitable<void> PeerLogic::send_request_message(PeerConnection& conn, uint32_t index, uint32_t begin, uint32_t length) {
    std::vector<std::byte> msg_body;
    msg_body.push_back(static_cast<std::byte>(MessageType::Request));
    auto payload = RequestPayload::serialize(index, begin, length);
    msg_body.insert(msg_body.end(), payload.begin(), payload.end());

    // LOGDBG("REQUEST msg[0] raw byte value: {:#04x}", static_cast<uint8_t>(msg_body[0]));

    co_await conn.socket.send_message(msg_body);
}

asio::awaitable<void> PeerLogic::send_cancel_message(PeerConnection& conn, uint32_t index, uint32_t begin, uint32_t length) {
    std::vector<std::byte> msg_body;
    msg_body.push_back(static_cast<std::byte>(MessageType::Cancel));
    auto payload = RequestPayload::serialize(index, begin, length);
    msg_body.insert(msg_body.end(), payload.begin(), payload.end());
    
    // LOGDBG("Sending CANCEL for piece {}, offset {} to {}", index, begin, conn.peer_id);
    co_await conn.socket.send_message(msg_body);
}

asio::awaitable<void> PeerLogic::send_simple_message(PeerConnection& conn, MessageType type) {
    std::vector<std::byte> msg_body;
    msg_body.push_back(static_cast<std::byte>(type));

    // LOGDBG("Sending simple message of type {}. Raw byte value: {:#04x}", 
    //          static_cast<int>(type), static_cast<uint8_t>(msg_body[0]));

    co_await conn.socket.send_message(msg_body);
}

ThreadPool& PeerLogic::get_file_io_pool() {
    static ThreadPool instance(4);
    return instance;
}

asio::awaitable<void> PeerLogic::async_write_to_file(std::filesystem::path path, uint64_t offset, const std::vector<std::byte>& data) {
    auto token = asio::use_awaitable;
    co_await asio::async_initiate<void(std::error_code)>(
        [this, path = std::move(path), offset, &data] (auto&& completion_handler) {
            file_io_pool_.enqueue([
                path = std::move(path),
                offset,
                &data,
                lock_manager = file_lock_manager_,
                handler = std::move(completion_handler)
            ] () mutable {
                try {
                    std::lock_guard lock(lock_manager->get_lock(path));
                    std::filesystem::create_directories(path.parent_path());

                    std::ofstream output_file(path, std::ios::binary | std::ios::out);
                    if (!output_file) {
                        throw std::runtime_error("Failed to open file for writing: " + path.string());
                    }

                    output_file.write(reinterpret_cast<const char*>(data.data()), data.size());
                    if (!output_file) {
                        throw std::runtime_error("Failed to write all data to file: " + path.string());
                    }
                    
                    output_file.flush();
                    if (!output_file) {
                        throw std::runtime_error("Failed to flush data to file: " + path.string());
                    }
                    
                    output_file.close();
                    
                    std::move(handler)(std::error_code{});
                } catch (const std::exception& e) {
                    LOGERR("File write error for {} at offset {}: {}", path.string(), offset, e.what());
                    std::move(handler)(make_error_code(std::errc::io_error));
                }
            });
        },
        token
    );
}

asio::awaitable<std::vector<std::byte>> PeerLogic::async_read_from_file(std::filesystem::path path, uint64_t offset, uint32_t size) {
    auto token = asio::use_awaitable;
    // 1. co_await the operation and capture its result (the tuple)
    auto [ec, block_data] = co_await asio::async_initiate<void(std::error_code, std::vector<std::byte>)>(
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
                    if (size == 0) {
                        size = std::filesystem::file_size(path);
                    }
                    std::vector<std::byte> buffer(size);
                    data_file.seekg(offset);
                    data_file.read(reinterpret_cast<char *>(buffer.data()), size);
                    if (static_cast<std::size_t>(data_file.gcount()) != size) {
                        throw std::runtime_error("Incomplete read from file: " + path.string());
                    }
                    
                    std::move(handler)(std::error_code{}, std::move(buffer));
                } catch (const std::exception& e) {
                    LOGERR("File read error: {}", e.what());
                    std::move(handler)(make_error_code(std::errc::io_error), std::vector<std::byte>{});
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
    // This correctly satisfies the function's awaitable<std::vector<std::byte>> return type.
    co_return block_data;
}


asio::awaitable<void> PeerLogic::verify_existing_file() {
    const auto& info = meta_info_.get_torrent_info();
    const size_t num_pieces = info.pieces.size() / 20;

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

        std::vector<std::byte> piece_data(piece_size_to_read);
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

            auto actual_hash_bytes = Crypto::calculate_sha1_hash_data(piece_data);
            auto start = info.pieces.begin() + i * 20;
            std::vector<std::byte> expected_hash_bytes(start, start + 20);

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

    const size_t num_pieces = info.pieces.size() / 20;

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

        std::vector<std::byte> piece_data(piece_size_to_read);
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

            auto actual_hash_bytes = Crypto::calculate_sha1_hash_data(piece_data);
            auto start = info.pieces.begin() + i * 20;
            std::vector<std::byte> expected_hash_bytes(start, start + 20);

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


asio::awaitable<void> PeerLogic::await_upload_tokens(size_t amount) {
    co_await upload_limiter_.await_tokens(amount);
}

asio::awaitable<void> PeerLogic::await_download_tokens(size_t amount) {
    co_await download_limiter_.await_tokens(amount);
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
    const size_t num_pieces = info.pieces.size() / 20;

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


asio::awaitable<void> PeerLogic::send_cancel_for_block(uint32_t piece_index, uint32_t block_index, const PeerId& exclude_peer_id) {
    co_await asio::dispatch(strand_, asio::use_awaitable);

    auto it = in_progress_pieces_.find(piece_index);
    if (it == in_progress_pieces_.end() || block_index >= it->second.outstanding_requests.size()) {
        co_return ;
    }

    auto& requests_for_block = it->second.outstanding_requests[block_index];
    std::vector<PeerId> peer_ids_to_cancel = requests_for_block;
    requests_for_block.clear();

    const auto& info = meta_info_.get_torrent_info();
    const size_t num_pieces = info.pieces.size() / 20;

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
        const size_t num_pieces = info.pieces.size() / 20;

        uint64_t downloaded_bytes = pieces_done_count_.load() * info.piece_size;
        uint64_t left = is_seeder_() ? 0 : (num_pieces - pieces_done_count_) * info.piece_size;

        if (is_download_complete_ && !completed_event_sent && !is_seeder_()) {
            event = "completed";
            completed_event_sent = true;
        }

        AnnounceRequestParams params {
            .info_hash_bytes = info_hash_bytes_,
            .peer_id = my_peer_id_,
            .event = event,
            .port = static_cast<uint16_t>(peer_port_),
            .uploaded = downloaded_bytes,
            .downloaded = pieces_done_count_ * info.piece_size,
            .left = left,
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
        
    } catch (const boost::system::system_error& e) {
        // This is a common way clients disconnect. No need to log as an error.
        if (e.code() == asio::error::eof ||
            e.code() == asio::error::connection_reset ||
            e.code() == asio::error::broken_pipe)
        {
            // Normal disconnect, do nothing or log at a debug level
        } else {
            LOGERR("Failed to connect to peer {}: {}", peer_addr, e.what());
        }
    } catch (const std::exception& e) {
        // Catch other potential standard exceptions
        LOGERR("Failed to connect to peer {}: {}", peer_addr, e.what());
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

void PeerLogic::update_piece_rarity(int piece_index, uint32_t old_rarity, uint32_t new_rarity) {
    // This function must be called from within the strand_
    // Remove from the old rarity set
    if (auto it = pieces_by_rarity_.find(old_rarity); it != pieces_by_rarity_.end()) {
        it->second.erase(piece_index);
        // If the set for the old rarity is now empty, remove the map entry
        if (it->second.empty()) {
            pieces_by_rarity_.erase(it);
        }
    }
    // Add to the new rarity set
    pieces_by_rarity_[new_rarity].insert(piece_index);
}


asio::awaitable<void> PeerLogic::save_progress() {
    bool expected = false;
    if (!is_saving_.compare_exchange_strong(expected, true)) {
        LOGDBG("Save already in progress, skipping");
        co_return;
    }

    SaveGuard guard(is_saving_);

    co_await asio::dispatch(strand_, asio::use_awaitable);
    const auto &info = meta_info_.get_torrent_info();
    const size_t num_pieces = info.pieces.size() / 20;
    
    std::string have_bitfield_str;
    have_bitfield_str.resize((num_pieces + 7) / 8, '\0');

    for (size_t i = 0; i < num_pieces; ++i) {
        if (piece_status_[i] == PieceStatus::Have) {
            have_bitfield_str[i / 8] |= (1 << (7 - (i % 8)));
        }
    }

    Dict files_metadata_dict;
    for (const auto& file_info : info.files) {
        Dict this_file_dict;
        this_file_dict["size"] = Value(static_cast<Integer>(file_info.size));
        try {
            std::filesystem::path full_path = get_full_path_for_file(file_info);
            if (std::filesystem::exists(full_path)) {
                auto ftime = std::filesystem::last_write_time(full_path);
                this_file_dict["mtime"] = Value(static_cast<Integer>(ftime.time_since_epoch().count()));
            }
        } catch (const std::exception& e) {
            LOGWARN("Could not get mtime for {}: {}", file_info.path.string(), e.what());
        }
        // CRITICAL: Use the full relative path as the key
        files_metadata_dict[file_info.path.string()] = Value(std::move(this_file_dict));
    }

    Dict stats_dict;
    stats_dict["uploaded"] = Value(static_cast<Integer>(total_bytes_uploaded_.load()));
    stats_dict["downloaded"] = Value(static_cast<Integer>(total_bytes_downloaded_.load()));

    Dict in_progress_dict;
    for (const auto& [piece_idx, progress] : in_progress_pieces_) {
        std::string block_bitfield_str;
        block_bitfield_str.resize((progress.blocks_received.size() + 7) / 8, '\0');
        for (size_t i = 0; i < progress.blocks_received.size(); ++i) {
            if (progress.blocks_received[i]) {
                block_bitfield_str[i / 8] |= (1 << (7 - (i % 8)));
            }
        }
        in_progress_dict[std::to_string(piece_idx)] = Value(block_bitfield_str);
    }

    Dict resume_dict;
    resume_dict["have_bitfield"] = Value(have_bitfield_str);
    resume_dict["files_metadata"] = Value(std::move(files_metadata_dict));
    resume_dict["stats"] = Value(std::move(stats_dict));
    resume_dict["in_progress"] = Value(std::move(in_progress_dict));
    
    auto encoded = encode(Value(std::move(resume_dict)));

    std::filesystem::path p;
    if (info.files.size() > 1) {
        p = data_file_path_ / info.name / ".resume";
    } else {
        // For single file, place .resume alongside the file
        p = data_file_path_.parent_path() / (data_file_path_.filename().string() + ".resume");
    }

    std::filesystem::path temp_path = p;
    temp_path += ".tmp";

    try {
        std::filesystem::create_directories(p.parent_path());
        co_await async_write_to_file(temp_path, 0, encoded);
        std::filesystem::rename(temp_path, p);
        LOGDBG("Progress saved successfully");
    } catch(const std::exception& e) {
        LOGERR("Failed to save resume file: {}", e.what());
        
        std::error_code ec;
        std::filesystem::remove(temp_path, ec);
        if (ec) {
            LOGDBG("Could not remove temporary file: {}", ec.message());
        }
    }
}


asio::awaitable<void> PeerLogic::load_progress() {
    const auto &info = meta_info_.get_torrent_info();
    const size_t num_pieces = info.pieces.size() / 20;
    std::filesystem::path p;
    if (info.files.size() > 1) {
        p = data_file_path_ / info.name / ".resume";
    } else {
        p = data_file_path_.parent_path() / (data_file_path_.filename().string() + ".resume");
    }
    
    if (!std::filesystem::exists(p)) {
        LOGINFO("No downloaded data");
        co_await asio::dispatch(strand_, asio::use_awaitable);
        piece_status_.assign(num_pieces, PieceStatus::Needed);
        pieces_done_count_ = 0;
        in_progress_pieces_.clear();
        co_return;
    }

    auto file_size = std::filesystem::file_size(p);
    if (file_size == 0) {
        LOGWARN("Resume file is empty, starting fresh");
        std::filesystem::remove(p);
        co_await asio::dispatch(strand_, asio::use_awaitable);
        piece_status_.assign(num_pieces, PieceStatus::Needed);
        pieces_done_count_ = 0;
        in_progress_pieces_.clear();
        co_return;
    }

    LOGINFO("Loading history data...");

    try {
        auto resume_bytes = co_await async_read_from_file(p, 0);
        auto decoded = decode(resume_bytes);
        const auto *resume_dict_ptr = std::get_if<std::unique_ptr<Dict>>(&decoded.get_variant());
        if (!resume_dict_ptr) {
            throw std::runtime_error("Resume file is not a dictionary");
        }
        const Dict& resume_dict = **resume_dict_ptr;

        // SAFELY access dictionary elements
        if (!resume_dict.count("have_bitfield")) {
            throw std::runtime_error("Resume file missing 'have_bitfield'");
        }

        std::string have_bitfield_str = std::get<String>(resume_dict.at("have_bitfield").get_variant());
        
        // Validate bitfield length
        size_t expected_bitfield_size = (num_pieces + 7) / 8;
        if (have_bitfield_str.size() != expected_bitfield_size) {
            throw std::runtime_error(std::format("Invalid bitfield size: expected {}, got {}", 
                expected_bitfield_size, have_bitfield_str.size()));
        }

        // SAFELY access files_metadata_dict
        const Dict* files_metadata_dict = nullptr;
        if (resume_dict.count("files_metadata")) {
            const auto* files_metadata_ptr = std::get_if<std::unique_ptr<Dict>>(&resume_dict.at("files_metadata").get_variant());
            if (files_metadata_ptr) {
                files_metadata_dict = files_metadata_ptr->get();
            }
        }

        // SAFELY access in_progress_dict
        const Dict* in_progress_dict = nullptr;
        if (resume_dict.count("in_progress")) {
            const auto* in_progress_ptr = std::get_if<std::unique_ptr<Dict>>(&resume_dict.at("in_progress").get_variant());
            if (in_progress_ptr) {
                in_progress_dict = in_progress_ptr->get();
            }
        }
        
        co_await asio::dispatch(strand_, asio::use_awaitable);
        
        pieces_done_count_ = 0;
        for (size_t i = 0; i < num_pieces; ++i) {
            if (i / 8 >= have_bitfield_str.size()) {
                piece_status_[i] = PieceStatus::Needed;
                continue;
            }
            
            uint8_t byte = static_cast<uint8_t>(have_bitfield_str[i / 8]);
            uint8_t bit_position = 7 - (i % 8);
            bool has_piece = (byte & (1 << bit_position)) != 0;
            
            if (has_piece) {
                piece_status_[i] = PieceStatus::Have;
                ++pieces_done_count_;

                if (auto it = pieces_by_rarity_.find(0); it != pieces_by_rarity_.end()) {
                    it->second.erase(i);
                    if (it->second.empty()) {
                        pieces_by_rarity_.erase(it);
                    }
                }
            } else {
                // This piece is not in the 'have' bitfield.
                // Only mark it as 'Needed' if it wasn't already marked 'Skipped'
                // by the selective download logic which runs before this.
                if (piece_status_[i] != PieceStatus::Skipped) {
                    piece_status_[i] = PieceStatus::Needed;
                }
            }
        }

        // Check for corrupted resume files
        if (pieces_done_count_ > num_pieces) {
            throw std::runtime_error(std::format("Corrupted resume file: {} pieces marked as done, but torrent only has {}", 
                pieces_done_count_.load(), num_pieces));
        }

        // Check file metadata
        if (files_metadata_dict) {
            for (size_t file_idx = 0; file_idx < info.files.size(); ++file_idx) {
                const auto &file_info = info.files[file_idx];
                std::filesystem::path full_path = get_full_path_for_file(file_info);
                const std::string file_key = file_info.path.string();

                if (!files_metadata_dict->count(file_key)) {
                    LOGWARN("File metadata missing for: {}", file_key);
                    continue;
                }

                if (!std::filesystem::exists(full_path)) {
                    // Mark all pieces for this file as needed
                    for (size_t piece_idx : file_to_pieces_map_[file_idx]) {
                        if (piece_status_[piece_idx] == PieceStatus::Have) {
                            piece_status_[piece_idx] = PieceStatus::Needed;
                            --pieces_done_count_;
                        }
                    }
                    continue;
                }
                
                const auto *this_file_dict_ptr = std::get_if<std::unique_ptr<Dict>>(&(files_metadata_dict->at(file_key).get_variant()));
                if (!this_file_dict_ptr) {
                    continue;
                }
                const Dict& this_file_dict = **this_file_dict_ptr;

                // Check file size
                if (this_file_dict.count("size")) {
                    Integer saved_size = std::get<Integer>(this_file_dict.at("size").get_variant());
                    if (file_info.size != static_cast<uint64_t>(saved_size)) {
                        LOGWARN("File size mismatch for {}: expected {}, got {}", 
                                file_info.path.string(), file_info.size, saved_size);
                        // Mark pieces as needed
                        for (size_t piece_idx : file_to_pieces_map_[file_idx]) {
                            if (piece_status_[piece_idx] == PieceStatus::Have) {
                                piece_status_[piece_idx] = PieceStatus::Needed;
                                --pieces_done_count_;
                            }
                        }
                    }
                }

                // Check modification time
                try {
                    if (this_file_dict.count("mtime")) {
                        Integer saved_mtime = std::get<Integer>(this_file_dict.at("mtime").get_variant());
                        auto current_mtime = std::filesystem::last_write_time(full_path).time_since_epoch().count();
                        if (current_mtime != saved_mtime) {
                            LOGWARN("File modification time changed for: {}", file_info.path.string());
                            // Mark pieces as needed
                            for (size_t piece_idx : file_to_pieces_map_[file_idx]) {
                                if (piece_status_[piece_idx] == PieceStatus::Have) {
                                    piece_status_[piece_idx] = PieceStatus::Needed;
                                    --pieces_done_count_;
                                }
                            }
                        }
                    }
                } catch (const std::exception& e) {
                    LOGWARN("Could not check mtime for {}: {}", file_info.path.string(), e.what());
                }
            }
        }

        // Rebuild the rarity map
        pieces_by_rarity_.clear();

        for (size_t i = 0; i < num_pieces; ++i) {
            if (piece_status_[i] == PieceStatus::Needed) {
                // For needed pieces, use their current availability
                // If we don't know the availability yet, assume 0 (will be updated when we receive peer bitfields)
                uint32_t rarity = piece_availability_[i];
                pieces_by_rarity_[rarity].insert(i);
            } else if (piece_status_[i] == PieceStatus::Have) {
                // Have pieces go to rarity 0 (we have them)
                pieces_by_rarity_[0].insert(i);
            }
        }

        // Load in-progress pieces
        if (in_progress_dict) {
            for (const auto& [key, value] : *in_progress_dict) {
                try {
                    size_t piece_idx = std::stoul(key);
                    if (piece_idx >= num_pieces || piece_status_[piece_idx] != PieceStatus::Needed) {
                        continue; // Skip if already have or invalid index
                    }

                    const String* block_bitfield_str = std::get_if<String>(&value.get_variant());
                    if (!block_bitfield_str) continue;

                    uint64_t piece_size;
                    if (static_cast<uint64_t>(piece_idx) == num_pieces - 1) {
                        uint64_t last_piece_size = info.total_size % info.piece_size;
                        piece_size = (last_piece_size == 0) ? info.piece_size : last_piece_size;
                    } else {
                        piece_size = info.piece_size;
                    }

                    // Restore the state
                    InProgressPiece progress(piece_size);
                    for (size_t i = 0; i < progress.blocks_received.size(); ++i) {
                        if (i / 8 >= block_bitfield_str->size()) break;
                        
                        uint8_t byte = static_cast<uint8_t>((*block_bitfield_str)[i / 8]);
                        uint8_t bit_position = 7 - (i % 8);
                        if ((byte & (1 << bit_position)) != 0) {
                            progress.blocks_received[i] = true;
                            progress.received_count++;
                        }
                    }
                    // Don't resume if it was actually complete
                    if (progress.received_count == progress.total_blocks) continue;

                    // Update status and rarity maps
                    piece_status_[piece_idx] = PieceStatus::InProgress;
                    uint32_t current_rarity = piece_availability_[piece_idx];
                    update_piece_rarity(piece_idx, current_rarity, -1); // Remove from rarity map
                    in_progress_pieces_.emplace(piece_idx, std::move(progress));

                    // Re-initiate the action by spawning a resume task
                    asio::co_spawn(io_context_, resume_piece_download(piece_idx), asio::detached);
                } catch (const std::exception& e) {
                    LOGWARN("Could not parse in-progress piece '{}': {}", key, e.what());
                }
            }
        }

        co_await asio::dispatch(strand_, asio::use_awaitable);
        // Check if we should be in endgame mode (all pieces are either Have or InProgress)
        bool all_pieces_accounted_for = true;
        for (size_t i = 0; i < piece_status_.size(); ++i) {
            if (piece_status_[i] == PieceStatus::Needed) {
                all_pieces_accounted_for = false;
                break;
            }
        }

        if (all_pieces_accounted_for && !in_progress_pieces_.empty()) {
            LOGINFO("Resuming in ENDGAME MODE");
            is_in_endgame_mode_ = true;
            // Re-broadcast outstanding requests
            asio::co_spawn(io_context_, broadcast_outstanding_requests(), asio::detached);
        }

    } catch (const std::exception& e) {
        LOGERR("Failed to load resume file: {}. Starting fresh.", e.what());
        
        // Delete the corrupted resume file
        try {
            std::filesystem::remove(p);
            LOGINFO("Deleted corrupted resume file: {}", p.string());
        } catch (const std::exception& remove_e) {
            LOGWARN("Failed to delete corrupted resume file: {}", remove_e.what());
        }
        
        // Reset to initial state
        piece_status_.assign(num_pieces, PieceStatus::Needed);
        pieces_done_count_ = 0;
        in_progress_pieces_.clear();
    }
    

    if (pieces_done_count_ > 0) {
        float progress = (static_cast<float>(pieces_done_count_) / num_pieces) * 100.0f;
        LOGINFO("Loading progress complete. Found {}/{} valid pieces ({:.2f}% progress).", 
                pieces_done_count_.load(), num_pieces, progress);
    } else {
        LOGINFO("No valid progress found in resume file. Starting fresh download.");
    }
}


void PeerLogic::build_pieces_files_map() {
    const auto& info = meta_info_.get_torrent_info();
    const size_t num_pieces = info.pieces.size() / 20;
    const uint32_t piece_size = info.piece_size;
    piece_to_files_map_.resize(num_pieces);
    uint64_t current_total_offset = 0;
    for (size_t file_idx = 0; file_idx < info.files.size(); ++file_idx) {
        const auto& file = info.files[file_idx];
        uint64_t file_start_offset = current_total_offset;
        uint64_t file_end_offset = current_total_offset + file.size;
        // Determine which pieces this file overlaps with
        uint32_t start_piece = file_start_offset / piece_size;
        uint32_t end_piece = (file_end_offset - 1) / piece_size;
        for (uint32_t piece_idx = start_piece; piece_idx <= end_piece; ++piece_idx) {
            uint64_t piece_start_offset = static_cast<uint64_t>(piece_idx) * piece_size;
            
            // Calculate the precise overlap between this piece and this file
            uint64_t overlap_start = std::max(file_start_offset, piece_start_offset);
            uint64_t overlap_end = std::min(file_end_offset, piece_start_offset + piece_size);
            uint32_t overlap_length = overlap_end - overlap_start;
            PieceFileOverlap overlap;
            overlap.file_index = file_idx;
            overlap.offset_in_file = overlap_start - file_start_offset;
            overlap.offset_in_piece = overlap_start - piece_start_offset;
            overlap.length = overlap_length;
            piece_to_files_map_[piece_idx].push_back(overlap);
        }
        current_total_offset += file.size;
    }

    for (size_t file_idx = 0; file_idx < info.files.size(); ++file_idx) {
        std::vector<size_t> pieces;
        for (size_t piece_idx = 0; piece_idx < num_pieces; ++piece_idx) {
            for (const auto &overlap : piece_to_files_map_[piece_idx]) {
                if (overlap.file_index == file_idx) {
                    pieces.push_back(piece_idx);
                    break;
                }
            }
        }
        file_to_pieces_map_.push_back(std::move(pieces));
    }
}


asio::awaitable<void> PeerLogic::resume_piece_download(int piece_index) {
    auto self = shared_from_this();
    asio::steady_timer timer(io_context_);

    // Loop until successfully re-request all missing blocks
    while (true) {
        // Wait for a short period to allow peer connections to be established
        timer.expires_after(std::chrono::seconds(1));
        co_await timer.async_wait(asio::use_awaitable);

        co_await asio::dispatch(strand_, asio::use_awaitable);
        // Check if the piece is still in progress (it might have been completed or cancelled)
        auto piece_it = in_progress_pieces_.find(piece_index);
        if (piece_it == in_progress_pieces_.end()) {
            // The piece is no longer in progress, our job here is done.
            co_return;
        }

        auto& piece_progress = piece_it->second;
        // Find peers that have this piece and are not choking us
        std::vector<std::shared_ptr<PeerConnection>> available_peers;
        for (const auto& [id, conn] : active_connections_) {
            if (!conn->peer_is_choking && conn->has_piece(piece_index)) {
                available_peers.push_back(conn);

                if (!conn->am_interested) {
                    conn->am_interested = true;
                    // Send INTERESTED message asynchronously
                    asio::co_spawn(io_context_, 
                        [this, conn]() -> asio::awaitable<void> {
                            co_await send_simple_message(*conn, MessageType::Interested);
                        }, 
                        asio::detached
                    );
                }
            }
        }
        if (available_peers.empty()) {
            LOGDBG("Resumer for piece {}: No available peers yet, will retry...", piece_index);
            continue; // Go back to the start of the loop and wait again
        }

        LOGINFO("Resuming download for piece {}. Requesting missing blocks.", piece_index);
        
        if (is_in_endgame_mode_.load()) {
            // In endgame mode, request from all available peers
            for (uint32_t block_idx = 0; block_idx < piece_progress.total_blocks; ++block_idx) {
                if (!piece_progress.blocks_received[block_idx]) {
                    uint32_t offset = block_idx * BLOCK_SIZE;
                    uint32_t length = (block_idx == piece_progress.total_blocks - 1) 
                        ? (piece_progress.data.size() - offset)
                        : BLOCK_SIZE;
                    
                    // Request from all available peers for this block
                    for (const auto& peer_conn : available_peers) {
                        if (!peer_conn->peer_is_choking) {
                            asio::co_spawn(io_context_,
                                [this, peer_conn, piece_index, offset, length]() -> asio::awaitable<void> {
                                    co_await send_request_message(*peer_conn, piece_index, offset, length);
                                },
                                asio::detached
                            );
                            piece_progress.outstanding_requests[block_idx].push_back(peer_conn->peer_id);
                        }
                    }
                }
            }
        } else {
            // Request all missing blocks for this piece
            for (uint32_t block_idx = 0; block_idx < piece_progress.total_blocks; ++block_idx) {
                if (!piece_progress.blocks_received[block_idx]) {
                    uint32_t offset = block_idx * BLOCK_SIZE;
                    uint32_t length = (block_idx == piece_progress.total_blocks - 1) 
                        ? (piece_progress.data.size() - offset)
                        : BLOCK_SIZE;
                    // Pick a peer to request from (round-robin)
                    auto& peer_conn = available_peers[block_idx % available_peers.size()];
                    
                    asio::co_spawn(io_context_,
                        [this, peer_conn, piece_index, offset, length]() -> asio::awaitable<void> {
                            co_await send_request_message(*peer_conn, piece_index, offset, length);
                        },
                        asio::detached
                    );
                    
                    // Track that we made a request for this block
                    piece_progress.outstanding_requests[block_idx].push_back(peer_conn->peer_id);
                }
            }
        }
        co_return;
    }
}

asio::awaitable<int> Leecher::async_prompt(const std::string& question) {
    // Display the question
    std::cout << question << std::flush;
    
    // Read input asynchronously with timeout
    asio::steady_timer timer{io_context_};
    timer.expires_after(std::chrono::seconds(300)); // 5 minute timeout
    
    std::string input;
    asio::streambuf buffer;
    asio::posix::stream_descriptor in(io_context_, STDIN_FILENO);
    
    auto read_op = asio::async_read_until(
        in, buffer, '\n', asio::use_awaitable
    );
    auto timer_op = timer.async_wait(asio::use_awaitable);
    
    // Wait for either input or timeout/shutdown
    auto result = co_await (std::move(read_op) || std::move(timer_op));
    
    if (result.index() == 0) {
        // Input received
        std::istream is(&buffer);
        std::getline(is, input);
        
        try {
            co_return std::stoi(input);
        } catch (const std::exception&) {
            co_return -1; // Invalid input
        }
    } else {
        // Timeout or interrupted
        if (shutting_down_.load()) {
            throw std::runtime_error("Shutdown during prompt");
        } else {
            throw std::runtime_error("Input timeout");
        }
    }
}

/*
#####################################################
            Seeder implementation
#####################################################
*/

Seeder::Seeder(private_key, asio::io_context& io_context, PeerId peer_id, const std::filesystem::path& torrent_path, const std::filesystem::path& content_dir, 
                int peer_port, uint64_t upload_rate_bps)
    : PeerLogic(io_context, std::move(peer_id), torrent_path, peer_port, upload_rate_bps, 0), peer_port_(peer_port) {

    const auto& info = meta_info_.get_torrent_info();
    const size_t num_pieces = info.pieces.size() / 20;

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

asio::awaitable<bool> Seeder::async_init() {
    bool ok = co_await verify_seed_data();
    if (!ok) {
        LOGCRITICAL("Seeder data verification failed. File may be corrupt.");
        co_return false;
    }

    co_return true;
}

asio::awaitable<std::shared_ptr<Seeder>> Seeder::create(
    asio::io_context& io_context, 
    PeerId peer_id, 
    const std::filesystem::path& torrent_path, 
    const std::filesystem::path& content_dir, 
    int peer_port, uint64_t upload_rate_bps
) {
    auto seeder = std::make_shared<Seeder>(
        private_key{}, io_context, std::move(peer_id), torrent_path, content_dir, peer_port, upload_rate_bps
    );

    bool success = co_await seeder->async_init();
    if (!success) {
        // Return nullptr or throw to indicate failure
        co_return nullptr; 
    }

    co_return seeder;
}

asio::awaitable<void> Seeder::run() {
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

Leecher::Leecher(private_key, asio::io_context& io_context, PeerId peer_id, const std::filesystem::path& torrent_path, const std::filesystem::path& save_path,
                int peer_port,
                uint64_t upload_rate_bps, uint64_t download_rate_bps)
    : PeerLogic(io_context, std::move(peer_id), torrent_path, peer_port, upload_rate_bps, download_rate_bps) { 

    data_file_path_ = save_path;

    const auto& info = meta_info_.get_torrent_info();
    const size_t num_pieces = info.pieces.size() / 20;

    piece_status_.resize(num_pieces, PieceStatus::Needed);

    LOGINFO("Leecher initialized for '{}', {} pieces to download.", info.name, num_pieces);
}


asio::awaitable<bool> Leecher::async_init() {
    auto& info = meta_info_.get_torrent_info();
    const size_t num_pieces = info.pieces.size() / 20;
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

        // selective downloading
        bool skip_all = false;
        for (size_t i = 0; i < info.files.size(); ++i) {
            auto& file = info.files[i];
            std::filesystem::path full_path = get_full_path_for_file(file);

            bool skip_this_file = false;
            int ans = co_await async_prompt(
                std::format("Download file {}?\n    1 - Yes\n    2 - No\n    3 - Yes for all\n    4 - No for all\n:", 
                full_path.filename().string())
            );

            if (ans == 1) {
                
            } else if (ans == 2) {
                skip_this_file = true;
            } else if (ans == 3) {
                skip_all = true;
            } else if (ans == 4) {
                skip_this_file = true;
                skip_all = true;
            } else {
                LOGWARN("Unknown input '{}', defaulting to Yes", ans);
            }

            if (skip_this_file) {
                file.download = false;
                continue;
            }

            if (skip_this_file && skip_all) {
                for (size_t j = i; j < info.files.size(); ++j) {
                    info.files[j].download = false;
                }
                break;
            }

            // Only create/truncate the file if it DOES NOT exist
            if (!std::filesystem::exists(full_path) && file.download) {
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

            if (skip_all) {
                break;
            }
        }

        for (size_t piece_idx = 0; piece_idx < num_pieces; ++piece_idx) {
            bool is_needed = false;
            for (const auto& overlap : piece_to_files_map_[piece_idx]) {
                if (meta_info_.get_torrent_info().files[overlap.file_index].download) {
                    is_needed = true;
                    break;
                }
            }
            piece_status_[piece_idx] = is_needed ? PieceStatus::Needed : PieceStatus::Skipped;
        }

        co_await load_progress();
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
    PeerId peer_id, 
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

    asio::co_spawn(io_context_, [self = std::static_pointer_cast<Leecher>(shared_from_this())]() -> asio::awaitable<void> {
        try {
            AsyncServerSocket peer_server(self->io_context_, self->peer_port_);
            LOGINFO("Leecher now listening for incoming connections on port {}", self->peer_port_);
            while (true) {
                AsyncSocket new_peer_socket = co_await peer_server.accept();
                auto endpoint = new_peer_socket.get_socket().remote_endpoint();
                std::string addr = endpoint.address().to_string() + ":" + std::to_string(endpoint.port());
                // Handle the new connection just like a seeder would
                asio::co_spawn(self->io_context_, self->handle_new_connection(std::move(new_peer_socket), addr), asio::detached);
            }
        } catch (const std::exception& e) {
            LOGCRITICAL("Leecher listening loop failed: {}", e.what());
        }
    }, asio::detached);

    asio::co_spawn(io_context_, choke_loop(), asio::detached);
    asio::co_spawn(io_context_, downloader_loop(), asio::detached);
    asio::co_spawn(io_context_, tracker_announce_loop(), asio::detached);
    asio::co_spawn(io_context_, periodically_save(), asio::detached);

    try {
        co_await completion_timer_.async_wait(asio::use_awaitable);
    } catch (const boost::system::system_error& e) {
        if (e.code() != asio::error::operation_aborted) {
            LOGERR("Completion timer failed with unexpected error: {}", e.what());
        }
    }

    co_await save_progress();

    co_return is_download_complete_;
}

asio::awaitable<void> Leecher::periodically_save() {
    using namespace std::chrono_literals;

    asio::steady_timer timer{io_context_};

    while (!shutting_down_) {
        timer.expires_after(1min);
        auto [ec] = co_await timer.async_wait(asio::as_tuple(asio::use_awaitable));
    
        if (shutting_down_ || (ec && ec == asio::error::operation_aborted)) {
            break;
        }

        if (shutting_down_) {
            break;
        }

        co_await save_progress();
    }
}

void Leecher::stop() {
    shutting_down_.store(true);

    asio::co_spawn(io_context_, [self = std::static_pointer_cast<Leecher>(shared_from_this())]() -> asio::awaitable<void> {
        LOGINFO("Shutdown initiated, saving final progress...");

        asio::steady_timer timer(self->io_context_);
        timer.expires_after(std::chrono::seconds(5));
        
        auto save_op = self->save_progress();
        auto timer_op = timer.async_wait(asio::use_awaitable);
        
        // Wait for either save to complete or timeout
        auto result = co_await (std::move(save_op) || std::move(timer_op));
        
        if (result.index() == 1) {
            LOGWARN("Save operation timed out, forcing shutdown");
        } else {
            LOGINFO("Final save complete.");
        }

        self->io_context_.stop();
    }, asio::detached);
}