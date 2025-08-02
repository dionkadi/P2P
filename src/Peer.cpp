#include "Peer.hpp"
#include "Crypto.hpp"
#include "Logger.hpp"
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
#include "asio/steady_timer.hpp"
#include <memory>


PeerLogic::PeerLogic(asio::io_context& io_context, std::string peer_id, const std::filesystem::path& torrent_path)
    : io_context_(io_context),
      my_peer_id_(peer_id),
      strand_(asio::make_strand(io_context)),
      completion_timer_(io_context),
      file_io_pool_(get_file_io_pool()) 
{

    if (!meta_info_.load_from_file(torrent_path)) {
        throw std::runtime_error("Could not load torrent file: " + torrent_path.string());
    }

    info_hash_bytes_ = Crypto::hex_to_bytes(meta_info_.get_info_hash());
    if (info_hash_bytes_.size() != HASH_SIZE) {
        throw std::runtime_error("Invalid info hash size after conversion.");
    }

    piece_availability_.resize(meta_info_.get_torrent_info().piece_hashes.size(), 0);

    completion_timer_.expires_at(asio::steady_timer::time_point::max());
}

asio::awaitable<void> PeerLogic::handle_new_connection(AsyncSocket socket, std::string peer_addr) {
    auto conn = std::make_shared<PeerConnection>(std::move(socket));
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

    } catch (const asio::system_error& e) {
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
                LOGDBG("Received keep-alive from {}", conn->peer_id);
                continue ;
            }


            // LOGDBG("Received raw message of size {} from {}. First byte is: {:#04x}",
            //          msg.size(), conn->peer_id, static_cast<uint8_t>(msg[0]));

            MessageType type = static_cast<MessageType>(msg[0]);
            
            if (type == MessageType::Request) {
                LOGDBG("Received REQUEST from {}", conn->peer_id);
                asio::co_spawn(io_context_, 
                    handle_request_message(conn, {msg.data() + 1, msg.size() - 1}), 
                    asio::detached);
                continue;
            }
            
            std::span<const char> payload(msg.data() + 1, msg.size() - 1);

            co_await asio::dispatch(strand_, asio::use_awaitable);

            switch (type) {
                case MessageType::Choke:
                    LOGDBG("Received CHOKE from {}", conn->peer_id);
                    conn->peer_is_choking = true;
                    break;
                case MessageType::Unchoke:
                    LOGDBG("Received UNCHOKE from {}", conn->peer_id);    
                    conn->peer_is_choking =false;
                    break;
                case MessageType::Interested:
                    LOGDBG("Received INTERESTED from {}", conn->peer_id);    
                    conn->peer_is_interested = true;
                    break;
                case MessageType::NotInterested:
                    LOGDBG("Received NOT INTERESTED from {}", conn->peer_id);    
                    conn->peer_is_interested =false;
                    break;
                case MessageType::Bitfield: {
                    LOGDBG("Received BITFIELD message from {}", conn->peer_id);
                    
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

                    break;
                }
                case MessageType::Have: {
                    if (payload.size() < 4) {
                        break ;
                    } 
                    BufferReader reader(payload);
                    uint32_t index = asio::detail::socket_ops::network_to_host_long(reader.read<uint32_t>());

                    LOGDBG("Received HAVE for piece {} from {}", index, conn->peer_id);

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

                    break;
                }
                case MessageType::Piece: {
                    co_await handle_piece_message(conn, payload);
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

    while (!is_download_complete_) {
        timer.expires_after(std::chrono::seconds(10));
        co_await timer.async_wait(asio::use_awaitable);

        co_await asio::dispatch(strand_, asio::use_awaitable);

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
        for (size_t i = 0; i < interested_peers.size(); ++i) {
            auto& conn = interested_peers[i];
            if (i < unchoke_slots - 1) {
                if (conn->am_choking) {
                    LOGDBG("Unchoking fast peer {}", conn->peer_id);
                    co_await send_simple_message(*conn, MessageType::Unchoke);
                    conn->am_choking = false;
                }
            } else {
                if (!conn->am_choking) {
                    LOGDBG("Choking slow peer {}", conn->peer_id);
                    co_await send_simple_message(*conn, MessageType::Choke);
                    conn->am_choking = true;
                }
            }
        }

        if (!interested_peers.empty()) {
            std::uniform_int_distribution<size_t> dist(0, interested_peers.size() - 1);
            auto& conn = interested_peers[dist(rng)];
            if (conn->am_choking) {
                LOGINFO("Optimistically unchoking peer {}", conn->peer_id);
                co_await send_simple_message(*conn, MessageType::Unchoke);
                conn->am_choking = false;
            }
        }


        for (auto const& [id, conn] : active_connections_) {
            conn->bytes_downloaded = 0;
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
            LOGDBG("Downloader has {} slots to fill. Spawning {} request tasks.", slots_to_fill, slots_to_fill);
            for (int i = 0; i < slots_to_fill; ++i) {
                asio::co_spawn(io_context_, request_one_piece_loop(), asio::detached);
            }
        }

        timer.expires_after(std::chrono::seconds(1));
        co_await timer.async_wait(asio::use_awaitable);
    }
}

asio::awaitable<void> PeerLogic::request_one_piece_loop() {
    auto self = shared_from_this();

    int best_piece_index = -1;
    int piece_index = -1;
    std::vector<std::shared_ptr<PeerConnection>> availabel_peers;
    // uint32_t min_availability = UINT32_MAX;

    const auto& t_info = meta_info_.get_torrent_info();
    uint64_t piece_size;
    if (static_cast<uint64_t>(piece_index) == t_info.piece_hashes.size() - 1) {
        piece_size = t_info.file_size - (static_cast<uint64_t>(piece_index) * t_info.piece_size);
    } else {
        piece_size = t_info.piece_size;
    }

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
            LOGWARN("Candidates is empty");
            co_return ;
        }

        std::sort(candidates.begin(), candidates.end(), [] (const auto& a, const auto& b) {
            return a.availability < b.availability;
        });

        best_piece_index = candidates.front().index;

        piece_index = best_piece_index;

        in_progress_pieces_.emplace(piece_index, InProgressPiece(piece_size));
        piece_status_[piece_index] = PieceStatus::InProgress;

        LOGDBG("Selected rarest piece {} (availability: {}). Starting download.", 
            piece_index, piece_availability_[piece_index]);

        
        for (const auto& [id, conn] : active_connections_) {
            if (conn->am_interested && !conn->peer_is_choking && conn->has_piece(piece_index)) {
                availabel_peers.push_back(conn);
            }
        }

        if (availabel_peers.empty()) {
            LOGWARN("No available peers for piece {}. Returning to queue.", piece_index);
            piece_status_[piece_index] = PieceStatus::Needed;
            in_progress_pieces_.erase(piece_index);
            co_return ;
        }
    }

    uint32_t num_blocks = (piece_size + BLOCK_SIZE - 1) / BLOCK_SIZE;
    for (uint32_t block_idx = 0; block_idx < num_blocks; ++block_idx) {
        uint32_t offset = block_idx * BLOCK_SIZE;
        uint32_t length = (block_idx == num_blocks - 1) 
            ? (piece_size - offset)
            : BLOCK_SIZE;
        auto& peer_conn = availabel_peers[block_idx % availabel_peers.size()];
        co_await send_request_message(*peer_conn, piece_index, offset, length);
    }
    
}

asio::awaitable<void> PeerLogic::handle_piece_message(std::shared_ptr<PeerConnection> conn, std::span<const char> payload) {
    if (payload.size() < 8) {
        co_return ;
    }

    BufferReader reader(payload);
    uint32_t index = asio::detail::socket_ops::network_to_host_long(reader.read<uint32_t>());
    uint32_t begin = asio::detail::socket_ops::network_to_host_long(reader.read<uint32_t>());
    auto block = reader.read_bytes(reader.remaining());

    LOGDBG("Received block for piece {}, offset {}, from peer {}", index, begin, conn->peer_id);

    conn->bytes_downloaded += block.size();

    co_await asio::dispatch(strand_, asio::use_awaitable);

    auto it = in_progress_pieces_.find(index);
    if (it != in_progress_pieces_.end()) {
        auto& p = it->second;
        uint32_t block_index = begin / BLOCK_SIZE;

        if (block_index < p.blocks_received.size() && !p.blocks_received[block_index]) {
            std::copy(block.begin(), block.end(), p.data.data() + begin);
            p.blocks_received[block_index] = true;
            ++p.received_count;

            if (p.received_count == p.total_blocks) {
                std::string received_hash_hex = Crypto::calculate_data_hash(p.data);
                if (received_hash_hex == meta_info_.get_torrent_info().piece_hashes[index]) {
                    co_await handle_completed_piece(index);
                } else {
                    LOGERR("Hash mismatch for piece {}. Returning to queue.", index);
                    co_await return_piece_to_queue(index);
                }
                in_progress_pieces_.erase(it);
            }
        }
    }
}

asio::awaitable<void> PeerLogic::handle_completed_piece(int piece_index) {
    const auto& info = meta_info_.get_torrent_info();
    const auto& piece_data = in_progress_pieces_.at(piece_index).data;
    uint64_t offset = static_cast<uint64_t>(piece_index) * info.piece_size;

    try {
        co_await async_write_to_file(data_file_path_, offset, piece_data);
    } catch (const std::exception& e) {
        LOGCRITICAL("Failed to write piece {} to disk: {}", piece_index, e.what());
        co_return; 
    }

    co_await asio::dispatch(strand_, asio::use_awaitable);
    piece_status_[piece_index] = PieceStatus::Have;
    ++pieces_done_count_;

    float progress = (static_cast<float>(pieces_done_count_) / info.piece_hashes.size()) * 100.0f;
    LOGINFO("Piece {} downloaded and verified. Progress: {:.2f}% ({}/{})", 
            piece_index, progress, pieces_done_count_.load(), info.piece_hashes.size());

    co_await send_have_message_to_all(piece_index);

    if (pieces_done_count_ == info.piece_hashes.size()) {
        is_download_complete_ = true;
        LOGINFO("🎉 Download complete! File saved to {}", data_file_path_.string());
        completion_timer_.cancel();
    }
}

asio::awaitable<void> PeerLogic::return_piece_to_queue(int piece_index) {
    piece_status_[piece_index] = PieceStatus::Needed;
    in_progress_pieces_.erase(piece_index);
    co_return ;
}

asio::awaitable<void> PeerLogic::handle_request_message(std::shared_ptr<PeerConnection> conn, std::span<const char> payload) {
    auto self = shared_from_this();

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
        uint64_t file_offset = static_cast<uint64_t>(req.index) * meta_info_.get_torrent_info().piece_size + req.begin;
        block_data = co_await async_read_from_file(data_file_path_, file_offset, req.length);
    } catch (const std::exception& e) {
        LOGERR("File read error for request: {}", e.what());
        co_return;
    }
    
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

    return asio::async_initiate<asio::use_awaitable_t<>, void(std::error_code)>(
        [this, path = std::move(path), offset, data] (auto&& completion_handler) {
            
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

    return asio::async_initiate<asio::use_awaitable_t<>, void(std::error_code, std::vector<char>)>(
        [this, path = std::move(path), offset, size] (auto&& completion_handler) {

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
                    std::vector<char> block_data(size);
                    data_file.seekg(offset);
                    data_file.read(block_data.data(), size);
                    if (static_cast<std::size_t>(data_file.gcount()) != size) {
                        throw std::runtime_error("Incomplete read from file: " + path.string());
                    }
                    
                    std::move(handler)(std::error_code{}, std::move(block_data));
                } catch (const std::exception& e) {
                    LOGERR("File read error: {}", e.what());
                    std::move(handler)(make_error_code(std::errc::io_error), std::vector<char>());
                }
        
            });
        },
        token
    );      
}

Seeder::Seeder(asio::io_context& io_context, std::string peer_id, const std::filesystem::path& torrent_path, const std::filesystem::path& content_dir, int peer_port)
    : PeerLogic(io_context, std::move(peer_id), torrent_path), peer_port_(peer_port) {

    const auto& info = meta_info_.get_torrent_info();
    piece_status_.assign(info.piece_hashes.size(), PieceStatus::Have);
    pieces_done_count_ = info.piece_hashes.size();
    is_download_complete_ = true;

    data_file_path_ = content_dir / info.file_name;

    if (!std::filesystem::exists(data_file_path_)) {
        throw std::runtime_error("Data file not found: " + data_file_path_.string());
    }

    LOGINFO("Seeder initialized for '{}'", info.file_name);
}

asio::awaitable<void> Seeder::run(const std::string& tracker_host, int tracker_port) {
    try {
        AsyncSocket tracker_socket(tcp::socket{io_context_});
        co_await tracker_socket.connect(tracker_host, tracker_port);

        TrackerAnnouceReqeust req;
        req.info_hash_bytes = info_hash_bytes_;
        req.port = peer_port_;

        auto request = TrackerAnnouceReqeust::serialize(req);

        co_await tracker_socket.send_message(request);

        auto response = co_await tracker_socket.receive_message();
        if (response.empty() || static_cast<TrackerMessageType>(response[0]) != TrackerMessageType::AnnounceResponse) {
            LOGWARN("Tracker did not acknowledge Announce request correctly.");
        } else {
            LOGINFO("Tracker successfully acknowledged Announce.");
        }

    } catch (const std::exception& e) {
        LOGCRITICAL("Failed to register with tracker: {}", e.what());
        co_return ;
    }

    try {
        AsyncServerSocket seeder_server(io_context_, peer_port_);
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

Leecher::Leecher(asio::io_context& io_context, std::string peer_id, const std::filesystem::path& torrent_path, const std::filesystem::path& save_path)
    : PeerLogic(io_context, std::move(peer_id), torrent_path) { 

    data_file_path_ = save_path;

    const auto& info = meta_info_.get_torrent_info();
    const size_t num_pieces = info.piece_hashes.size();

    piece_status_.resize(num_pieces, PieceStatus::Needed);

    LOGINFO("Leecher initialized for '{}', {} pieces to download.", info.file_name, num_pieces);
}


asio::awaitable<bool> Leecher::run(const std::string& tracker_host, int tracker_port) {
    const auto& info = meta_info_.get_torrent_info();
    std::vector<std::string> peer_list;
    
    try {
        AsyncSocket tracker_socket(tcp::socket{io_context_});
        co_await tracker_socket.connect(tracker_host, tracker_port);

        TrackerQueryRequest req;
        req.info_hash_bytes = info_hash_bytes_;
        auto request = TrackerQueryRequest::serialize(req);

        co_await tracker_socket.send_message(request);

        auto res = co_await tracker_socket.receive_message();
        if (res.empty()) {
            throw std::runtime_error("Received empty response from tracker");
        }

        auto msg_type = static_cast<TrackerMessageType>(res[0]);
        std::span<const char> payload(res.data() + 1, res.size() - 1);

        if (msg_type == TrackerMessageType::QueryResponse) {
            auto response = TrackerQueryResponse::deserialize(payload);
            peer_list = std::move(response.peer_addrs);
        } else {
            throw std::runtime_error("Tracker returned an unexpected message type.");
        }

        if (peer_list.empty()) {
            LOGWARN("No peers found. Cannot start download.");
            co_return false;
        }

    } catch (const std::exception& e) {
        LOGCRITICAL("Could not get peer list from tracker: {}", e.what());
        co_return false;
    }

    try {
        if (data_file_path_.has_parent_path()) {
            std::filesystem::create_directories(data_file_path_.parent_path());
        }
        std::ofstream output_file(data_file_path_, std::ios::binary | std::ios::trunc);
        if(info.file_size > 0) {
            output_file.seekp(info.file_size - 1);
            output_file.write("", 1);
        }
    } catch (const std::exception& e) {
        LOGCRITICAL("Failed to create/pre-allocate output file {}: {}", data_file_path_.string(), e.what());
        co_return false;
    }

    LOGINFO("Found {} peers. Starting connections.", peer_list.size());

    
    asio::co_spawn(io_context_, choke_loop(), asio::detached);
    asio::co_spawn(io_context_, downloader_loop(), asio::detached);

    for (const auto& peer_addr : peer_list) {
        asio::co_spawn(io_context_,
            connect_to_peer(peer_addr),
            asio::detached
        );
    }

    try {
        co_await completion_timer_.async_wait(asio::use_awaitable);
    } catch (const asio::system_error& e) {
        if (e.code() != asio::error::operation_aborted) {
            LOGERR("Completion timer failed with unexpected error: {}", e.what());
        }
    }

    co_return is_download_complete_;
}

asio::awaitable<void> Leecher::connect_to_peer(const std::string& peer_addr) {
    try {
        size_t colon_pos = peer_addr.find(':');
        if (colon_pos == std::string::npos) {
            LOGWARN("Invalid peer address format: {}", peer_addr);
            co_return;
        }
        std::string ip = peer_addr.substr(0, colon_pos);
        int peer_port = std::stoi(peer_addr.substr(colon_pos+1));
 
        AsyncSocket peer_socket(tcp::socket{io_context_});
        co_await peer_socket.connect(ip, peer_port);
        LOGINFO("Successfully connected to peer {}", peer_addr);
 
        co_await handle_new_connection(std::move(peer_socket), peer_addr);
        
    } catch (const std::exception& e) {
        LOGWARN("Failed to connect or lost connection to peer {}: {}", peer_addr, e.what());
    }
}
