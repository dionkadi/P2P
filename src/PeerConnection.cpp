#include "PeerConnection.hpp"
#include "Bencode.hpp"
#include "Utils.hpp"

#include <algorithm>

asio::awaitable<std::shared_ptr<PeerConnection>> 
PeerConnection::create(
    asio::io_context& io_context, AsyncSocket socket, std::string peer_addr,
    const PeerId& my_id, std::shared_ptr<SessionState> state,
    std::shared_ptr<IPeerConnectionEvents> events
) {
    struct EnableMakeShared : public PeerConnection {
        EnableMakeShared(
            asio::io_context& io_context, AsyncSocket socket, std::string peer_addr, 
            std::shared_ptr<SessionState> state, std::shared_ptr<IPeerConnectionEvents> events
        ) : PeerConnection(
                io_context, std::move(socket), std::move(peer_addr), 
                std::move(state), std::move(events)
            ) 
        {}
    };

    auto conn = std::make_shared<EnableMakeShared>(
        io_context, std::move(socket), std::move(peer_addr), 
        std::move(state), std::move(events)
    );

    LOGDBG("PeerConnection object created for {}. About to call perform_handshake.", conn->peer_addr());
    bool success = co_await conn->perform_handshake(my_id);
    if (!success) {
        LOGERR("Failed to handshake for peer {}", conn->peer_addr());
        co_return nullptr;
    }

    LOGINFO("Handshake successful with peer ID: {} (for address {})", conn->peer_id(), conn->peer_addr());

    conn->start_loops();
    LOGDBG("Message and keep-alive loops started for {}", conn->peer_id());

    conn->upload_limiter_ = std::make_shared<AsyncRateLimiter<>>(io_context, 10 * 1024 * 1024);
    conn->download_limiter_ = std::make_shared<AsyncRateLimiter<>>(io_context, 10 * 1024 * 1024);

    co_return conn;
}

PeerConnection::PeerConnection(
    asio::io_context& io_context, AsyncSocket socket, std::string peer_addr,
    std::shared_ptr<SessionState> state, std::shared_ptr<IPeerConnectionEvents> events
) noexcept : io_context_(io_context),
    strand_(asio::make_strand(io_context_)),
    keep_alive_timer_(strand_),
    socket_(std::move(socket)),
    peer_addr_(std::move(peer_addr)),
    state_(std::move(state)),
    events_(std::move(events)),
    supported_pex_(false)
{}

asio::awaitable<bool> PeerConnection::perform_handshake(const PeerId& my_id) {
    try {
        LOGDBG("Starting handshake with peer {}", peer_addr_);
        const auto& info_hash_vec = state_->info_hash();
        InfoHash info_hash_arr{};
        std::ranges::copy(info_hash_vec, info_hash_arr.begin());
        Handshake my_handshake {
            .info_hash_bytes = info_hash_arr,
            .peer_id_bytes = my_id,
            .extended = true,
            .fast_extension = true
        };

        co_await socket_.send_raw(my_handshake.serialize());

        std::vector<std::byte> handshake_buffer = co_await socket_.receive_raw(HANDSHAKE_BASE_LEN);
        Handshake peer_handshake = Handshake::deserialize(handshake_buffer);

        if (peer_handshake.info_hash_bytes != info_hash_arr) {
            LOGERR("Info hash mismatch with {}. Expected {}, got {}.",
                   peer_addr_, Crypto::bytes_to_hex(info_hash_arr), Crypto::bytes_to_hex(peer_handshake.info_hash_bytes));
            throw std::runtime_error("Info hash mismatch");
        }

        peer_id_ = peer_handshake.peer_id_bytes;
        
        if (peer_id_ == my_id) {
            LOGWARN("Connected to self ({}:{}). Dropping connection.", peer_addr_, Crypto::bytes_to_hex(my_id));
            co_return false;
        }
        
        LOGDBG("Peer {} handshake successful. Peer ID: {}. Checking for extended handshake support.", peer_addr_, peer_id_);
        
        if (peer_handshake.fast_extension) {
            LOGINFO("Peer {} supports fast extension (BEP-6).", peer_id_);
            fast_extension_supported_ = true;
        }
        
        if (peer_handshake.extended) {
            LOGINFO("Peer {} supports extended plugins. Sending extended handshake.", peer_id_);

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
            
            co_await socket_.send_message(message);
            LOGDBG("Extended handshake sent to {}", peer_id_);
        }

        // Send have-none, have-all, or bitfield based on our state
        {
            size_t completed = state_->completed_pieces();
            size_t total = state_->num_pieces();
            if (total == 0) {
                // Metadata not yet available; send have-none
                co_await send_simple_message(MessageType::HaveNone);
            } else if (completed == 0) {
                co_await send_simple_message(MessageType::HaveNone);
            } else if (completed == total) {
                co_await send_simple_message(MessageType::HaveAll);
            } else {
                std::vector<uint8_t> bitfield((total + 7) / 8, 0);
                std::string have_bitfield_str = state_->get_have_bitfield_str();
                std::ranges::copy(have_bitfield_str, bitfield.begin());
                co_await send_bitfield(bitfield);
            }
        }

        co_return true;
    } catch (const boost::system::system_error& e) {
        if (e.code() != asio::error::eof && e.code() != asio::error::connection_reset && e.code() != asio::error::operation_aborted) {
            LOGERR("Network error with {}: {}", peer_addr_, e.what());
        } else {
            LOGDBG("Connection to {} closed gracefully during handshake: {}", peer_addr_, e.what());
        }
    } catch (const std::exception& e) {
        LOGERR("Exception with {}: {}", peer_addr_, e.what());
    }
    co_return false;
}

void PeerConnection::start_loops() {
    auto self = shared_from_this();
    asio::co_spawn(
        strand_, 
        [self] () -> asio::awaitable<void> {
            co_await self->message_loop();
        }, 
        asio::detached
    );
    asio::co_spawn(
        strand_, 
        [self] () -> asio::awaitable<void> {
            co_await self->keep_alive_loop();
        }, 
        asio::detached
    );
}

asio::awaitable<void> PeerConnection::message_loop() {
    auto self = shared_from_this();
    try {
        while (true) {
            std::vector<std::byte> msg = co_await socket_.receive_message();
            if (msg.empty()) {
                continue ;  // keep-alive
            }

            MessageType type = static_cast<MessageType>(msg[0]);
            std::span<const std::byte> payload(msg.data() + 1, msg.size() - 1);

            switch (type) {
                case MessageType::Choke:
                    // LOGDBG("Peer {} sent CHOKE. Setting peer_is_choking to true.", peer_id_);
                    peer_is_choking_.store(true, std::memory_order_relaxed);
                    co_await events_->on_choke_status_changed(self, true);
                    break;
                case MessageType::Unchoke: {
                    // LOGDBG("Peer {} sent UNCHOKE. Setting peer_is_choking to false.", peer_id_);
                    peer_is_choking_.store(false, std::memory_order_relaxed);
                    co_await events_->on_choke_status_changed(self, false);
                    break;
                }
                case MessageType::Interested:
                    // LOGDBG("Peer {} sent INTERESTED. Setting peer_is_interested to true.", peer_id_);
                    peer_is_interested_.store(true, std::memory_order_relaxed);
                    break;
                case MessageType::NotInterested:  
                    // LOGDBG("Peer {} sent NOT INTERESTED. Setting peer_is_interested to false.", peer_id_);
                    peer_is_interested_.store(false, std::memory_order_relaxed);
                    break;
                case MessageType::Bitfield: {
                    co_await events_->on_peer_bitfield(self, payload);
                    break;
                }
                case MessageType::Have: {
                    if (payload.size() < sizeof(uint32_t)) {
                        break;
                    }
                    BufferReader reader(payload);
                    uint32_t index = asio::detail::socket_ops::network_to_host_long(reader.read<uint32_t>());
                    co_await events_->on_peer_has_piece(self, index);
                    break;
                }
                case MessageType::Piece: {
                    if (payload.size() < 8) {
                        break;
                    }
                    BufferReader piece_reader(payload);
                    uint32_t piece_index = asio::detail::socket_ops::network_to_host_long(piece_reader.read<uint32_t>());
                    uint32_t piece_begin = asio::detail::socket_ops::network_to_host_long(piece_reader.read<uint32_t>());
                    uint32_t block_length = asio::detail::socket_ops::network_to_host_long(piece_reader.read<uint32_t>());
                    std::span<const std::byte> block_data = piece_reader.read_all();                 
                    if (block_data.size() != block_length) {
                        LOGERR("Received Piece message block data size mismatch. Expected {}, got {} for piece {} offset {}.",
                               block_length, block_data.size(), piece_index, piece_begin);
                        break;
                    }
                    // PIECE received — a request slot just freed up
                    on_request_completed(block_length);
                    co_await download_limiter_->await_tokens(block_length);
                    co_await events_->on_piece_block(self, piece_index, piece_begin, block_data);
                    break;
                }
                case MessageType::Cancel: {
                    // Handle cancel if needed (e.g., remove from upload queue, but for now ignore)
                    break;
                }
                case MessageType::ExtendedMessage: {
                    co_await events_->on_extended_message(self, payload);
                    break;
                }
                case MessageType::Request: {
                    if (payload.size() < 12) {
                        break;
                    }
                    RequestPayload req = RequestPayload::deserialize(payload);
                    // LOGDBG("Peer {} sent REQUEST for piece {} begin {} length {}. Current state: am_choking={}, peer_is_interested={}", 
                    //        peer_id_, req.index, req.begin, req.length, am_choking_.load(), peer_is_interested_.load());
                    co_await events_->on_block_request(self, req.index, req.begin, req.length);
                    break;
                }
                case MessageType::Reject: {
                    if (payload.size() < 12) {
                        break;
                    }
                    RequestPayload req = RequestPayload::deserialize(payload);
                    // REJECT frees a request slot just like a completed PIECE
                    on_request_rejected(req.length);
                    co_await events_->on_piece_rejected(self, req.index, req.begin, req.length);
                    break;
                }
                default:
                    LOGWARN("Received unhandled message type: {}", static_cast<int>(type));
            }
        }
    } catch (const std::exception& e) {
        LOGINFO("Connection to {} lost or closed: {}", peer_id_, e.what());
    }

    co_await events_->on_disconnect(self);
    co_return ;
}

asio::awaitable<void> PeerConnection::keep_alive_loop() {
    auto self = shared_from_this();

    while (true) {
        keep_alive_timer_.expires_after(std::chrono::minutes(2));
        auto [ec] = co_await keep_alive_timer_.async_wait(asio::as_tuple(asio::use_awaitable));
        if (ec == asio::error::operation_aborted) {
            LOGDBG("Keep-alive timer for {} aborted. Exiting loop.", peer_id_);
            co_return;
        }
        if (ec) {
            LOGWARN("Keep-alive timer for {} failed: {}", peer_id_, ec.message());
            break;
        }

        try {
            co_await socket_.send_message({});
        } catch (const boost::system::system_error& e) {
            if (e.code() == asio::error::eof ||
                e.code() == asio::error::connection_reset ||
                e.code() == asio::error::broken_pipe ||
                e.code() == asio::error::operation_aborted)
            {
                LOGDBG("Failed to send keep-alive to peer {}, it has disconnected.", peer_id_);
            } else {
                LOGWARN("Failed to send keep-alive to peer {}, closing connection: {}", peer_id_, e.what());
            }
            break;
        } catch (const std::exception& e) { // Catch other standard exceptions
            LOGWARN("Failed to send keep-alive to peer {}, closing connection (exception: {}).", peer_id_, e.what());
            break;
        } catch (...) { // Fallback for any other exception
            LOGWARN("Failed to send keep-alive to peer {}, closing connection (unknown exception).", peer_id_);
            break;
        }
    }

    socket_.close();
    co_await events_->on_disconnect(self);
    co_return ;
}

asio::awaitable<void> PeerConnection::send_simple_message(MessageType type) {
    std::vector<std::byte> msg_body(1, static_cast<std::byte>(type));
    co_await socket_.send_message(msg_body);
}

asio::awaitable<bool> PeerConnection::send_request(size_t index, uint32_t begin, uint32_t length) {
    // Check pipeline limits
    uint64_t pipeline_bytes = total_bytes_requested_ - total_bytes_received_;
    if (outstanding_request_count_ >= max_outstanding_requests_ ||
        pipeline_bytes >= MAX_PIPELINE_BUFFER) {
        // At limit — queue for later
        pending_requests_.push_back({static_cast<uint32_t>(index), begin, length});
        co_return false;
    }

    // Slot available — send immediately
    std::vector<std::byte> msg_body(1, static_cast<std::byte>(MessageType::Request));
    auto payload = RequestPayload::serialize(index, begin, length);
    msg_body.insert(msg_body.end(), payload.begin(), payload.end());
    co_await socket_.send_message(msg_body);

    total_bytes_requested_ += length;
    ++outstanding_request_count_;
    co_return true;
}

void PeerConnection::flush_pending_requests() {
    while (!pending_requests_.empty()) {
        uint64_t pipeline_bytes = total_bytes_requested_ - total_bytes_received_;
        if (outstanding_request_count_ >= max_outstanding_requests_ ||
            pipeline_bytes >= MAX_PIPELINE_BUFFER) {
            break;  // still at limit
        }

        auto req = pending_requests_.front();
        pending_requests_.pop_front();

        // Spawn the actual send (non-blocking)
        auto self = shared_from_this();
        asio::co_spawn(strand_,
            [self, req]() -> asio::awaitable<void> {
                std::vector<std::byte> msg_body(1, static_cast<std::byte>(MessageType::Request));
                auto payload = RequestPayload::serialize(req.index, req.begin, req.length);
                msg_body.insert(msg_body.end(), payload.begin(), payload.end());
                co_await self->socket_.send_message(msg_body);
            },
            asio::detached
        );

        total_bytes_requested_ += req.length;
        ++outstanding_request_count_;
    }
}

void PeerConnection::on_request_completed(uint32_t length) {
    total_bytes_received_ += length;
    if (outstanding_request_count_ > 0) {
        --outstanding_request_count_;
    }
    flush_pending_requests();
}

void PeerConnection::on_request_rejected(uint32_t length) {
    // A rejected request never fulfilled — remove from requested bytes
    if (total_bytes_requested_ >= length) {
        total_bytes_requested_ -= length;
    } else {
        total_bytes_requested_ = 0;
    }
    if (outstanding_request_count_ > 0) {
        --outstanding_request_count_;
    }
    flush_pending_requests();
}

asio::awaitable<void> PeerConnection::send_piece(size_t index, uint32_t begin, std::span<const std::byte> block_data) {
    co_await upload_limiter_->await_tokens(block_data.size());
    std::vector<std::byte> msg_body(1, static_cast<std::byte>(MessageType::Piece));
    auto payload = RequestPayload::serialize(index, begin, block_data.size());
    msg_body.insert(msg_body.end(), payload.begin(), payload.end());
    msg_body.insert(msg_body.end(), block_data.begin(), block_data.end());
    co_await socket_.send_message(msg_body);
}

asio::awaitable<void> PeerConnection::send_bitfield(const std::vector<uint8_t>& bitfield_data) {
    std::vector<std::byte> msg_body(1, static_cast<std::byte>(MessageType::Bitfield));
    msg_body.insert(msg_body.end(), reinterpret_cast<const std::byte*>(bitfield_data.data()), reinterpret_cast<const std::byte*>(bitfield_data.data() + bitfield_data.size()));
    co_await socket_.send_message(msg_body);
}

asio::awaitable<void> PeerConnection::send_cancel(size_t index, uint32_t begin, uint32_t length) {
    auto it = std::ranges::find_if(pending_requests_, [&](const RequestPayload& r) {
        return r.index == index && r.begin == begin && r.length == length;
    });
    if (it != pending_requests_.end()) {
        // Request was never sent (still queued) — just remove it, no wire cancel needed
        pending_requests_.erase(it);
        co_return;
    }

    // Request was already sent — adjust pipeline tracking and send cancel on wire
    if (outstanding_request_count_ > 0) {
        --outstanding_request_count_;
    }
    if (total_bytes_requested_ >= length) {
        total_bytes_requested_ -= length;
    } else {
        total_bytes_requested_ = 0;
    }

    std::vector<std::byte> msg_body(1, static_cast<std::byte>(MessageType::Cancel));
    auto payload = RequestPayload::serialize(index, begin, length);
    msg_body.insert(msg_body.end(), payload.begin(), payload.end());
    co_await socket_.send_message(msg_body);
}

asio::awaitable<void> PeerConnection::send_reject(size_t index, uint32_t begin, uint32_t length) {
    std::vector<std::byte> msg_body(1, static_cast<std::byte>(MessageType::Reject));
    auto payload = RequestPayload::serialize(index, begin, length);
    msg_body.insert(msg_body.end(), payload.begin(), payload.end());
    co_await socket_.send_message(msg_body);
}

asio::awaitable<void> PeerConnection::send_have(size_t index) {
    std::vector<std::byte> have_msg(1, static_cast<std::byte>(MessageType::Have));
    BufferWriter writer(have_msg);
    writer.write(asio::detail::socket_ops::host_to_network_long(static_cast<uint32_t>(index)));
    co_await socket_.send_message(have_msg);
}

asio::awaitable<void> PeerConnection::send_extended_message(uint8_t type_id, std::span<const std::byte> payload) {
    std::vector<std::byte> msg_body;
    msg_body.push_back(static_cast<std::byte>(MessageType::ExtendedMessage));
    msg_body.push_back(static_cast<std::byte>(type_id));
    msg_body.insert(msg_body.end(), payload.begin(), payload.end());
    co_await socket_.send_message(msg_body);
}

asio::awaitable<void> PeerConnection::send_metadata_request(uint8_t ext_id, int piece) {
    Dict msg_dict;
    msg_dict["msg_type"] = Value(static_cast<Integer>(0));
    msg_dict["piece"] = Value(static_cast<Integer>(piece));
    auto encoded = encode(Value(std::move(msg_dict)));
    co_await send_extended_message(ext_id, encoded);
}

void PeerConnection::set_upload_rate(uint64_t bps) noexcept {
    if (upload_limiter_) {
        upload_limiter_->set_rate(bps);
    }
}

void PeerConnection::set_download_rate(uint64_t bps) noexcept {
    if (download_limiter_) {
        download_limiter_->set_rate(bps);
    }
}