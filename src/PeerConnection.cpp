#include "PeerConnection.hpp"
#include "Bencode.hpp"
#include "Utils.hpp"

#include <algorithm>

asio::awaitable<std::shared_ptr<PeerConnection>> 
PeerConnection::create(
    asio::io_context& io_context, AsyncSocket socket, std::string peer_addr,
    const PeerId& my_id, std::shared_ptr<SessionState> state,
    std::shared_ptr<IPeerConnectionEvents> events,
    bool mse_enabled, bool inbound
) {
    CTRACK_ASYNC("PeerConnection::create");
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
    conn->mse_enabled_ = mse_enabled;
    conn->inbound_ = inbound;

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

PeerConnection::~PeerConnection() {
    if (upload_limiter_) {
        upload_limiter_->stop();
    }
    if (download_limiter_) {
        download_limiter_->stop();
    }
}

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

        std::vector<std::byte> handshake_buffer;
        bool handshake_sent = false;

        if (mse_enabled_) {
            if (inbound_) {
                // Responder: detect MSE vs plaintext by the peer's first byte.
                auto [result, peer_hs] = co_await socket_.mse_handshake_acceptor(info_hash_arr);
                if (result == AsyncSocket::MseResult::Failed) {
                    LOGWARN("MSE acceptor handshake failed for {}", peer_addr_);
                    co_return false;
                }
                if (result != AsyncSocket::MseResult::Plaintext) {
                    // MSE completed: the peer's handshake was decrypted and
                    // returned; send ours now (encrypted if RC4 was selected).
                    co_await socket_.send_raw(my_handshake.serialize());
                    handshake_buffer = std::move(peer_hs);
                    handshake_sent = true;
                }
                // Plaintext: the first byte stays buffered; fall through to
                // the plaintext path below.
            } else {
                // Initiator: MSE first, plaintext fallback on a fresh
                // connection (matches libtorrent's pe_enabled behavior).
                auto [result, peer_hs] = co_await socket_.mse_handshake_initiator(
                    info_hash_arr, my_handshake.serialize());
                if (result != AsyncSocket::MseResult::Failed) {
                    // Our handshake was already sent inside the MSE exchange.
                    handshake_buffer = std::move(peer_hs);
                    handshake_sent = true;
                } else {
                    LOGDBG("MSE initiator failed for {}; retrying plaintext on a fresh connection", peer_addr_);
                    socket_.close();
                    auto [ip, port] = decode_address(peer_addr_);
                    co_await socket_.connect(ip, port);
                }
            }
        }

        if (!handshake_sent) {
            co_await socket_.send_raw(my_handshake.serialize());
            handshake_buffer = co_await socket_.receive_raw(HANDSHAKE_BASE_LEN);
        }

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
            ehs_dict["v"] = Value("qBittorrent/5.2.3");

            auto encoded_payload = encode(Value(ehs_dict));
            message.insert(message.end(), encoded_payload.begin(), encoded_payload.end());
            
            co_await socket_.send_message(message);
            LOGDBG("Extended handshake sent to {}", peer_id_);
        }

        // Send have-none, have-all, or bitfield based on our state.
        // BEP-6: HaveNone/HaveAll are only legal when the peer advertised
        // fast-extension support; otherwise a strict peer will drop us. Peers
        // without fast extension always get a real bitfield (BEP-3), which is
        // omitted entirely when we hold no pieces (also legal per BEP-3).
        {
            size_t completed = state_->completed_pieces();
            size_t total = state_->num_pieces();

            bool should_send_bitfield = false;
            bool send_have_none_or_all = fast_extension_supported_;

            if (send_have_none_or_all) {
                if (total == 0 || completed == 0) {
                    // Metadata not yet available or no pieces; send have-none
                    co_await send_simple_message(MessageType::HaveNone);
                } else if (completed == total) {
                    co_await send_simple_message(MessageType::HaveAll);
                } else {
                    should_send_bitfield = true;
                }
            } else if (total > 0) {
                should_send_bitfield = true;
            }

            if (should_send_bitfield) {
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
    CTRACK_ASYNC("PeerConnection::message_loop");
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
                    LOGDBG("Peer {} sent CHOKE. Setting peer_is_choking to true.", peer_id_);
                    peer_is_choking_.store(true, std::memory_order_relaxed);
                    co_await events_->on_choke_status_changed(self, true);
                    // Free the pipeline accounting, not just the unsent queue.
                    // Requests that were on the wire when the peer choked will
                    // never be fulfilled, and non-BEP-6 peers never send REJECT
                    // to free their slots — so outstanding_request_count_ and
                    // total_bytes_requested_ would stay pinned at the cap and
                    // every future send_request/flush_pending_requests would
                    // stall (the pipeline gate never clears). Blocks assigned
                    // to this peer would sit queued-and-unsent forever: with
                    // request_times only armed on an actual write, the timeout
                    // checker skips them, pieces stall, and the whole window
                    // deadlocks into 0 B/s. The piece resume path (invoked via
                    // on_choke_status_changed above) re-requests those blocks
                    // elsewhere. Late-arriving blocks are harmless: the
                    // completion path clamps against total_bytes_requested_.
                    {
                        std::lock_guard lock(pipeline_mutex_);
                        pending_requests_.clear();
                        outstanding_request_count_ = 0;
                        total_bytes_requested_ = 0;
                        total_bytes_received_ = 0;
                    }
                    break;
                case MessageType::Unchoke: {
                    LOGDBG("Peer {} sent UNCHOKE. Setting peer_is_choking to false.", peer_id_);
                    peer_is_choking_.store(false, std::memory_order_relaxed);
                    last_unchoke_time_ = std::chrono::steady_clock::now();
                    {
                        std::lock_guard lock(pipeline_mutex_);
                        flush_pending_requests();
                    }
                    co_await events_->on_choke_status_changed(self, false);
                    break;
                }
                case MessageType::Interested:
                    LOGDBG("Peer {} sent INTERESTED. Setting peer_is_interested to true.", peer_id_);
                    peer_is_interested_.store(true, std::memory_order_relaxed);
                    if (interest_change_hook_) {
                        interest_change_hook_();
                    }
                    break;
                case MessageType::NotInterested:
                    LOGDBG("Peer {} sent NOT INTERESTED. Setting peer_is_interested to false.", peer_id_);
                    peer_is_interested_.store(false, std::memory_order_relaxed);
                    if (interest_change_hook_) {
                        interest_change_hook_();
                    }
                    break;
                case MessageType::HaveAll: {
                    co_await events_->on_peer_has_all(self);
                    break;
                }
                case MessageType::HaveNone: {
                    co_await events_->on_peer_has_none(self);
                    break;
                }
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
                    std::span<const std::byte> block_data = piece_reader.read_all();
                    uint32_t block_length = static_cast<uint32_t>(block_data.size());
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
                case MessageType::Port: {
                    // DHT port message (BEP 14) — ignore for now
                    break;
                }
                case MessageType::AllowedFast: {
                    // BEP 6 — peer indicates we can download from them even while choked
                    // For now, just ignore
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

    // The connection is dead, so tear down the keep-alive loop as well.
    // Otherwise its `self = shared_from_this()` keeps this PeerConnection
    // (and through events_ the whole session) alive until the next 2-minute
    // tick. If this conn is no longer in the PeerManager map (peer dropped
    // earlier), nothing else will ever close its socket or cancel this timer,
    // so the entire session graph leaks at process exit (LSan: indirect leaks
    // with no direct root — a pure shared_ptr cycle).
    keep_alive_timer_.cancel();
    co_await events_->on_disconnect(self);
    co_return ;
}

asio::awaitable<void> PeerConnection::keep_alive_loop() {
    CTRACK_ASYNC("PeerConnection::keep_alive_loop");
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
    // Keep the connection alive while the write is in flight: with the
    // serialized write queue, this coroutine (or the queue drain it owns)
    // may be suspended across socket close, and a raw `this` would dangle.
    auto self = shared_from_this();
    (void)self;
    std::vector<std::byte> msg_body(1, static_cast<std::byte>(type));
    co_await socket_.send_message(msg_body);
}

asio::awaitable<bool> PeerConnection::send_request(size_t index, uint32_t begin, uint32_t length) {
    // Keep the connection alive while the write is in flight (see
    // send_simple_message).
    auto self = shared_from_this();
    (void)self;
    {
        std::lock_guard lock(pipeline_mutex_);

        uint64_t pipeline_bytes = total_bytes_requested_ - total_bytes_received_;
        if (outstanding_request_count_ >= max_outstanding_requests_ ||
            pipeline_bytes >= MAX_PIPELINE_BUFFER) {
            pending_requests_.push_back({static_cast<uint32_t>(index), begin, length});
            co_return false;
        }

        total_bytes_requested_ += length;
        ++outstanding_request_count_;
    }

    std::vector<std::byte> msg_body(1, static_cast<std::byte>(MessageType::Request));
    auto payload = RequestPayload::serialize(index, begin, length);
    msg_body.insert(msg_body.end(), payload.begin(), payload.end());
    try {
        co_await socket_.send_message(msg_body);
        notify_request_sent(static_cast<uint32_t>(index), begin, length);
    } catch (...) {
        std::lock_guard lock(pipeline_mutex_);
        if (total_bytes_requested_ >= length) {
            total_bytes_requested_ -= length;
        } else {
            total_bytes_requested_ = 0;
        }
        if (outstanding_request_count_ > 0) {
            --outstanding_request_count_;
        }
        flush_pending_requests();
        throw;
    }

    co_return true;
}

void PeerConnection::flush_pending_requests() {
    // Caller MUST hold pipeline_mutex_ — this is called from
    // on_request_completed/on_request_rejected which already hold it.
    // Do NOT lock here.

    if (peer_is_choking_.load(std::memory_order_relaxed)) {
        return;
    }

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
                self->notify_request_sent(req.index, req.begin, req.length);
            },
            asio::detached
        );

        total_bytes_requested_ += req.length;
        ++outstanding_request_count_;
    }
}

void PeerConnection::on_request_completed(uint32_t length) {
    std::lock_guard lock(pipeline_mutex_);

    // Endgame re-requests can deliver duplicate blocks (or blocks for
    // already-complete pieces) whose request was never counted in
    // total_bytes_requested_. Crediting them unconditionally makes
    // total_bytes_received_ exceed total_bytes_requested_, and the
    // unsigned pipeline_bytes = requested - received underflows to ~2^64,
    // permanently blocking flush_pending_requests (pipeline looks "full"
    // forever) -> silent download stall. Clamp to the requested total.
    if (total_bytes_received_ + length > total_bytes_requested_) {
        total_bytes_received_ = total_bytes_requested_;
    } else {
        total_bytes_received_ += length;
    }
    if (outstanding_request_count_ > 0) {
        --outstanding_request_count_;
    }
    flush_pending_requests();
}

void PeerConnection::on_request_rejected(uint32_t length) {
    std::lock_guard lock(pipeline_mutex_);

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
    CTRACK_ASYNC("PeerConnection::send_piece");
    auto self = shared_from_this(); // keep alive across the write (see send_simple_message)
    (void)self;
    co_await upload_limiter_->await_tokens(block_data.size());
    std::vector<std::byte> msg_body(1, static_cast<std::byte>(MessageType::Piece));
    BufferWriter writer(msg_body);
    writer.write(asio::detail::socket_ops::host_to_network_long(static_cast<uint32_t>(index)));
    writer.write(asio::detail::socket_ops::host_to_network_long(begin));
    msg_body.insert(msg_body.end(), block_data.begin(), block_data.end());
    co_await socket_.send_message(msg_body);
}

asio::awaitable<void> PeerConnection::send_bitfield(const std::vector<uint8_t>& bitfield_data) {
    auto self = shared_from_this(); // keep alive across the write (see send_simple_message)
    (void)self;
    std::vector<std::byte> msg_body(1, static_cast<std::byte>(MessageType::Bitfield));
    msg_body.insert(msg_body.end(), reinterpret_cast<const std::byte*>(bitfield_data.data()), reinterpret_cast<const std::byte*>(bitfield_data.data() + bitfield_data.size()));
    co_await socket_.send_message(msg_body);
}

asio::awaitable<void> PeerConnection::send_cancel(size_t index, uint32_t begin, uint32_t length) {
    auto self = shared_from_this(); // keep alive across the write (see send_simple_message)
    (void)self;
    bool send_wire_cancel = false;
    {
        std::lock_guard lock(pipeline_mutex_);

        auto it = std::ranges::find_if(pending_requests_, [&](const RequestPayload& r) {
            return r.index == index && r.begin == begin && r.length == length;
        });
        if (it != pending_requests_.end()) {
            pending_requests_.erase(it);
            flush_pending_requests();
            co_return;
        }

        if (outstanding_request_count_ > 0) {
            --outstanding_request_count_;
        }
        if (total_bytes_requested_ >= length) {
            total_bytes_requested_ -= length;
        } else {
            total_bytes_requested_ = 0;
        }
        flush_pending_requests();
        send_wire_cancel = true;
    }

    if (send_wire_cancel) {
        std::vector<std::byte> msg_body(1, static_cast<std::byte>(MessageType::Cancel));
        auto payload = RequestPayload::serialize(index, begin, length);
        msg_body.insert(msg_body.end(), payload.begin(), payload.end());
        co_await socket_.send_message(msg_body);
    }
}

asio::awaitable<void> PeerConnection::send_reject(size_t index, uint32_t begin, uint32_t length) {
    auto self = shared_from_this(); // keep alive across the write (see send_simple_message)
    (void)self;
    std::vector<std::byte> msg_body(1, static_cast<std::byte>(MessageType::Reject));
    auto payload = RequestPayload::serialize(index, begin, length);
    msg_body.insert(msg_body.end(), payload.begin(), payload.end());
    co_await socket_.send_message(msg_body);
}

asio::awaitable<void> PeerConnection::send_have(size_t index) {
    auto self = shared_from_this(); // keep alive across the write (see send_simple_message)
    (void)self;
    std::vector<std::byte> have_msg(1, static_cast<std::byte>(MessageType::Have));
    BufferWriter writer(have_msg);
    writer.write(asio::detail::socket_ops::host_to_network_long(static_cast<uint32_t>(index)));
    co_await socket_.send_message(have_msg);
}

asio::awaitable<void> PeerConnection::send_extended_message(uint8_t type_id, std::span<const std::byte> payload) {
    auto self = shared_from_this(); // keep alive across the write (see send_simple_message)
    (void)self;
    std::vector<std::byte> msg_body;
    msg_body.push_back(static_cast<std::byte>(MessageType::ExtendedMessage));
    msg_body.push_back(static_cast<std::byte>(type_id));
    msg_body.insert(msg_body.end(), payload.begin(), payload.end());
    co_await socket_.send_message(msg_body);
}

asio::awaitable<void> PeerConnection::send_metadata_request(uint8_t ext_id, int piece) {
    auto self = shared_from_this(); // keep alive across the write (see send_simple_message)
    (void)self;
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
