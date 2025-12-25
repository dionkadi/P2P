#include "PeerConnection.hpp"
#include "Bencode.hpp"
#include "Protocol.hpp"

#include <boost/asio/awaitable.hpp>
#include <cstdint>
#include <exception>
#include <memory>

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

    co_return conn;
}

PeerConnection::PeerConnection(
    asio::io_context& io_context, AsyncSocket socket, std::string peer_addr,
    std::shared_ptr<SessionState> state, std::shared_ptr<IPeerConnectionEvents> events
) noexcept : io_context_(io_context),
    socket_(std::move(socket)),
    peer_addr_(std::move(peer_addr)),
    state_(std::move(state)),
    events_(std::move(events))
{}

asio::awaitable<bool> PeerConnection::perform_handshake(const PeerId& my_id) {
    LOGDBG("Entered perform_handshake for peer {}", peer_addr_);
    try {
        LOGDBG("Starting handshake with peer {}", peer_addr_);
        Handshake my_handshake {
            .info_hash_bytes = state_->info_hash(), 
            .peer_id_bytes = my_id,
            .extended = true
        };
        LOGDBG("Sending handshake message ({} bytes) to {}", HANDSHAKE_BASE_LEN, peer_addr_);
        co_await socket_.send_raw(my_handshake.serialize());
        LOGDBG("Handshake message sent to {}. Awaiting peer's handshake.", peer_addr_);
        std::vector<std::byte> handshake_buffer = co_await socket_.receive_raw(HANDSHAKE_BASE_LEN);
        LOGDBG("Received handshake message ({} bytes) from {}. Deserializing.", HANDSHAKE_BASE_LEN, peer_addr_);
        Handshake peer_handshake = Handshake::deserialize(handshake_buffer);
        LOGDBG("Deserialized handshake from {}. Checking info hash.", peer_addr_);

        if (peer_handshake.info_hash_bytes != state_->info_hash()) {
            LOGERR("Info hash mismatch with {}. Expected {}, got {}.",
                   peer_addr_, Crypto::bytes_to_hex(state_->info_hash()), Crypto::bytes_to_hex(peer_handshake.info_hash_bytes));
            throw std::runtime_error("Info hash mismatch");
        }

        peer_id_ = peer_handshake.peer_id_bytes;
        
        if (peer_id_ == my_id) {
            LOGWARN("Connected to self ({}:{}). Dropping connection.", peer_addr_, Crypto::bytes_to_hex(my_id));
            co_return false;
        }
        
        LOGDBG("Peer {} handshake successful. Peer ID: {}. Checking for extended handshake support.", peer_addr_, peer_id_);
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
        io_context_, 
        [self] () -> asio::awaitable<void> {
            co_await self->message_loop();
        }, 
        asio::detached
    );
    asio::co_spawn(
        io_context_, 
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
                // LOGDBG("Received keep-alive from {}", conn->peer_id);
                continue ;
            }

            // LOGDBG("Received raw message of size {} from {}. First byte is: {:#04x}",
            //          msg.size(), conn->peer_id, static_cast<uint8_t>(msg[0]));

            MessageType type = static_cast<MessageType>(msg[0]);
            std::span<const std::byte> payload(msg.data() + 1, msg.size() - 1);

            switch (type) {
                case MessageType::Choke:
                    // LOGDBG("Received CHOKE from {}", conn->peer_id);
                    peer_is_choking_ = true;
                    co_await events_->on_choke_status_changed(self, true);
                    break;
                case MessageType::Unchoke: {
                    peer_is_choking_ = false;
                    co_await events_->on_choke_status_changed(self, false);
                    break;
                }
                case MessageType::Interested:
                    // LOGDBG("Received INTERESTED from {}", conn->peer_id);    
                    peer_is_interested_ = true;
                    break;
                case MessageType::NotInterested:
                    // LOGDBG("Received NOT INTERESTED from {}", conn->peer_id);    
                    peer_is_interested_ = false;
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
                    co_await events_->on_piece_block(self, piece_index, piece_begin, block_data);
                    break;
                }
                case MessageType::Cancel: {
                    // LOGDBG("Received CANCEL from {}", conn->peer_id);
                    // Handle cancel if needed (e.g., remove from upload queue, but for now ignore)
                    break;
                }
                case MessageType::ExtendedMessage: {
                    // LOGDBG("Received ExtendedMessage from {}", conn->peer_id);
                    co_await events_->on_extended_message(self, payload);
                    break;
                }
                case MessageType::Request: {
                    if (payload.size() < 12) {
                        break;
                    }
                    RequestPayload req = RequestPayload::deserialize(payload);
                    co_await events_->on_block_request(self, req.index, req.begin, req.length);
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
    asio::steady_timer timer(io_context_);

    while (true) {
        timer.expires_after(std::chrono::minutes(2));
        auto [ec] = co_await timer.async_wait(asio::as_tuple(asio::use_awaitable));
        if (ec == asio::error::operation_aborted) {
            LOGDBG("Keep-alive timer for {} aborted. Exiting loop.", peer_id_);
            co_return;
        }
        if (ec) {
            LOGWARN("Keep-alive timer for {} failed: {}", peer_id_, ec.message());
            break;
        }

        // check if has active connections to *this?

        try {
            co_await socket_.send_message({});
        } catch (const boost::system::system_error& e) { // Be more specific
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

void PeerConnection::set_has_piece(size_t index) noexcept {
    if (bitfield_.empty() || index / 8 >= bitfield_.size()) {
        LOGWARN("Attempted to set piece {} out of bitfield bounds (size: {}) for peer {}", index, bitfield_.size(), peer_id_);
        return ;
    }
    bitfield_[index / 8] |= (1 << (7 - (index % 8)));
}

bool PeerConnection::has_piece(size_t index) const noexcept {
    if (bitfield_.empty() || index / 8 >= bitfield_.size()) {
        return false;
    }
    return (bitfield_[index / 8] >> (7 - (index % 8))) & 1;
}

asio::awaitable<void> PeerConnection::send_simple_message(MessageType type) {
    std::vector<std::byte> msg_body(1, static_cast<std::byte>(type));

    // LOGDBG("Sending simple message of type {}. Raw byte value: {:#04x}", 
    //          static_cast<int>(type), static_cast<uint8_t>(msg_body[0]));

    co_await socket_.send_message(msg_body);
}

asio::awaitable<void> PeerConnection::send_request(size_t index, uint32_t begin, uint32_t length) {
    std::vector<std::byte> msg_body(1, static_cast<std::byte>(MessageType::Request));
    auto payload = RequestPayload::serialize(index, begin, length);
    msg_body.insert(msg_body.end(), payload.begin(), payload.end());

    // LOGDBG("REQUEST msg[0] raw byte value: {:#04x}", static_cast<uint8_t>(msg_body[0]));

    co_await socket_.send_message(msg_body);
}

asio::awaitable<void> PeerConnection::send_piece(size_t index, uint32_t begin, std::span<const std::byte> block_data) {
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
    std::vector<std::byte> msg_body(1, static_cast<std::byte>(MessageType::Cancel));
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