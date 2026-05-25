#pragma once

#include "Utils.hpp"
#include "SessionState.hpp"
#include "PeerManager.hpp"

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/error.hpp>
#include <boost/asio/ip/multicast.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/system/detail/error_code.hpp>

#include <array>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <string_view>

// Base32 encoder/decoder (RFC 4648, no padding)
namespace LsdCrypto {

inline std::string base32_encode(std::span<const std::byte> input) {
    static constexpr std::string_view alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
    if (input.empty()) {
        return {};
    }

    std::string output;
    output.reserve((input.size() * 8 + 4) / 5);

    int buffer = 0;
    int bits_remaining = 0;
    auto it = input.begin();

    while (it != input.end()) {
        buffer = (buffer << 8) | static_cast<int>(*it);
        bits_remaining += 8;
        ++it;

        while (bits_remaining >= 5) {
            bits_remaining -= 5;
            output.push_back(alphabet[(buffer >> bits_remaining) & 0x1F]);
        }
    }

    if (bits_remaining > 0) {
        output.push_back(alphabet[(buffer << (5 - bits_remaining)) & 0x1F]);
    }

    return output;
}

inline std::vector<std::byte> base32_decode(std::string_view input) {
    static constexpr auto build_reverse_table = []() -> std::array<int, 256> {
        std::array<int, 256> table{};
        table.fill(-1);
        constexpr std::string_view upper = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
        constexpr std::string_view lower = "abcdefghijklmnopqrstuvwxyz234567";
        for (size_t i = 0; i < upper.size(); ++i) {
            table[static_cast<unsigned char>(upper[i])] = static_cast<int>(i);
            table[static_cast<unsigned char>(lower[i])] = static_cast<int>(i);
        }
        return table;
    };

    static constexpr std::array<int, 256> reverse_table = build_reverse_table();

    if (input.empty()) {
        return {};
    }

    std::vector<std::byte> output;
    output.reserve((input.size() * 5 + 7) / 8);

    int buffer = 0;
    int bits_remaining = 0;

    for (char ch : input) {
        if (ch == '=') {
            break;
        }
        int val = reverse_table[static_cast<unsigned char>(ch)];
        if (val == -1) {
            continue;
        }
        buffer = (buffer << 5) | val;
        bits_remaining += 5;

        if (bits_remaining >= 8) {
            bits_remaining -= 8;
            output.push_back(static_cast<std::byte>((buffer >> bits_remaining) & 0xFF));
        }
    }

    return output;
}

} // namespace LsdCrypto

// Local Service Discovery per BEP 14
class LsdDiscovery : public std::enable_shared_from_this<LsdDiscovery> {
public:
    LsdDiscovery(
        asio::io_context& io_context,
        uint16_t listening_port,
        std::shared_ptr<PeerManager> peer_manager,
        std::shared_ptr<SessionState> state
    );

    LsdDiscovery(const LsdDiscovery&) = delete;
    LsdDiscovery& operator=(const LsdDiscovery&) = delete;

    void start();
    void stop();

    static std::string build_announce_message(
        std::span<const std::byte> info_hash,
        uint16_t listening_port
    );

    struct ParsedAnnouncement {
        std::string infohash_b32;
        uint16_t port;
    };
    static std::optional<ParsedAnnouncement> parse_announcement(std::string_view announcement);

private:
    asio::awaitable<void> announce_loop();
    asio::awaitable<void> listen_loop();
    asio::awaitable<void> send_announce();
    void parse_and_add_peer(std::string_view announcement, asio::ip::address sender_addr);

    asio::io_context& io_context_;
    asio::strand<asio::io_context::executor_type> strand_;
    uint16_t listening_port_;
    std::shared_ptr<PeerManager> peer_manager_;
    std::shared_ptr<SessionState> state_;

    asio::ip::udp::socket socket_;
    asio::ip::udp::endpoint multicast_endpoint_;
    asio::steady_timer announce_timer_;
    bool running_ = false;

    static constexpr std::string_view MULTICAST_ADDR = "239.192.152.143";
    static constexpr uint16_t MULTICAST_PORT = 6771;
    static constexpr auto ANNOUNCE_INTERVAL = 2min;
};

inline LsdDiscovery::LsdDiscovery(
    asio::io_context& io_context,
    uint16_t listening_port,
    std::shared_ptr<PeerManager> peer_manager,
    std::shared_ptr<SessionState> state
)
    : io_context_(io_context)
    , strand_(asio::make_strand(io_context))
    , listening_port_(listening_port)
    , peer_manager_(std::move(peer_manager))
    , state_(std::move(state))
    , socket_(io_context_)
    , multicast_endpoint_(asio::ip::make_address_v4(MULTICAST_ADDR), MULTICAST_PORT)
    , announce_timer_(io_context_)
{
}

inline std::string LsdDiscovery::build_announce_message(
    std::span<const std::byte> info_hash,
    uint16_t listening_port
) {
    std::string info_hash_b32 = LsdCrypto::base32_encode(info_hash);

    return std::format(
        "BT-SEARCH * HTTP/1.1\r\n"
        "Host: {}:{}\r\n"
        "Port: {}\r\n"
        "Infohash: {}\r\n"
        "\r\n",
        MULTICAST_ADDR, MULTICAST_PORT,
        listening_port,
        info_hash_b32
    );
}

inline std::optional<LsdDiscovery::ParsedAnnouncement>
LsdDiscovery::parse_announcement(std::string_view announcement) {
    if (announcement.find("BT-SEARCH") == std::string_view::npos) {
        return std::nullopt;
    }

    auto ih_pos = announcement.find("Infohash: ");
    if (ih_pos == std::string_view::npos) {
        return std::nullopt;
    }
    ih_pos += 10;
    auto ih_end = announcement.find("\r\n", ih_pos);
    if (ih_end == std::string_view::npos) {
        return std::nullopt;
    }
    std::string_view infohash_b32 = announcement.substr(ih_pos, ih_end - ih_pos);

    auto port_pos = announcement.find("Port: ");
    if (port_pos == std::string_view::npos) {
        return std::nullopt;
    }
    port_pos += 6;
    auto port_end = announcement.find("\r\n", port_pos);
    if (port_end == std::string_view::npos) {
        return std::nullopt;
    }
    std::string_view port_str = announcement.substr(port_pos, port_end - port_pos);

    uint16_t port = 0;
    try {
        port = static_cast<uint16_t>(std::stoul(std::string(port_str)));
    } catch (...) {
        return std::nullopt;
    }

    return ParsedAnnouncement{std::string(infohash_b32), port};
}

inline void LsdDiscovery::start() {
    auto self = shared_from_this();

    boost::system::error_code ec;
    socket_.open(asio::ip::udp::v4(), ec);
    if (ec) {
        LOGERR("LSD: Failed to open socket: {}", ec.message());
        return;
    }

    socket_.set_option(asio::ip::udp::socket::reuse_address(true), ec);
    if (ec) {
        LOGERR("LSD: Failed to set reuse_address: {}", ec.message());
        socket_.close();
        return;
    }

    socket_.bind(asio::ip::udp::endpoint(asio::ip::udp::v4(), MULTICAST_PORT), ec);
    if (ec) {
        LOGERR("LSD: Failed to bind to port {}: {}", MULTICAST_PORT, ec.message());
        socket_.close();
        return;
    }

    {
        auto multicast_addr = asio::ip::make_address_v4(MULTICAST_ADDR);
        socket_.set_option(asio::ip::multicast::join_group(multicast_addr), ec);
        if (ec) {
            LOGERR("LSD: Failed to join multicast group {}: {}", MULTICAST_ADDR, ec.message());
            socket_.close();
            return;
        }
    }

    socket_.set_option(asio::ip::multicast::hops(1), ec);
    if (ec) {
        LOGWARN("LSD: Failed to set multicast hops(1): {}", ec.message());
    }

    running_ = true;

    LOGINFO("LSD: Joined multicast group {}:{}, advertising on port {}",
            MULTICAST_ADDR, MULTICAST_PORT, listening_port_);

    asio::co_spawn(strand_, self->announce_loop(), asio::detached);
    asio::co_spawn(strand_, self->listen_loop(), asio::detached);
}

inline void LsdDiscovery::stop() {
    running_ = false;
    announce_timer_.cancel();

    boost::system::error_code ec;
    if (socket_.is_open()) {
        auto multicast_addr = asio::ip::make_address_v4(MULTICAST_ADDR);
        socket_.set_option(asio::ip::multicast::leave_group(multicast_addr), ec);
        if (ec) {
            LOGWARN("LSD: Failed to leave multicast group: {}", ec.message());
        }
        socket_.close(ec);
        if (ec) {
            LOGWARN("LSD: Failed to close socket: {}", ec.message());
        }
    }
    LOGINFO("LSD: Stopped.");
}

inline asio::awaitable<void> LsdDiscovery::send_announce() {
    const auto& info_hash_vec = state_->info_hash();
    if (info_hash_vec.empty()) {
        co_return;
    }

    std::string msg = build_announce_message(info_hash_vec, listening_port_);
    auto data = std::as_bytes(std::span(msg));

    try {
        co_await socket_.async_send_to(
            asio::buffer(data.data(), data.size()),
            multicast_endpoint_,
            asio::use_awaitable
        );
        LOGDBG("LSD: Sent announcement for {}", LsdCrypto::base32_encode(info_hash_vec));
    } catch (const std::exception& e) {
        LOGERR("LSD: Failed to send announcement: {}", e.what());
    }
}

inline asio::awaitable<void> LsdDiscovery::announce_loop() {
    auto self = shared_from_this();

    while (running_) {
        co_await send_announce();

        announce_timer_.expires_after(ANNOUNCE_INTERVAL);
        boost::system::error_code ec;
        co_await announce_timer_.async_wait(asio::redirect_error(asio::use_awaitable, ec));
        if (ec == asio::error::operation_aborted || !running_) {
            break;
        }
    }
}

inline asio::awaitable<void> LsdDiscovery::listen_loop() {
    auto self = shared_from_this();
    std::array<std::byte, 2048> recv_buffer;

    while (running_) {
        try {
            asio::ip::udp::endpoint remote_endpoint;
            boost::system::error_code ec;
            size_t bytes_received = co_await socket_.async_receive_from(
                asio::buffer(recv_buffer),
                remote_endpoint,
                asio::redirect_error(asio::use_awaitable, ec)
            );

            if (ec == asio::error::operation_aborted || !running_) {
                break;
            }
            if (ec) {
                LOGERR("LSD: Receive error: {}", ec.message());
                continue;
            }

            std::string_view announcement(
                reinterpret_cast<const char*>(recv_buffer.data()),
                bytes_received
            );
            parse_and_add_peer(announcement, remote_endpoint.address());
        } catch (const std::exception& e) {
            if (!running_) break;
            LOGERR("LSD: Error in listen loop: {}", e.what());
        }
    }
}

inline void LsdDiscovery::parse_and_add_peer(
    std::string_view announcement,
    asio::ip::address sender_addr
) {
    auto parsed = parse_announcement(announcement);
    if (!parsed) {
        return;
    }

    const auto& our_info_hash = state_->info_hash();
    if (our_info_hash.empty()) {
        return;
    }

    std::string our_b32 = LsdCrypto::base32_encode(our_info_hash);
    if (parsed->infohash_b32 != our_b32) {
        return;
    }

    uint16_t port = parsed->port;

    if (port == listening_port_) {
        LOGDBG("LSD: Ignoring self-announcement from {} (port matches listening port)", sender_addr.to_string());
        return;
    }

    if (port == 0) {
        return;
    }

    asio::ip::tcp::endpoint ep(sender_addr, port);
    peer_manager_->add_discovered_peer(ep);
    LOGDBG("LSD: Discovered peer {}:{} via LSD", sender_addr.to_string(), port);
}
