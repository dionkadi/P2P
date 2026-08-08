#include "AsyncSocket.hpp"

#include <gtest/gtest.h>

#include <cstddef>
#include <string>
#include <vector>

namespace {

InfoHash make_skey() {
    InfoHash h{};
    for (size_t i = 0; i < h.size(); ++i) {
        h[i] = static_cast<std::byte>(0xA0 + i);
    }
    return h;
}

std::vector<std::byte> make_bt_handshake(const InfoHash& skey, uint8_t peer_tag) {
    std::vector<std::byte> hs;
    hs.push_back(std::byte{19});
    for (char c : std::string("BitTorrent protocol")) {
        hs.push_back(static_cast<std::byte>(c));
    }
    for (int i = 0; i < 8; ++i) {
        hs.push_back(std::byte{0});
    }
    hs[5 + 1 + 19 - 1] = std::byte{0x10}; // extended
    hs[7 + 1 + 19 - 1] = std::byte{0x05}; // DHT + fast
    hs.insert(hs.end(), skey.begin(), skey.end());
    for (int i = 0; i < 20; ++i) {
        hs.push_back(static_cast<std::byte>(peer_tag));
    }
    return hs;
}

} // namespace

// Full MSE handshake between our own initiator and acceptor implementations
// over a real TCP pair, followed by encrypted message traffic in both
// directions. Validates the negotiation state machines, RC4 stream
// alignment, and the transparent encryption layer.
TEST(MseTest, Rc4HandshakeAndTraffic) {
    asio::io_context io;
    asio::ip::tcp::acceptor acceptor(io, {asio::ip::tcp::v4(), 0});
    const uint16_t port = acceptor.local_endpoint().port();
    const InfoHash skey = make_skey();
    const auto init_hs = make_bt_handshake(skey, 0x11);
    const auto acc_hs = make_bt_handshake(skey, 0x22);

    std::vector<std::byte> acc_received_hs;
    std::vector<std::byte> init_received_hs;
    bool init_ok = false;
    bool acc_ok = false;

    asio::co_spawn(io, [&]() -> asio::awaitable<void> {
        auto sock = co_await acceptor.async_accept(asio::use_awaitable);
        AsyncSocket as(std::move(sock));

        auto [result, peer_hs] = co_await as.mse_handshake_acceptor(skey);
        if (result != AsyncSocket::MseResult::Rc4) {
            co_return;
        }
        acc_received_hs = std::move(peer_hs);
        co_await as.send_raw(acc_hs); // our handshake, now encrypted

        // Encrypted message traffic both ways.
        const std::string hello = "hello-from-acceptor";
        co_await as.send_message({reinterpret_cast<const std::byte*>(hello.data()), hello.size()});
        auto msg = co_await as.receive_message();
        acc_ok = std::equal(msg.begin(), msg.end(),
                            reinterpret_cast<const std::byte*>("hello-from-initiator"),
                            reinterpret_cast<const std::byte*>("hello-from-initiator") + 20);
    }, asio::detached);

    asio::co_spawn(io, [&]() -> asio::awaitable<void> {
        asio::ip::tcp::socket s(io);
        co_await s.async_connect(
            asio::ip::tcp::endpoint(asio::ip::address_v4::loopback(), port), asio::use_awaitable);
        AsyncSocket as(std::move(s));

        auto [result, peer_hs] = co_await as.mse_handshake_initiator(skey, init_hs);
        if (result != AsyncSocket::MseResult::Rc4) {
            co_return;
        }
        init_received_hs = std::move(peer_hs);

        auto msg = co_await as.receive_message();
        init_ok = std::equal(msg.begin(), msg.end(),
                             reinterpret_cast<const std::byte*>("hello-from-acceptor"),
                             reinterpret_cast<const std::byte*>("hello-from-acceptor") + 19);
        const std::string hello = "hello-from-initiator";
        co_await as.send_message({reinterpret_cast<const std::byte*>(hello.data()), hello.size()});
    }, asio::detached);

    io.run();

    EXPECT_TRUE(init_ok);
    EXPECT_TRUE(acc_ok);
    EXPECT_EQ(acc_received_hs, init_hs); // initiator's handshake (IA) arrived intact
    EXPECT_EQ(init_received_hs, acc_hs); // acceptor's handshake arrived intact
}

// An inbound connection whose first byte is 0x13 is a plaintext peer: the
// acceptor must detect it, leave the bytes buffered, and let the caller
// proceed with the normal plaintext handshake.
TEST(MseTest, PlaintextPeerDetection) {
    asio::io_context io;
    asio::ip::tcp::acceptor acceptor(io, {asio::ip::tcp::v4(), 0});
    const uint16_t port = acceptor.local_endpoint().port();
    const InfoHash skey = make_skey();
    const auto plain_hs = make_bt_handshake(skey, 0x33);

    bool plaintext_detected = false;
    bool bytes_preserved = false;

    asio::co_spawn(io, [&]() -> asio::awaitable<void> {
        auto sock = co_await acceptor.async_accept(asio::use_awaitable);
        AsyncSocket as(std::move(sock));
        auto [result, peer_hs] = co_await as.mse_handshake_acceptor(skey);
        plaintext_detected = (result == AsyncSocket::MseResult::Plaintext) && peer_hs.empty();
        // The peer's plaintext handshake must still be readable afterwards.
        auto hs = co_await as.receive_raw(plain_hs.size());
        bytes_preserved = (hs == plain_hs);
    }, asio::detached);

    asio::co_spawn(io, [&]() -> asio::awaitable<void> {
        asio::ip::tcp::socket s(io);
        co_await s.async_connect(
            asio::ip::tcp::endpoint(asio::ip::address_v4::loopback(), port), asio::use_awaitable);
        co_await asio::async_write(s, asio::buffer(plain_hs), asio::use_awaitable);
    }, asio::detached);

    io.run();

    EXPECT_TRUE(plaintext_detected);
    EXPECT_TRUE(bytes_preserved);
}
