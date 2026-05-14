#pragma once

#include <boost/asio.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/udp.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/system/detail/error_code.hpp>
#include <cstdint>
#include <string>
namespace asio = boost::asio;

#include "Utils.hpp"


static constexpr uint32_t MAX_MESSAGE_SIZE = 10 * 1024 * 1024;

class AsyncSocket {
public:
    explicit AsyncSocket(asio::ip::tcp::socket socket) noexcept
        : socket_(std::move(socket)) {}

    AsyncSocket(const AsyncSocket&) = delete;
    AsyncSocket& operator=(const AsyncSocket&) = delete;
    AsyncSocket(AsyncSocket&&) noexcept = default;
    AsyncSocket& operator=(AsyncSocket&&) noexcept = default;

    asio::awaitable<void> connect(std::string_view host, int port) {
        asio::ip::tcp::resolver resolver(socket_.get_executor());
        auto endpoints = co_await resolver.async_resolve(host, std::to_string(port), asio::use_awaitable);
        co_await asio::async_connect(socket_, endpoints, asio::use_awaitable);
        LOGINFO("Successfully connected to {}:{}", host, port);
    }

    asio::awaitable<void> send_raw(std::span<const std::byte> data) {
        co_await asio::async_write(socket_, asio::buffer(data), asio::use_awaitable);
    }

    asio::awaitable<std::vector<std::byte>> receive_raw(size_t size) {
        std::vector<std::byte> buffer(size);
        co_await asio::async_read(socket_, asio::buffer(buffer), asio::use_awaitable);
        co_return buffer;
    }

    asio::awaitable<void> send_message(std::span<const std::byte> message) {
        // keep-alive message
        if (message.empty()) {
            uint32_t zero_len = 0; // Already in host order, will be converted by buffer.
            co_await asio::async_write(socket_, asio::buffer(&zero_len, sizeof(zero_len)), asio::use_awaitable);
            co_return;
        }

        uint32_t length = asio::detail::socket_ops::host_to_network_long(static_cast<uint32_t>(message.size()));

        std::array<asio::const_buffer, 2> buffer_sequence = {
            asio::buffer(&length, sizeof(uint32_t)),
            asio::buffer(message)
        };
        co_await asio::async_write(socket_, buffer_sequence, asio::use_awaitable);
    }

    asio::awaitable<std::vector<std::byte>> receive_message() {
        uint32_t net_length;
        co_await asio::async_read(socket_, asio::buffer(&net_length, sizeof(net_length)), asio::use_awaitable);
        uint32_t length = asio::detail::socket_ops::network_to_host_long(net_length);
        if (length > MAX_MESSAGE_SIZE) {
            throw std::runtime_error("Message size limit exceeded: " + std::to_string(length));
        }
        if (length == 0) {
            co_return std::vector<std::byte>();
        }

        std::vector<std::byte> buffer(length);
        co_await asio::async_read(socket_, asio::buffer(buffer, buffer.size()), asio::use_awaitable);

        co_return buffer;
    }

    asio::ip::tcp::endpoint remote_endpoint() const noexcept {
        boost::system::error_code ec;
        auto ep = socket_.remote_endpoint(ec);
        if (ec) {
            LOGWARN("Error getting remote_endpoint: {}", ec.message());
            return {};
        }
        return ep;
    }

    asio::ip::tcp::endpoint local_endpoint() const noexcept {
        boost::system::error_code ec;
        auto ep = socket_.local_endpoint(ec);
        if (ec) {
            LOGWARN("Error getting local_endpoint: {}", ec.message());
            return {};
        }
        return ep;
    }

    void close() noexcept {
        boost::system::error_code ec;
        if(socket_.is_open()) {
            socket_.shutdown(asio::ip::tcp::socket::shutdown_both, ec);
            if (ec && ec != asio::error::not_connected) { // Ignore 'not_connected' during shutdown
                LOGWARN("Socket shutdown error: {}", ec.message());
            }
            socket_.close(ec);
            if (ec) {
                LOGWARN("Socket close error: {}", ec.message());
            }
        }
    }

    bool is_open() const noexcept {
        return socket_.is_open();
    }

private:
    asio::ip::tcp::socket socket_;
};

class AsyncServerSocket {
public:
    explicit AsyncServerSocket(asio::io_context& io_context, int port) noexcept
        : acceptor_(io_context, {asio::ip::tcp::v4(), static_cast<asio::ip::port_type>(port)}) 
    {    
        acceptor_.set_option(asio::ip::tcp::acceptor::reuse_address(true));
        LOGINFO("Server listening on port {}", port);
    }

    AsyncServerSocket(const AsyncServerSocket&) = delete;
    AsyncServerSocket& operator=(const AsyncServerSocket&) = delete;
    AsyncServerSocket(AsyncServerSocket&&) noexcept = default;
    AsyncServerSocket& operator=(AsyncServerSocket&&) noexcept = default;

    asio::awaitable<AsyncSocket> accept() {
        asio::ip::tcp::socket socket = co_await acceptor_.async_accept(asio::use_awaitable);
        auto endpoint = socket.remote_endpoint();
        LOGINFO("Accepted connection from {}:{}", endpoint.address().to_string(), endpoint.port());
        co_return AsyncSocket(std::move(socket));
    }

    void close() {
        boost::system::error_code ec;
        acceptor_.cancel(ec);
        acceptor_.close(ec);
    }

private:
    asio::ip::tcp::acceptor acceptor_;
};

using udp = asio::ip::udp;

class AsyncUdpSocket {
public:
    explicit AsyncUdpSocket(asio::io_context& io_context, uint16_t port = 0)
        : socket_(io_context)
    {
        if (port > 0) {
            socket_.open(udp::v4());
            socket_.bind(udp::endpoint(udp::v4(), port));
            LOGINFO("UDP socket bound to port {}", port);
        }
    }

    AsyncUdpSocket(const AsyncUdpSocket&) = delete;
    AsyncUdpSocket& operator=(const AsyncUdpSocket&) = delete;
    AsyncUdpSocket(AsyncUdpSocket&&) noexcept = default;
    AsyncUdpSocket& operator=(AsyncUdpSocket&&) noexcept = default;

    asio::awaitable<void> send_to(std::span<const std::byte> data, const udp::endpoint& remote) {
        if (!socket_.is_open()) {
            socket_.open(udp::v4());
        }

        boost::system::error_code ec;
        co_await socket_.async_send_to(asio::buffer(data.data(), data.size()), remote, asio::redirect_error(asio::use_awaitable, ec));
        if (ec) {
            LOGERR("UDP send_to error to {}: {}. Data size: {}", remote.address().to_string(), ec.message(), data.size());
            throw boost::system::system_error(ec, "UDP send_to failed");
        }
    }

    // Returns a pair: (std::vector<std::byte> received_bytes, udp::endpoint remote_endpoint)
    asio::awaitable<std::tuple<std::vector<std::byte>, udp::endpoint>>
    receive_from(size_t max_buffer_size = 2048) {
        std::vector<std::byte> buffer(max_buffer_size);
        udp::endpoint remote;
        boost::system::error_code ec;
        size_t bytes_received = co_await socket_.async_receive_from(asio::buffer(buffer), remote, asio::redirect_error(asio::use_awaitable, ec));
        if (ec) {
            // Operation aborted is normal during shutdown
            if (ec == asio::error::operation_aborted) {
                throw boost::system::system_error(ec, "UDP receive_from aborted");
            }
            LOGERR("UDP receive_from error: {}", ec.message());
            throw boost::system::system_error(ec, "UDP receive_from failed");
        }

        buffer.resize(bytes_received);
        co_return std::make_tuple(std::move(buffer), remote);
    }

    static udp::endpoint resolve_endpoint(asio::io_context io_context, const std::string& host, uint16_t port) {
        udp::resolver resolver(io_context);
        return *resolver.resolve(host, std::to_string(port)).begin();
    }

    udp::endpoint local_endpoint() const {
        boost::system::error_code ec;
        auto ep = socket_.local_endpoint(ec);
        if (ec) {
            LOGWARN("Error getting UDP local_endpoint: {}", ec.message());
            return {};
        }
        return ep;
    }

    void close() {
        if (socket_.is_open()) {
            boost::system::error_code ec;
            socket_.cancel(ec); // Cancel any pending async operations
            socket_.close(ec);
        }
    }

private:
    udp::socket socket_;
};