#pragma once

#include "Logger.hpp"
#include <cstddef>
#include <cstdint>
#include <string_view>
#include <vector>
#include <span>
#include <boost/asio.hpp>

namespace asio = boost::asio;

static constexpr uint32_t MAX_MESSAGE_SIZE = 10 * 1024 * 1024;

class AsyncSocket {
    public:
    explicit AsyncSocket(asio::ip::tcp::socket socket) noexcept
        : socket_(std::move(socket)) {}

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

    const asio::ip::tcp::socket& get_socket() const noexcept { return socket_; }

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

private:
    asio::ip::tcp::socket socket_;
};

class AsyncServerSocket {
public:
    explicit AsyncServerSocket(asio::io_context& io_context, int port)
        : acceptor_(io_context, {asio::ip::tcp::v4(), static_cast<asio::ip::port_type>(port)}) 
    {    
        LOGINFO("Server listening on port {}", port);
    }

    asio::awaitable<AsyncSocket> accept() {
        asio::ip::tcp::socket socket = co_await acceptor_.async_accept(asio::use_awaitable);
        auto endpoint = socket.remote_endpoint();
        LOGINFO("Accepted connection from {}:{}", endpoint.address().to_string(), endpoint.port());
        co_return AsyncSocket(std::move(socket));
    }

private:
    asio::ip::tcp::acceptor acceptor_;
};