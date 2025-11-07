#pragma once

#include "Utils/Logger.hpp"
#include <cstddef>
#include <string_view>
#include <vector>
#include <boost/asio.hpp>

namespace asio = boost::asio;

class AsyncSocket {
    public:
    explicit AsyncSocket(asio::ip::tcp::socket socket): socket_(std::move(socket)) {}

    asio::awaitable<void> connect(std::string_view host, int port) {
        asio::ip::tcp::resolver resolver(socket_.get_executor());
        auto endpoints = co_await resolver.async_resolve(host, std::to_string(port), asio::use_awaitable);
        co_await asio::async_connect(socket_, endpoints, asio::use_awaitable);
        LOGINFO("Successfully connected to {}:{}", host, port);
    }

    asio::awaitable<void> send_raw(const std::vector<char>& data) {
        co_await asio::async_write(socket_, asio::buffer(data), asio::use_awaitable);
    }

    asio::awaitable<std::vector<char>> receive_raw(size_t size) {
        std::vector<char> buffer(size);
        co_await asio::async_read(socket_, asio::buffer(buffer), asio::use_awaitable);
        co_return buffer;
    }

    asio::awaitable<void> send_message(std::vector<char> message) {
        // keep-alive message
        if (message.empty()) {
            uint32_t zero_len = 0; // Already in host order, will be converted by buffer.
            co_await asio::async_write(socket_, asio::buffer(&zero_len, sizeof(zero_len)), asio::use_awaitable);
            co_return;
        }

        auto shared_message = std::make_shared<std::vector<char>>(std::move(message));
        auto shared_length = std::make_shared<uint32_t>(
            asio::detail::socket_ops::host_to_network_long(
                static_cast<uint32_t>(shared_message->size())
            )
        );

        std::array<asio::const_buffer, 2> buffer_sequence = {
            asio::buffer(shared_length.get(), sizeof(uint32_t)),
            asio::buffer(*shared_message)
        };
        co_await asio::async_write(socket_, buffer_sequence, asio::use_awaitable);
    }

    asio::awaitable<std::vector<char>> receive_message() {
        uint32_t net_length;
        co_await asio::async_read(socket_, asio::buffer(&net_length, sizeof(net_length)), asio::use_awaitable);
        uint32_t length = asio::detail::socket_ops::network_to_host_long(net_length);
        if (length > 10 * 1024 * 1024) {
            throw std::runtime_error("Message size limit exceeded: " + std::to_string(length));
        }
        if (length == 0) {
            co_return std::vector<char>();
        }

        std::vector<char> buffer(length);
        co_await asio::async_read(socket_, asio::buffer(buffer, buffer.size()), asio::use_awaitable);

        co_return buffer;
    }

    const asio::ip::tcp::socket& get_socket() const { return socket_; }

    void close() {
        if(socket_.is_open()) {
            socket_.shutdown(asio::ip::tcp::socket::shutdown_both);
            socket_.close();
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