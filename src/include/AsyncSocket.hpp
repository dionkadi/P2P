#pragma once

#include "asio.hpp"
#include <cstddef>
#include <string_view>
#include <vector>

using asio::ip::tcp;

class AsyncSocket {
public:
    explicit AsyncSocket(tcp::socket socket);


    asio::awaitable<void> connect(std::string_view host, int port);
    asio::awaitable<void> send_raw(const std::vector<char>& data);
    asio::awaitable<std::vector<char>> receive_raw(size_t size);

    asio::awaitable<void> send_message(std::vector<char> message);
    asio::awaitable<std::vector<char>> receive_message();

    const tcp::socket& get_socket() const { return socket_; }

    void close();

private:
    tcp::socket socket_;
};

class AsyncServerSocket {
public:
    explicit AsyncServerSocket(asio::io_context& io_context, int port);

    asio::awaitable<AsyncSocket> accept();

private:
    tcp::acceptor acceptor_;
};