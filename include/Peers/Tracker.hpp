#pragma once

#include <memory>
#include <span>
#include <string>
#include <unordered_map>
#include <set>
#include <boost/asio.hpp>
#include "Http/HttpServer.hpp"

namespace asio = boost::asio;

class Tracker {
public:
    Tracker();

    Tracker(const Tracker&) = delete;
    Tracker& operator= (const Tracker&) = delete;

    void listen_http(int port);
    void listen_udp(int port);
    void run();
    asio::io_context& get_io_context();

private:
    asio::awaitable<void> udp_listen_loop(int port);
    asio::awaitable<void> handle_udp_request(asio::ip::udp::endpoint remote_endpoint, std::span<const char> request, asio::ip::udp::socket& socket);

    HttpHandler create_announce_handler();
    
    asio::io_context io_context_;
    asio::strand<asio::io_context::executor_type> strand_;
    std::unordered_map<std::string, std::set<std::string>> peers_;

    std::shared_ptr<HttpRouter> http_router_;
};
