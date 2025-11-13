#pragma once

#include <cstddef>
#include <memory>
#include <span>
#include <string>
#include <random>
#include <map>
#include <boost/asio.hpp>
#include <boost/container/flat_set.hpp>
#include <vector>
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
    std::map<std::vector<std::byte>, boost::container::flat_set<std::string>> peers_;

    std::shared_ptr<HttpRouter> http_router_;

    struct UdpClientInfo {
        uint64_t connection_id;
        std::chrono::steady_clock::time_point expiry;
    };
    // Use asio::ip::address as the key for protocol independence
    std::unordered_map<asio::ip::address, UdpClientInfo> udp_clients_;
    std::mt19937_64 rng_{std::random_device{}()};
};
