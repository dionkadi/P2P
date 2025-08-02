#pragma once


#include <string>
#include <unordered_map>
#include <set>

#include "asio.hpp"
#include "AsyncSocket.hpp"

class Tracker {
public:
    Tracker();

    Tracker(const Tracker&) = delete;
    Tracker& operator= (const Tracker&) = delete;

    void listen(int port);
    void run();
    asio::io_context& get_io_context();

private:
    asio::awaitable<void> accept_loop(int port);
    asio::awaitable<void> handle_client(AsyncSocket socket);
    
    asio::io_context io_context_;
    asio::strand<asio::io_context::executor_type> strand_;
    std::unordered_map<std::string, std::set<std::string>> peers_;
};
