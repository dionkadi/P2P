#include "Tracker.hpp"
#include "Logger.hpp"
#include "Protocol.hpp"
#include "Crypto.hpp"
#include "asio/detached.hpp"
#include "asio/dispatch.hpp"
#include "asio/use_awaitable.hpp"
#include <exception>
#include <format>
#include <span>
#include <utility>
#include <vector>

Tracker::Tracker(): strand_(asio::make_strand(io_context_)) {}


void Tracker::listen(int port) {
    asio::co_spawn(
        io_context_, 
        accept_loop(port), 
        [](std::exception_ptr p) {
            if (p) {
                try {
                    std::rethrow_exception(p);
                } catch (const std::exception& e) {
                    LOGCRITICAL("Tracker accept loop failed: {}", e.what());
                }
            }
        }
    );
}


void Tracker::run() {
    LOGINFO("Tracker is running...");
    io_context_.run();
    LOGINFO("Tracker stopped");
}


asio::awaitable<void> Tracker::accept_loop(int port) {
    auto acceptor = AsyncServerSocket(io_context_, port);

    while (true) {
        AsyncSocket socket = co_await acceptor.accept();
        asio::co_spawn(io_context_, handle_client(std::move(socket)), asio::detached);
    }
}


asio::awaitable<void> Tracker::handle_client(AsyncSocket socket) {
    std::string peer_ip = socket.get_socket().remote_endpoint().address().to_string();

    try {
        std::vector<char> request_data = co_await socket.receive_message();
        if (request_data.empty()) {
            LOGWARN("Client {} sent empty request", peer_ip);
            co_return ;
        }

        LOGINFO("Received request from {}", peer_ip);

        auto msg_type = static_cast<TrackerMessageType>(request_data[0]);
        std::span<const char> payload(request_data.data() + 1, request_data.size() - 1);

        switch (msg_type) {
            case TrackerMessageType::AnnounceRequest: {
                auto req = TrackerAnnouceReqeust::deserialize(payload);
                std::string file_hash_hex = Crypto::bytes_to_hex(req.info_hash_bytes);
                std::string peer_addr = std::format("{}:{}", peer_ip, req.port);

                LOGINFO("Received Announce from {} for hash {}", peer_addr, file_hash_hex);

                co_await asio::dispatch(strand_, asio::use_awaitable);
                peers_[file_hash_hex].insert(peer_addr);

                std::vector<char> res_body;
                res_body.push_back(static_cast<char>(TrackerMessageType::AnnounceResponse));
                co_await socket.send_message(res_body);

                break ;
            }
            case TrackerMessageType::QueryRequest: {
                auto req = TrackerQueryRequest::deserialize(payload);
                std::string file_hash_hex = Crypto::bytes_to_hex(req.info_hash_bytes);

                LOGINFO("Received Query from {} for hash {}", peer_ip, file_hash_hex);

                co_await asio::dispatch(strand_, asio::use_awaitable);
                TrackerQueryResponse res;
                if (peers_.count(file_hash_hex)) {
                    const auto& peer_set = peers_.at(file_hash_hex);
                    res.peer_addrs.assign(peer_set.begin(), peer_set.end());
                }

                auto response = TrackerQueryResponse::serialize(res);
                co_await socket.send_message(response);
                
                break ;
            } 
            default:
                LOGWARN("Unknown message type {} from {}", static_cast<int>(msg_type), peer_ip);
                
                break;
        }

    } catch (const std::exception& e) {
        LOGERR("Error while handling client: {}", e.what());
    }
}


asio::io_context& Tracker::get_io_context() {
    return io_context_;
}