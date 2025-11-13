#include "Peers/Tracker.hpp"
#include "Utils/Logger.hpp"
#include "Utils/Crypto.hpp"
#include "Utils/Bencode.hpp"
#include "Protocols/Protocol.hpp"
#include <cstddef>
#include <cstdint>
#include <endian.h>
#include <exception>
#include <map>
#include <memory>
#include <span>
#include <sstream>
#include <string>
#include <utility>
#include <vector>


// Helper to URL-decode a string (essential for info_hash)
std::string url_decode(std::string_view str) {
    std::string decoded;
    decoded.reserve(str.length());
    for (size_t i = 0; i < str.length(); ++i) {
        if (str[i] == '%' && i + 2 < str.length()) {
            int value;
            std::istringstream hex_stream(std::string(str.substr(i + 1, 2)));
            hex_stream >> std::hex >> value;
            decoded += static_cast<char>(value);
            i += 2;
        } else if (str[i] == '+') {
            decoded += ' ';
        } else {
            decoded += str[i];
        }
    }
    return decoded;
}

// Helper function to parse URL-encoded query strings
std::map<std::string, std::string> parse_query_params(std::string_view query) {
    std::map<std::string, std::string> params;
    std::stringstream query_stream{std::string(query)};
    std::string pair_str;
    while (std::getline(query_stream, pair_str, '&')) {
        if (pair_str.empty()) {
            continue;
        }
        auto eq_pos = pair_str.find('=');
        if (eq_pos != std::string::npos) {
            std::string key = pair_str.substr(0, eq_pos);
            std::string value = pair_str.substr(eq_pos + 1);
            if (!key.empty()) {
                params[std::move(key)] = std::move(value);
            }
        }
    }
    return params;
}


HttpHandler Tracker::create_announce_handler() {
    // The handler lambda is now a coroutine itself
    return [this](HttpRequest req) -> asio::awaitable<HttpResponse> {
        HttpResponse res{http::status::ok, req.version()};
        res.set(http::field::server, BOOST_BEAST_VERSION_STRING);
        res.set(http::field::content_type, "text/plain");
        res.keep_alive(false);
        try {
            auto target = req.target();
            auto query_pos = target.find('?');
            std::string_view query_str = (query_pos != std::string_view::npos) ? target.substr(query_pos + 1) : "";
            
            auto params = parse_query_params(query_str);
            
            std::string info_hash = url_decode(params.at("info_hash"));
            std::vector<std::byte> info_hash_bytes(reinterpret_cast<std::byte *>(info_hash.data()), reinterpret_cast<std::byte *>(info_hash.data()) + info_hash.size());
            uint16_t port = std::stoi(params.at("port"));
            std::string ip = std::string(req["remote_endpoint"]);
            
            std::string compact_peer_addr(6, '\0');
            asio::ip::address_v4::bytes_type ip_bytes = asio::ip::make_address_v4(ip).to_bytes();
            uint16_t port_asio = htons(port);
            std::memcpy(&compact_peer_addr[0], ip_bytes.data(), 4);
            std::memcpy(&compact_peer_addr[4], &port_asio, 2);
            
            co_await asio::dispatch(strand_, asio::use_awaitable);

            std::string peers_binary;
            auto it = peers_.find(info_hash_bytes);
            if (it != peers_.end()) {
                for (const auto& p_addr : it->second) {
                    if (p_addr != compact_peer_addr) {
                        peers_binary.append(p_addr);
                    }
                }
            }
            peers_[info_hash_bytes].insert(compact_peer_addr);
            LOGINFO("HTTP announce from {}:{} for hash {}. Total peers for this hash: {}. Returning {} peers.",
                    ip, port, Crypto::bytes_to_hex(info_hash_bytes), peers_[info_hash_bytes].size(), peers_binary.size() / 6);
            

            Dict response_dict;
            response_dict["interval"] = Value(static_cast<Integer>(1800));
            response_dict["peers"] = Value(peers_binary);
            auto bencoded_body_vec = encode(Value(response_dict));
            
            res.body().assign(bencoded_body_vec.begin(), bencoded_body_vec.end());
        } catch (const std::exception& e) {
            LOGERR("HTTP Announce error: {}", e.what());
            res.result(http::status::bad_request);
            Dict err_dict;
            err_dict["failure reason"] = Value(std::string(e.what()));
            auto bencoded_err_vec = encode(Value(err_dict));
            res.body().assign(bencoded_err_vec.begin(), bencoded_err_vec.end());
        }
        res.prepare_payload();
        co_return res;
    };
}


Tracker::Tracker(): strand_(asio::make_strand(io_context_)) {}

void Tracker::listen_http(int port) {
    http_router_ = std::make_shared<HttpRouter>();
    http_router_->add_route("/announce", create_announce_handler());
    http_router_->add_route("/", create_announce_handler());
    auto const address = asio::ip::make_address("0.0.0.0");
    auto endpoint = tcp::endpoint{address, static_cast<unsigned short>(port)};
    // Spawn the listener coroutine. It will run until the io_context is stopped.
    asio::co_spawn(
        io_context_,
        http_listener(io_context_, endpoint, http_router_),
        asio::detached
    );
}

void Tracker::listen_udp(int port) {
    asio::co_spawn(
        io_context_, 
        udp_listen_loop(port), 
        [](std::exception_ptr p) {
            if (p) {
                try {
                    std::rethrow_exception(p);
                } catch (const std::exception& e) {
                    LOGCRITICAL("Tracker UDP listen loop failed: {}", e.what());
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


asio::awaitable<void> Tracker::udp_listen_loop(int port) {
    auto executor = co_await asio::this_coro::executor;
    asio::ip::udp::socket udp_socket(executor, asio::ip::udp::endpoint(asio::ip::udp::v4(), port));
    LOGINFO("UDP server listening on port {}", port);
    std::vector<char> buffer(2048); 
    asio::ip::udp::endpoint remote_endpoint;
    while (true) {
        size_t bytes_recvd = co_await udp_socket.async_receive_from(asio::buffer(buffer), remote_endpoint, asio::use_awaitable);
        asio::co_spawn(
            executor,
            handle_udp_request(remote_endpoint, {buffer.data(), bytes_recvd}, udp_socket),
            asio::detached
        );
    }
}

asio::awaitable<void> Tracker::handle_udp_request(asio::ip::udp::endpoint remote_endpoint, std::span<const char> request, asio::ip::udp::socket& socket) {
    if (request.size() < sizeof(UdpConnectRequest)) {
        LOGWARN("Received too-small UDP packet from {}", remote_endpoint.address().to_string());
        co_return;
    }
    try {
        uint32_t action = be32toh(*reinterpret_cast<const uint32_t *>(request.data() + 8));
        if (action == 0) { // Connect
            LOGINFO("Received UDP Connect request from {}", remote_endpoint.address().to_string());
            uint64_t new_connection_id = rng_();
            udp_clients_[remote_endpoint.address()] = {
                new_connection_id,
                std::chrono::steady_clock::now() + std::chrono::minutes(2)
            };

            auto *req = reinterpret_cast<const UdpConnectRequest *>(request.data());
            UdpConnectResponse res;
            res.action = htobe32(0);
            res.transaction_id = req->transaction_id;
            res.connection_id = htobe64(new_connection_id);
            co_await socket.async_send_to(asio::buffer(&res, sizeof(res)), remote_endpoint, asio::use_awaitable);
        
        } else if (action == 1) { // Announce
            if (request.size() < sizeof(UdpAnnounceRequest)) {
                LOGWARN("Received too-small UDP Announce packet from {}", remote_endpoint.address().to_string());
                co_return;
            }
            auto *req = reinterpret_cast<const UdpAnnounceRequest *>(request.data());

            uint64_t received_conn_id = be64toh(req->connection_id);
            auto it = udp_clients_.find(remote_endpoint.address());
            if (it == udp_clients_.end() || 
                it->second.connection_id != received_conn_id ||
                it->second.expiry < std::chrono::steady_clock::now()) 
            {
                LOGWARN("UDP Announce from {} with invalid/expired connection ID. Ignoring.", remote_endpoint.address().to_string());
                // Optionally send an error packet back
                co_return;
            }

            // For UDP, the key for the map should be the raw bytes of the info_hash
            std::vector<std::byte> info_hash_bytes(reinterpret_cast<const std::byte *>(req->info_hash.begin()), reinterpret_cast<const std::byte *>(req->info_hash.begin()) + req->info_hash.size());
            
            // Create the compact peer address (IP:Port)
            std::string peer_addr(6, '\0');
            auto ip_bytes = remote_endpoint.address().to_v4().to_bytes(); // Already asiowork byte order from endpoint
            uint16_t port_asio = htons(be16toh(req->port)); // req->port is asiowork order, convert to host, then back just to be sure
            std::memcpy(&peer_addr[0], ip_bytes.data(), 4);
            std::memcpy(&peer_addr[4], &port_asio, 2);
            LOGINFO("Received UDP Announce from {} for hash {}", remote_endpoint.address().to_string(), Crypto::bytes_to_hex(info_hash_bytes));
            
            co_await asio::dispatch(strand_, asio::use_awaitable);
            peers_[info_hash_bytes].insert(peer_addr);
            // Prepare the response
            UdpAnnounceResponse res_header;
            res_header.action = htobe32(1);
            res_header.transaction_id = req->transaction_id;
            res_header.interval = htobe32(1800); 
            
            std::vector<char> response_body;
            if (peers_.count(info_hash_bytes)) {
                for (const auto& addr : peers_.at(info_hash_bytes)) {
                    if (addr != peer_addr) {
                        response_body.insert(response_body.end(), addr.begin(), addr.end());
                    }
                }
            }
            res_header.seeders = htobe32(peers_.at(info_hash_bytes).size()); // Simple logic, not distinguishing seeders/leechers
            res_header.leechers = htobe32(0);
            
            std::vector<char> full_response;
            full_response.insert(full_response.end(), (char *)&res_header, (char *)&res_header + sizeof(res_header));
            full_response.insert(full_response.end(), response_body.begin(), response_body.end());
            co_await socket.async_send_to(asio::buffer(full_response), remote_endpoint, asio::use_awaitable);
        } else {
            LOGWARN("Received unknown UDP action {} from {}", action, remote_endpoint.address().to_string());
        }
    } catch (const std::exception& e) {
        LOGERR("Error handling UDP request from {}: {}", remote_endpoint.address().to_string(), e.what());
    }
}

asio::io_context& Tracker::get_io_context() {
    return io_context_;
}