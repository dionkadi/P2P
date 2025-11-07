#include "Peers/TrackerClient.hpp"
#include "Protocols/Protocol.hpp"
#include "Utils/Logger.hpp"
#include "Utils/Bencode.hpp"
#include "Http/HttpServer.hpp"
#include <array>
#include <cctype>
#include <chrono>
#include <cstddef>
#include <endian.h>
#include <exception>
#include <iomanip>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <boost/asio/experimental/awaitable_operators.hpp>
using namespace boost::asio::experimental::awaitable_operators;

std::string url_encode(const std::string& value) {
    std::ostringstream escaped;
    escaped.fill('0');
    escaped << std::hex;
    for (char c : value) {
        if (std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
            escaped << c;
        } else {
            escaped << '%' << std::setw(2) << static_cast<int>(static_cast<unsigned char>(c));
        }
    }
    return escaped.str();
}

std::shared_ptr<ITrackerClient> create_tracker_client(asio::io_context& io_context, const std::string& tracker_url) {
    std::string url = tracker_url;
    std::string host;
    int port;

    size_t scheme_pos = url.find("://");
    if (scheme_pos == std::string::npos) {
        throw std::runtime_error("Invalid tracker URL: missing scheme.");
    }

    std::string scheme = url.substr(0, scheme_pos);
    url.erase(0, scheme_pos + 3);

    size_t path_pos = url.find("/");
    std::string target = "/";
    if (path_pos != std::string::npos) {
        target = url.substr(path_pos);
        url = url.substr(0, path_pos);
    }

    size_t colon_pos = url.find(':');
    if (colon_pos == std::string::npos) {
        throw std::runtime_error("Invalid tracker URL: missing port.");
    }

    host = url.substr(0, colon_pos);
    port = std::stoi(url.substr(colon_pos + 1));

    if (scheme == "udp") {
        LOGINFO("Creating UDP Tracker client for {}:{}", host, port);
        return std::make_unique<UdpTrackerClient>(io_context, host, port);
    } 
    else if (scheme == "http" || scheme == "tcp") {
        LOGINFO("Creating HTTP Tracker client for {}:{}", host, port);
        return std::make_unique<HttpTrackerClient>(io_context, host, port, target);
    }
    else {
        throw std::runtime_error("Unsupported tracker scheme: " + scheme);
    }
}


UdpTrackerClient::UdpTrackerClient(asio::io_context& io_context, std::string host, int port)
    : io_context_(io_context), socket_(io_context), url_str_(std::format("{}:{}", host, port)) {

    asio::ip::udp::resolver resolver(io_context_);
    tracker_endpoint_ = *resolver.resolve(host, std::to_string(port)).begin();
    socket_.open(tracker_endpoint_.protocol());

    std::random_device rd;
    next_transaction_id_ = rd();
}


asio::awaitable<bool> UdpTrackerClient::connect_to_tracker() {
    auto self = shared_from_this();

    int n = 0;
    const int max_retries = 4;
    
    while (n < max_retries) {
        UdpConnectRequest req;
        req.transaction_id = htobe32(next_transaction_id_++);

        try {
            LOGDBG("Sending UDP connect request to tracker ({})", n);
            co_await socket_.async_send_to(asio::buffer(&req, sizeof(req)), tracker_endpoint_, asio::use_awaitable);

            asio::steady_timer timer(io_context_);
            timer.expires_after(std::chrono::seconds(15 * (1 << n)));

            std::array<char, sizeof(UdpConnectResponse)> response_buffer;
            asio::ip::udp::endpoint sender_endpoint;

            auto result = co_await (
                socket_.async_receive_from(asio::buffer(response_buffer), sender_endpoint, asio::use_awaitable) ||
                timer.async_wait(asio::as_tuple(asio::use_awaitable))
            );

            if (result.index() == 0) {
                if (std::get<0>(result) >= sizeof(UdpConnectResponse)) {
                    auto *res = reinterpret_cast<UdpConnectResponse*>(response_buffer.data());
                    if (be32toh(res->transaction_id) == be32toh(req.transaction_id) && be32toh(res->action) == 0) {
                        connection_id_ = be64toh(res->connection_id);
                        connection_id_expiry_ = std::chrono::steady_clock::now() + std::chrono::minutes(1);
                        LOGINFO("Successfully connected to UDP tracker. Connection ID: {}", connection_id_);
                        co_return true;
                    }
                }
            } else {
                LOGWARN("UDP connect request timed out. Retrying...");
            }
        } catch (const std::exception& e) {
            LOGERR("Error in UDP connect: {}. Retrying...", e.what());
        }

        ++n;
    }
    LOGCRITICAL("Failed to connect to UDP tracker after {} retries.", max_retries);
    co_return false;
}


asio::awaitable<TrackerAnnounceResult> UdpTrackerClient::announce(const AnnounceRequestParams& params) {
    auto self = shared_from_this();

    if (connection_id_ == 0 || std::chrono::steady_clock::now() >= connection_id_expiry_) {
        bool connected = co_await connect_to_tracker();
        if (!connected) {
            throw std::runtime_error("Failed to get UDP tracker connection ID.");
        }
    }

    
    int n = 0;
    const int max_retries = 4;
    
    while (n <= max_retries) {
        
        UdpAnnounceRequest req;
        req.connection_id = htobe64(connection_id_);
        req.transaction_id = htobe32(next_transaction_id_++);
        std::copy(params.info_hash_bytes.begin(), params.info_hash_bytes.end(), req.info_hash.begin());
        std::copy(params.peer_id.begin(), params.peer_id.end(), req.peer_id.begin());
        req.downloaded = htobe64(params.downloaded);
        req.left = htobe64(params.left);
        req.uploaded = htobe64(params.uploaded);
        req.port = htobe16(params.port);
        if (params.event == "started") req.event = htobe32(2);
        else if (params.event == "completed") req.event = htobe32(1);
        else if (params.event == "stopped") req.event = htobe32(3);


        try {
            LOGDBG("Sending UDP announce request to tracker (try {})", n);
            co_await socket_.async_send_to(asio::buffer(&req, sizeof(req)), tracker_endpoint_, asio::use_awaitable);

            asio::steady_timer timer(io_context_);
            timer.expires_after(std::chrono::seconds(15 * (1 << n)));

            std::vector<char> response_buffer(2048);
            asio::ip::udp::endpoint sender_endpoint;

            auto result = co_await (
                socket_.async_receive_from(asio::buffer(response_buffer), sender_endpoint, asio::use_awaitable) ||
                timer.async_wait(asio::as_tuple(asio::use_awaitable))
            );

            if (result.index() == 0) {
                size_t bytes_received = std::get<0>(result);
                if (bytes_received >= sizeof(UdpAnnounceResponse)) {
                    auto* res = reinterpret_cast<UdpAnnounceResponse*>(response_buffer.data());
                    if (be32toh(res->transaction_id) == be32toh(req.transaction_id) && be32toh(res->action) == 1) {
                        TrackerAnnounceResult announce_result;
                        announce_result.interval_seconds = be32toh(res->interval);

                        size_t num_peers = (bytes_received - sizeof(UdpAnnounceResponse)) / sizeof(UdpPeerInfo);
                        LOGINFO("Received announce response with {} peers and interval {}", num_peers, announce_result.interval_seconds);

                        auto* peer_info_ptr = reinterpret_cast<UdpPeerInfo*>(response_buffer.data() + sizeof(UdpAnnounceResponse));
                        for (size_t i = 0; i < num_peers; ++i) {
                            asio::ip::address_v4 ip(be32toh(peer_info_ptr[i].ip));
                            uint16_t port = be16toh(peer_info_ptr[i].port);
                            announce_result.peers.push_back(ip.to_string() + ":" + std::to_string(port));
                        }
                        co_return announce_result;
                    }
                } 
            } else {
                LOGWARN("UDP announce request timed out. Retrying...");
            }
        } catch (const std::exception& e) {
            LOGERR("Error in UDP announce: {}", e.what());
        }

        ++n;
    }

    throw std::runtime_error("Failed to announce to UDP tracker after " + std::to_string(max_retries) + " retries.");
}


HttpTrackerClient::HttpTrackerClient(asio::io_context& io_context, std::string host, int port, std::string target)
    : io_context_(io_context), host_(std::move(host)), port_(port), target_(std::move(target)) {}


asio::awaitable<TrackerAnnounceResult> HttpTrackerClient::announce(const AnnounceRequestParams& params) {
    auto self = shared_from_this();

    try {
        std::stringstream query_ss;
        query_ss << "info_hash=" << url_encode(params.info_hash_bytes)
                 << "&peer_id=" << url_encode(params.peer_id)
                 << "&port=" << params.port
                 << "&uploaded=" << params.uploaded
                 << "&downloaded=" << params.downloaded
                 << "&left=" << params.left
                 << "&compact=1";

        if (!params.event.empty()) {
            query_ss << "&event=" << params.event;
        }
        std::string full_target = target_ + "?" + query_ss.str();

        // These objects perform our I/O
        tcp::resolver resolver(io_context_);
        beast::tcp_stream stream(io_context_);

        // Look up the domain name
        auto const results = co_await resolver.async_resolve(host_, std::to_string(port_), asio::use_awaitable);
        // Make the connection on the IP address we get from a lookup
        stream.expires_after(std::chrono::seconds(30));
        co_await stream.async_connect(results, asio::use_awaitable);
        // Set up an HTTP GET request message
        http::request<http::string_body> req{http::verb::get, full_target, 11};
        req.set(http::field::host, host_);
        req.set(http::field::user_agent, "Cpp-P2P-Client/1.0");
        // Send the HTTP request to the remote host
        co_await http::async_write(stream, req, asio::use_awaitable);
        // This buffer is used for reading and must be persisted
        beast::flat_buffer buffer;
        // Declare a container to hold the response
        http::response<http::string_body> res;
        // Receive the HTTP response
        co_await http::async_read(stream, buffer, res, asio::use_awaitable);
        
        // Gracefully close the socket
        beast::error_code ec;
        ec = stream.socket().shutdown(tcp::socket::shutdown_both, ec);
        if(ec && ec != beast::errc::not_connected)
            throw beast::system_error{ec};
        if (res.result() != http::status::ok) {
            throw std::runtime_error("Tracker returned non-200 status: " + std::to_string(res.result_int()));
        }
        
        Value decoded_body = decode({res.body().data(), res.body().size()});
        const auto* dict = std::get_if<std::unique_ptr<Dict>>(&decoded_body.get_variant());
        if (!dict) {
            throw std::runtime_error("Tracker response body is not a dictionary");
        }

        TrackerAnnounceResult result;
        result.interval_seconds = std::get<Integer>((*dict)->at("interval").get_variant());
        const auto& peers_str = std::get<String>((*dict)->at("peers").get_variant());
        if (peers_str.length() % 6 != 0) {
            throw std::runtime_error("Invalid peers list length in tracker response");
        }
        for (size_t i = 0; i < peers_str.length(); i += 6) {
            asio::ip::address_v4::bytes_type ip_bytes;
            std::copy_n(peers_str.data() + i, 4, ip_bytes.begin());
            
            uint16_t port_asio;
            std::memcpy(&port_asio, peers_str.data() + i + 4, 2);
            uint16_t port_host = ntohs(port_asio);
            result.peers.push_back(asio::ip::address_v4(ip_bytes).to_string() + ":" + std::to_string(port_host));
        }
        co_return result;
    } catch (const std::exception& e) {
        LOGERR("HTTP announce to {} failed: {}", get_url(), e.what());
        throw;
    }
}
 
