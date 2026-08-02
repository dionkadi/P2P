#pragma once

#include <boost/asio.hpp>

#include <array>
#include <cstddef>
#include <cstdint>
#include <format>
#include <memory>
#include <string>
#include <vector>
#include <random>
#include <boost/url.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>
#include <boost/beast/ssl.hpp>

#include "Utils.hpp"
#include "Bencode.hpp"
#include "HttpServer.hpp"

namespace asio = boost::asio;
using namespace boost::asio::experimental::awaitable_operators;

class PeerLogic;

class ITrackerClient {
public:
    virtual ~ITrackerClient() = default;

    virtual asio::awaitable<TrackerAnnounceResult> announce(const AnnounceRequestParams& params) = 0;
    virtual const std::string get_url() const = 0;

    /// Cancel any in-flight announce() operation.
    /// After cancel(), the next announce() call creates a fresh connection.
    /// Thread-safe: implementations must not require external synchronization.
    virtual void cancel() = 0;
};

class UdpTrackerClient: public ITrackerClient, public std::enable_shared_from_this<UdpTrackerClient> {
public:
    UdpTrackerClient(asio::io_context& io_context, std::string host, int port)
        : io_context_(io_context), socket_(io_context),
          host_(std::move(host)), port_(port),
          url_str_(std::format("{}:{}", host_, port_))
    {
        std::random_device rd;
        next_transaction_id_ = rd();
    }

    asio::awaitable<TrackerAnnounceResult> announce(const AnnounceRequestParams& params) override;
    const std::string get_url() const override { return "udp://" + url_str_; }
    void cancel() override {
        boost::system::error_code ec;
        socket_.close(ec);
    }

private:
    asio::awaitable<void> ensure_resolved();
    asio::awaitable<bool> connect_to_tracker();

    asio::io_context& io_context_;
    asio::ip::udp::socket socket_;
    std::string host_;
    int port_;
    asio::ip::udp::endpoint tracker_endpoint_;
    bool resolved_{false};

    asio::steady_timer::time_point connection_id_expiry_;
    uint32_t next_transaction_id_{0};
    uint64_t connection_id_{0};

    std::string url_str_;
};

class HttpTrackerClient: public ITrackerClient, public std::enable_shared_from_this<HttpTrackerClient> {
public:
    HttpTrackerClient(asio::io_context& io_context, std::string host, int port, std::string target)
        : io_context_(io_context), host_(std::move(host)), target_(std::move(target)), port_(port) {}

    asio::awaitable<TrackerAnnounceResult> announce(const AnnounceRequestParams& params) override;
    const std::string get_url() const override { return std::format("http://{}:{}", host_, port_); }
    void cancel() override {
        if (active_stream_) {
            boost::beast::error_code ec;
            active_stream_->socket().close(ec);
        }
    }

private:
    asio::io_context& io_context_;
    std::string host_;
    std::string target_;
    int port_;
    // Non-owning pointer set during announce(), cleared when announce() completes.
    // Points to a local tcp_stream on the coroutine frame (or the lowest layer
    // of an ssl_stream). Valid only while announce() is in-flight.
    boost::beast::tcp_stream* active_stream_ = nullptr;
};

class HttpsTrackerClient: public ITrackerClient, public std::enable_shared_from_this<HttpsTrackerClient> {
public:
    HttpsTrackerClient(asio::io_context& io_context, std::string host, int port, std::string target)
        : io_context_(io_context), host_(std::move(host)), target_(std::move(target)), port_(port) {}

    asio::awaitable<TrackerAnnounceResult> announce(const AnnounceRequestParams& params) override;
    const std::string get_url() const override { return std::format("https://{}:{}", host_, port_); }
    void cancel() override {
        if (active_stream_) {
            boost::beast::error_code ec;
            active_stream_->socket().close(ec);
        }
    }

private:
    asio::io_context& io_context_;
    std::string host_;
    std::string target_;
    int port_;
    // Non-owning pointer to the lowest layer (tcp_stream) of the SSL stream.
    // Set during announce(), cleared when announce() completes.
    boost::beast::tcp_stream* active_stream_ = nullptr;
};

template <typename Cont>
std::string url_encode(const Cont& value) {
    std::ostringstream escaped;
    escaped.fill('0');
    escaped << std::hex;
    for (auto b : value) {
        char c = static_cast<char>(b);
        if (std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
            escaped << c;
        } else {
            escaped << '%' << std::setw(2) << static_cast<int>(static_cast<unsigned char>(c));
        }
    }
    return escaped.str();
}

// Template helper: performs HTTP/HTTPS announce request/response cycle.
// Works with both beast::tcp_stream and beast::ssl_stream<beast::tcp_stream>.
template <typename Stream>
asio::awaitable<TrackerAnnounceResult> http_announce_impl(
    Stream& stream,
    const std::string& host,
    const std::string& target,
    const AnnounceRequestParams& params)
{
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
    std::string full_target = target + "?" + query_ss.str();

    constexpr int max_redirects = 5;
    for (int redirect = 0; redirect <= max_redirects; ++redirect) {
        http::request<http::string_body> req{http::verb::get, full_target, 11};
        req.set(http::field::host, host);
        req.set(http::field::user_agent, "Cpp-P2P-Client/1.0");

        co_await http::async_write(stream, req, asio::use_awaitable);

        beast::flat_buffer buffer;
        http::response<http::string_body> res;
        co_await http::async_read(stream, buffer, res, asio::use_awaitable);

        if (res.result() == http::status::ok) {
            // Success — parse and return below
            Value decoded_body = decode({reinterpret_cast<std::byte *>(res.body().data()), res.body().size()});
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

                uint16_t port_bytes;
                std::memcpy(&port_bytes, peers_str.data() + i + 4, 2);
                uint16_t port_host = ntohs(port_bytes);
                result.peers.push_back(asio::ip::address_v4(ip_bytes).to_string() + ":" + std::to_string(port_host));
            }
            co_return result;
        }

        if (res.result() == http::status::found ||
            res.result() == http::status::moved_permanently ||
            res.result() == http::status::temporary_redirect ||
            res.result() == http::status::permanent_redirect) {
            auto location = res[http::field::location];
            if (location.empty()) {
                throw std::runtime_error("Tracker returned redirect with no Location header");
            }
            std::string loc(location);
            LOGDBG("Tracker redirect ({}): {} -> {}", res.result_int(), full_target, loc);
            // For same-host path redirects, update target and retry
            full_target = loc;
            continue;
        }

        throw std::runtime_error("Tracker returned non-200 status: " + std::to_string(res.result_int()) +
                                 " (" + std::string(res.reason()) + ")");
    }

    throw std::runtime_error("Tracker returned too many redirects (" + std::to_string(max_redirects) + ")");
}

inline std::shared_ptr<ITrackerClient> create_tracker_client(asio::io_context& io_context, const std::string& tracker_url) {
    using namespace boost;
    system::result<urls::url_view> r = urls::parse_uri(tracker_url);
    if (!r) {
        throw std::runtime_error("Invalid tracker URL: " + r.error().message());
    }
    urls::url_view uv = *r;

    std::string scheme = uv.scheme();
    std::string host = uv.host();
    std::string target = uv.path();
    if (target.empty()) target = "/";
    if (!uv.query().empty()) {
        target += "?";
        target += uv.query();
    }

    uint16_t port = 0;
    if (uv.has_port()) {
        port = uv.port_number();
    } else {
        if (scheme == "http") port = 80;
        else if (scheme == "https") port = 443;
        // UDP doesn't have a standard default port in this context, 
        // but often it's 6969. Your manual parsing required it.
    }
    if (port == 0) {
        throw std::runtime_error("Tracker URL must specify a port.");
    }
    
    if (scheme == "udp") {
        LOGINFO("Creating UDP Tracker client for {}:{}", host, port);
        return std::make_shared<UdpTrackerClient>(io_context, host, port);
    } 
    else if (scheme == "http") {
        LOGINFO("Creating HTTP Tracker client for {}:{}", host, port);
        return std::make_shared<HttpTrackerClient>(io_context, host, port, target);
    }
    else if (scheme == "https") {
        LOGINFO("Creating HTTPS Tracker client for {}:{}", host, port);
        return std::make_shared<HttpsTrackerClient>(io_context, host, port, target);
    }
    else {
        throw std::runtime_error("Unsupported tracker scheme: " + scheme);
    }
}

inline asio::awaitable<void> UdpTrackerClient::ensure_resolved() {
    if (resolved_) co_return;
    asio::ip::udp::resolver resolver(io_context_);
    auto results = co_await resolver.async_resolve(host_, std::to_string(port_), asio::use_awaitable);
    tracker_endpoint_ = *results.begin();
    socket_.open(tracker_endpoint_.protocol());
    resolved_ = true;
}

inline asio::awaitable<bool> UdpTrackerClient::connect_to_tracker() {
    CTRACK_ASYNC("UdpTrackerClient::connect_to_tracker");
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

inline asio::awaitable<TrackerAnnounceResult> UdpTrackerClient::announce(const AnnounceRequestParams& params) {
    CTRACK_ASYNC("UdpTrackerClient::announce");
    auto self = shared_from_this();

    co_await ensure_resolved();

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

inline asio::awaitable<TrackerAnnounceResult> HttpTrackerClient::announce(const AnnounceRequestParams& params) {
    CTRACK_ASYNC("HttpTrackerClient::announce");
    auto self = shared_from_this();
    beast::tcp_stream stream(io_context_);
    active_stream_ = &stream;
    try {
        tcp::resolver resolver(io_context_);

        auto const results = co_await resolver.async_resolve(host_, std::to_string(port_), asio::use_awaitable);
        stream.expires_after(std::chrono::seconds(30));
        co_await stream.async_connect(results, asio::use_awaitable);

        auto result = co_await http_announce_impl(stream, host_, target_, params);

        beast::error_code ec;
        ec = stream.socket().shutdown(tcp::socket::shutdown_both, ec);
        if (ec && ec != beast::errc::not_connected)
            throw beast::system_error{ec};

        active_stream_ = nullptr;
        co_return result;
    } catch (const std::exception& e) {
        active_stream_ = nullptr;
        LOGERR("HTTP announce to {} failed: {}", get_url(), e.what());
        throw;
    }
}

inline asio::awaitable<TrackerAnnounceResult> HttpsTrackerClient::announce(const AnnounceRequestParams& params) {
    CTRACK_ASYNC("HttpsTrackerClient::announce");
    auto self = shared_from_this();
    boost::asio::ssl::context ssl_ctx(boost::asio::ssl::context::tlsv12_client);
    ssl_ctx.set_default_verify_paths();
    beast::ssl_stream<beast::tcp_stream> stream(io_context_, ssl_ctx);
    active_stream_ = &beast::get_lowest_layer(stream);
    try {
        if (!SSL_set_tlsext_host_name(stream.native_handle(), host_.c_str())) {
            beast::error_code ec{static_cast<int>(::ERR_get_error()), asio::error::get_ssl_category()};
            throw beast::system_error{ec};
        }

        tcp::resolver resolver(io_context_);
        auto const results = co_await resolver.async_resolve(host_, std::to_string(port_), asio::use_awaitable);
        beast::get_lowest_layer(stream).expires_after(std::chrono::seconds(30));
        co_await beast::get_lowest_layer(stream).async_connect(results, asio::use_awaitable);
        co_await stream.async_handshake(asio::ssl::stream_base::client);

        auto result = co_await http_announce_impl(stream, host_, target_, params);

        beast::error_code ec;
        stream.shutdown(ec);
        active_stream_ = nullptr;
        co_return result;
    } catch (const std::exception& e) {
        active_stream_ = nullptr;
        LOGERR("HTTPS announce to {} failed: {}", get_url(), e.what());
        throw;
    }
}