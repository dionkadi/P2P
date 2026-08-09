#pragma once

#include <boost/asio.hpp>

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <format>
#include <memory>
#include <optional>
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
    ///
    /// Must never close a socket from the caller's thread: a cross-thread
    /// close of a socket with a pending async op is undefined behavior
    /// (double deregister of the epoll descriptor state -> SEGV in
    /// epoll_reactor::deregister_descriptor, hit with ~80 concurrent HTTP
    /// announces). The HTTP/HTTPS implementations instead dispatch the
    /// socket close onto the stream's own strand, so the cancel is
    /// serialized with the op that owns the socket and the pending op
    /// completes with operation_canceled in-place. The UDP implementation
    /// closes its socket, which asio handles safely by completing all
    /// pending ops with operation_aborted (no internal second closer exists
    /// for UDP ops).
    ///
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
        // Safe: asio completes all ops pending on a closed socket with
        // operation_aborted, and UDP ops never close the socket themselves
        // (no multi-endpoint iteration), so no concurrent deregister exists.
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
        // Dispatch the socket close onto the stream's own strand: the
        // stream is only ever mutated there, so this is serialized with the
        // in-flight op, which completes with operation_canceled in-place.
        // Never close the socket from the caller's thread — a foreign-thread
        // close racing Beast's connect op (or its own deadline close) is a
        // use-after-free in epoll_reactor::deregister_descriptor.
        std::shared_ptr<beast::tcp_stream> stream;
        {
            std::lock_guard lock(stream_mutex_);
            stream = active_stream_;
        }
        if (stream) {
            asio::dispatch(stream->get_executor(), [stream] {
                boost::beast::error_code ec;
                stream->socket().close(ec);
            });
        }
    }

private:
    asio::io_context& io_context_;
    std::string host_;
    std::string target_;
    int port_;
    // Stream of the in-flight announce(), kept alive by a shared_ptr so a
    // cancel() dispatch can outlive the announce coroutine's frame. The
    // stream runs on its own strand (see announce()): Beast's internal
    // deadline timer closes the socket, and without a strand that close
    // races the connect op's own socket close in process() across io
    // threads (SEGV in epoll_reactor::deregister_descriptor once the
    // multi-tracker fan-out put ~80 concurrent announces in flight).
    std::mutex stream_mutex_;
    std::shared_ptr<beast::tcp_stream> active_stream_;
};

class HttpsTrackerClient: public ITrackerClient, public std::enable_shared_from_this<HttpsTrackerClient> {
public:
    HttpsTrackerClient(asio::io_context& io_context, std::string host, int port, std::string target)
        : io_context_(io_context), host_(std::move(host)), target_(std::move(target)), port_(port) {}

    asio::awaitable<TrackerAnnounceResult> announce(const AnnounceRequestParams& params) override;
    const std::string get_url() const override { return std::format("https://{}:{}", host_, port_); }
    void cancel() override {
        // Same strand-dispatched socket close as HttpTrackerClient.
        std::shared_ptr<beast::ssl_stream<beast::tcp_stream>> stream;
        {
            std::lock_guard lock(stream_mutex_);
            stream = active_stream_;
        }
        if (stream) {
            auto& lowest = beast::get_lowest_layer(*stream);
            asio::dispatch(lowest.get_executor(), [stream] {
                boost::beast::error_code ec;
                beast::get_lowest_layer(*stream).socket().close(ec);
            });
        }
    }

private:
    asio::io_context& io_context_;
    std::string host_;
    std::string target_;
    int port_;
    // In-flight announce()'s ssl_stream; see HttpTrackerClient for why it is
    // a mutex-guarded shared_ptr running on its own strand.
    std::mutex stream_mutex_;
    std::shared_ptr<beast::ssl_stream<beast::tcp_stream>> active_stream_;
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

// Returns the HTTP proxy (host, port) to use for `host`.
//
// Always returns nullopt: we connect directly, matching qBittorrent's
// "Proxy: None" default. System proxy env vars (http_proxy / all_proxy) are
// set by tools like Clash Verge and would hijack tracker announces while
// DHT (UDP) cannot be carried by an HTTP proxy — proxying only the tracker
// half leaves DHT unproxied and breaks swarm discovery. Users who genuinely
// need a relay can restore env-var proxying here.
static std::optional<std::pair<std::string, std::string>> announce_proxy_for(
    const std::string& scheme, const std::string& host) {
    (void)scheme; (void)host;
    return std::nullopt;
}

// HTTP proxy CONNECT tunnel: plain TCP to the proxy is already established;
// send CONNECT host:port and verify the proxy accepted the tunnel. After
// this the stream is end-to-end to the target (TLS handshake proceeds over
// it). read_header, not read: a 2xx CONNECT response has no body and no
// Content-Length/Transfer-Encoding, so http::read cannot frame it and would
// block until the connection closes (observed: hang on every proxy fetch).
static asio::awaitable<void> proxy_connect_tunnel(
    beast::tcp_stream& stream, const std::string& host, const std::string& port) {
    http::request<http::empty_body> connect_req{http::verb::connect, host + ":" + port, 11};
    connect_req.set(http::field::host, host + ":" + port);
    stream.expires_after(std::chrono::seconds(30));
    co_await http::async_write(stream, connect_req, asio::use_awaitable);

    beast::flat_buffer connect_buffer;
    http::response_parser<http::empty_body> connect_parser;
    stream.expires_after(std::chrono::seconds(30));
    co_await http::async_read_header(stream, connect_buffer, connect_parser, asio::use_awaitable);
    auto connect_res = connect_parser.release();
    if (connect_res.result() != http::status::ok) {
        throw std::runtime_error("Proxy CONNECT to " + host + ":" + port +
                                 " failed: HTTP " + std::to_string(static_cast<int>(connect_res.result())));
    }
}

// Parse a tracker announce response body (BEP-3). Tolerant of the quirks of
// real-world HTTP trackers: trailing data after the bencoded dictionary,
// missing or odd-typed "interval", and "peers" in either compact (BEP-23)
// or non-compact list-of-dicts form. Throws std::runtime_error when the
// body is not a bencoded dictionary at all.
inline TrackerAnnounceResult parse_tracker_response_body(std::span<const std::byte> body) {
    // decode() is strict and rejects trailing data ("Trailing data left
    // after decoding"); decode_prefix parses the first value and ignores
    // the rest.
    size_t consumed = 0;
    Value decoded_body = decode_prefix(body, consumed);
    const auto* dict = std::get_if<std::unique_ptr<Dict>>(&decoded_body.get_variant());
    if (!dict) {
        throw std::runtime_error("Tracker response body is not a dictionary");
    }

    TrackerAnnounceResult result;
    result.interval_seconds = 1800; // BEP-3 fallback when "interval" is absent
    if (auto interval_it = (*dict)->find("interval"); interval_it != (*dict)->end()) {
        if (const auto* interval = std::get_if<Integer>(&interval_it->second.get_variant())) {
            result.interval_seconds = static_cast<int>(*interval);
        }
    }

    auto peers_it = (*dict)->find("peers");
    if (peers_it == (*dict)->end()) {
        return result;
    }

    if (const auto* peers_str = std::get_if<String>(&peers_it->second.get_variant())) {
        // Compact 6-byte IP:port format (BEP-23). Tolerate a trailing
        // remainder instead of failing the whole announce.
        if (peers_str->length() % 6 != 0) {
            LOGWARN("Tracker 'peers' length {} is not a multiple of 6; truncating", peers_str->length());
        }
        for (size_t i = 0; i + 6 <= peers_str->length(); i += 6) {
            asio::ip::address_v4::bytes_type ip_bytes;
            std::copy_n(peers_str->data() + i, 4, ip_bytes.begin());

            uint16_t port_bytes;
            std::memcpy(&port_bytes, peers_str->data() + i + 4, 2);
            uint16_t port_host = ntohs(port_bytes);
            result.peers.push_back(asio::ip::address_v4(ip_bytes).to_string() + ":" + std::to_string(port_host));
        }
    } else if (const auto* peers_list = std::get_if<std::unique_ptr<List>>(&peers_it->second.get_variant())) {
        // Non-compact list-of-dicts (BEP-3): {"ip": str, "port": int}.
        for (const auto& entry : **peers_list) {
            const auto* entry_dict = std::get_if<std::unique_ptr<Dict>>(&entry.get_variant());
            if (!entry_dict) continue;
            const Dict& d = **entry_dict;
            auto ip_it = d.find("ip");
            auto port_it = d.find("port");
            if (ip_it == d.end() || port_it == d.end()) continue;
            const auto* ip_str = std::get_if<String>(&ip_it->second.get_variant());
            const auto* port_int = std::get_if<Integer>(&port_it->second.get_variant());
            if (!ip_str || !port_int) continue;
            if (ip_str->find(':') != std::string::npos) {
                LOGDBG("Tracker returned IPv6 peer {}; skipping (IPv4-only peer path)", *ip_str);
                continue;
            }
            result.peers.push_back(*ip_str + ":" + std::to_string(static_cast<uint16_t>(*port_int)));
        }
    } else {
        LOGWARN("Tracker 'peers' field has unexpected type; ignoring");
    }
    return result;
}

// Extract the tracker's own "failure reason" (BEP-3) from a non-200 body.
// Returns "" when the body isn't a bencoded dict or has no such key.
inline std::string parse_tracker_failure_reason(std::span<const std::byte> body) {
    try {
        size_t consumed = 0;
        Value err_body = decode_prefix(body, consumed);
        if (const auto* err_dict = std::get_if<std::unique_ptr<Dict>>(&err_body.get_variant())) {
            if (auto it = (*err_dict)->find("failure reason"); it != (*err_dict)->end()) {
                if (const auto* s = std::get_if<String>(&it->second.get_variant())) {
                    return *s;
                }
            }
        }
    } catch (...) {
        // Body isn't bencode — no failure reason available.
    }
    return {};
}

// Template helper: performs HTTP/HTTPS announce request/response cycle.
// Works with both beast::tcp_stream and beast::ssl_stream<beast::tcp_stream>.
// `proxy` (http proxy host, port) is non-empty when the connection was made
// through a proxy: plain HTTP then uses absolute-form request targets
// (RFC 7230) and HTTPS tunnels via CONNECT with path-form targets.
template <typename Stream>
asio::awaitable<TrackerAnnounceResult> http_announce_impl(
    Stream& stream,
    asio::io_context& io,
    const std::string& host,
    const std::string& target,
    const AnnounceRequestParams& params,
    const std::optional<std::pair<std::string, std::string>>& proxy = std::nullopt)
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
    std::string current_host = host;

    constexpr bool kIsTls = requires { stream.async_handshake(asio::ssl::stream_base::client, asio::use_awaitable); };
    // Plain HTTP through a proxy requires absolute-form targets; inside a
    // CONNECT tunnel (HTTPS) and on direct connections, path-form is used.
    const bool absolute_form = proxy && !kIsTls;

    constexpr int max_redirects = 5;
    for (int redirect = 0; redirect <= max_redirects; ++redirect) {
        http::request<http::string_body> req{http::verb::get,
            absolute_form ? "http://" + current_host + full_target : full_target, 11};
        req.set(http::field::host, current_host);
        req.set(http::field::user_agent, "qBittorrent/5.2.3");

        co_await http::async_write(stream, req, asio::use_awaitable);

        beast::flat_buffer buffer;
        http::response<http::string_body> res;
        co_await http::async_read(stream, buffer, res, asio::use_awaitable);

        if (res.result() == http::status::ok) {
            // Success — parse and return below. Real-world HTTP trackers
            // append trailing data (newlines, chunked-encoding remnants);
            // parse_tracker_response_body tolerates it.
            TrackerAnnounceResult result = parse_tracker_response_body(
                {reinterpret_cast<const std::byte *>(res.body().data()), res.body().size()});
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

            // Resolve the Location into an authority + path-form target. Sending
            // the absolute-form target with the stale Host header gets rejected
            // (403) by many tracker endpoints, so always use path-form with a
            // matching Host, and reconnect when the authority differs.
            std::string redirect_host = current_host;
            auto loc_r = boost::urls::parse_uri(loc);
            if (loc_r && loc_r->has_authority()) {
                redirect_host = std::string(loc_r->host());
                if (loc_r->has_port()) {
                    redirect_host += ":" + std::string(loc_r->port());
                }
                full_target = std::string(loc_r->encoded_path());
                if (loc_r->has_query()) {
                    full_target += "?" + std::string(loc_r->encoded_query());
                }
                if (full_target.empty()) {
                    full_target = "/";
                }
            } else {
                // Relative Location (path or /path?query): same authority.
                full_target = loc;
            }

            if (redirect_host != current_host) {
                // Cross-authority redirect: reconnect before following it.
                // Through a proxy, the target is the proxy itself — the proxy
                // resolves/connects to the new authority (curl behavior).
                LOGINFO("Tracker redirect to different host {}, reconnecting.", redirect_host);
                std::string port_str = loc_r && loc_r->has_port() ? std::string(loc_r->port()) : (kIsTls ? "443" : "80");
                std::string conn_host = proxy ? proxy->first : std::string(loc_r->host());
                std::string conn_port = proxy ? proxy->second : port_str;
                tcp::resolver resolver(io);
                auto const results = co_await resolver.async_resolve(
                    conn_host, conn_port, asio::use_awaitable);

                auto& lowest = beast::get_lowest_layer(stream);
                beast::error_code ignore;
                lowest.socket().close(ignore);
                lowest.expires_after(std::chrono::seconds(30));
                co_await lowest.async_connect(results, asio::use_awaitable);

                // Re-establish TLS if the stream is an SSL stream: open a new
                // CONNECT tunnel through the proxy, then handshake.
                if constexpr (kIsTls) {
                    if (proxy) {
                        co_await proxy_connect_tunnel(lowest, std::string(loc_r->host()), port_str);
                    }
                    co_await stream.async_handshake(asio::ssl::stream_base::client);
                }
            }
            current_host = redirect_host;
            continue;
        }

        // Surface the tracker's own BEP-3 "failure reason" when the body is
        // bencoded, so the error message is actionable instead of "<none>".
        std::string failure_reason = parse_tracker_failure_reason(
            {reinterpret_cast<const std::byte *>(res.body().data()), res.body().size()});
        throw std::runtime_error("Tracker returned non-200 status: " + std::to_string(res.result_int()) +
                                 (failure_reason.empty() ? std::string{} : " (" + failure_reason + ")"));
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
    std::string target = std::string(uv.encoded_path());
    if (target.empty()) target = "/";
    if (!uv.query().empty()) {
        target += "?";
        target += std::string(uv.encoded_query());
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
        if (!socket_.is_open()) {
            // Cancelled (client->cancel() closed the socket): stop retrying,
            // or the exponential backoff timers (15s/30s/60s/120s) would keep
            // the io_context alive for minutes after shutdown.
            break;
        }
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
        if (!socket_.is_open()) {
            // Cancelled (client->cancel() closed the socket): stop retrying,
            // or the exponential backoff timers (15s/30s/60s/120s) would keep
            // the io_context alive for minutes after shutdown.
            break;
        }
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
    // The stream runs on its own strand. Without it, Beast's internal 30s
    // deadline timer (expires_after) closes the socket from whichever io
    // thread the timer fires on, racing the connect op's own socket close in
    // process() on another io thread -> double deregister of the epoll
    // descriptor state -> SEGV in epoll_reactor::deregister_descriptor. This
    // surfaced once the multi-tracker fan-out put ~80 concurrent announces
    // in flight. On the strand, the timer handler and every op completion
    // are serialized, so the deadline close can never overlap op processing.
    auto stream = std::make_shared<beast::tcp_stream>(asio::make_strand(io_context_));
    {
        std::lock_guard lock(stream_mutex_);
        active_stream_ = stream;
    }
    try {
        auto proxy = announce_proxy_for("http", host_);
        tcp::resolver resolver(io_context_);

        // Through a proxy: connect to the proxy itself; requests then use
        // absolute-form targets (see http_announce_impl).
        auto const results = co_await resolver.async_resolve(
            proxy ? proxy->first : host_,
            proxy ? proxy->second : std::to_string(port_), asio::use_awaitable);
        stream->expires_after(std::chrono::seconds(30));
        // The connect op's internal work runs on the stream's strand, but
        // its FINAL completion (range_connect_op::process, which closes the
        // socket between endpoint attempts) executes on the handler's plain
        // io executor. A concurrent cancel() closes the socket on the
        // strand -> both deregister the epoll descriptor -> SEGV. Binding
        // the completion to the strand serializes it with cancel()'s close;
        // append() pins the stream so it outlives the pending op.
        co_await stream->async_connect(results,
            asio::bind_executor(stream->get_executor(),
                asio::append(asio::use_awaitable, stream)));

        auto result = co_await http_announce_impl(*stream, io_context_, host_, target_, params, proxy);

        beast::error_code ec;
        ec = stream->socket().shutdown(tcp::socket::shutdown_both, ec);
        if (ec && ec != beast::errc::not_connected)
            throw beast::system_error{ec};

        {
            std::lock_guard lock(stream_mutex_);
            active_stream_.reset();
        }
        co_return result;
    } catch (const std::exception& e) {
        {
            std::lock_guard lock(stream_mutex_);
            active_stream_.reset();
        }
        LOGERR("HTTP announce to {} failed: {}", get_url(), e.what());
        throw;
    }
}

inline asio::awaitable<TrackerAnnounceResult> HttpsTrackerClient::announce(const AnnounceRequestParams& params) {
    CTRACK_ASYNC("HttpsTrackerClient::announce");
    auto self = shared_from_this();
    // tls_client lets OpenSSL negotiate the highest mutually supported
    // version. Hardcoding tlsv12_client rejects trackers running legacy TLS
    // servers ("tlsv1 alert protocol version").
    boost::asio::ssl::context ssl_ctx(boost::asio::ssl::context::tls_client);
    ssl_ctx.set_default_verify_paths();
    // Same strand rationale as HttpTrackerClient::announce.
    auto stream = std::make_shared<beast::ssl_stream<beast::tcp_stream>>(asio::make_strand(io_context_), ssl_ctx);
    {
        std::lock_guard lock(stream_mutex_);
        active_stream_ = stream;
    }
    try {
        auto proxy = announce_proxy_for("https", host_);
        if (!SSL_set_tlsext_host_name(stream->native_handle(), host_.c_str())) {
            beast::error_code ec{static_cast<int>(::ERR_get_error()), asio::error::get_ssl_category()};
            throw beast::system_error{ec};
        }

        tcp::resolver resolver(io_context_);
        // Through a proxy: plain TCP to the proxy, then a CONNECT tunnel to
        // the tracker; TLS handshakes inside the tunnel.
        auto const results = co_await resolver.async_resolve(
            proxy ? proxy->first : host_,
            proxy ? proxy->second : std::to_string(port_), asio::use_awaitable);
        auto& lowest = beast::get_lowest_layer(*stream);
        lowest.expires_after(std::chrono::seconds(30));
        // Same completion-to-strand binding and stream pinning as
        // HttpTrackerClient::announce.
        co_await lowest.async_connect(results,
            asio::bind_executor(lowest.get_executor(),
                asio::append(asio::use_awaitable, stream)));
        if (proxy) {
            co_await proxy_connect_tunnel(lowest, host_, std::to_string(port_));
        }
        co_await stream->async_handshake(asio::ssl::stream_base::client);

        auto result = co_await http_announce_impl(*stream, io_context_, host_, target_, params, proxy);

        beast::error_code ec;
        stream->shutdown(ec);
        {
            std::lock_guard lock(stream_mutex_);
            active_stream_.reset();
        }
        co_return result;
    } catch (const std::exception& e) {
        {
            std::lock_guard lock(stream_mutex_);
            active_stream_.reset();
        }
        LOGERR("HTTPS announce to {} failed: {}", get_url(), e.what());
        throw;
    }
}