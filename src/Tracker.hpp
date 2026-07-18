#pragma once

#include <cstddef>
#include <memory>
#include <span>
#include <string>
#include <random>
#include <map>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <boost/asio.hpp>
#include <boost/container/flat_set.hpp>
#include <vector>

#include "HttpServer.hpp"
#include "Bencode.hpp"
#include "Utils.hpp"

namespace asio = boost::asio;

class Tracker {
public:
    struct PeerEntry {
        int64_t left = 0;
        std::chrono::system_clock::time_point last_announce;
    };

    Tracker(asio::io_context& ioc) noexcept : io_context_(ioc), strand_(asio::make_strand(ioc)) {}

    ~Tracker() { stop_http_listener(); }

    Tracker(const Tracker&) = delete;
    Tracker& operator= (const Tracker&) = delete;

    void listen_http(int port) {
        http_router_ = std::make_shared<HttpRouter>();
        http_router_->add_route("/announce", create_announce_handler());
        http_router_->add_route("/", create_announce_handler());
        auto const address = asio::ip::make_address("0.0.0.0");
        auto endpoint = tcp::endpoint{address, static_cast<unsigned short>(port)};
        http_acceptor_ = std::make_shared<tcp::acceptor>(io_context_);
        http_acceptor_->open(endpoint.protocol());
        http_acceptor_->set_option(tcp::acceptor::reuse_address(true));
        http_acceptor_->bind(endpoint);
        http_acceptor_->listen();
        asio::co_spawn(
            io_context_,
            http_listener(http_acceptor_, http_router_),
            asio::detached
        );
    }

    void stop_http_listener() {
        if (http_acceptor_ && http_acceptor_->is_open()) {
            boost::system::error_code ec;
            http_acceptor_->cancel(ec);
            http_acceptor_->close(ec);
            http_acceptor_.reset();
        }
    }

    void listen_udp(int port) {
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
    
    void run() {
        throw std::runtime_error("Tracker::run() should not be called in this test setup. Use shared io_context.");
    }

    void stop_udp_listener();

    void start_background_tasks(const std::filesystem::path& data_dir) {
        data_dir_ = data_dir;
        asio::co_spawn(io_context_, cleanup_loop(), asio::detached);
    }

    void set_interval(int interval_secs) noexcept { interval_ = interval_secs; }

    asio::io_context& get_io_context() noexcept { return io_context_; }

    void save_state() {
        try {
            if (data_dir_.empty()) return;
            auto state_path = data_dir_ / "tracker_state.bencode";
            std::filesystem::create_directories(data_dir_);

            Dict root;
            Dict peers_dict;
            for (const auto& [info_hash, peer_map] : peers_) {
                List peer_list;
                for (const auto& [addr, entry] : peer_map) {
                    Dict pd;
                    pd["compact"] = Value(addr);
                    pd["left"] = Value(static_cast<Integer>(entry.left));
                    int64_t ts = std::chrono::system_clock::to_time_t(entry.last_announce);
                    pd["last_announce"] = Value(static_cast<Integer>(ts));
                    peer_list.push_back(Value(std::move(pd)));
                }
                String hash_key(reinterpret_cast<const char*>(info_hash.data()), info_hash.size());
                peers_dict[hash_key] = Value(std::move(peer_list));
            }
            root["peers"] = Value(std::move(peers_dict));

            auto encoded = encode(Value(std::move(root)));
            std::ofstream ofs(state_path, std::ios::binary);
            ofs.write(reinterpret_cast<const char*>(encoded.data()), static_cast<std::streamsize>(encoded.size()));
            LOGINFO("Tracker state saved to {} ({} info hashes)", state_path.string(), peers_.size());
        } catch (const std::exception& e) {
            LOGERR("Failed to save tracker state: {}", e.what());
        }
    }

    void load_state(const std::filesystem::path& data_dir) {
        data_dir_ = data_dir;
        auto state_path = data_dir_ / "tracker_state.bencode";

        if (!std::filesystem::exists(state_path)) {
            LOGINFO("No tracker state file found at {}. Starting fresh.", state_path.string());
            return;
        }

        try {
            std::ifstream ifs(state_path, std::ios::binary | std::ios::ate);
            if (!ifs) {
                LOGWARN("Cannot open tracker state file {}. Starting fresh.", state_path.string());
                return;
            }
            auto size = ifs.tellg();
            if (size <= 0) {
                LOGWARN("Tracker state file {} is empty. Starting fresh.", state_path.string());
                return;
            }
            ifs.seekg(0);

            std::vector<std::byte> file_data(static_cast<size_t>(size));
            ifs.read(reinterpret_cast<char*>(file_data.data()), size);

            auto decoded = decode(std::span<const std::byte>(file_data));
            auto& root_dict = *std::get<std::unique_ptr<Dict>>(decoded.get_variant());

            auto& peers_val = root_dict.at("peers");
            auto& peers_dict = *std::get<std::unique_ptr<Dict>>(peers_val.get_variant());

            for (const auto& [hash_key, peer_list_val] : peers_dict) {
                std::vector<std::byte> info_hash;
                for (char c : hash_key) {
                    info_hash.push_back(static_cast<std::byte>(c));
                }

                auto& peer_list = *std::get<std::unique_ptr<List>>(peer_list_val.get_variant());
                auto& peer_map = peers_[info_hash];

                for (const auto& pv : peer_list) {
                    auto& pd = *std::get<std::unique_ptr<Dict>>(pv.get_variant());
                    auto& compact_addr = std::get<String>(pd.at("compact").get_variant());
                    int64_t left_val = std::get<Integer>(pd.at("left").get_variant());
                    int64_t ts = std::get<Integer>(pd.at("last_announce").get_variant());

                    PeerEntry entry;
                    entry.left = left_val;
                    entry.last_announce = std::chrono::system_clock::from_time_t(static_cast<std::time_t>(ts));
                    peer_map[compact_addr] = entry;
                }
            }

            LOGINFO("Tracker state loaded from {} ({} info hashes)", state_path.string(), peers_.size());
        } catch (const std::exception& e) {
            LOGERR("Failed to load tracker state: {} (starting fresh)", e.what());
            peers_.clear();
        }
    }

    using PeerMap = std::map<std::string, PeerEntry>;
    
    const std::map<std::vector<std::byte>, PeerMap>& get_peers() const noexcept { return peers_; }
    
    std::pair<int, int> count_seeders_leechers(const std::vector<std::byte>& info_hash) const {
        auto it = peers_.find(info_hash);
        if (it == peers_.end()) return {0, 0};
        int seeders = 0, leechers = 0;
        for (const auto& [addr, entry] : it->second) {
            if (entry.left == 0) ++seeders;
            else ++leechers;
        }
        return {seeders, leechers};
    }

    int get_interval() const noexcept { return interval_; }

    auto& get_strand() noexcept { return strand_; }

    // UI counters
    std::atomic<uint64_t> announce_count_{0};
    std::atomic<uint64_t> scrape_count_{0};

private:
    asio::awaitable<void> udp_listen_loop(int port);
    asio::awaitable<void> handle_udp_request(asio::ip::udp::endpoint remote_endpoint, std::span<const char> request, asio::ip::udp::socket& socket);

    HttpHandler create_announce_handler();
    
    asio::awaitable<void> cleanup_loop() {
        CTRACK_ASYNC("Tracker::cleanup_loop");
        auto executor = co_await asio::this_coro::executor;
        asio::steady_timer timer(executor);

        while (true) {
            timer.expires_after(std::chrono::seconds(interval_ * 2));
            boost::system::error_code ec;
            co_await timer.async_wait(asio::redirect_error(asio::use_awaitable, ec));
            if (ec == asio::error::operation_aborted) {
                co_return;
            }

            co_await asio::dispatch(strand_, asio::use_awaitable);

            auto now = std::chrono::system_clock::now();
            auto threshold = now - std::chrono::seconds(interval_ * 2);

            LOGDBG("Tracker cleanup: checking for stale peers (threshold: {} sec ago)", interval_ * 2);

            size_t removed = 0;
            for (auto it = peers_.begin(); it != peers_.end(); ) {
                auto& peer_map = it->second;
                for (auto pit = peer_map.begin(); pit != peer_map.end(); ) {
                    if (pit->second.last_announce < threshold) {
                        pit = peer_map.erase(pit);
                        ++removed;
                    } else {
                        ++pit;
                    }
                }
                if (peer_map.empty()) {
                    it = peers_.erase(it);
                } else {
                    ++it;
                }
            }

            if (removed > 0) {
                LOGINFO("Tracker cleanup: removed {} stale peers", removed);
            }

            if (!data_dir_.empty()) {
                save_state();
            }
        }
    }
    
    asio::io_context& io_context_;
    asio::strand<asio::io_context::executor_type> strand_;
    
    std::map<std::vector<std::byte>, std::map<std::string, PeerEntry>> peers_;
    
    int interval_ = 1800;
    std::filesystem::path data_dir_;

    std::shared_ptr<HttpRouter> http_router_;
    std::shared_ptr<tcp::acceptor> http_acceptor_;

    struct UdpClientInfo {
        uint64_t connection_id;
        std::chrono::steady_clock::time_point expiry;
    };
    std::unique_ptr<asio::ip::udp::socket> udp_socket_;
    std::map<asio::ip::udp::endpoint, UdpClientInfo> udp_clients_;
    std::mt19937_64 rng_{std::random_device{}()};
};

inline std::string url_decode(std::string_view str) {
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

inline std::map<std::string, std::string> parse_query_params(std::string_view query) {
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

inline HttpHandler Tracker::create_announce_handler() {
    return [this](HttpRequest req) -> asio::awaitable<HttpResponse> {
        CTRACK_ASYNC("Tracker::handle_announce");
        HttpResponse res{http::status::ok, req.version()};
        res.set(http::field::server, BOOST_BEAST_VERSION_STRING);
        res.set(http::field::content_type, "text/plain");
        res.keep_alive(false);
        try {
            auto target = req.target();
            auto query_pos = target.find('?');
            std::string_view query_str = (query_pos != std::string_view::npos) ? target.substr(query_pos + 1) : "";
            
            auto params = parse_query_params(query_str);
            
            std::string info_hash_raw = url_decode(params.at("info_hash"));
            std::vector<std::byte> info_hash_bytes;
            for (char c : info_hash_raw) {
                info_hash_bytes.push_back(static_cast<std::byte>(c));
            }
            uint16_t port = static_cast<uint16_t>(std::stoi(params.at("port")));
            std::string ip = std::string(req["remote_endpoint"]);
            
            int64_t left = 0;
            auto left_it = params.find("left");
            if (left_it != params.end()) {
                left = std::stoll(left_it->second);
            }
            
            std::string event;
            auto event_it = params.find("event");
            if (event_it != params.end()) {
                event = event_it->second;
            }
            
            std::string compact_peer_addr(6, '\0');
            asio::ip::address_v4::bytes_type ip_bytes = asio::ip::make_address_v4(ip).to_bytes();
            uint16_t port_net = htons(port);
            std::memcpy(&compact_peer_addr[0], ip_bytes.data(), 4);
            std::memcpy(&compact_peer_addr[4], &port_net, 2);
            
            announce_count_.fetch_add(1, std::memory_order_relaxed);

            co_await asio::dispatch(strand_, asio::use_awaitable);

            if (event == "stopped") {
                auto it = peers_.find(info_hash_bytes);
                if (it != peers_.end()) {
                    it->second.erase(compact_peer_addr);
                    if (it->second.empty()) {
                        peers_.erase(it);
                    }
                }
                Dict response_dict;
                response_dict["interval"] = Value(static_cast<Integer>(interval_));
                response_dict["complete"] = Value(static_cast<Integer>(0));
                response_dict["incomplete"] = Value(static_cast<Integer>(0));
                response_dict["peers"] = Value(std::string());
                auto bencoded_body_vec = encode(Value(response_dict));
                res.body().assign(bencoded_body_vec.begin(), bencoded_body_vec.end());
                res.prepare_payload();
                co_return res;
            }
            
            auto& peer_map = peers_[info_hash_bytes];
            PeerEntry entry;
            entry.left = left;
            entry.last_announce = std::chrono::system_clock::now();
            peer_map[compact_peer_addr] = entry;
            
            std::string peers_binary;
            int complete = 0, incomplete = 0;
            for (const auto& [addr, pe] : peer_map) {
                if (pe.left == 0) ++complete;
                else ++incomplete;
                if (addr != compact_peer_addr) {
                    peers_binary.append(addr);
                }
            }
            
            LOGINFO("HTTP announce from {}:{} for hash {}. Seeders: {}, Leechers: {}. Returning {} peers.",
                    ip, port, Crypto::bytes_to_hex(info_hash_bytes), complete, incomplete, peers_binary.size() / 6);
            
            Dict response_dict;
            response_dict["interval"] = Value(static_cast<Integer>(interval_));
            response_dict["complete"] = Value(static_cast<Integer>(complete));
            response_dict["incomplete"] = Value(static_cast<Integer>(incomplete));
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

inline asio::awaitable<void> Tracker::udp_listen_loop(int port) {
    CTRACK_ASYNC("Tracker::udp_listen_loop");
    auto executor = co_await asio::this_coro::executor;
    udp_socket_ = std::make_unique<asio::ip::udp::socket>(executor, asio::ip::udp::endpoint(asio::ip::udp::v4(), port));
    udp_socket_->set_option(asio::ip::udp::socket::reuse_address(true));
    LOGINFO("UDP server listening on port {}", port);
    std::vector<char> buffer(2048); 
    asio::ip::udp::endpoint remote_endpoint;
    while (true) {
        boost::system::error_code ec;
        size_t bytes_recvd = co_await udp_socket_->async_receive_from(asio::buffer(buffer), remote_endpoint, asio::redirect_error(asio::use_awaitable, ec));
        if (ec == asio::error::operation_aborted) {
            LOGDBG("UDP listener aborted.");
            co_return;
        }
        if (ec) {
            LOGERR("UDP receive error: {}", ec.message());
            co_return;
        }
        
        asio::co_spawn(
            executor,
            handle_udp_request(remote_endpoint, {buffer.data(), bytes_recvd}, *udp_socket_),
            asio::detached
        );
    }
}

inline void Tracker::stop_udp_listener() {
    if (udp_socket_ && udp_socket_->is_open()) {
        boost::system::error_code ec;
        udp_socket_->cancel(ec);
        udp_socket_->close(ec);
        if (ec) {
            LOGWARN("Error closing UDP socket: {}", ec.message());
        }
        udp_socket_.reset();
    }
}

inline asio::awaitable<void> Tracker::handle_udp_request(asio::ip::udp::endpoint remote_endpoint, std::span<const char> request, asio::ip::udp::socket& socket) {
    CTRACK_ASYNC("Tracker::handle_udp_request");
    if (request.size() < sizeof(UdpConnectRequest)) {
        LOGWARN("Received too-small UDP packet from {}", remote_endpoint.address().to_string());
        co_return;
    }
    try {
        uint32_t action = be32toh(*reinterpret_cast<const uint32_t *>(request.data() + 8));
        if (action == 0) {
            LOGINFO("Received UDP Connect request from {}", remote_endpoint.address().to_string());
            uint64_t new_connection_id = rng_();
            udp_clients_[remote_endpoint] = {
                new_connection_id,
                std::chrono::steady_clock::now() + std::chrono::minutes(2)
            };

            auto *req = reinterpret_cast<const UdpConnectRequest *>(request.data());
            UdpConnectResponse res;
            res.action = htobe32(0);
            res.transaction_id = req->transaction_id;
            res.connection_id = htobe64(new_connection_id);
            co_await socket.async_send_to(asio::buffer(&res, sizeof(res)), remote_endpoint, asio::use_awaitable);
        
        } else if (action == 1) {
            if (request.size() < sizeof(UdpAnnounceRequest)) {
                LOGWARN("Received too-small UDP Announce packet from {}", remote_endpoint.address().to_string());
                co_return;
            }
            auto *req = reinterpret_cast<const UdpAnnounceRequest *>(request.data());

            uint64_t received_conn_id = be64toh(req->connection_id);
            auto it = udp_clients_.find(remote_endpoint);
            if (it == udp_clients_.end() || 
                it->second.connection_id != received_conn_id ||
                it->second.expiry < std::chrono::steady_clock::now()) 
            {
                LOGWARN("UDP Announce from {} with invalid/expired connection ID. Ignoring.", remote_endpoint.address().to_string());
                co_return;
            }

            std::vector<std::byte> info_hash_bytes(
                reinterpret_cast<const std::byte *>(req->info_hash.begin()), 
                reinterpret_cast<const std::byte *>(req->info_hash.begin()) + req->info_hash.size()
            );
            
            int64_t left = static_cast<int64_t>(be64toh(req->left));
            
            std::string peer_addr(6, '\0');
            auto ip_bytes = remote_endpoint.address().to_v4().to_bytes();
            uint16_t port_net = htons(be16toh(req->port));
            std::memcpy(&peer_addr[0], ip_bytes.data(), 4);
            std::memcpy(&peer_addr[4], &port_net, 2);
            announce_count_.fetch_add(1, std::memory_order_relaxed);

            LOGINFO("Received UDP Announce from {} for hash {} (left: {})", 
                    remote_endpoint.address().to_string(), Crypto::bytes_to_hex(info_hash_bytes), left);
            
            co_await asio::dispatch(strand_, asio::use_awaitable);
            
            auto& peer_map = peers_[info_hash_bytes];
            PeerEntry entry;
            entry.left = left;
            entry.last_announce = std::chrono::system_clock::now();
            peer_map[peer_addr] = entry;
            
            UdpAnnounceResponse res_header;
            res_header.action = htobe32(1);
            res_header.transaction_id = req->transaction_id;
            res_header.interval = htobe32(static_cast<uint32_t>(interval_)); 
            
            std::vector<char> response_body;
            int complete = 0, incomplete = 0;
            for (const auto& [addr, pe] : peer_map) {
                if (pe.left == 0) ++complete;
                else ++incomplete;
                if (addr != peer_addr) {
                    response_body.insert(response_body.end(), addr.begin(), addr.end());
                }
            }
            res_header.seeders = htobe32(static_cast<uint32_t>(complete));
            res_header.leechers = htobe32(static_cast<uint32_t>(incomplete));
            
            LOGDBG("UDP Announce response: {} seeders, {} leechers, returning {} peers",
                   complete, incomplete, response_body.size() / 6);
            
            std::vector<char> full_response;
            full_response.insert(full_response.end(), reinterpret_cast<char *>(&res_header), reinterpret_cast<char *>(&res_header) + sizeof(res_header));
            full_response.insert(full_response.end(), response_body.begin(), response_body.end());
            co_await socket.async_send_to(asio::buffer(full_response), remote_endpoint, asio::use_awaitable);
        } else {
            LOGWARN("Received unknown UDP action {} from {}", action, remote_endpoint.address().to_string());
        }
    } catch (const std::exception& e) {
        LOGERR("Error handling UDP request from {}: {}", remote_endpoint.address().to_string(), e.what());
    }
}
