#pragma once

#include <boost/asio.hpp>

#include <array>
#include <cstddef>
#include <cstdint>
#include <format>
#include <memory>
#include <string>
#include <vector>

namespace asio = boost::asio;

using PeerId = std::array<std::byte, 20>;

class PeerLogic;

struct AnnounceRequestParams {
    std::vector<std::byte> info_hash_bytes;
    PeerId peer_id;
    std::string event;
    uint16_t port;
    uint64_t uploaded;
    uint64_t downloaded;
    uint64_t left;
};

struct TrackerAnnounceResult {
    int interval_seconds;
    std::vector<std::string> peers;
};

class ITrackerClient {
public:
    virtual ~ITrackerClient() = default;

    virtual asio::awaitable<TrackerAnnounceResult> announce(const AnnounceRequestParams& params) = 0;
    virtual const std::string get_url() const = 0;
};

class UdpTrackerClient: public ITrackerClient, public std::enable_shared_from_this<UdpTrackerClient> {
public:
    UdpTrackerClient(asio::io_context& io_context, std::string host, int port);
    asio::awaitable<TrackerAnnounceResult> announce(const AnnounceRequestParams& params) override;
    const std::string get_url() const override { return "udp://" + url_str_; }

private:
    asio::awaitable<bool> connect_to_tracker();

    asio::io_context& io_context_;
    asio::ip::udp::socket socket_;
    asio::ip::udp::endpoint tracker_endpoint_;

    asio::steady_timer::time_point connection_id_expiry_;
    uint32_t next_transaction_id_{0};
    uint64_t connection_id_{0};

    std::string url_str_;
};

class HttpTrackerClient: public ITrackerClient, public std::enable_shared_from_this<HttpTrackerClient> {
public:
    HttpTrackerClient(asio::io_context&, std::string host, int port, std::string target);
    asio::awaitable<TrackerAnnounceResult> announce(const AnnounceRequestParams& params) override;
    const std::string get_url() const override { return std::format("http://{}:{}", host_, port_); }

private:
    asio::io_context& io_context_;
    std::string host_;
    std::string target_;
    int port_;
};

std::shared_ptr<ITrackerClient> create_tracker_client(asio::io_context& io_context, const std::string& tracker_url);