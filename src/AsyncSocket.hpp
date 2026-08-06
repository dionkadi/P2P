#pragma once

#include <boost/asio.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/udp.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/system/detail/error_code.hpp>
#include <cstdint>
#include <cstring>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <vector>
namespace asio = boost::asio;

#include "Utils.hpp"


static constexpr uint32_t MAX_MESSAGE_SIZE = 10 * 1024 * 1024;

class AsyncSocket {
public:
    explicit AsyncSocket(asio::ip::tcp::socket socket) noexcept
        : write_state_(std::make_shared<WriteQueueState>()), socket_(std::move(socket)) {}

    AsyncSocket(const AsyncSocket&) = delete;
    AsyncSocket& operator=(const AsyncSocket&) = delete;
    AsyncSocket(AsyncSocket&&) noexcept = default;
    AsyncSocket& operator=(AsyncSocket&&) noexcept = default;

    asio::awaitable<void> connect(std::string_view host, int port) {
        asio::ip::tcp::resolver resolver(socket_.get_executor());
        auto endpoints = co_await resolver.async_resolve(host, std::to_string(port), asio::use_awaitable);
        co_await asio::async_connect(socket_, endpoints, asio::use_awaitable);
        LOGINFO("Successfully connected to {}:{}", host, port);
    }

    // All writes (send_raw, send_message) funnel through a single per-socket
    // write queue. The io_context runs on multiple threads, and several
    // coroutines (message handlers, request flushes, keep-alive, choke loop,
    // piece uploads) can initiate socket writes concurrently. Unserialized
    // async_write calls interleave their frames on the wire, which peers
    // (qBittorrent etc.) read as a corrupt length prefix and drop the
    // connection within seconds. Serializing every frame guarantees atomic
    // wire frames regardless of caller thread.
    asio::awaitable<void> send_raw(std::span<const std::byte> data) {
        co_await enqueue_write(std::vector<std::byte>(data.begin(), data.end()));
    }

    asio::awaitable<std::vector<std::byte>> receive_raw(size_t size) {
        std::vector<std::byte> buffer(size);
        co_await asio::async_read(socket_, asio::buffer(buffer), asio::use_awaitable);
        co_return buffer;
    }

    asio::awaitable<void> send_message(std::span<const std::byte> message) {
        // keep-alive message (zero length prefix only)
        if (message.empty()) {
            co_await enqueue_write(std::vector<std::byte>(sizeof(uint32_t), std::byte{0}));
            co_return;
        }

        uint32_t length = asio::detail::socket_ops::host_to_network_long(static_cast<uint32_t>(message.size()));

        std::vector<std::byte> frame(sizeof(uint32_t) + message.size());
        std::memcpy(frame.data(), &length, sizeof(uint32_t));
        std::copy(message.begin(), message.end(), frame.begin() + sizeof(uint32_t));
        co_await enqueue_write(std::move(frame));
    }

    asio::awaitable<std::vector<std::byte>> receive_message() {
        uint32_t net_length;
        co_await asio::async_read(socket_, asio::buffer(&net_length, sizeof(net_length)), asio::use_awaitable);
        uint32_t length = asio::detail::socket_ops::network_to_host_long(net_length);
        if (length > MAX_MESSAGE_SIZE) {
            throw std::runtime_error("Message size limit exceeded: " + std::to_string(length));
        }
        if (length == 0) {
            co_return std::vector<std::byte>();
        }

        std::vector<std::byte> buffer(length);
        co_await asio::async_read(socket_, asio::buffer(buffer, buffer.size()), asio::use_awaitable);

        co_return buffer;
    }

    asio::ip::tcp::endpoint remote_endpoint() const noexcept {
        boost::system::error_code ec;
        auto ep = socket_.remote_endpoint(ec);
        if (ec) {
            LOGWARN("Error getting remote_endpoint: {}", ec.message());
            return {};
        }
        return ep;
    }

    asio::ip::tcp::endpoint local_endpoint() const noexcept {
        boost::system::error_code ec;
        auto ep = socket_.local_endpoint(ec);
        if (ec) {
            LOGWARN("Error getting local_endpoint: {}", ec.message());
            return {};
        }
        return ep;
    }

    void close() noexcept {
        boost::system::error_code ec;
        if(socket_.is_open()) {
            socket_.shutdown(asio::ip::tcp::socket::shutdown_both, ec);
            if (ec && ec != asio::error::not_connected) { // Ignore 'not_connected' during shutdown
                LOGWARN("Socket shutdown error: {}", ec.message());
            }
            socket_.close(ec);
            if (ec) {
                LOGWARN("Socket close error: {}", ec.message());
            }
        }
    }

    bool is_open() const noexcept {
        return socket_.is_open();
    }

private:
    // A frame is fully built (length prefix + payload) before enqueueing, so
    // the draining coroutine only ever issues one async_write at a time and
    // wire frames can never interleave.
    struct PendingWrite {
        std::vector<std::byte> frame;
        std::shared_ptr<boost::system::error_code> result;
        std::shared_ptr<asio::steady_timer> waiter;
    };

    struct WriteQueueState {
        std::mutex mutex;
        std::deque<PendingWrite> queue;
        bool writing = false;
    };

    asio::awaitable<void> enqueue_write(std::vector<std::byte> frame) {
        auto result = std::make_shared<boost::system::error_code>();
        auto waiter = std::make_shared<asio::steady_timer>(socket_.get_executor());
        bool become_drain = false;
        {
            std::lock_guard lock(write_state_->mutex);
            write_state_->queue.push_back(PendingWrite{std::move(frame), result, waiter});
            if (!write_state_->writing) {
                write_state_->writing = true;
                become_drain = true;
            }
        }

        if (become_drain) {
            // This coroutine is the drain: it writes its own frame first, then
            // keeps draining until the queue is empty (see drain_write_queue).
            co_await drain_write_queue();
            co_return;
        }

        // An active drain will write our frame and cancel our waiter.
        boost::system::error_code ec;
        co_await waiter->async_wait(asio::redirect_error(asio::use_awaitable, ec));
        if (*result) {
            throw boost::system::system_error(*result, "AsyncSocket write failed");
        }
    }

    asio::awaitable<void> drain_write_queue() {
        while (true) {
            PendingWrite entry;
            {
                std::lock_guard lock(write_state_->mutex);
                if (write_state_->queue.empty()) {
                    write_state_->writing = false;
                    co_return;
                }
                entry = std::move(write_state_->queue.front());
                write_state_->queue.pop_front();
            }

            try {
                co_await asio::async_write(socket_, asio::buffer(entry.frame), asio::use_awaitable);
            } catch (const boost::system::system_error& e) {
                fail_all_writes(e.code());
                throw;
            } catch (...) {
                fail_all_writes(asio::error::operation_aborted);
                throw;
            }

            // Signal the waiter. It must check *result for errors, but on the
            // success path the error_code stays clear.
            entry.waiter->cancel();
        }
    }

    // Socket failed mid-drain: unblock every queued writer with the error and
    // reset the queue so future sends start a fresh drain (and fail fast on
    // the closed socket).
    void fail_all_writes(const boost::system::error_code& ec) {
        std::deque<PendingWrite> remaining;
        {
            std::lock_guard lock(write_state_->mutex);
            remaining.swap(write_state_->queue);
            write_state_->writing = false;
        }
        for (auto& pw : remaining) {
            *pw.result = ec;
            pw.waiter->cancel();
        }
    }

    std::shared_ptr<WriteQueueState> write_state_;

    asio::ip::tcp::socket socket_;
};

class AsyncServerSocket {
public:
    explicit AsyncServerSocket(asio::io_context& io_context, int port) noexcept
        : acceptor_(io_context)
    {
        boost::system::error_code ec;

        acceptor_.open(asio::ip::tcp::v4(), ec);
        if (ec) {
            LOGERR("Failed to open TCP acceptor: {}", ec.message());
            return;
        }

        // SO_REUSEPORT (Linux 3.9+) — allows multiple sockets on the same port.
        // The kernel distributes incoming connections among them. Must be set
        // on EVERY socket (including the first) before bind().
        {
            int reuse_port = 1;
            if (::setsockopt(acceptor_.native_handle(), SOL_SOCKET, SO_REUSEPORT,
                             &reuse_port, sizeof(reuse_port)) < 0) {
                // Non-Linux or old kernel — not fatal, just means only the
                // first session can bind; subsequent sessions fall back
                // gracefully in TorrentSession::run().
                LOGWARN("SO_REUSEPORT not available (non-Linux/old kernel), "
                        "multi-torrent on same port limited");
            }
        }

        acceptor_.set_option(asio::ip::tcp::acceptor::reuse_address(true), ec);

        acceptor_.bind({asio::ip::tcp::v4(), static_cast<asio::ip::port_type>(port)}, ec);
        if (ec) {
            LOGERR("Failed to bind to port {}: {}", port, ec.message());
            return;
        }

        acceptor_.listen(asio::socket_base::max_listen_connections, ec);
        if (ec) {
            LOGERR("Failed to listen on port {}: {}", port, ec.message());
            return;
        }

        LOGINFO("Server listening on port {}", port);
    }

    AsyncServerSocket(const AsyncServerSocket&) = delete;
    AsyncServerSocket& operator=(const AsyncServerSocket&) = delete;
    AsyncServerSocket(AsyncServerSocket&&) noexcept = default;
    AsyncServerSocket& operator=(AsyncServerSocket&&) noexcept = default;

    bool is_listening() const noexcept {
        return acceptor_.is_open();
    }

    asio::awaitable<AsyncSocket> accept() {
        asio::ip::tcp::socket socket = co_await acceptor_.async_accept(asio::use_awaitable);
        auto endpoint = socket.remote_endpoint();
        LOGINFO("Accepted connection from {}:{}", endpoint.address().to_string(), endpoint.port());
        co_return AsyncSocket(std::move(socket));
    }

    void close() {
        boost::system::error_code ec;
        acceptor_.cancel(ec);
        acceptor_.close(ec);
    }

private:
    asio::ip::tcp::acceptor acceptor_;
};

using udp = asio::ip::udp;

class AsyncUdpSocket {
public:
    explicit AsyncUdpSocket(asio::io_context& io_context, uint16_t port = 0)
        : socket_(io_context)
    {
        if (port > 0) {
            socket_.open(udp::v4());
            // SO_REUSEPORT — allows multiple DHT nodes on the same port
            int reuse_port = 1;
            ::setsockopt(socket_.native_handle(), SOL_SOCKET, SO_REUSEPORT,
                         &reuse_port, sizeof(reuse_port));
            socket_.bind(udp::endpoint(udp::v4(), port));
            LOGINFO("UDP socket bound to port {}", port);
        }
    }

    AsyncUdpSocket(const AsyncUdpSocket&) = delete;
    AsyncUdpSocket& operator=(const AsyncUdpSocket&) = delete;
    AsyncUdpSocket(AsyncUdpSocket&&) noexcept = default;
    AsyncUdpSocket& operator=(AsyncUdpSocket&&) noexcept = default;

    // Takes both arguments BY VALUE: a suspended async_send_to may outlive
    // the caller's coroutine frame (detached DHT query sends), and a
    // reference into that frame is a stack-use-after-return the moment the
    // frame dies (ASan: endpoint::is_v4() read of a returned frame in the
    // send_to error path). DHT messages are small and low-rate; the copies
    // are negligible.
    asio::awaitable<void> send_to(std::vector<std::byte> data, udp::endpoint remote) {
        if (!socket_.is_open()) {
            socket_.open(udp::v4());
        }

        boost::system::error_code ec;
        co_await socket_.async_send_to(asio::buffer(data.data(), data.size()), remote, asio::redirect_error(asio::use_awaitable, ec));
        if (ec) {
            LOGERR("UDP send_to error to {}: {}. Data size: {}", remote.address().to_string(), ec.message(), data.size());
            throw boost::system::system_error(ec, "UDP send_to failed");
        }
    }

    // Returns a pair: (std::vector<std::byte> received_bytes, udp::endpoint remote_endpoint)
    asio::awaitable<std::tuple<std::vector<std::byte>, udp::endpoint>>
    receive_from(size_t max_buffer_size = 2048) {
        std::vector<std::byte> buffer(max_buffer_size);
        udp::endpoint remote;
        boost::system::error_code ec;
        size_t bytes_received = co_await socket_.async_receive_from(asio::buffer(buffer), remote, asio::redirect_error(asio::use_awaitable, ec));
        if (ec) {
            // Operation aborted is normal during shutdown
            if (ec == asio::error::operation_aborted) {
                throw boost::system::system_error(ec, "UDP receive_from aborted");
            }
            LOGERR("UDP receive_from error: {}", ec.message());
            throw boost::system::system_error(ec, "UDP receive_from failed");
        }

        buffer.resize(bytes_received);
        co_return std::make_tuple(std::move(buffer), remote);
    }

    static udp::endpoint resolve_endpoint(asio::io_context io_context, const std::string& host, uint16_t port) {
        udp::resolver resolver(io_context);
        return *resolver.resolve(host, std::to_string(port)).begin();
    }

    udp::endpoint local_endpoint() const {
        boost::system::error_code ec;
        auto ep = socket_.local_endpoint(ec);
        if (ec) {
            LOGWARN("Error getting UDP local_endpoint: {}", ec.message());
            return {};
        }
        return ep;
    }

    void close() {
        if (socket_.is_open()) {
            boost::system::error_code ec;
            socket_.cancel(ec); // Cancel any pending async operations
            socket_.close(ec);
        }
    }

private:
    udp::socket socket_;
};