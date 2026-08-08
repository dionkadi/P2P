#pragma once

#include <boost/asio.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>
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
#include <optional>
#include <string>
#include <utility>
#include <vector>
namespace asio = boost::asio;

#include "Utils.hpp"
#include "Mse.hpp"


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
        std::vector<std::byte> frame(data.begin(), data.end());
        co_await enqueue_write(std::move(frame));
    }

    // Returns exactly `size` bytes; buffered bytes (e.g., left over from MSE
    // negotiation) are consumed first, then the socket is read.
    asio::awaitable<std::vector<std::byte>> receive_raw(size_t size) {
        co_await fill_buffer(size);
        auto out = consume_bytes(size);
        decrypt_incoming(out);
        co_return out;
    }

    asio::awaitable<void> send_message(std::span<const std::byte> message) {
        // keep-alive message (zero length prefix only)
        if (message.empty()) {
            std::vector<std::byte> frame(sizeof(uint32_t), std::byte{0});
            co_await enqueue_write(std::move(frame));
            co_return;
        }

        uint32_t length = asio::detail::socket_ops::host_to_network_long(static_cast<uint32_t>(message.size()));

        std::vector<std::byte> frame(sizeof(uint32_t) + message.size());
        std::memcpy(frame.data(), &length, sizeof(uint32_t));
        std::copy(message.begin(), message.end(), frame.begin() + sizeof(uint32_t));
        co_await enqueue_write(std::move(frame));
    }

    asio::awaitable<std::vector<std::byte>> receive_message() {
        co_await fill_buffer(sizeof(uint32_t));
        auto len_bytes = consume_bytes(sizeof(uint32_t));
        decrypt_incoming(len_bytes);
        uint32_t net_length;
        std::memcpy(&net_length, len_bytes.data(), sizeof(net_length));
        uint32_t length = asio::detail::socket_ops::network_to_host_long(net_length);
        if (length > MAX_MESSAGE_SIZE) {
            throw std::runtime_error("Message size limit exceeded: " + std::to_string(length));
        }
        if (length == 0) {
            co_return std::vector<std::byte>();
        }

        co_await fill_buffer(length);
        auto buffer = consume_bytes(length);
        decrypt_incoming(buffer);

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

    // ---- MSE (BitTorrent protocol encryption) ----

    enum class MseResult { Rc4, Plaintext, Failed };

    bool mse_active() const noexcept { return mse_ && mse_->active; }

    // Negotiate MSE as the initiator (outbound connection). `bt_handshake`
    // is the caller's 68-byte BT handshake, sent RC4-encrypted inside the
    // exchange. On success the socket transparently encrypts/decrypts all
    // further I/O and the peer's decrypted 68-byte handshake is returned.
    // On Failed the socket is untouched; the caller may retry plaintext on a
    // fresh connection (libtorrent's fallback behavior).
    asio::awaitable<std::pair<MseResult, std::vector<std::byte>>>
    mse_handshake_initiator(const InfoHash& skey, std::span<const std::byte> bt_handshake);

    // Negotiate MSE as the responder (inbound connection). Peeks the peer's
    // first byte: a plaintext peer (0x13) is detected and its bytes stay
    // buffered (return Plaintext with an empty handshake — the caller
    // proceeds with the normal plaintext handshake). Otherwise the MSE
    // exchange completes and the peer's decrypted handshake is returned.
    asio::awaitable<std::pair<MseResult, std::vector<std::byte>>>
    mse_handshake_acceptor(const InfoHash& skey);

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
                // Encrypt here, in the single drain coroutine: the write
                // queue serializes the writes, so the RC4 stream state is
                // only ever touched by one thread at a time. Encrypting at
                // enqueue time races when several io threads enqueue
                // concurrently, corrupting the keystream (peers then drop
                // the garbage frames).
                if (encrypting()) {
                    mse_->encrypt.crypt(entry.frame);
                }
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

    // ---- MSE state + buffered reads ----
    std::unique_ptr<mse::MseCrypto> mse_;      // set once negotiation completes
    std::vector<std::byte> read_buf_;          // bytes read from socket, not yet consumed
    size_t read_pos_ = 0;                      // consumed prefix of read_buf_

    bool encrypting() const noexcept { return mse_ && mse_->active && mse_->rc4; }

    void encrypt_outgoing(std::vector<std::byte>& frame) {
        if (encrypting()) {
            mse_->encrypt.crypt(frame);
        }
    }

    void decrypt_incoming(std::vector<std::byte>& data) {
        if (encrypting()) {
            mse_->decrypt.crypt(data);
        }
    }

    static std::span<const std::byte> str_bytes(std::string_view s) {
        return {reinterpret_cast<const std::byte*>(s.data()), s.size()};
    }

    static mse::Secret to_secret(std::span<const std::byte> b) {
        if (b.size() != mse::kDhKeySize) {
            throw std::runtime_error("MSE: bad DH key size");
        }
        mse::Secret s{};
        std::ranges::copy(b, s.begin());
        return s;
    }

    static void discard_rc4(mse::Rc4& rc4) {
        std::vector<std::byte> dummy(mse::kRc4DiscardBytes);
        rc4.crypt(dummy);
    }

    // Make sure at least `need` bytes are available in read_buf_ (reading
    // from the socket as required). Throws on EOF so callers can treat a
    // closed connection like a failed handshake.
    asio::awaitable<void> fill_buffer(size_t need) {
        while (read_buf_.size() - read_pos_ < need) {
            std::vector<std::byte> chunk(4096);
            boost::system::error_code ec;
            size_t got = co_await socket_.async_read_some(
                asio::buffer(chunk), asio::redirect_error(asio::use_awaitable, ec));
            if (got == 0) {
                throw boost::system::system_error(ec ? ec : asio::error::eof, "AsyncSocket read");
            }
            chunk.resize(got);
            read_buf_.insert(read_buf_.end(), chunk.begin(), chunk.end());
            if (read_pos_ >= (1u << 16)) { // compact occasionally
                read_buf_.erase(read_buf_.begin(), read_buf_.begin() + static_cast<std::ptrdiff_t>(read_pos_));
                read_pos_ = 0;
            }
        }
    }

    std::vector<std::byte> consume_bytes(size_t n) {
        std::vector<std::byte> out(read_buf_.begin() + static_cast<std::ptrdiff_t>(read_pos_),
                                   read_buf_.begin() + static_cast<std::ptrdiff_t>(read_pos_ + n));
        read_pos_ += n;
        return out;
    }

    // True if `op` finished before the (already armed) deadline timer.
    template <typename Op>
    asio::awaitable<bool> op_before_deadline(Op&& op, asio::steady_timer& deadline) {
        using namespace boost::asio::experimental::awaitable_operators;
        auto res = co_await (std::forward<Op>(op) || deadline.async_wait(asio::use_awaitable));
        co_return res.index() == 0;
    }

    asio::ip::tcp::socket socket_;
};
// ---- MSE negotiation implementations (wire format v1.0, see Mse.hpp) ----
    inline asio::awaitable<std::pair<AsyncSocket::MseResult, std::vector<std::byte>>>
AsyncSocket::mse_handshake_initiator(const InfoHash& skey, std::span<const std::byte> bt_handshake) {
        try {
            if (bt_handshake.size() != mse::kHandshakeLen) {
                throw std::runtime_error("MSE: BT handshake must be 68 bytes");
            }
            asio::steady_timer deadline(socket_.get_executor());
            const auto arm = [&]() { deadline.expires_after(std::chrono::seconds(10)); };

            // Step 1: [Ya 96] (no pad)
            mse::Dh dh;
            auto ya = dh.generate_keypair();
            co_await send_raw(ya);

            // Step 2: read Yb (96)
            arm();
            if (!(co_await op_before_deadline(fill_buffer(96), deadline))) {
                throw std::runtime_error("MSE: timeout waiting for DH key");
            }
            auto yb = consume_bytes(96);

            auto S = dh.compute_secret(to_secret(yb));
            mse_ = std::make_unique<mse::MseCrypto>();
            mse_->encrypt.init(mse::sha1_concat3(str_bytes("keyA"), S, skey)); // A->B
            mse_->decrypt.init(mse::sha1_concat3(str_bytes("keyB"), S, skey)); // B->A
            discard_rc4(mse_->encrypt);
            discard_rc4(mse_->decrypt);

            // Step 3: [sync_hash 20][skey_obfusc 20][RC4(block)][RC4(IA)]
            auto sync_hash = mse::sha1_concat(str_bytes("req1"), S);
            auto skey_obf = mse::sha1_concat(str_bytes("req2"), skey);
            auto mask = mse::sha1_concat(str_bytes("req3"), S);
            for (size_t i = 0; i < skey_obf.size(); ++i) {
                skey_obf[i] ^= mask[i];
            }

            std::vector<std::byte> block;
            block.insert(block.end(), 8, std::byte{0});                  // VC
            auto provide = mse::to_be32(mse::kCryptoAll);
            block.insert(block.end(), provide.begin(), provide.end());   // offer both
            auto len_pad = mse::to_be16(0);
            block.insert(block.end(), len_pad.begin(), len_pad.end());
            auto len_ia = mse::to_be16(mse::kHandshakeLen);
            block.insert(block.end(), len_ia.begin(), len_ia.end());

            std::vector<std::byte> step3;
            step3.insert(step3.end(), sync_hash.begin(), sync_hash.end());
            step3.insert(step3.end(), skey_obf.begin(), skey_obf.end());
            mse_->encrypt.crypt(block);
            step3.insert(step3.end(), block.begin(), block.end());
            std::vector<std::byte> ia(bt_handshake.begin(), bt_handshake.end());
            mse_->encrypt.crypt(ia); // handshake is always RC4 here
            step3.insert(step3.end(), ia.begin(), ia.end());
            co_await send_raw(step3);

            // Step 4: scan for the VC ciphertext. Prime the decrypt stream
            // with 8 zero bytes; the peer's step-4 starts with the VC
            // encrypted by those exact keystream bytes (crypt is in-place).
            std::array<std::byte, 8> vc_ct{};
            mse_->decrypt.crypt(vc_ct);

            std::optional<size_t> vc_off;
            while (true) {
                auto begin = read_buf_.begin() + static_cast<std::ptrdiff_t>(read_pos_);
                auto it = std::search(begin, read_buf_.end(), vc_ct.begin(), vc_ct.end());
                if (it != read_buf_.end()) {
                    vc_off = static_cast<size_t>(it - begin);
                    break;
                }
                if (read_buf_.size() - read_pos_ >= mse::kMaxPadSize + 8) {
                    throw std::runtime_error("MSE: verification constant not found");
                }
                arm();
                if (!(co_await op_before_deadline(fill_buffer(read_buf_.size() - read_pos_ + 1), deadline))) {
                    throw std::runtime_error("MSE: timeout scanning for verification constant");
                }
            }
            consume_bytes(*vc_off + 8); // PadB + VC; stream is now aligned at select

            // select(4) + len_pad(2), then pad, then the peer's handshake
            arm();
            if (!(co_await op_before_deadline(fill_buffer(6), deadline))) {
                throw std::runtime_error("MSE: timeout reading crypto select");
            }
            auto hdr = consume_bytes(6);
            mse_->decrypt.crypt(hdr);
            uint32_t select = mse::from_be32(std::span<const std::byte>(hdr).first(4));
            uint16_t peer_len_pad = mse::from_be16(std::span<const std::byte>(hdr).subspan(4, 2));
            if (select == mse::kCryptoPlaintext) {
                mse_->rc4 = false;
            } else if (select == mse::kCryptoRc4) {
                mse_->rc4 = true;
            } else {
                throw std::runtime_error("MSE: peer selected unsupported crypto method");
            }
            if (peer_len_pad > mse::kMaxPadSize) {
                throw std::runtime_error("MSE: invalid pad size");
            }
            if (peer_len_pad > 0) {
                arm();
                if (!(co_await op_before_deadline(fill_buffer(peer_len_pad), deadline))) {
                    throw std::runtime_error("MSE: timeout reading pad");
                }
                auto pad = consume_bytes(peer_len_pad);
                mse_->decrypt.crypt(pad);
            }

            arm();
            if (!(co_await op_before_deadline(fill_buffer(mse::kHandshakeLen), deadline))) {
                throw std::runtime_error("MSE: timeout reading peer handshake");
            }
            auto peer_hs = consume_bytes(mse::kHandshakeLen);
            mse_->decrypt.crypt(peer_hs);

            mse_->active = true;
            LOGINFO("MSE handshake complete (initiator, RC4={})", mse_->rc4);
            co_return std::pair<MseResult, std::vector<std::byte>>{mse_->rc4 ? MseResult::Rc4 : MseResult::Plaintext, std::move(peer_hs)};
        } catch (const std::exception& e) {
            LOGWARN("MSE initiator handshake failed: {}", e.what());
            mse_.reset();
            co_return std::pair<MseResult, std::vector<std::byte>>{MseResult::Failed, {}};
        }
    }

    inline asio::awaitable<std::pair<AsyncSocket::MseResult, std::vector<std::byte>>>
AsyncSocket::mse_handshake_acceptor(const InfoHash& skey) {
        try {
            asio::steady_timer deadline(socket_.get_executor());
            const auto arm = [&]() { deadline.expires_after(std::chrono::seconds(10)); };

            // Peek the first byte: plaintext peers start with 0x13.
            arm();
            if (!(co_await op_before_deadline(fill_buffer(1), deadline))) {
                throw std::runtime_error("MSE: timeout waiting for first byte");
            }
            if (read_buf_[read_pos_] == std::byte{0x13}) {
                co_return std::pair<MseResult, std::vector<std::byte>>{MseResult::Plaintext, {}}; // plaintext; bytes stay buffered
            }

            // Read Ya (96 bytes total).
            arm();
            if (!(co_await op_before_deadline(fill_buffer(96), deadline))) {
                throw std::runtime_error("MSE: timeout reading DH key");
            }
            auto ya = consume_bytes(96);

            // Step 2: [Yb 96]
            mse::Dh dh;
            auto yb = dh.generate_keypair();
            co_await send_raw(yb);

            auto S = dh.compute_secret(to_secret(ya));
            mse_ = std::make_unique<mse::MseCrypto>();
            mse_->decrypt.init(mse::sha1_concat3(str_bytes("keyA"), S, skey)); // A->B traffic
            mse_->encrypt.init(mse::sha1_concat3(str_bytes("keyB"), S, skey)); // B->A traffic
            discard_rc4(mse_->decrypt);
            discard_rc4(mse_->encrypt);

            // Scan for the sync hash (clear) after PadA.
            auto sync_hash = mse::sha1_concat(str_bytes("req1"), S);
            std::optional<size_t> sync_off;
            while (true) {
                auto begin = read_buf_.begin() + static_cast<std::ptrdiff_t>(read_pos_);
                auto it = std::search(begin, read_buf_.end(), sync_hash.begin(), sync_hash.end());
                if (it != read_buf_.end()) {
                    sync_off = static_cast<size_t>(it - begin);
                    break;
                }
                if (read_buf_.size() - read_pos_ >= mse::kMaxPadSize + 40) {
                    throw std::runtime_error("MSE: sync hash not found");
                }
                arm();
                if (!(co_await op_before_deadline(fill_buffer(read_buf_.size() - read_pos_ + 1), deadline))) {
                    throw std::runtime_error("MSE: timeout scanning for sync hash");
                }
            }
            consume_bytes(*sync_off); // PadA
            consume_bytes(20);        // the sync hash itself (pattern-verified)

            arm();
            if (!(co_await op_before_deadline(fill_buffer(20), deadline))) {
                throw std::runtime_error("MSE: timeout reading skey");
            }
            auto obf = consume_bytes(20);
            auto mask = mse::sha1_concat(str_bytes("req3"), S);
            for (size_t i = 0; i < obf.size(); ++i) {
                obf[i] ^= mask[i];
            }
            // The recovered value is SHA1("req2" || SKEY), not the SKEY
            // itself — compare against the hash of our torrent's infohash.
            auto expect = mse::sha1_concat(str_bytes("req2"), skey);
            if (!std::equal(obf.begin(), obf.end(), expect.begin(), expect.end())) {
                throw std::runtime_error("MSE: peer is looking for a different torrent");
            }

            // Decrypt the block: VC(8) | provide(4) | len_pad(2) | pad | len_IA(2) | IA.
            auto read_dec = [&](size_t n) -> asio::awaitable<std::vector<std::byte>> {
                arm();
                if (!(co_await op_before_deadline(fill_buffer(n), deadline))) {
                    throw std::runtime_error("MSE: timeout reading encrypted block");
                }
                auto v = consume_bytes(n);
                mse_->decrypt.crypt(v);
                co_return v;
            };

            auto vc = co_await read_dec(8);
            std::array<std::byte, 8> zeros{};
            if (!std::equal(vc.begin(), vc.end(), zeros.begin(), zeros.end())) {
                throw std::runtime_error("MSE: invalid encryption constant");
            }
            auto provide_b = co_await read_dec(4);
            auto len_pad_b = co_await read_dec(2);
            uint32_t provide = mse::from_be32(provide_b);
            uint16_t len_pad = mse::from_be16(len_pad_b);
            if (len_pad > mse::kMaxPadSize) {
                throw std::runtime_error("MSE: invalid pad size");
            }
            if (len_pad > 0) {
                co_await read_dec(len_pad);
            }
            auto len_ia_b = co_await read_dec(2);
            uint16_t len_ia = mse::from_be16(len_ia_b);
            if (len_ia != mse::kHandshakeLen) {
                throw std::runtime_error("MSE: unexpected initial payload length");
            }
            auto ia = co_await read_dec(len_ia);

            // Select the crypto method: prefer RC4 when offered (matches the
            // encryption-gated seeders), else plaintext.
            uint32_t select = 0;
            if (provide & mse::kCryptoRc4) {
                select = mse::kCryptoRc4;
            } else if (provide & mse::kCryptoPlaintext) {
                select = mse::kCryptoPlaintext;
            } else {
                throw std::runtime_error("MSE: no common crypto method");
            }
            mse_->rc4 = (select == mse::kCryptoRc4);

            // Step 4: [VC 8 | select 4 | len_pad 2] (+ no pad), encrypted if RC4.
            std::vector<std::byte> step4;
            step4.insert(step4.end(), 8, std::byte{0});
            auto sel_b = mse::to_be32(select);
            step4.insert(step4.end(), sel_b.begin(), sel_b.end());
            auto zero16 = mse::to_be16(0);
            step4.insert(step4.end(), zero16.begin(), zero16.end());
            if (mse_->rc4) {
                mse_->encrypt.crypt(step4);
            }
            co_await send_raw(step4);

            mse_->active = true;
            LOGINFO("MSE handshake complete (acceptor, RC4={})", mse_->rc4);
            co_return std::pair<MseResult, std::vector<std::byte>>{mse_->rc4 ? MseResult::Rc4 : MseResult::Plaintext, std::move(ia)};
        } catch (const std::exception& e) {
            LOGWARN("MSE acceptor handshake failed: {}", e.what());
            mse_.reset();
            co_return std::pair<MseResult, std::vector<std::byte>>{MseResult::Failed, {}};
        }
    }




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