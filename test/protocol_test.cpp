#include "helper.hpp"

#include <fstream>
#include <future>
#include <thread>

TEST(HandshakeTest, SerializeDeserializeBasic) {
    Handshake original_hs;
    original_hs.info_hash_bytes = hex_string_to_info_hash("e29fc0e5dceeefea80401e32723796c0a86a8695");
    original_hs.peer_id_bytes = string_to_peer_id("-MI0001-TESTPEERID00");
    original_hs.extended = false;

    std::vector<std::byte> serialized = original_hs.serialize();
    ASSERT_EQ(serialized.size(), HANDSHAKE_BASE_LEN);

    Handshake deserialized_hs = Handshake::deserialize(serialized);

    EXPECT_EQ(deserialized_hs.info_hash_bytes, original_hs.info_hash_bytes);
    EXPECT_EQ(deserialized_hs.peer_id_bytes, original_hs.peer_id_bytes);
    EXPECT_FALSE(deserialized_hs.extended);
}

TEST(HandshakeTest, SerializeDeserializeExtended) {
    Handshake original_hs;
    original_hs.info_hash_bytes = hex_string_to_info_hash("e29fc0e5dceeefea80401e32723796c0a86a8695");
    original_hs.peer_id_bytes = string_to_peer_id("-MI0001-EXTENDEDP2P0");
    original_hs.extended = true;

    std::vector<std::byte> serialized = original_hs.serialize();
    ASSERT_EQ(serialized.size(), HANDSHAKE_BASE_LEN);

    // Verify the extended flag bit is set
    // Protocol string length (1 byte) + Protocol string (19 bytes) = 20 bytes
    // Reserved bytes start at offset 20, reserved[5] is at index 25
    EXPECT_EQ(serialized[25] & static_cast<std::byte>(0x10), static_cast<std::byte>(0x10));

    Handshake deserialized_hs = Handshake::deserialize(serialized);

    EXPECT_EQ(deserialized_hs.info_hash_bytes, original_hs.info_hash_bytes);
    EXPECT_EQ(deserialized_hs.peer_id_bytes, original_hs.peer_id_bytes);
    EXPECT_TRUE(deserialized_hs.extended);
}

TEST(HandshakeTest, DeserializeInvalidSize) {
    std::vector<std::byte> too_small(HANDSHAKE_BASE_LEN - 1);
    EXPECT_THROW(Handshake::deserialize(too_small), std::runtime_error);

    std::vector<std::byte> too_large(HANDSHAKE_BASE_LEN + 1);
    // Even if it has more bytes, it tries to read exactly HANDSHAKE_BASE_LEN
    // This might not throw on deserialize but might indicate trailing data
    // However, the current deserialize takes a span, so length mismatch is a strict check
    EXPECT_THROW(Handshake::deserialize(too_large), std::runtime_error); 
}

TEST(HandshakeTest, DeserializeProtocolMismatch) {
    Handshake original_hs;
    original_hs.info_hash_bytes = hex_string_to_info_hash("e29fc0e5dceeefea80401e32723796c0a86a8695");
    original_hs.peer_id_bytes = string_to_peer_id("-MI0001-TESTPEERID00");
    original_hs.extended = false;
    std::vector<std::byte> serialized = original_hs.serialize();

    // Corrupt protocol string length
    serialized[0] = static_cast<std::byte>(0x01); // should be 17 (PROTOCOL_STRING.length())
    EXPECT_THROW(Handshake::deserialize(serialized), std::runtime_error);

    // Corrupt protocol string content
    serialized[0] = static_cast<std::byte>(PROTOCOL_STRING.length());
    serialized[1] = static_cast<std::byte>('X'); // first char of "MIT-P2P-V1.0"
    EXPECT_THROW(Handshake::deserialize(serialized), std::runtime_error);
}

TEST(RequestPayloadTest, SerializeDeserialize) {
    uint32_t index = 10;
    uint32_t begin = 16384 * 2; // block 2
    uint32_t length = BLOCK_SIZE;

    std::vector<std::byte> serialized = RequestPayload::serialize(index, begin, length);
    ASSERT_EQ(serialized.size(), 12);

    RequestPayload deserialized = RequestPayload::deserialize(serialized);

    EXPECT_EQ(deserialized.index, index);
    EXPECT_EQ(deserialized.begin, begin);
    EXPECT_EQ(deserialized.length, length);
}

TEST(RequestPayloadTest, DeserializeInvalidSize) {
    std::vector<std::byte> too_small(11);
    EXPECT_THROW(RequestPayload::deserialize(too_small), std::runtime_error);
    // A 12-byte payload should parse without throwing, even if uninitialized.
    // The current `deserialize` explicitly checks for minimum size.
    std::vector<std::byte> just_right_empty(12);
    EXPECT_NO_THROW(RequestPayload::deserialize(just_right_empty));
}

namespace {

using tcp = asio::ip::tcp;

struct ConnectedSockets {
    tcp::socket client;
    tcp::socket server;
};

PeerId make_fixed_peer_id(const std::string& suffix) {
    PeerId id{};
    std::string text = "-PT0001-" + suffix;
    text.resize(PEER_ID_SIZE, '_');
    std::transform(text.begin(), text.end(), id.begin(),
                   [](char c) { return static_cast<std::byte>(c); });
    return id;
}

std::shared_ptr<SessionState> make_protocol_test_state() {
    InfoHash dummy_hash{};
    dummy_hash.fill(std::byte{0});
    return std::make_shared<SessionState>(
        dummy_hash,
        std::vector<std::vector<std::string>>{},
        std::filesystem::temp_directory_path() / "p2p_protocol_test"
    );
}

ConnectedSockets make_connected_sockets(asio::io_context& io) {
    tcp::acceptor acceptor(io, tcp::endpoint(asio::ip::make_address("127.0.0.1"), 0));
    tcp::socket client(io);
    client.connect(tcp::endpoint(asio::ip::make_address("127.0.0.1"), acceptor.local_endpoint().port()));
    tcp::socket server = acceptor.accept();
    return ConnectedSockets{std::move(client), std::move(server)};
}

struct RecordingPeerEvents : public IPeerConnectionEvents {
    std::promise<void> piece_promise;
    size_t piece_index = 0;
    uint32_t begin = 0;
    std::vector<std::byte> block_data;
    bool piece_received = false;

    asio::awaitable<void> on_piece_block(
        std::shared_ptr<PeerConnection>,
        size_t received_piece_index,
        uint32_t received_begin,
        std::span<const std::byte> received_block_data
    ) override {
        piece_index = received_piece_index;
        begin = received_begin;
        block_data.assign(received_block_data.begin(), received_block_data.end());
        if (!piece_received) {
            piece_received = true;
            piece_promise.set_value();
        }
        co_return;
    }

    asio::awaitable<void> on_block_request(std::shared_ptr<PeerConnection>, size_t, uint32_t, uint32_t) override { co_return; }
    asio::awaitable<void> on_peer_has_piece(std::shared_ptr<PeerConnection>, size_t) override { co_return; }
    asio::awaitable<void> on_peer_has_all(std::shared_ptr<PeerConnection>) override { co_return; }
    asio::awaitable<void> on_peer_has_none(std::shared_ptr<PeerConnection>) override { co_return; }
    asio::awaitable<void> on_peer_bitfield(std::shared_ptr<PeerConnection>, std::span<const std::byte>) override { co_return; }
    asio::awaitable<void> on_choke_status_changed(std::shared_ptr<PeerConnection>, bool) override { co_return; }
    asio::awaitable<void> on_piece_rejected(std::shared_ptr<PeerConnection>, size_t, uint32_t, uint32_t) override { co_return; }
    asio::awaitable<void> on_disconnect(std::shared_ptr<PeerConnection>) override { co_return; }
    asio::awaitable<void> on_extended_message(std::shared_ptr<PeerConnection>, std::span<const std::byte>) override { co_return; }
};

struct ConnectedTestPeerConn : public PeerConnection {
    ConnectedTestPeerConn(asio::io_context& io, AsyncSocket socket, const std::string& addr)
        : PeerConnection(io, std::move(socket), addr, nullptr, nullptr) {}

    void set_upload_limiter(std::shared_ptr<AsyncRateLimiter<>> limiter) {
        upload_limiter_ = std::move(limiter);
    }

    void set_download_limiter(std::shared_ptr<AsyncRateLimiter<>> limiter) {
        download_limiter_ = std::move(limiter);
    }
};

class TorrentSessionHaveTest : public ::testing::Test {
protected:
    std::filesystem::path temp_dir;
    std::filesystem::path torrent_path;
    std::filesystem::path source_path;
    std::filesystem::path download_dir;

    void SetUp() override {
        temp_dir = std::filesystem::temp_directory_path() / "torrentsession_have_test_temp";
        std::filesystem::remove_all(temp_dir);
        std::filesystem::create_directories(temp_dir);
        torrent_path = temp_dir / "test.torrent";
        source_path = temp_dir / "source.bin";
        download_dir = temp_dir / "download";
        std::filesystem::create_directories(download_dir);

        std::ofstream source_file(source_path, std::ios::binary);
        ASSERT_TRUE(source_file.is_open());
        std::string data(9 * BLOCK_SIZE, 'x');
        source_file.write(data.data(), static_cast<std::streamsize>(data.size()));
        source_file.close();

        ASSERT_TRUE(MetaInfo::create_from_file(
            source_path,
            torrent_path,
            {"http://127.0.0.1:1/announce"},
            BLOCK_SIZE
        ));
    }

    void TearDown() override {
        std::filesystem::remove_all(temp_dir);
    }
};

} // namespace

TEST(PeerConnectionPieceMessageTest, ParsesStandardPieceMessageFromPeer) {
    asio::io_context io;
    auto sockets = make_connected_sockets(io);
    auto state = make_protocol_test_state();
    auto events = std::make_shared<RecordingPeerEvents>();
    auto piece_future = events->piece_promise.get_future();

    std::promise<std::shared_ptr<PeerConnection>> connection_promise;
    auto connection_future = connection_promise.get_future();

    PeerId my_id = make_fixed_peer_id("localPeer");
    std::string peer_addr = sockets.client.local_endpoint().address().to_string() + ":" +
                            std::to_string(sockets.client.local_endpoint().port());

    asio::co_spawn(
        io,
        PeerConnection::create(io, AsyncSocket(std::move(sockets.client)), peer_addr, my_id, state, events),
        [&connection_promise](std::exception_ptr e, std::shared_ptr<PeerConnection> conn) mutable {
            if (e) {
                connection_promise.set_exception(e);
            } else {
                connection_promise.set_value(std::move(conn));
            }
        }
    );

    std::jthread io_thread([&io] { io.run(); });

    std::vector<std::byte> handshake_buffer(HANDSHAKE_BASE_LEN);
    asio::read(sockets.server, asio::buffer(handshake_buffer));

    Handshake peer_handshake;
    peer_handshake.info_hash_bytes.fill(std::byte{0});
    peer_handshake.peer_id_bytes = make_fixed_peer_id("remoteOne");
    peer_handshake.extended = false;
    peer_handshake.fast_extension = false;
    auto peer_handshake_bytes = peer_handshake.serialize();
    asio::write(sockets.server, asio::buffer(peer_handshake_bytes));

    auto conn = connection_future.get();
    ASSERT_NE(conn, nullptr);

    std::vector<std::byte> expected_block = string_to_bytes("piece-data");
    std::vector<std::byte> msg_body(1, static_cast<std::byte>(MessageType::Piece));
    BufferWriter writer(msg_body);
    writer.write(asio::detail::socket_ops::host_to_network_long(7));
    writer.write(asio::detail::socket_ops::host_to_network_long(2 * BLOCK_SIZE));
    msg_body.insert(msg_body.end(), expected_block.begin(), expected_block.end());

    uint32_t message_length = asio::detail::socket_ops::host_to_network_long(static_cast<uint32_t>(msg_body.size()));
    std::array<asio::const_buffer, 2> buffers = {
        asio::buffer(&message_length, sizeof(message_length)),
        asio::buffer(msg_body)
    };
    asio::write(sockets.server, buffers);

    ASSERT_EQ(piece_future.wait_for(2s), std::future_status::ready);
    EXPECT_EQ(events->piece_index, 7U);
    EXPECT_EQ(events->begin, 2U * BLOCK_SIZE);
    EXPECT_EQ(events->block_data, expected_block);

    conn->close();
    sockets.server.close();
    io.stop();
}

TEST(PeerConnectionPieceMessageTest, SendsStandardPieceMessageToPeer) {
    asio::io_context io;
    auto sockets = make_connected_sockets(io);

    auto conn = std::make_shared<ConnectedTestPeerConn>(
        io,
        AsyncSocket(std::move(sockets.client)),
        "127.0.0.1:6881"
    );
    conn->set_upload_limiter(std::make_shared<AsyncRateLimiter<>>(io, 0));
    conn->set_download_limiter(std::make_shared<AsyncRateLimiter<>>(io, 0));

    std::vector<std::byte> expected_block = string_to_bytes("piece-wire-format");
    RunAsync(io, conn->send_piece(11, 3 * BLOCK_SIZE, expected_block));

    uint32_t net_length = 0;
    asio::read(sockets.server, asio::buffer(&net_length, sizeof(net_length)));
    uint32_t message_length = asio::detail::socket_ops::network_to_host_long(net_length);
    ASSERT_EQ(message_length, 1U + 8U + expected_block.size());

    std::vector<std::byte> msg_body(message_length);
    asio::read(sockets.server, asio::buffer(msg_body));
    ASSERT_FALSE(msg_body.empty());
    EXPECT_EQ(msg_body.front(), static_cast<std::byte>(MessageType::Piece));

    BufferReader reader(std::span<const std::byte>(msg_body.data() + 1, msg_body.size() - 1));
    uint32_t piece_index = asio::detail::socket_ops::network_to_host_long(reader.read<uint32_t>());
    uint32_t piece_begin = asio::detail::socket_ops::network_to_host_long(reader.read<uint32_t>());
    std::span<const std::byte> block_data = reader.read_all();

    EXPECT_EQ(piece_index, 11U);
    EXPECT_EQ(piece_begin, 3U * BLOCK_SIZE);
    EXPECT_TRUE(std::ranges::equal(block_data, expected_block));

    sockets.server.close();
}

TEST(PeerConnectionRequestQueueTest, QueuedRequestsDoNotFlushWhileChoked) {
    asio::io_context io;
    auto sockets = make_connected_sockets(io);

    auto conn = std::make_shared<ConnectedTestPeerConn>(
        io,
        AsyncSocket(std::move(sockets.client)),
        "127.0.0.1:6881"
    );

    std::atomic<int> sent_count{0};
    conn->set_request_sent_hook([&sent_count](uint32_t, uint32_t, uint32_t, const PeerId&) {
        sent_count.fetch_add(1, std::memory_order_relaxed);
    });
    conn->peer_is_choking(false);

    size_t limit = conn->max_outstanding_requests();
    RunAsync(io, [&]() -> asio::awaitable<void> {
        for (uint32_t block = 0; block < limit + 1; ++block) {
            co_await conn->send_request(7, block * BLOCK_SIZE, BLOCK_SIZE);
        }
    });

    EXPECT_EQ(sent_count.load(std::memory_order_relaxed), static_cast<int>(limit));
    EXPECT_EQ(conn->pending_request_count(), 1U);

    conn->peer_is_choking(true);
    conn->on_request_completed(BLOCK_SIZE);
    io.restart();
    io.run_for(20ms);
    EXPECT_EQ(sent_count.load(std::memory_order_relaxed), static_cast<int>(limit));
    EXPECT_EQ(conn->pending_request_count(), 1U);

    conn->peer_is_choking(false);
    conn->on_request_completed(BLOCK_SIZE);
    io.restart();
    io.run_for(20ms);
    EXPECT_EQ(sent_count.load(std::memory_order_relaxed), static_cast<int>(limit + 1));
    EXPECT_EQ(conn->pending_request_count(), 0U);

    sockets.server.close();
}

TEST(PieceResumeRecoveryTest, EnsureResumeReRequestsOrphanedBlock) {
    asio::io_context io;
    auto state = make_protocol_test_state();
    state->init_pieces(1);
    state->piece_status(0, PieceStatus::InProgress);

    auto piece_manager = std::make_shared<PieceManager>(io, state);
    auto progress = std::make_shared<InProgressPiece>(BLOCK_SIZE);
    piece_manager->emplace_in_progress_pieces(0, progress);

    auto sockets = make_connected_sockets(io);
    auto conn = std::make_shared<ConnectedTestPeerConn>(
        io,
        AsyncSocket(std::move(sockets.client)),
        "127.0.0.1:6881"
    );
    conn->peer_is_choking(false);

    std::atomic<int> sent_count{0};
    conn->set_request_sent_hook([&sent_count](uint32_t, uint32_t, uint32_t, const PeerId&) {
        sent_count.fetch_add(1, std::memory_order_relaxed);
    });

    piece_manager->set_callback([conn](size_t) -> asio::awaitable<std::vector<std::shared_ptr<PeerConnection>>> {
        co_return std::vector<std::shared_ptr<PeerConnection>>{conn};
    });

    piece_manager->ensure_resume_piece_download(0);
    piece_manager->ensure_resume_piece_download(0);

    io.run_for(1500ms);

    EXPECT_EQ(sent_count.load(std::memory_order_relaxed), 1);
    {
        std::lock_guard lock(progress->piece_mutex_);
        EXPECT_FALSE(progress->resume_task_active);
    }

    sockets.server.close();
}

TEST_F(TorrentSessionHaveTest, HaveMessageMarksPiecesBeyondFirstBitfieldByte) {
    asio::io_context io;
    auto session = std::make_shared<TorrentSession>(
        io,
        generate_peer_id(),
        torrent_path,
        download_dir,
        6881,
        Mode::Leech,
        0,
        0
    );
    session->get_state()->piece_status(8, PieceStatus::Have);

    auto conn = std::make_shared<ConnectedTestPeerConn>(
        io,
        AsyncSocket(asio::ip::tcp::socket(io)),
        "1.2.3.4:6881"
    );
    conn->bitfield(std::vector<uint8_t>((session->get_state()->num_pieces() + 7) / 8, 0));

    RunAsync(io, session->on_peer_has_piece(conn, 8));

    EXPECT_TRUE(conn->has_piece(8));
}

TEST(UdpConnectRequestTest, StructSizeAndEndianness) {
    UdpConnectRequest req;
    // Protocol ID is a fixed value, check its endian conversion
    EXPECT_EQ(req.protocol_id, htobe64(0x41727101980ULL)); // Literal should be ULL
    // Action is fixed to 0
    EXPECT_EQ(req.action, htobe32(0));

    // Size of the struct
    EXPECT_EQ(sizeof(UdpConnectRequest), 16); // 8 bytes protocol_id + 4 bytes action + 4 bytes transaction_id
}

TEST(UdpConnectResponseTest, StructSizeAndEndianness) {
    UdpConnectResponse res;
    // Transaction ID and Connection ID are filled by tracker, action is 0
    res.action = htobe32(0);
    res.transaction_id = htobe32(12345);
    res.connection_id = htobe64(0xABCDEF0123456789ULL);

    EXPECT_EQ(sizeof(UdpConnectResponse), 16); // 4 bytes action + 4 bytes transaction_id + 8 bytes connection_id
}

TEST(UdpAnnounceRequestTest, StructSizeAndEndianness) {
    UdpAnnounceRequest req;
    // The sizes of fixed-size members
    EXPECT_EQ(req.info_hash.size(), 20);
    EXPECT_EQ(req.peer_id.size(), 20);
    EXPECT_EQ(sizeof(req.connection_id), 8);
    EXPECT_EQ(sizeof(req.action), 4);
    // ... and so on for all members

    EXPECT_EQ(sizeof(UdpAnnounceRequest), 98); // 8+4+4+20+20+8+8+8+4+4+4+4+2
}

TEST(UdpAnnounceResponseTest, StructSizeAndEndianness) {
    UdpAnnounceResponse res;
    // The sizes of fixed-size members
    EXPECT_EQ(sizeof(res.action), 4);
    EXPECT_EQ(sizeof(res.transaction_id), 4);
    // ... and so on

    EXPECT_EQ(sizeof(UdpAnnounceResponse), 20); // 4+4+4+4+4
}

TEST(ExtendedMessageTypeTest, ToExtendedType) {
    EXPECT_EQ(to_extended_type("ut_pex"), ExtendedMessageType::ut_pex);
    EXPECT_EQ(to_extended_type("ut_metadata"), ExtendedMessageType::ut_metadata);
    // Unknown extensions return UNKNOWN instead of throwing
    EXPECT_EQ(to_extended_type("unknown_ext"), ExtendedMessageType::UNKNOWN);
}

// Added a quick test for this as it's directly used in PeerConnection
TEST(MessageTypeTest, ExtendedMessageValue) {
    // Ensure ExtendedMessage maps to 20 as per BitTorrent spec
    EXPECT_EQ(static_cast<uint8_t>(MessageType::ExtendedMessage), 20);
}

// ============================================================
// PeerManager Connection Limit Tests
// ============================================================

// Test helper: a PeerConnection subclass that can be constructed for testing
struct TestPeerConn : public PeerConnection {
    TestPeerConn(asio::io_context& io, const std::string& addr, const PeerId& pid)
        : PeerConnection(io, AsyncSocket(asio::ip::tcp::socket(io)), addr, nullptr, nullptr)
    {
        peer_id_ = pid;
    }

    void set_upload_limiter(std::shared_ptr<AsyncRateLimiter<>> limiter) {
        upload_limiter_ = std::move(limiter);
    }
    void set_download_limiter(std::shared_ptr<AsyncRateLimiter<>> limiter) {
        download_limiter_ = std::move(limiter);
    }
    std::shared_ptr<AsyncRateLimiter<>>& upload_limiter() { return upload_limiter_; }
    std::shared_ptr<AsyncRateLimiter<>>& download_limiter() { return download_limiter_; }

    // Pipeline test helpers
    void set_pipeline_state(size_t outstanding, uint64_t req_bytes, uint64_t recv_bytes) {
        outstanding_request_count_ = outstanding;
        total_bytes_requested_ = req_bytes;
        total_bytes_received_ = recv_bytes;
    }
    void queue_request(uint32_t index, uint32_t begin, uint32_t length) {
        pending_requests_.push_back({index, begin, length});
    }
    size_t cancel_and_count(uint32_t index, uint32_t begin, uint32_t length) {
        auto it = std::ranges::find_if(pending_requests_, [&](const RequestPayload& r) {
            return r.index == index && r.begin == begin && r.length == length;
        });
        if (it != pending_requests_.end()) {
            pending_requests_.erase(it);
        }
        return pending_requests_.size();
    }
};

// Factory helper to create test peer connections with transfer rates
static std::shared_ptr<TestPeerConn> make_test_conn(
    asio::io_context& io, const std::string& addr, const PeerId& pid,
    uint64_t dl = 0, uint64_t ul = 0)
{
    auto conn = std::make_shared<TestPeerConn>(io, addr, pid);
    conn->bytes_downloaded(dl);
    conn->bytes_uploaded(ul);
    return conn;
}

// Generates a unique PeerId for testing
static PeerId test_peer_id(const std::string& suffix) {
    PeerId id{};
    std::string s = "-MI0001-" + suffix;
    s.resize(PEER_ID_SIZE, ' ');
    std::transform(s.begin(), s.end(), id.begin(),
                   [](char c) { return static_cast<std::byte>(c); });
    return id;
}

TEST(PeerManagerLimitsTest, ExtractIpFromAddr) {
    EXPECT_EQ(PeerManager::extract_ip_from_addr("1.2.3.4:6881"), "1.2.3.4");
    EXPECT_EQ(PeerManager::extract_ip_from_addr("192.168.1.1:12345"), "192.168.1.1");
    EXPECT_EQ(PeerManager::extract_ip_from_addr("10.0.0.1:80"), "10.0.0.1");
    // IPv6 with port
    EXPECT_EQ(PeerManager::extract_ip_from_addr("[::1]:6881"), "[::1]");
    // No port
    EXPECT_EQ(PeerManager::extract_ip_from_addr("1.2.3.4"), "1.2.3.4");
    // Empty string
    EXPECT_EQ(PeerManager::extract_ip_from_addr(""), "");
}

// Helper to create a minimal SessionState for PeerManager testing
static std::shared_ptr<SessionState> make_test_state() {
    InfoHash dummy_hash{};
    dummy_hash.fill(std::byte{0});
    return std::make_shared<SessionState>(
        dummy_hash,
        std::vector<std::vector<std::string>>{},
        std::filesystem::temp_directory_path() / "p2p_test_peermgr"
    );
}

TEST(PeerManagerLimitsTest, DefaultLimits) {
    asio::io_context io;
    auto state = make_test_state();
    auto pm = std::make_shared<PeerManager>(io, state);

    EXPECT_EQ(pm->max_total_connections(), 200);
    EXPECT_EQ(pm->max_connections_per_ip(), 2);
    EXPECT_EQ(pm->max_half_open_connections(), 100);
    EXPECT_EQ(pm->half_open_connections(), 0);
}

TEST(PeerManagerLimitsTest, SettersUpdateLimits) {
    asio::io_context io;
    auto state = make_test_state();
    auto pm = std::make_shared<PeerManager>(io, state);

    pm->set_max_total_connections(10);
    pm->set_max_connections_per_ip(1);
    pm->set_max_half_open_connections(5);

    EXPECT_EQ(pm->max_total_connections(), 10);
    EXPECT_EQ(pm->max_connections_per_ip(), 1);
    EXPECT_EQ(pm->max_half_open_connections(), 5);
}

TEST(PeerManagerLimitsTest, ConnectToPeerRejectsAtHalfOpenLimit) {
    asio::io_context io;
    auto state = make_test_state();
    auto pm = std::make_shared<PeerManager>(io, state);

    // Set half-open limit to 0 so any connection attempt is rejected immediately
    pm->set_max_half_open_connections(0);

    // connect_to_peer should return nullopt without attempting connection
    auto result = std::optional<AsyncSocket>{};
    asio::co_spawn(io, [&]() -> asio::awaitable<void> {
        result = co_await pm->connect_to_peer("1.2.3.4:6881");
    }, asio::detached);
    io.run();

    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(pm->half_open_connections(), 0);
}

TEST(PeerManagerLimitsTest, AddConnectionRejectsPerIpLimit) {
    asio::io_context io;
    auto state = make_test_state();
    auto pm = std::make_shared<PeerManager>(io, state);

    pm->set_max_connections_per_ip(1);  // Only 1 per IP
    pm->set_max_total_connections(10);

    PeerId id1 = test_peer_id("peer1");
    PeerId id2 = test_peer_id("peer2");

    // First connection from 1.2.3.4 should succeed
    EXPECT_TRUE(pm->add_connection(id1, make_test_conn(io, "1.2.3.4:6881", id1)));
    EXPECT_EQ(pm->connection_count(), 1);

    // Second connection from same IP should be rejected
    EXPECT_FALSE(pm->add_connection(id2, make_test_conn(io, "1.2.3.4:6882", id2)));
    EXPECT_EQ(pm->connection_count(), 1);
}

TEST(PeerManagerLimitsTest, AddConnectionAllowsDifferentIps) {
    asio::io_context io;
    auto state = make_test_state();
    auto pm = std::make_shared<PeerManager>(io, state);

    pm->set_max_connections_per_ip(1);
    pm->set_max_total_connections(10);

    PeerId id1 = test_peer_id("peer1");
    PeerId id2 = test_peer_id("peer2");

    EXPECT_TRUE(pm->add_connection(id1, make_test_conn(io, "1.2.3.4:6881", id1)));
    // Different IP should succeed
    EXPECT_TRUE(pm->add_connection(id2, make_test_conn(io, "5.6.7.8:6881", id2)));
    EXPECT_EQ(pm->connection_count(), 2);
}

TEST(PeerManagerLimitsTest, AddConnectionReplacesWorstPeer) {
    asio::io_context io;
    auto state = make_test_state();
    auto pm = std::make_shared<PeerManager>(io, state);

    pm->set_max_total_connections(2);  // Only allow 2 connections

    PeerId id1 = test_peer_id("peer1");
    PeerId id2 = test_peer_id("peer2");
    PeerId id3 = test_peer_id("peer3");

    // Add two peers with different transfer rates
    // peer1 has low activity (worst)
    EXPECT_TRUE(pm->add_connection(id1, make_test_conn(io, "1.2.3.4:6881", id1, 10, 10)));
    // peer2 has higher activity
    EXPECT_TRUE(pm->add_connection(id2, make_test_conn(io, "5.6.7.8:6881", id2, 100, 200)));
    EXPECT_EQ(pm->connection_count(), 2);

    // Adding a third peer should replace the worst (peer1 with rate 20)
    EXPECT_TRUE(pm->add_connection(id3, make_test_conn(io, "9.10.11.12:6881", id3, 50, 50)));
    EXPECT_EQ(pm->connection_count(), 2);

    // peer1 should have been removed, peer2 and peer3 should remain
    EXPECT_FALSE(pm->contains_peer(id1));
    EXPECT_TRUE(pm->contains_peer(id2));
    EXPECT_TRUE(pm->contains_peer(id3));
}

TEST(PeerManagerLimitsTest, AddConnectionRejectsWhenNoPeerToReplace) {
    asio::io_context io;
    auto state = make_test_state();
    auto pm = std::make_shared<PeerManager>(io, state);
    pm->set_max_total_connections(0);
    pm->set_max_connections_per_ip(1);

    PeerId id1 = test_peer_id("peer1");

    // With max_total_connections=0, find_worst_peer_locked returns nullptr
    // (empty map), so the connection is rejected.
    EXPECT_FALSE(pm->add_connection(id1, make_test_conn(io, "1.2.3.4:6881", id1)));
    EXPECT_EQ(pm->connection_count(), 0);
}

// ============================================================
// Block Request Timeout Tests (PieceManager)
// ============================================================

// Helper: create a SessionState with initialized pieces for PieceManager testing
static std::shared_ptr<SessionState> make_test_state_with_pieces(size_t num_pieces = 5, uint64_t piece_size = 262144) {
    InfoHash dummy_hash{};
    dummy_hash.fill(std::byte{0});
    auto state = std::make_shared<SessionState>(
        dummy_hash,
        std::vector<std::vector<std::string>>{},
        std::filesystem::temp_directory_path() / "p2p_test_pm_timeout"
    );
    state->init_pieces(num_pieces);

    auto& t_info = state->torrent_info();
    t_info.total_size = num_pieces * piece_size;
    t_info.piece_size = piece_size;
    t_info.pieces.resize(num_pieces * 20, std::byte{0});
    t_info.name = "test_torrent";

    return state;
}

TEST(PieceManagerTimeoutTest, BlockWithinTimeoutDoesNotTrigger) {
    asio::io_context io;
    auto state = make_test_state_with_pieces(1, BLOCK_SIZE);
    auto pm = std::make_shared<PieceManager>(io, state);

    bool timeout_fired = false;
    pm->set_block_timeout_callback([&](uint32_t, uint32_t) -> asio::awaitable<void> {
        timeout_fired = true;
        co_return;
    });
    pm->set_callback([&](size_t) -> asio::awaitable<std::vector<std::shared_ptr<PeerConnection>>> {
        co_return std::vector<std::shared_ptr<PeerConnection>>{};
    });

    state->piece_status(0, PieceStatus::InProgress);
    auto piece = std::make_shared<InProgressPiece>(static_cast<uint64_t>(BLOCK_SIZE));
    pm->emplace_in_progress_pieces(0, piece);

    // Set request time to 5 seconds ago (well within the 30s timeout)
    piece->request_times[0] = std::chrono::steady_clock::now() - 5s;

    RunAsync(io, pm->check_block_timeouts());

    EXPECT_FALSE(timeout_fired) << "Timeout callback should not fire for a block within the timeout period";
}

TEST(PieceManagerTimeoutTest, BlockPastTimeoutTriggersCancelAndRerequest) {
    asio::io_context io;
    auto state = make_test_state_with_pieces(1, BLOCK_SIZE);
    auto pm = std::make_shared<PieceManager>(io, state);

    bool timeout_fired = false;
    uint32_t timed_out_piece = 999;
    uint32_t timed_out_block = 999;
    pm->set_block_timeout_callback([&](uint32_t piece_idx, uint32_t block_idx) -> asio::awaitable<void> {
        timeout_fired = true;
        timed_out_piece = piece_idx;
        timed_out_block = block_idx;
        co_return;
    });
    pm->set_callback([&](size_t) -> asio::awaitable<std::vector<std::shared_ptr<PeerConnection>>> {
        co_return std::vector<std::shared_ptr<PeerConnection>>{};
    });

    state->piece_status(0, PieceStatus::InProgress);
    auto piece = std::make_shared<InProgressPiece>(static_cast<uint64_t>(BLOCK_SIZE));
    pm->emplace_in_progress_pieces(0, piece);

    // Set request time to 31 seconds ago (past the 30s timeout)
    piece->request_times[0] = std::chrono::steady_clock::now() - 31s;

    // Use RunAsyncFor so the detached resumer loop spawned by ensure_resume_piece_download
    // (when get_available_peers_ returns empty) doesn't keep io.run() alive forever.
    RunAsyncFor(io, 200ms, pm->check_block_timeouts());

    EXPECT_TRUE(timeout_fired) << "Timeout callback should fire for a block past the timeout period";
    EXPECT_EQ(timed_out_piece, 0);
    EXPECT_EQ(timed_out_block, 0);
}

TEST(PieceManagerTimeoutTest, UnrequestedBlockDoesNotTrigger) {
    asio::io_context io;
    auto state = make_test_state_with_pieces(1, BLOCK_SIZE);
    auto pm = std::make_shared<PieceManager>(io, state);

    bool timeout_fired = false;
    pm->set_block_timeout_callback([&](uint32_t, uint32_t) -> asio::awaitable<void> {
        timeout_fired = true;
        co_return;
    });
    pm->set_callback([&](size_t) -> asio::awaitable<std::vector<std::shared_ptr<PeerConnection>>> {
        co_return std::vector<std::shared_ptr<PeerConnection>>{};
    });

    state->piece_status(0, PieceStatus::InProgress);
    auto piece = std::make_shared<InProgressPiece>(static_cast<uint64_t>(BLOCK_SIZE));
    pm->emplace_in_progress_pieces(0, piece);

    // request_times[0] is default TimePoint{} (never requested) - should be skipped
    RunAsync(io, pm->check_block_timeouts());

    EXPECT_FALSE(timeout_fired) << "Timeout callback should not fire for an unrequested block";
}

TEST(PieceManagerTimeoutTest, ReceivedBlockDoesNotTrigger) {
    asio::io_context io;
    auto state = make_test_state_with_pieces(1, BLOCK_SIZE);
    auto pm = std::make_shared<PieceManager>(io, state);

    bool timeout_fired = false;
    pm->set_block_timeout_callback([&](uint32_t, uint32_t) -> asio::awaitable<void> {
        timeout_fired = true;
        co_return;
    });
    pm->set_callback([&](size_t) -> asio::awaitable<std::vector<std::shared_ptr<PeerConnection>>> {
        co_return std::vector<std::shared_ptr<PeerConnection>>{};
    });

    state->piece_status(0, PieceStatus::InProgress);
    auto piece = std::make_shared<InProgressPiece>(static_cast<uint64_t>(BLOCK_SIZE));
    pm->emplace_in_progress_pieces(0, piece);

    // Block has been received (marks as received), even with old request time it should not fire
    piece->request_times[0] = std::chrono::steady_clock::now() - 31s;
    piece->blocks_received[0] = true;

    RunAsync(io, pm->check_block_timeouts());

    EXPECT_FALSE(timeout_fired) << "Timeout callback should not fire for a block already received";
}

// ============================================================
// Per-Peer Rate Limiting Tests
// ============================================================

TEST(PerPeerRateLimitTest, UploadRateLimiterCreatedAndSettable) {
    asio::io_context io;
    PeerId pid = test_peer_id("rateUL");
    auto conn = std::make_shared<TestPeerConn>(io, "1.2.3.4:6881", pid);

    // Create and assign limiters (normally done by PeerConnection::create)
    conn->set_upload_limiter(std::make_shared<AsyncRateLimiter<>>(io, 10 * 1024 * 1024));
    conn->set_download_limiter(std::make_shared<AsyncRateLimiter<>>(io, 10 * 1024 * 1024));

    // Verify limiters are non-null
    ASSERT_NE(conn->upload_limiter(), nullptr);
    ASSERT_NE(conn->download_limiter(), nullptr);

    // Verify set_rate works via the public interface
    EXPECT_NO_THROW(conn->set_upload_rate(5 * 1024 * 1024));
    EXPECT_NO_THROW(conn->set_download_rate(5 * 1024 * 1024));

    // Setting to 0 disables rate limiting
    EXPECT_NO_THROW(conn->set_upload_rate(0));
    EXPECT_NO_THROW(conn->set_download_rate(0));
}

TEST(PerPeerRateLimitTest, UploadRateLimitingAppliesBackpressure) {
    asio::io_context io;
    PeerId pid = test_peer_id("rateBP");
    auto conn = std::make_shared<TestPeerConn>(io, "1.2.3.4:6881", pid);

    // Very low rate: 100 bytes/s, capacity factor 1 = 100 bytes capacity
    conn->set_upload_limiter(std::make_shared<AsyncRateLimiter<>>(io, 100, 1));
    conn->set_download_limiter(std::make_shared<AsyncRateLimiter<>>(io, 100 * 1024 * 1024));

    auto start = std::chrono::steady_clock::now();

    // Request 1000 bytes through the upload limiter
    // Initial 100 are instant from capacity, remaining 900 need refill
    // At 100 bytes/s, need ~9 seconds
    RunAsyncFor(io, 15s, [&]() -> asio::awaitable<void> {
        co_await conn->upload_limiter()->await_tokens(1000);
    });

    auto elapsed = std::chrono::steady_clock::now() - start;
    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();

    // Should have taken at least 9 seconds for 1000 bytes at 100 bps
    // (100 from initial capacity, 900 at 10 bytes per 100ms = 9000ms)
    EXPECT_GE(elapsed_ms, 8000) << "Upload rate limiting should apply backpressure";
    EXPECT_LE(elapsed_ms, 16000) << "Upload rate limiting should not take excessively long";
}

TEST(PerPeerRateLimitTest, DownloadRateLimitingAppliesBackpressure) {
    asio::io_context io;
    PeerId pid = test_peer_id("rateBP2");
    auto conn = std::make_shared<TestPeerConn>(io, "1.2.3.4:6881", pid);

    // Very low rate: 200 bytes/s, capacity factor 1 = 200 bytes capacity
    conn->set_upload_limiter(std::make_shared<AsyncRateLimiter<>>(io, 100 * 1024 * 1024));
    conn->set_download_limiter(std::make_shared<AsyncRateLimiter<>>(io, 200, 1));

    auto start = std::chrono::steady_clock::now();

    // Request 2000 bytes through the download limiter
    // Initial 200 are instant, remaining 1800 need refill
    // At 200 bytes/s, need ~9 seconds
    RunAsyncFor(io, 15s, [&]() -> asio::awaitable<void> {
        co_await conn->download_limiter()->await_tokens(2000);
    });

    auto elapsed = std::chrono::steady_clock::now() - start;
    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();

    // Should have taken at least 9 seconds for 2000 bytes at 200 bps
    EXPECT_GE(elapsed_ms, 8000) << "Download rate limiting should apply backpressure";
    EXPECT_LE(elapsed_ms, 16000) << "Download rate limiting should not take excessively long";
}

// ============================================================
// BEP-6 Fast Extension Tests
// ============================================================

TEST(BEP6FastExtensionTest, MessageTypeValues) {
    EXPECT_EQ(static_cast<uint8_t>(MessageType::Reject), 16);
    // BEP 52: have_none = 15, have_all = 14
    // BEP 36: reject = 16, allowed_fast = 17
    EXPECT_EQ(static_cast<uint8_t>(MessageType::HaveNone), 15);
    EXPECT_EQ(static_cast<uint8_t>(MessageType::HaveAll), 14);
    EXPECT_EQ(static_cast<uint8_t>(MessageType::AllowedFast), 17);
}

TEST(BEP6FastExtensionTest, HandshakeSerializeFastExtensionFlag) {
    Handshake hs;
    hs.info_hash_bytes = hex_string_to_info_hash("e29fc0e5dceeefea80401e32723796c0a86a8695");
    hs.peer_id_bytes = string_to_peer_id("-PU0001-FASTEXT-TEST");
    hs.extended = true;
    hs.fast_extension = true;

    std::vector<std::byte> serialized = hs.serialize();
    ASSERT_EQ(serialized.size(), HANDSHAKE_BASE_LEN);

    // reserved[7] is at byte offset: 1 (pstrlen) + 19 (protocol) + 7 = 27
    // Check both DHT (0x01) and fast extension (0x04) bits are set
    EXPECT_NE(serialized[27] & static_cast<std::byte>(0x04), static_cast<std::byte>(0));
    EXPECT_NE(serialized[27] & static_cast<std::byte>(0x01), static_cast<std::byte>(0));
}

TEST(BEP6FastExtensionTest, HandshakeSerializeDeserializeFastExtension) {
    Handshake original_hs;
    original_hs.info_hash_bytes = hex_string_to_info_hash("e29fc0e5dceeefea80401e32723796c0a86a8695");
    original_hs.peer_id_bytes = string_to_peer_id("-PU0001-FASTEXT-TEST");
    original_hs.extended = true;
    original_hs.fast_extension = true;

    std::vector<std::byte> serialized = original_hs.serialize();
    Handshake deserialized_hs = Handshake::deserialize(serialized);

    EXPECT_EQ(deserialized_hs.info_hash_bytes, original_hs.info_hash_bytes);
    EXPECT_EQ(deserialized_hs.peer_id_bytes, original_hs.peer_id_bytes);
    EXPECT_TRUE(deserialized_hs.extended);
    EXPECT_TRUE(deserialized_hs.fast_extension);
}

TEST(BEP6FastExtensionTest, HandshakeFastExtensionDefaultsToFalse) {
    Handshake hs;
    EXPECT_FALSE(hs.fast_extension);
}

TEST(BEP6FastExtensionTest, HandshakeFastExtensionFalseSerialize) {
    Handshake hs;
    hs.info_hash_bytes = hex_string_to_info_hash("e29fc0e5dceeefea80401e32723796c0a86a8695");
    hs.peer_id_bytes = string_to_peer_id("-PU0001-FASTEXT-TEST");
    hs.extended = true;
    hs.fast_extension = false;

    std::vector<std::byte> serialized = hs.serialize();
    // reserved[7] should have DHT (0x01) but NOT fast extension (0x04)
    EXPECT_EQ(serialized[27] & static_cast<std::byte>(0x04), static_cast<std::byte>(0));
    EXPECT_NE(serialized[27] & static_cast<std::byte>(0x01), static_cast<std::byte>(0));
}

TEST(BEP6FastExtensionTest, HandshakeDeserializeWithoutFastExtension) {
    Handshake original_hs;
    original_hs.info_hash_bytes = hex_string_to_info_hash("e29fc0e5dceeefea80401e32723796c0a86a8695");
    original_hs.peer_id_bytes = string_to_peer_id("-PU0001-FASTEXT-TEST");
    original_hs.extended = true;
    original_hs.fast_extension = false;

    std::vector<std::byte> serialized = original_hs.serialize();
    Handshake deserialized_hs = Handshake::deserialize(serialized);

    EXPECT_TRUE(deserialized_hs.extended);
    EXPECT_FALSE(deserialized_hs.fast_extension);
}

TEST(BEP6FastExtensionTest, RejectPayloadSerializeDeserialize) {
    uint32_t index = 5;
    uint32_t begin = 16384;
    uint32_t length = BLOCK_SIZE;

    std::vector<std::byte> serialized = RejectPayload::serialize(index, begin, length);
    ASSERT_EQ(serialized.size(), 12);

    RejectPayload deserialized = RejectPayload::deserialize(serialized);

    EXPECT_EQ(deserialized.index, index);
    EXPECT_EQ(deserialized.begin, begin);
    EXPECT_EQ(deserialized.length, length);
}

TEST(BEP6FastExtensionTest, RejectPayloadDeserializeInvalidSize) {
    std::vector<std::byte> too_small(11);
    EXPECT_THROW(RejectPayload::deserialize(too_small), std::runtime_error);

    std::vector<std::byte> just_right_empty(12);
    EXPECT_NO_THROW(RejectPayload::deserialize(just_right_empty));
}

TEST(BEP6FastExtensionTest, RejectPayloadWireFormatMatchesRequestPayload) {
    uint32_t index = 42;
    uint32_t begin = 0;
    uint32_t length = 16384;

    auto reject_bytes = RejectPayload::serialize(index, begin, length);
    auto request_bytes = RequestPayload::serialize(index, begin, length);

    EXPECT_EQ(reject_bytes, request_bytes);
}

// ============================================================
// LSD (Local Service Discovery) BEP 14 Tests
// ============================================================

TEST(LsdDiscoveryTest, Base32EncodeKnownValue) {
    // Test vector: SHA1 of empty string = da39a3ee5e6b4b0d3255bfef95601890afd80709
    // RFC 4648 base32 (no padding): 3I42H3S6NNFQ2MSVX7XZKYAYSCX5QBYJ
    std::vector<std::byte> input(20);
    std::string hex = "da39a3ee5e6b4b0d3255bfef95601890afd80709";
    for (size_t i = 0; i < 20; ++i) {
        unsigned int val;
        std::from_chars(hex.data() + i * 2, hex.data() + i * 2 + 2, val, 16);
        input[i] = static_cast<std::byte>(val);
    }

    std::string encoded = LsdCrypto::base32_encode(input);
    EXPECT_EQ(encoded, "3I42H3S6NNFQ2MSVX7XZKYAYSCX5QBYJ");
}

TEST(LsdDiscoveryTest, Base32EncodeEmpty) {
    std::vector<std::byte> empty;
    EXPECT_TRUE(LsdCrypto::base32_encode(empty).empty());
}

TEST(LsdDiscoveryTest, Base32Roundtrip) {
    std::vector<std::byte> original(20);
    for (size_t i = 0; i < 20; ++i) {
        original[i] = static_cast<std::byte>(i * 17 + 13);
    }

    std::string encoded = LsdCrypto::base32_encode(original);
    std::vector<std::byte> decoded = LsdCrypto::base32_decode(encoded);

    EXPECT_EQ(original, decoded);
}

TEST(LsdDiscoveryTest, AnnounceMessageFormat) {
    std::vector<std::byte> info_hash(20);
    for (size_t i = 0; i < 20; ++i) {
        info_hash[i] = static_cast<std::byte>(i);
    }

    std::string msg = LsdDiscovery::build_announce_message(info_hash, 6881);

    // Must start with BT-SEARCH
    EXPECT_TRUE(msg.starts_with("BT-SEARCH * HTTP/1.1\r\n"));
    // Must contain Host
    EXPECT_NE(msg.find("Host: 239.192.152.143:6771\r\n"), std::string::npos);
    // Must contain Port
    EXPECT_NE(msg.find("Port: 6881\r\n"), std::string::npos);
    // Must contain Infohash (base32 encoded)
    std::string expected_b32 = LsdCrypto::base32_encode(info_hash);
    EXPECT_NE(msg.find("Infohash: " + expected_b32 + "\r\n"), std::string::npos);
    // Must end with \r\n\r\n
    EXPECT_TRUE(msg.ends_with("\r\n\r\n"));
}

TEST(LsdDiscoveryTest, ParseValidAnnouncement) {
    std::vector<std::byte> info_hash(20);
    for (size_t i = 0; i < 20; ++i) {
        info_hash[i] = static_cast<std::byte>(i);
    }
    std::string b32 = LsdCrypto::base32_encode(info_hash);

    std::string announce = std::format(
        "BT-SEARCH * HTTP/1.1\r\n"
        "Host: 239.192.152.143:6771\r\n"
        "Port: 6881\r\n"
        "Infohash: {}\r\n"
        "\r\n",
        b32
    );

    auto parsed = LsdDiscovery::parse_announcement(announce);
    ASSERT_TRUE(parsed.has_value());
    EXPECT_EQ(parsed->infohash_b32, b32);
    EXPECT_EQ(parsed->port, 6881);
}

TEST(LsdDiscoveryTest, ParseAnnouncementMissingBtSearch) {
    std::string invalid = "NOT_BT_SEARCH * HTTP/1.1\r\n\r\n";
    auto parsed = LsdDiscovery::parse_announcement(invalid);
    EXPECT_FALSE(parsed.has_value());
}

TEST(LsdDiscoveryTest, ParseAnnouncementMissingInfohash) {
    std::string invalid = "BT-SEARCH * HTTP/1.1\r\nPort: 6881\r\n\r\n";
    auto parsed = LsdDiscovery::parse_announcement(invalid);
    EXPECT_FALSE(parsed.has_value());
}

TEST(LsdDiscoveryTest, ParseAnnouncementMissingPort) {
    std::string invalid = "BT-SEARCH * HTTP/1.1\r\nInfohash: ABCDEF\r\n\r\n";
    auto parsed = LsdDiscovery::parse_announcement(invalid);
    EXPECT_FALSE(parsed.has_value());
}

TEST(LsdDiscoveryTest, ParseAnnouncementNonNumericPort) {
    std::string invalid = "BT-SEARCH * HTTP/1.1\r\nPort: notanumber\r\nInfohash: ABCDEF\r\n\r\n";
    auto parsed = LsdDiscovery::parse_announcement(invalid);
    EXPECT_FALSE(parsed.has_value());
}

TEST(LsdDiscoveryTest, ParseAnnouncementDifferentPort) {
    std::vector<std::byte> info_hash(20);
    std::string b32 = LsdCrypto::base32_encode(info_hash);

    std::string announce = std::format(
        "BT-SEARCH * HTTP/1.1\r\n"
        "Host: 239.192.152.143:6771\r\n"
        "Port: 9999\r\n"
        "Infohash: {}\r\n"
        "\r\n",
        b32
    );

    auto parsed = LsdDiscovery::parse_announcement(announce);
    ASSERT_TRUE(parsed.has_value());
    EXPECT_EQ(parsed->port, 9999);
}

// ============================================================
// Choking Algorithm (BEP 3 Tit-for-Tat) Tests
// ============================================================

TEST(ChokingAlgorithmTest, TopUploadersUnchoked) {
    asio::io_context io;
    auto state = make_test_state();
    auto pm = std::make_shared<PeerManager>(io, state, std::chrono::milliseconds(50));
    pm->set_max_total_connections(10);
    pm->set_max_connections_per_ip(10);

    auto p1 = make_test_conn(io, "1.2.3.1:6881", test_peer_id("p1"), 0, 1000);
    auto p2 = make_test_conn(io, "1.2.3.2:6881", test_peer_id("p2"), 0, 800);
    auto p3 = make_test_conn(io, "1.2.3.3:6881", test_peer_id("p3"), 0, 600);
    auto p4 = make_test_conn(io, "1.2.3.4:6881", test_peer_id("p4"), 0, 400);
    auto p5 = make_test_conn(io, "1.2.3.5:6881", test_peer_id("p5"), 0, 200);
    auto p6 = make_test_conn(io, "1.2.3.6:6881", test_peer_id("p6"), 0, 50);

    for (auto& p : {p1, p2, p3, p4, p5, p6}) {
        p->peer_is_interested(true);
    }

    ASSERT_TRUE(pm->add_connection(p1->peer_id(), p1));
    ASSERT_TRUE(pm->add_connection(p2->peer_id(), p2));
    ASSERT_TRUE(pm->add_connection(p3->peer_id(), p3));
    ASSERT_TRUE(pm->add_connection(p4->peer_id(), p4));
    ASSERT_TRUE(pm->add_connection(p5->peer_id(), p5));
    ASSERT_TRUE(pm->add_connection(p6->peer_id(), p6));
    ASSERT_EQ(pm->connection_count(), 6);

    asio::co_spawn(io, pm->choke_loop(), asio::detached);
    io.run_for(std::chrono::milliseconds(70));
    pm->cancel();
    io.run_for(std::chrono::milliseconds(5));

    EXPECT_FALSE(p1->am_choking()) << "p1 (top uploader 1000) should be unchoked";
    EXPECT_FALSE(p2->am_choking()) << "p2 (top uploader 800) should be unchoked";
    EXPECT_FALSE(p3->am_choking()) << "p3 (top uploader 600) should be unchoked";
    EXPECT_TRUE(p6->am_choking()) << "p6 (lowest uploader 50) should remain choked";
}

TEST(ChokingAlgorithmTest, UninterestedPeersNeverUnchoked) {
    asio::io_context io;
    auto state = make_test_state();
    auto pm = std::make_shared<PeerManager>(io, state, std::chrono::milliseconds(50));
    pm->set_max_total_connections(10);
    pm->set_max_connections_per_ip(10);

    auto p1 = make_test_conn(io, "1.2.3.1:6881", test_peer_id("p1"), 0, 1000);
    auto p2 = make_test_conn(io, "1.2.3.2:6881", test_peer_id("p2"), 0, 500);

    p1->peer_is_interested(true);
    p2->peer_is_interested(false);

    ASSERT_TRUE(pm->add_connection(p1->peer_id(), p1));
    ASSERT_TRUE(pm->add_connection(p2->peer_id(), p2));

    asio::co_spawn(io, pm->choke_loop(), asio::detached);
    io.run_for(std::chrono::milliseconds(70));
    pm->cancel();
    io.run_for(std::chrono::milliseconds(5));

    EXPECT_FALSE(p1->am_choking()) << "Interested peer should be unchoked";
    EXPECT_TRUE(p2->am_choking()) << "Uninterested peer should never be unchoked";
}

TEST(ChokingAlgorithmTest, SnubbedPeerGetsChoked) {
    asio::io_context io;
    auto state = make_test_state();
    auto pm = std::make_shared<PeerManager>(io, state, std::chrono::milliseconds(50));
    pm->set_max_total_connections(10);
    pm->set_max_connections_per_ip(10);

    auto p1 = make_test_conn(io, "1.2.3.1:6881", test_peer_id("p1"), 0, 500);
    auto p2 = make_test_conn(io, "1.2.3.2:6881", test_peer_id("p2"), 0, 500);

    p1->peer_is_interested(true);
    p2->peer_is_interested(true);

    p1->am_choking(false);
    p2->am_choking(false);

    p1->last_data_received(std::chrono::steady_clock::now() - std::chrono::seconds(90));
    p2->last_data_received(std::chrono::steady_clock::now() - std::chrono::seconds(10));

    ASSERT_TRUE(pm->add_connection(p1->peer_id(), p1));
    ASSERT_TRUE(pm->add_connection(p2->peer_id(), p2));

    asio::co_spawn(io, pm->choke_loop(), asio::detached);
    io.run_for(std::chrono::milliseconds(70));
    pm->cancel();
    io.run_for(std::chrono::milliseconds(5));

    EXPECT_TRUE(p1->am_choking()) << "Snubbed peer (no data 90s) should be choked";
    EXPECT_FALSE(p2->am_choking()) << "Non-snubbed peer (data 10s ago) should remain unchoked";
}

TEST(ChokingAlgorithmTest, OptimisticRotationIncreasesUnchokeCount) {
    asio::io_context io;
    auto state = make_test_state();
    auto pm = std::make_shared<PeerManager>(io, state, std::chrono::milliseconds(50));
    pm->set_max_total_connections(10);
    pm->set_max_connections_per_ip(10);

    std::vector<std::shared_ptr<TestPeerConn>> peers;
    for (int i = 1; i <= 6; ++i) {
        std::string suffix = "p" + std::to_string(i);
        std::string addr = "1.2.3." + std::to_string(i) + ":6881";
        auto pid = test_peer_id(suffix);
        auto conn = make_test_conn(io, addr, pid, 0, 100);
        conn->peer_is_interested(true);
        ASSERT_TRUE(pm->add_connection(conn->peer_id(), conn));
        peers.push_back(conn);
    }

    asio::co_spawn(io, pm->choke_loop(), asio::detached);
    io.run_for(std::chrono::milliseconds(200));
    pm->cancel();
    io.run_for(std::chrono::milliseconds(5));

    auto unchoked = pm->get_unchoked_peers();
    EXPECT_EQ(unchoked.size(), 4)
        << "After optimistic rotation, should have 4 unchoked peers (3 regular + 1 optimistic)";
}

TEST(ChokingAlgorithmTest, PreviousOptimisticPeerIsChokedOnRotation) {
    asio::io_context io;
    auto state = make_test_state();
    auto pm = std::make_shared<PeerManager>(io, state, std::chrono::milliseconds(50));
    pm->set_max_total_connections(10);
    pm->set_max_connections_per_ip(10);

    std::vector<std::shared_ptr<TestPeerConn>> peers;
    for (int i = 1; i <= 6; ++i) {
        std::string suffix = "p" + std::to_string(i);
        std::string addr = "1.2.3." + std::to_string(i) + ":6881";
        auto pid = test_peer_id(suffix);
        auto conn = make_test_conn(io, addr, pid, 0, 100);
        conn->peer_is_interested(true);
        ASSERT_TRUE(pm->add_connection(conn->peer_id(), conn));
        peers.push_back(conn);
    }

    asio::co_spawn(io, pm->choke_loop(), asio::detached);
    io.run_for(std::chrono::milliseconds(350));
    pm->cancel();
    io.run_for(std::chrono::milliseconds(5));

    auto unchoked = pm->get_unchoked_peers();
    EXPECT_EQ(unchoked.size(), 4)
        << "After multiple rotations, exactly 4 peers should be unchoked";
}

// ============================================================
// Request Pipelining Tests
// ============================================================

TEST(RequestPipeliningTest, RequestsAreQueuedAtOutstandingLimit) {
    asio::io_context io;
    PeerId pid = test_peer_id("pipe1");
    auto conn = std::make_shared<TestPeerConn>(io, "1.2.3.4:6881", pid);

    size_t limit = conn->max_outstanding_requests();
    conn->set_pipeline_state(limit, limit * BLOCK_SIZE, 0);

    bool sent = false;
    asio::co_spawn(io, [&]() -> asio::awaitable<void> {
        sent = co_await conn->send_request(0, 0, BLOCK_SIZE);
    }, asio::detached);
    io.run();

    EXPECT_FALSE(sent);
    EXPECT_EQ(conn->pending_request_count(), 1);
    EXPECT_EQ(conn->outstanding_request_count(), limit);
    ASSERT_FALSE(conn->pending_requests().empty());
    EXPECT_EQ(conn->pending_requests().front().index, 0);
    EXPECT_EQ(conn->pending_requests().front().begin, 0);
    EXPECT_EQ(conn->pending_requests().front().length, BLOCK_SIZE);
}

TEST(RequestPipeliningTest, RequestsAreQueuedAtPipelineBufferLimit) {
    asio::io_context io;
    PeerId pid = test_peer_id("pipe2");
    auto conn = std::make_shared<TestPeerConn>(io, "1.2.3.4:6881", pid);

    uint64_t pipeline_buffer = PeerConnection::MAX_PIPELINE_BUFFER;
    conn->set_pipeline_state(1, pipeline_buffer, 0);

    bool sent = false;
    asio::co_spawn(io, [&]() -> asio::awaitable<void> {
        sent = co_await conn->send_request(0, 0, BLOCK_SIZE);
    }, asio::detached);
    io.run();

    EXPECT_FALSE(sent);
    EXPECT_EQ(conn->pending_request_count(), 1);
    EXPECT_EQ(conn->total_bytes_requested(), pipeline_buffer);
}

TEST(RequestPipeliningTest, QueuedRequestFlushedOnPieceReceived) {
    asio::io_context io;
    PeerId pid = test_peer_id("pipe3");
    auto conn = std::make_shared<TestPeerConn>(io, "1.2.3.4:6881", pid);

    size_t limit = conn->max_outstanding_requests();
    conn->set_pipeline_state(limit, limit * BLOCK_SIZE, 0);
    conn->peer_is_choking(false);
    asio::co_spawn(io, [&]() -> asio::awaitable<void> {
        co_await conn->send_request(0, 0, BLOCK_SIZE);
    }, asio::detached);
    io.run();
    ASSERT_EQ(conn->pending_request_count(), 1);

    conn->on_request_completed(BLOCK_SIZE);

    EXPECT_EQ(conn->pending_request_count(), 0);
    EXPECT_EQ(conn->total_bytes_received(), BLOCK_SIZE);
    EXPECT_EQ(conn->total_bytes_requested(), (limit + 1) * BLOCK_SIZE);
    EXPECT_EQ(conn->outstanding_request_count(), limit);
}

TEST(RequestPipeliningTest, QueuedRequestFlushedOnRejectReceived) {
    asio::io_context io;
    PeerId pid = test_peer_id("pipe4");
    auto conn = std::make_shared<TestPeerConn>(io, "1.2.3.4:6881", pid);

    size_t limit = conn->max_outstanding_requests();
    uint64_t req_bytes = limit * BLOCK_SIZE;
    conn->set_pipeline_state(limit, req_bytes, 0);
    conn->peer_is_choking(false);
    asio::co_spawn(io, [&]() -> asio::awaitable<void> {
        co_await conn->send_request(1, 0, BLOCK_SIZE);
    }, asio::detached);
    io.run();
    ASSERT_EQ(conn->pending_request_count(), 1);

    conn->on_request_rejected(BLOCK_SIZE);

    EXPECT_EQ(conn->pending_request_count(), 0);
    EXPECT_EQ(conn->total_bytes_requested(), req_bytes);
    EXPECT_EQ(conn->outstanding_request_count(), limit);
}

TEST(RequestPipeliningTest, CancelRemovesFromPendingQueue) {
    asio::io_context io;
    PeerId pid = test_peer_id("pipe5");
    auto conn = std::make_shared<TestPeerConn>(io, "1.2.3.4:6881", pid);

    conn->set_pipeline_state(5, 5 * BLOCK_SIZE, 0);
    conn->queue_request(2, 0, BLOCK_SIZE);
    conn->queue_request(3, 0, BLOCK_SIZE);
    ASSERT_EQ(conn->pending_request_count(), 2);

    EXPECT_EQ(conn->cancel_and_count(2, 0, BLOCK_SIZE), 1);
    EXPECT_EQ(conn->cancel_and_count(2, 0, BLOCK_SIZE), 1);
    EXPECT_EQ(conn->cancel_and_count(3, 0, BLOCK_SIZE), 0);
}

TEST(RequestPipeliningTest, MultipleRequestsQueuedAndFlushedInOrder) {
    asio::io_context io;
    PeerId pid = test_peer_id("pipe6");
    auto conn = std::make_shared<TestPeerConn>(io, "1.2.3.4:6881", pid);

    size_t limit = conn->max_outstanding_requests();
    conn->set_pipeline_state(limit, limit * BLOCK_SIZE, 0);
    conn->peer_is_choking(false);
    bool sent3 = true, sent4 = true, sent5 = true;
    asio::co_spawn(io, [&]() -> asio::awaitable<void> {
        sent3 = co_await conn->send_request(3, 0, BLOCK_SIZE);
        sent4 = co_await conn->send_request(4, 0, BLOCK_SIZE);
        sent5 = co_await conn->send_request(5, 0, BLOCK_SIZE);
    }, asio::detached);
    io.run();
    ASSERT_EQ(conn->pending_request_count(), 3);
    EXPECT_FALSE(sent3);
    EXPECT_FALSE(sent4);
    EXPECT_FALSE(sent5);

    conn->on_request_completed(BLOCK_SIZE);
    EXPECT_EQ(conn->pending_request_count(), 2);
    EXPECT_EQ(conn->outstanding_request_count(), limit);
    EXPECT_EQ(conn->total_bytes_requested(), (limit + 1) * BLOCK_SIZE);

    conn->on_request_rejected(BLOCK_SIZE);
    EXPECT_EQ(conn->pending_request_count(), 1);
    EXPECT_EQ(conn->pending_requests()[0].index, 5);
    EXPECT_EQ(conn->total_bytes_requested(), (limit + 1) * BLOCK_SIZE);

    conn->on_request_completed(BLOCK_SIZE);
    EXPECT_EQ(conn->pending_request_count(), 0);
}

TEST(RequestPipeliningTest, SlotAvailableRequestNotQueued) {
    asio::io_context io;
    PeerId pid = test_peer_id("pipe7");
    auto conn = std::make_shared<TestPeerConn>(io, "1.2.3.4:6881", pid);

    conn->set_pipeline_state(0, 0, 0);
    bool caught_error = false;
    asio::co_spawn(io, [&]() -> asio::awaitable<void> {
        try {
            co_await conn->send_request(0, 0, BLOCK_SIZE);
        } catch (...) {
            caught_error = true;
        }
    }, asio::detached);
    io.run();

    EXPECT_EQ(conn->pending_request_count(), 0);
    EXPECT_TRUE(caught_error);
}
