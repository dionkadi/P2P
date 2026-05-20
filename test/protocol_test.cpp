#include "helper.hpp"

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
    // Test for an unknown string
    EXPECT_THROW(to_extended_type("unknown_ext"), std::invalid_argument);
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
    EXPECT_EQ(pm->max_half_open_connections(), 40);
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

    RunAsync(io, pm->check_block_timeouts());

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
    EXPECT_EQ(static_cast<uint8_t>(MessageType::HaveNone), 17);
    EXPECT_EQ(static_cast<uint8_t>(MessageType::HaveAll), 18);
    EXPECT_EQ(static_cast<uint8_t>(MessageType::AllowedFast), 19);
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
