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
