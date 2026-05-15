#include <gtest/gtest.h>
#include <vector>
#include <string>
#include <cstddef>

#include "MagnetUri.hpp"
#include "SessionState.hpp"

// ==================== Magnet URI Parsing Tests ====================

TEST(MagnetUriTest, ParseHexInfoHash) {
    std::string uri = "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567";
    MagnetLink link = parse_magnet_uri(uri);
    EXPECT_TRUE(link.valid());

    // Verify the info hash bytes
    InfoHash expected{};
    // 01 23 45 67 89 ab cd ef 01 23 45 67 89 ab cd ef 01 23 45 67
    expected[0] = std::byte{0x01};
    expected[1] = std::byte{0x23};
    expected[2] = std::byte{0x45};
    expected[3] = std::byte{0x67};
    expected[4] = std::byte{0x89};
    expected[5] = std::byte{0xab};
    expected[6] = std::byte{0xcd};
    expected[7] = std::byte{0xef};
    expected[8] = std::byte{0x01};
    expected[9] = std::byte{0x23};
    expected[10] = std::byte{0x45};
    expected[11] = std::byte{0x67};
    expected[12] = std::byte{0x89};
    expected[13] = std::byte{0xab};
    expected[14] = std::byte{0xcd};
    expected[15] = std::byte{0xef};
    expected[16] = std::byte{0x01};
    expected[17] = std::byte{0x23};
    expected[18] = std::byte{0x45};
    expected[19] = std::byte{0x67};

    EXPECT_EQ(link.info_hash, expected);
    EXPECT_TRUE(link.display_name.empty());
    EXPECT_TRUE(link.tracker_urls.empty());
}

TEST(MagnetUriTest, ParseBase32InfoHash) {
    std::string uri = "magnet:?xt=urn:btih:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    MagnetLink link = parse_magnet_uri(uri);
    EXPECT_TRUE(link.valid());
    for (const auto& b : link.info_hash) {
        EXPECT_EQ(b, std::byte{0});
    }
}

TEST(MagnetUriTest, ParseWithDisplayName) {
    std::string uri = "magnet:?xt=urn:btih:abcdef0123456789abcdef0123456789abcdef01&dn=Ubuntu+20.04";
    MagnetLink link = parse_magnet_uri(uri);
    EXPECT_TRUE(link.valid());
    EXPECT_EQ(link.display_name, "Ubuntu 20.04");
}

TEST(MagnetUriTest, ParseWithTrackers) {
    std::string uri = "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567"
                       "&tr=http://tracker1.example.com/announce"
                       "&tr=udp://tracker2.example.com:6969";
    MagnetLink link = parse_magnet_uri(uri);
    EXPECT_TRUE(link.valid());
    ASSERT_EQ(link.tracker_urls.size(), 2);
    EXPECT_EQ(link.tracker_urls[0], "http://tracker1.example.com/announce");
    EXPECT_EQ(link.tracker_urls[1], "udp://tracker2.example.com:6969");
}

TEST(MagnetUriTest, ParseWithAllFields) {
    std::string uri = "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567"
                       "&dn=My+File"
                       "&tr=http://tracker.example.com/announce"
                       "&xs=http://source.example.com/file.torrent";
    MagnetLink link = parse_magnet_uri(uri);
    EXPECT_TRUE(link.valid());
    EXPECT_EQ(link.display_name, "My File");
    ASSERT_EQ(link.tracker_urls.size(), 1);
    EXPECT_EQ(link.tracker_urls[0], "http://tracker.example.com/announce");
    ASSERT_EQ(link.source_urls.size(), 1);
    EXPECT_EQ(link.source_urls[0], "http://source.example.com/file.torrent");
}

TEST(MagnetUriTest, Roundtrip) {
    MagnetLink original;
    std::ranges::fill(original.info_hash, std::byte{0});
    original.info_hash[0] = std::byte{0xAB};
    original.info_hash[19] = std::byte{0xCD};
    original.display_name = "Test Torrent";
    original.tracker_urls = {"http://tracker1.example.com/announce", "udp://tracker2.example.com:6969"};

    std::string serialized = to_magnet_uri(original);
    MagnetLink parsed = parse_magnet_uri(serialized);

    EXPECT_EQ(parsed.info_hash, original.info_hash);
    EXPECT_EQ(parsed.display_name, original.display_name);
    ASSERT_EQ(parsed.tracker_urls.size(), original.tracker_urls.size());
    EXPECT_EQ(parsed.tracker_urls[0], original.tracker_urls[0]);
    EXPECT_EQ(parsed.tracker_urls[1], original.tracker_urls[1]);
}

TEST(MagnetUriTest, InvalidUri) {
    EXPECT_THROW(parse_magnet_uri("not-a-magnet-uri"), std::runtime_error);
}

TEST(MagnetUriTest, MissingInfoHash) {
    std::string uri = "magnet:?dn=Test&tr=http://example.com/announce";
    EXPECT_THROW(parse_magnet_uri(uri), std::runtime_error);
}

TEST(MagnetUriTest, InvalidHexHash) {
    std::string uri = "magnet:?xt=urn:btih:nothex&dn=Test";
    EXPECT_THROW(parse_magnet_uri(uri), std::runtime_error);
}

TEST(MagnetUriTest, HexInfoHashNonTrivial) {
    std::string hex_uri = "magnet:?xt=urn:btih:"
                          "1234567890abcdef1234567890abcdef12345678";
    MagnetLink link = parse_magnet_uri(hex_uri);
    EXPECT_TRUE(link.valid());
    EXPECT_EQ(link.info_hash[0], std::byte{0x12});
    EXPECT_EQ(link.info_hash[1], std::byte{0x34});
    EXPECT_EQ(link.info_hash[2], std::byte{0x56});
}

// ==================== URL Decode Tests ====================

TEST(UrlDecodeTest, BasicDecode) {
    EXPECT_EQ(url_decode("hello"), "hello");
    EXPECT_EQ(url_decode("hello%20world"), "hello world");
    EXPECT_EQ(url_decode("a%2Fb%3Fc"), "a/b?c");
    EXPECT_EQ(url_decode("foo+bar"), "foo bar");
    EXPECT_EQ(url_decode("%3A%2F%3F%23"), ":/?#");
}

TEST(UrlDecodeTest, EmptyString) {
    EXPECT_EQ(url_decode(""), "");
}

// ==================== SessionState Magnet Constructor Tests ====================

TEST(SessionStateMagnetTest, CreateFromInfoHash) {
    InfoHash info_hash{};
    info_hash[0] = std::byte{0xAA};
    info_hash[19] = std::byte{0xBB};

    std::vector<std::vector<std::string>> tracker_tiers;
    tracker_tiers.push_back({"http://tracker.example.com/announce"});

    SessionState state(info_hash, tracker_tiers, "/tmp/test_download");

    // Verify info hash
    EXPECT_EQ(state.info_hash().size(), HASH_SIZE);
    EXPECT_EQ(state.info_hash()[0], std::byte{0xAA});
    EXPECT_EQ(state.info_hash()[19], std::byte{0xBB});

    // Verify no metadata yet
    EXPECT_EQ(state.num_pieces(), 0);
    EXPECT_TRUE(state.torrent_info().pieces.empty());

    // Verify tracker tiers
    const auto& tiers = state.tracker_tiers();
    ASSERT_EQ(tiers.size(), 1);
    ASSERT_EQ(tiers[0].size(), 1);
    EXPECT_EQ(tiers[0][0], "http://tracker.example.com/announce");
}

TEST(SessionStateMagnetTest, InitPiecesAfterMetadata) {
    InfoHash info_hash{};
    SessionState state(info_hash, {}, "/tmp/test");

    EXPECT_EQ(state.num_pieces(), 0);

    // Simulate metadata arriving
    state.torrent_info().piece_size = 16384;
    state.torrent_info().total_size = 32768;
    state.torrent_info().name = "test";
    // Two pieces: 2 * 20 = 40 bytes of SHA1 hashes
    state.torrent_info().pieces.resize(40, std::byte{0});

    state.init_pieces(2);
    EXPECT_EQ(state.num_pieces(), 2);
    EXPECT_EQ(state.piece_status(0), PieceStatus::Needed);
    EXPECT_EQ(state.piece_status(1), PieceStatus::Needed);
}
