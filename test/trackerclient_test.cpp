#include "TrackerClient.hpp"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

std::vector<std::byte> bytes_of(const std::string& s) {
    return {reinterpret_cast<const std::byte*>(s.data()),
            reinterpret_cast<const std::byte*>(s.data()) + s.size()};
}

// 6-byte compact (BEP-23) peer entry: IPv4 + big-endian port.
std::string compact_peer(uint8_t a, uint8_t b, uint8_t c, uint8_t d, uint16_t port) {
    std::string s;
    s += static_cast<char>(a);
    s += static_cast<char>(b);
    s += static_cast<char>(c);
    s += static_cast<char>(d);
    s += static_cast<char>((port >> 8) & 0xFF);
    s += static_cast<char>(port & 0xFF);
    return s;
}

} // namespace

// Real-world trackers (open.ftorrent.com, tr.nyacat.pw) append trailing data
// after the bencoded dict; the strict decode() used to reject the announce
// with "Trailing data left after decoding".
TEST(TrackerClientParseTest, CompactPeersWithTrailingGarbage) {
    std::string body = "d8:intervali1800e5:peers6:" + compact_peer(1, 2, 3, 4, 6881) + "e\r\n";
    auto result = parse_tracker_response_body(bytes_of(body));
    EXPECT_EQ(result.interval_seconds, 1800);
    ASSERT_EQ(result.peers.size(), 1u);
    EXPECT_EQ(result.peers[0], "1.2.3.4:6881");
}

// Trackers (tr.kxmp.cf, tracker.zhuqiy.dgj055.icu) returning the non-compact
// list-of-dicts form used to crash announce with "wrong index for variant".
TEST(TrackerClientParseTest, ListOfDictsPeers) {
    std::string body = "d8:intervali900e5:peersl"
                       "d2:ip8:10.0.0.14:porti51413ee"   // valid IPv4 entry
                       "d2:ip3:::14:porti5000ee"         // IPv6 entry — skipped
                       "d2:ip11:192.168.1.1e"            // missing port — skipped
                       "ee";
    auto result = parse_tracker_response_body(bytes_of(body));
    EXPECT_EQ(result.interval_seconds, 900);
    ASSERT_EQ(result.peers.size(), 1u);
    EXPECT_EQ(result.peers[0], "10.0.0.1:51413");
}

TEST(TrackerClientParseTest, MissingIntervalDefaultsTo1800) {
    std::string body = "d5:peers6:" + compact_peer(9, 9, 9, 9, 51413) + "e";
    auto result = parse_tracker_response_body(bytes_of(body));
    EXPECT_EQ(result.interval_seconds, 1800);
    ASSERT_EQ(result.peers.size(), 1u);
    EXPECT_EQ(result.peers[0], "9.9.9.9:51413");
}

TEST(TrackerClientParseTest, MissingPeersYieldsEmptyPeers) {
    auto result = parse_tracker_response_body(bytes_of("d8:intervali600ee"));
    EXPECT_EQ(result.interval_seconds, 600);
    EXPECT_TRUE(result.peers.empty());
}

TEST(TrackerClientParseTest, OddTypedPeersFieldIgnored) {
    auto result = parse_tracker_response_body(bytes_of("d8:intervali1800e5:peersi12345ee"));
    EXPECT_EQ(result.interval_seconds, 1800);
    EXPECT_TRUE(result.peers.empty());
}

TEST(TrackerClientParseTest, NonDictBodyThrows) {
    EXPECT_THROW(parse_tracker_response_body(bytes_of("hello")), std::runtime_error);
}

TEST(TrackerClientParseTest, FailureReasonExtracted) {
    EXPECT_EQ(parse_tracker_failure_reason(bytes_of("d14:failure reason17:torrent not founde")),
              "torrent not found");
}

TEST(TrackerClientParseTest, FailureReasonEmptyWhenAbsent) {
    EXPECT_TRUE(parse_tracker_failure_reason(bytes_of("hello")).empty());
    EXPECT_TRUE(
        parse_tracker_failure_reason(bytes_of("d5:peers6:" + compact_peer(1, 1, 1, 1, 1) + "e")).empty());
}
