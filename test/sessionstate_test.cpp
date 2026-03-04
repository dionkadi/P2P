#include "gtest/gtest.h"
#include "SessionState.hpp"
#include "helper.hpp" // For string_to_bytes, bytes_to_string
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

// Fixture for SessionState tests to handle temporary files
class SessionStateTest : public ::testing::Test {
protected:
    std::filesystem::path temp_dir;
    std::filesystem::path torrent_path;
    std::filesystem::path save_path; // For single file it's the file, for multi it's the dir

    void SetUp() override {
        temp_dir = std::filesystem::temp_directory_path() / "sessionstate_test_temp";
        std::filesystem::create_directories(temp_dir);
        torrent_path = temp_dir / "test.torrent";
        save_path = temp_dir / "download_dir";
        std::filesystem::create_directories(save_path); // Create save_path as a directory
    }

    void TearDown() override {
        std::filesystem::remove_all(temp_dir);
    }

    // Helper to generate a mock torrent file for SessionState initialization
    void create_mock_torrent_file(const std::string& name, uint64_t total_size, uint32_t piece_length, const std::vector<std::string>& tracker_urls, bool is_multi_file = false) {
        Dict info_dict;
        info_dict["name"] = Value(name);
        info_dict["piece length"] = Value(static_cast<Integer>(piece_length));
        
        // Generate enough dummy piece hashes
        size_t num_pieces = (total_size + piece_length - 1) / piece_length;
        std::string all_piece_hashes_str;
        for(size_t i = 0; i < num_pieces; ++i) {
            std::string dummy_piece_data(20, static_cast<char>('A' + (i % 26))); // Unique dummy hash for each 20-byte segment
            all_piece_hashes_str += dummy_piece_data;
        }
        info_dict["pieces"] = Value(all_piece_hashes_str);

        if (is_multi_file) {
            List files_list;
            // Assuming two files for simplicity in a multi-file torrent
            files_list.push_back(Value(Dict{
                {"length", Value(static_cast<Integer>(total_size / 2))},
                {"path", Value(List{Value(String("file1.dat"))})}
            }));
            files_list.push_back(Value(Dict{
                {"length", Value(static_cast<Integer>(total_size - (total_size / 2)))},
                {"path", Value(List{Value(String("file2.dat"))})}
            }));
            info_dict["files"] = Value(files_list);
        } else {
            info_dict["length"] = Value(static_cast<Integer>(total_size));
        }

        Dict metainfo_dict;
        // Use announce-list if multiple trackers, otherwise announce
        if (tracker_urls.size() > 1) {
            List announce_list_tiers;
            for (const auto& url : tracker_urls) {
                List tier;
                tier.push_back(Value(url));
                announce_list_tiers.push_back(Value(std::move(tier)));
            }
            metainfo_dict["announce-list"] = Value(std::move(announce_list_tiers));
        } else {
            metainfo_dict["announce"] = Value(tracker_urls[0]);
        }
        metainfo_dict["info"] = Value(info_dict);
        
        std::ofstream file(torrent_path, std::ios::binary);
        ASSERT_TRUE(file.is_open()) << "Failed to create mock torrent file: " << torrent_path;
        file.write(bytes_to_string(encode(Value(metainfo_dict))).data(), bytes_to_string(encode(Value(metainfo_dict))).size());
        file.close();
    }
};

TEST_F(SessionStateTest, ConstructorAndInitialStateSingleFile) {
    std::string torrent_name = "single_test_file";
    uint64_t total_size = 100000; // 100KB
    uint32_t piece_length = 16384; // 16KB
    std::vector<std::string> trackers = {"http://t1.com"};
    create_mock_torrent_file(torrent_name, total_size, piece_length, trackers, false);

    SessionState state(torrent_path, save_path);

    // Verify MetaInfo loading
    EXPECT_EQ(state.torrent_info().name, torrent_name);
    EXPECT_EQ(state.torrent_info().total_size, total_size);
    EXPECT_EQ(state.torrent_info().piece_size, piece_length);
    EXPECT_FALSE(state.info_hash().empty());

    // Verify initial piece status
    size_t expected_num_pieces = (total_size + piece_length - 1) / piece_length;
    ASSERT_EQ(state.num_pieces(), expected_num_pieces);
    for (size_t i = 0; i < expected_num_pieces; ++i) {
        EXPECT_EQ(state.piece_status(i), PieceStatus::Needed) << "Piece " << i << " should be Needed initially";
    }

    // Verify tracker tiers
    ASSERT_EQ(state.tracker_tiers().size(), 1);
    ASSERT_EQ(state.tracker_tiers()[0].size(), 1);
    EXPECT_EQ(state.tracker_tiers()[0][0], "http://t1.com");

    // Verify other atomic variables
    EXPECT_EQ(state.total_bytes_downloaded(), 0);
    EXPECT_EQ(state.total_bytes_uploaded(), 0);
    EXPECT_EQ(state.completed_pieces(), 0);
    EXPECT_FALSE(state.is_download_complete());
    EXPECT_FALSE(state.is_in_endgame_mode());
    EXPECT_EQ(state.save_path(), save_path);
}

TEST_F(SessionStateTest, ConstructorAndInitialStateMultiFile) {
    std::string torrent_name = "multi_test_dir";
    uint64_t total_size = 50000; // 50KB total
    uint32_t piece_length = 8192; // 8KB
    std::vector<std::string> trackers = {"http://t1.com", "http://t2.com/announce", "udp://t3.com:6969"};
    create_mock_torrent_file(torrent_name, total_size, piece_length, trackers, true);

    SessionState state(torrent_path, save_path);

    // Verify MetaInfo loading (specifically multi-file aspects)
    EXPECT_EQ(state.torrent_info().name, torrent_name);
    EXPECT_EQ(state.torrent_info().total_size, total_size);
    EXPECT_EQ(state.torrent_info().piece_size, piece_length);
    ASSERT_EQ(state.torrent_info().files.size(), 2);
    EXPECT_EQ(state.torrent_info().files[0].path.string(), "file1.dat");
    EXPECT_EQ(state.torrent_info().files[1].path.string(), "file2.dat");

    // Verify tracker tiers (multiple tiers this time)
    ASSERT_EQ(state.tracker_tiers().size(), 3);
    ASSERT_EQ(state.tracker_tiers()[0].size(), 1);
    EXPECT_EQ(state.tracker_tiers()[0][0], "http://t1.com");
    ASSERT_EQ(state.tracker_tiers()[1].size(), 1);
    EXPECT_EQ(state.tracker_tiers()[1][0], "http://t2.com/announce");
    ASSERT_EQ(state.tracker_tiers()[2].size(), 1);
    EXPECT_EQ(state.tracker_tiers()[2][0], "udp://t3.com:6969");
}

TEST_F(SessionStateTest, SettersAndGetters) {
    std::string torrent_name = "setter_getter_test";
    uint64_t total_size = 30000;
    uint32_t piece_length = 10000;
    std::vector<std::string> trackers = {"http://t.com"};
    create_mock_torrent_file(torrent_name, total_size, piece_length, trackers);
    SessionState state(torrent_path, save_path);

    state.piece_status(0, PieceStatus::InProgress);
    state.piece_status(1, PieceStatus::Have);
    state.piece_status(2, PieceStatus::Skipped); // Assuming 3 pieces total for 30000 bytes, 10000 piece size

    EXPECT_EQ(state.piece_status(0), PieceStatus::InProgress);
    EXPECT_EQ(state.piece_status(1), PieceStatus::Have);
    EXPECT_EQ(state.piece_status(2), PieceStatus::Skipped);

    state.add_total_bytes_downloaded(500);
    state.add_total_bytes_downloaded(250);
    EXPECT_EQ(state.total_bytes_downloaded(), 750);

    state.add_total_bytes_uploaded(100);
    state.add_total_bytes_uploaded(300);
    EXPECT_EQ(state.total_bytes_uploaded(), 400);

    state.add_completed_pieces(1);
    state.add_completed_pieces(2);
    EXPECT_EQ(state.completed_pieces(), 3);
    state.completed_pieces(1); // Set directly, overriding the sum
    EXPECT_EQ(state.completed_pieces(), 1);

    state.is_download_complete(true);
    EXPECT_TRUE(state.is_download_complete());
    state.is_in_endgame_mode(true);
    EXPECT_TRUE(state.is_in_endgame_mode());
}

TEST_F(SessionStateTest, ProgressString) {
    std::string torrent_name = "progress_test";
    uint64_t total_size = 1000;
    uint32_t piece_length = 100; // 10 pieces total
    std::vector<std::string> trackers = {"http://t.com"};
    create_mock_torrent_file(torrent_name, total_size, piece_length, trackers);
    SessionState state(torrent_path, save_path);
    
    EXPECT_EQ(state.progress(), "0.00% (0/10)");

    state.add_completed_pieces(1);
    EXPECT_EQ(state.progress(), "10.00% (1/10)");

    state.add_completed_pieces(9); // Now 10 completed
    EXPECT_EQ(state.progress(), "100.00% (10/10)");
}

TEST_F(SessionStateTest, GetHaveBitfieldStr) {
    std::string torrent_name = "bitfield_test";
    uint64_t total_size = 1000;
    uint32_t piece_length = 100; // 10 pieces total (0-9)
    std::vector<std::string> trackers = {"http://t.com"};
    create_mock_torrent_file(torrent_name, total_size, piece_length, trackers);
    SessionState state(torrent_path, save_path);

    // Initially all needed (bitfield should be all zeros)
    // 10 pieces: byte 0 covers pieces 0-7, byte 1 covers 8-9 (remaining bits are zero)
    std::string empty_bitfield(2, '\0'); 
    EXPECT_EQ(state.get_have_bitfield_str(), empty_bitfield);

    state.piece_status(0, PieceStatus::Have); // 10000000 (piece 0 is 1st bit of 1st byte)
    // First byte: 0x80 (128)
    std::string bitfield_p0_have(2, '\0');
    bitfield_p0_have[0] = static_cast<char>(0x80); // 10000000
    EXPECT_EQ(state.get_have_bitfield_str(), bitfield_p0_have);

    state.piece_status(7, PieceStatus::Have); // 10000001 (piece 7 is 8th bit of 1st byte)
    // First byte: 0x81 (129)
    std::string bitfield_p0p7_have(2, '\0');
    bitfield_p0p7_have[0] = static_cast<char>(0x81); // 10000001
    EXPECT_EQ(state.get_have_bitfield_str(), bitfield_p0p7_have);

    state.piece_status(8, PieceStatus::Have); // Piece 8 is 1st bit of 2nd byte
    // Second byte: 0x80 (128)
    std::string bitfield_p0p7p8_have(2, '\0');
    bitfield_p0p7p8_have[0] = static_cast<char>(0x81); // 10000001
    bitfield_p0p7p8_have[1] = static_cast<char>(0x80); // 10000000
    EXPECT_EQ(state.get_have_bitfield_str(), bitfield_p0p7p8_have);

    state.piece_status(9, PieceStatus::Have); // Piece 9 is 2nd bit of 2nd byte
    // Second byte: 0xC0 (192)
    std::string bitfield_all_have(2, '\0');
    bitfield_all_have[0] = static_cast<char>(0x81); // 10000001
    bitfield_all_have[1] = static_cast<char>(0xC0); // 11000000
    EXPECT_EQ(state.get_have_bitfield_str(), bitfield_all_have);

    // Test a piece status of InProgress (should not appear in have bitfield)
    state.piece_status(1, PieceStatus::InProgress);
    EXPECT_EQ(state.get_have_bitfield_str(), bitfield_all_have); // Should remain same

    // Test a piece status of Skipped (should not appear in have bitfield)
    state.piece_status(1, PieceStatus::Skipped);
    EXPECT_EQ(state.get_have_bitfield_str(), bitfield_all_have); // Should remain same
}

TEST_F(SessionStateTest, ConstructorThrowsOnInvalidTorrentFile) {
    // Create an empty file, which is invalid bencode
    std::ofstream file(torrent_path);
    file.close();

    EXPECT_THROW(SessionState(torrent_path, save_path), std::runtime_error);

    // Create a file with invalid info hash size (e.g., non-multiple of 20 in "pieces" string)
    create_mock_torrent_file("invalid_hash_size", 100, 10, {"http://t.com"});
    // Manually corrupt the 'pieces' entry to an invalid length
    Dict info_dict;
    info_dict["length"] = Value(static_cast<Integer>(100));
    info_dict["name"] = Value(String("invalid_hash_size"));
    info_dict["piece length"] = Value(static_cast<Integer>(10));
    info_dict["pieces"] = Value(String("AAAA")); // 4 bytes, not a multiple of 20
    Dict metainfo_dict;
    metainfo_dict["announce"] = Value(String("http://t.com"));
    metainfo_dict["info"] = Value(info_dict);
    std::ofstream invalid_file(torrent_path, std::ios::binary);
    invalid_file.write(bytes_to_string(encode(Value(metainfo_dict))).data(), bytes_to_string(encode(Value(metainfo_dict))).size());
    invalid_file.close();
    
    // The MetaInfo loading might catch this, or SessionState's info_hash size check.
    // The current `MetaInfo::load_from_file` does not explicitly check `info.pieces.size() % 20 == 0`,
    // but the `SessionState` constructor checks `meta_info_.get_info_hash().size() != HASH_SIZE`.
    // Let's refine the mock to trigger the `SessionState` check more directly if possible.
    // For now, rely on `MetaInfo` loading errors for simplicity.
    EXPECT_THROW(SessionState(torrent_path, save_path), std::runtime_error); 
}
