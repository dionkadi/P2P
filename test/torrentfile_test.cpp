#include "gtest/gtest.h"
#include "TorrentFile.hpp"
#include "Bencode.hpp"
#include "helper.hpp" // For string_to_bytes, bytes_to_string
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>
#include <algorithm>

// Fixture for torrent file tests to handle temporary files
class TorrentFileTest : public ::testing::Test {
protected:
    std::filesystem::path temp_dir;
    std::filesystem::path torrent_path;
    std::filesystem::path data_dir; // For multi-file torrents, this is the root content directory
    std::filesystem::path single_file_path; // For single-file torrents, this is the data file

    void SetUp() override {
        temp_dir = std::filesystem::temp_directory_path() / "torrentfile_test_temp";
        std::filesystem::create_directories(temp_dir);
        torrent_path = temp_dir / "test.torrent";
        data_dir = temp_dir / "data_content"; // Renamed to avoid confusion with torrent.name
        single_file_path = temp_dir / "single_file.txt";
    }

    void TearDown() override {
        std::filesystem::remove_all(temp_dir);
    }

    void create_data_file(const std::string& content, const std::filesystem::path& path) {
        std::filesystem::create_directories(path.parent_path());
        std::ofstream file(path, std::ios::binary);
        ASSERT_TRUE(file.is_open()) << "Failed to create data file: " << path;
        file << content;
        file.close();
    }

    void create_mock_torrent_file(const std::string& bencode_content) {
        std::ofstream file(torrent_path, std::ios::binary);
        ASSERT_TRUE(file.is_open()) << "Failed to create mock torrent file: " << torrent_path;
        file.write(bencode_content.data(), bencode_content.size());
        file.close();
    }

    // A more robust way to generate dummy SHA1 hashes for pieces
    std::string generate_dummy_sha1_data(size_t total_data_length, uint32_t piece_length) {
        std::string full_content(total_data_length, 'X'); // Arbitrary content
        std::string all_piece_hashes_str;
        size_t num_pieces = (total_data_length + piece_length - 1) / piece_length;

        for (size_t i = 0; i < num_pieces; ++i) {
            size_t start = i * piece_length;
            size_t len = std::min(static_cast<size_t>(piece_length), total_data_length - start);
            std::string piece_content = full_content.substr(start, len);
            std::vector<std::byte> hash_bytes = Crypto::calculate_sha1_hash_data(string_to_bytes(piece_content));
            all_piece_hashes_str.append(bytes_to_string(hash_bytes));
        }
        return all_piece_hashes_str;
    }
};

TEST_F(TorrentFileTest, LoadSingleFileTorrent) {
    std::string content = "This is a test file for torrenting. It needs to be long enough to span pieces or have realistic hashing.";
    create_data_file(content, single_file_path);

    uint32_t piece_length = 32; // Small piece size for exact hashing test
    std::string expected_piece_hashes = generate_dummy_sha1_data(content.length(), piece_length);
    
    Dict info_dict;
    info_dict["length"] = Value(static_cast<Integer>(content.length()));
    info_dict["name"] = Value(single_file_path.filename().string());
    info_dict["piece length"] = Value(static_cast<Integer>(piece_length));
    info_dict["pieces"] = Value(expected_piece_hashes);

    Dict metainfo_dict;
    metainfo_dict["announce"] = Value(String("http://testtracker.com/announce"));
    metainfo_dict["info"] = Value(info_dict);
    
    create_mock_torrent_file(bytes_to_string(encode(Value(metainfo_dict))));

    MetaInfo meta_info;
    std::vector<std::vector<std::string>> tracker_tiers;
    ASSERT_TRUE(meta_info.load_from_file(torrent_path.string(), tracker_tiers));

    // Verify tracker info
    ASSERT_EQ(tracker_tiers.size(), 1);
    ASSERT_EQ(tracker_tiers[0].size(), 1);
    EXPECT_EQ(tracker_tiers[0][0], "http://testtracker.com/announce");

    // Verify torrent info
    const TorrentInfo& info = meta_info.get_torrent_info();
    EXPECT_EQ(info.name, single_file_path.filename().string());
    EXPECT_EQ(info.total_size, content.length());
    EXPECT_EQ(info.piece_size, piece_length);
    EXPECT_EQ(bytes_to_string(info.pieces), expected_piece_hashes);
    ASSERT_EQ(info.files.size(), 1);
    EXPECT_EQ(info.files[0].path.string(), single_file_path.filename().string());
    EXPECT_EQ(info.files[0].size, content.length());

    // Verify info hash (this depends on the exact bencoding of info_dict)
    std::vector<std::byte> expected_info_bencoded = encode(Value(info_dict));
    std::vector<std::byte> expected_info_hash = Crypto::calculate_sha1_hash_data(expected_info_bencoded);
    EXPECT_EQ(meta_info.get_info_hash(), expected_info_hash);
}

TEST_F(TorrentFileTest, LoadMultiFileTorrent) {
    create_data_file("file1 content", data_dir / "file1.txt"); // 13 bytes
    create_data_file("subfile content", data_dir / "subdir" / "subfile.txt"); // 15 bytes
    uint64_t total_data_size = 13 + 15; // 28 bytes

    uint32_t piece_length = 10; // Small piece size for hashing demo
    std::string expected_piece_hashes = generate_dummy_sha1_data(total_data_size, piece_length);
    
    List files_list;
    files_list.push_back(Value(Dict{
        {"length", Value(static_cast<Integer>(13))},
        {"path", Value(List{Value(String("file1.txt"))})}
    }));
    files_list.push_back(Value(Dict{
        {"length", Value(static_cast<Integer>(15))},
        {"path", Value(List{Value(String("subdir")), Value(String("subfile.txt"))})}
    }));

    Dict info_dict;
    info_dict["files"] = Value(files_list);
    info_dict["name"] = Value(data_dir.filename().string());
    info_dict["piece length"] = Value(static_cast<Integer>(piece_length));
    info_dict["pieces"] = Value(expected_piece_hashes);

    List announce_list_tiers;
    announce_list_tiers.push_back(Value(List{Value(String("http://t1.com/a"))}));
    announce_list_tiers.push_back(Value(List{Value(String("http://t2.com/b")), Value(String("http://t3.com/c"))}));

    Dict metainfo_dict;
    metainfo_dict["announce-list"] = Value(announce_list_tiers);
    metainfo_dict["info"] = Value(info_dict);
    
    create_mock_torrent_file(bytes_to_string(encode(Value(metainfo_dict))));

    MetaInfo meta_info;
    std::vector<std::vector<std::string>> tracker_tiers;
    ASSERT_TRUE(meta_info.load_from_file(torrent_path.string(), tracker_tiers));

    // Verify tracker info
    ASSERT_EQ(tracker_tiers.size(), 2);
    ASSERT_EQ(tracker_tiers[0].size(), 1);
    EXPECT_EQ(tracker_tiers[0][0], "http://t1.com/a");
    ASSERT_EQ(tracker_tiers[1].size(), 2);
    EXPECT_EQ(tracker_tiers[1][0], "http://t2.com/b");
    EXPECT_EQ(tracker_tiers[1][1], "http://t3.com/c");

    // Verify torrent info
    const TorrentInfo& info = meta_info.get_torrent_info();
    EXPECT_EQ(info.name, data_dir.filename().string());
    EXPECT_EQ(info.total_size, total_data_size);
    EXPECT_EQ(info.piece_size, piece_length);
    EXPECT_EQ(bytes_to_string(info.pieces), expected_piece_hashes);
    ASSERT_EQ(info.files.size(), 2);
    EXPECT_EQ(info.files[0].path.string(), "file1.txt");
    EXPECT_EQ(info.files[0].size, 13);
    EXPECT_EQ(info.files[1].path.string(), "subdir/subfile.txt");
    EXPECT_EQ(info.files[1].size, 15);
}

TEST_F(TorrentFileTest, CreateSingleFileTorrent) {
    std::string content = "This is a single file for creation. It should have its hash calculated correctly.";
    create_data_file(content, single_file_path);

    std::vector<std::string> tracker_urls = {"http://create.tracker.com/announce", "udp://create.tracker.com:6969"};
    uint32_t piece_size = 32; 

    ASSERT_TRUE(MetaInfo::create_from_file(single_file_path, torrent_path, tracker_urls, piece_size));
    
    MetaInfo created_meta_info;
    std::vector<std::vector<std::string>> loaded_tracker_tiers;
    ASSERT_TRUE(created_meta_info.load_from_file(torrent_path.string(), loaded_tracker_tiers));

    const TorrentInfo& info = created_meta_info.get_torrent_info();
    EXPECT_EQ(info.name, single_file_path.filename().string());
    EXPECT_EQ(info.total_size, content.length());
    EXPECT_EQ(info.piece_size, piece_size);
    ASSERT_EQ(info.files.size(), 1);
    EXPECT_EQ(info.files[0].path.string(), single_file_path.filename().string());
    EXPECT_EQ(info.files[0].size, content.length());

    // Calculate expected piece hashes and compare
    std::vector<std::byte> expected_all_piece_hashes;
    size_t num_pieces = (content.length() + piece_size - 1) / piece_size;
    for (size_t i = 0; i < num_pieces; ++i) {
        size_t start = i * piece_size;
        size_t len = std::min(static_cast<size_t>(piece_size), content.length() - start);
        std::string piece_content_str = content.substr(start, len);
        std::vector<std::byte> piece_hash = Crypto::calculate_sha1_hash_data(string_to_bytes(piece_content_str));
        expected_all_piece_hashes.insert(expected_all_piece_hashes.end(), piece_hash.begin(), piece_hash.end());
    }
    EXPECT_EQ(info.pieces, expected_all_piece_hashes);

    ASSERT_EQ(loaded_tracker_tiers.size(), 2);
    ASSERT_EQ(loaded_tracker_tiers[0].size(), 1);
    EXPECT_EQ(loaded_tracker_tiers[0][0], tracker_urls[0]);
    ASSERT_EQ(loaded_tracker_tiers[1].size(), 1); // Each URL from announce-list becomes its own tier
    EXPECT_EQ(loaded_tracker_tiers[1][0], tracker_urls[1]);
}

TEST_F(TorrentFileTest, CreateMultiFileTorrent) {
    create_data_file("multi_file_content_1", data_dir / "fileA.txt");
    create_data_file("multi_file_content_2_longer", data_dir / "subfolder" / "fileB.txt"); // 25 bytes
    auto size1 = std::filesystem::file_size(data_dir / "fileA.txt");
    auto size2 = std::filesystem::file_size(data_dir / "subfolder" / "fileB.txt");
    uint64_t total_expected_size = size1 + size2;

    std::vector<std::string> tracker_urls = {"http://create.multi.tracker.com/announce"};
    uint32_t piece_size = 10; // Small piece size for hashing demo

    ASSERT_TRUE(MetaInfo::create_from_file(data_dir, torrent_path, tracker_urls, piece_size));

    MetaInfo created_meta_info;
    std::vector<std::vector<std::string>> loaded_tracker_tiers;
    ASSERT_TRUE(created_meta_info.load_from_file(torrent_path.string(), loaded_tracker_tiers));

    const TorrentInfo& info = created_meta_info.get_torrent_info();
    EXPECT_EQ(info.name, data_dir.filename().string());
    EXPECT_EQ(info.total_size, total_expected_size);
    EXPECT_EQ(info.piece_size, piece_size);
    ASSERT_EQ(info.files.size(), 2);
    EXPECT_EQ(info.files[0].path.string(), "fileA.txt");
    EXPECT_EQ(info.files[0].size, size1);
    EXPECT_EQ(info.files[1].path.string(), "subfolder/fileB.txt");
    EXPECT_EQ(info.files[1].size, size2);

    // Manual hash verification for a small multi-file case:
    std::string full_content = "multi_file_content_1multi_file_content_2_longer"; // 43 bytes
    std::vector<std::byte> expected_all_piece_hashes;
    size_t num_pieces = (full_content.length() + piece_size - 1) / piece_size; // 5 pieces (10, 10, 10, 10, 3)
    for (size_t i = 0; i < num_pieces; ++i) {
        size_t start = i * piece_size;
        size_t len = std::min(static_cast<size_t>(piece_size), full_content.length() - start);
        std::string piece_content_str = full_content.substr(start, len);
        std::vector<std::byte> piece_hash = Crypto::calculate_sha1_hash_data(string_to_bytes(piece_content_str));
        expected_all_piece_hashes.insert(expected_all_piece_hashes.end(), piece_hash.begin(), piece_hash.end());
    }
    EXPECT_EQ(info.pieces, expected_all_piece_hashes);
}

TEST_F(TorrentFileTest, LoadTorrentNonExistent) {
    std::filesystem::remove(torrent_path); // Ensure file doesn't exist
    MetaInfo meta_info;
    std::vector<std::vector<std::string>> tracker_tiers;
    EXPECT_FALSE(meta_info.load_from_file(torrent_path.string(), tracker_tiers));
}

TEST_F(TorrentFileTest, LoadTorrentInvalidBencode) {
    create_mock_torrent_file("d3:fooi123"); // Incomplete bencode
    MetaInfo meta_info;
    std::vector<std::vector<std::string>> tracker_tiers;
    EXPECT_FALSE(meta_info.load_from_file(torrent_path.string(), tracker_tiers));
}

TEST_F(TorrentFileTest, LoadTorrentMissingAnnounce) {
    Dict info_dict;
    info_dict["length"] = Value(static_cast<Integer>(100));
    info_dict["name"] = Value(String("dummy"));
    info_dict["piece length"] = Value(static_cast<Integer>(16384));
    info_dict["pieces"] = Value(generate_dummy_sha1_data(100, 16384));
    Dict metainfo_dict;
    metainfo_dict["info"] = Value(info_dict); // Missing announce/announce-list
    create_mock_torrent_file(bytes_to_string(encode(Value(metainfo_dict))));

    MetaInfo meta_info;
    std::vector<std::vector<std::string>> tracker_tiers;
    EXPECT_FALSE(meta_info.load_from_file(torrent_path.string(), tracker_tiers));
}

TEST_F(TorrentFileTest, LoadTorrentMissingInfo) {
    Dict metainfo_dict;
    metainfo_dict["announce"] = Value(String("http://testtracker.com/announce"));
    // Missing info dictionary
    create_mock_torrent_file(bytes_to_string(encode(Value(metainfo_dict))));

    MetaInfo meta_info;
    std::vector<std::vector<std::string>> tracker_tiers;
    EXPECT_FALSE(meta_info.load_from_file(torrent_path.string(), tracker_tiers));
}
