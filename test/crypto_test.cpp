#include "helper.hpp"

TEST(CryptoTest, HexToBytesValid) {
    std::string hex_str = "010a2fbcde";
    std::vector<std::byte> expected_bytes = {
        static_cast<std::byte>(0x01), static_cast<std::byte>(0x0a),
        static_cast<std::byte>(0x2f), static_cast<std::byte>(0xbc),
        static_cast<std::byte>(0xde)
    };
    std::vector<std::byte> actual_bytes = Crypto::hex_to_bytes(hex_str);
    ASSERT_EQ(actual_bytes, expected_bytes);

    // Empty string
    EXPECT_TRUE(Crypto::hex_to_bytes("").empty());
    // Single byte
    EXPECT_EQ(Crypto::hex_to_bytes("00"), std::vector<std::byte>{std::byte{0}});
}

TEST(CryptoTest, HexToBytesInvalid) {
    // Odd length
    EXPECT_THROW(Crypto::hex_to_bytes("012"), std::invalid_argument);
    // Invalid characters
    EXPECT_THROW(Crypto::hex_to_bytes("012G"), std::invalid_argument);
    EXPECT_THROW(Crypto::hex_to_bytes("01 23"), std::invalid_argument);
}

TEST(CryptoTest, BytesToHex) {
    std::vector<std::byte> bytes = {
        static_cast<std::byte>(0x01), static_cast<std::byte>(0x0a),
        static_cast<std::byte>(0x2f), static_cast<std::byte>(0xbc),
        static_cast<std::byte>(0xde)
    };
    std::string expected_hex = "010a2fbcde";
    std::string actual_hex = Crypto::bytes_to_hex(bytes);
    EXPECT_EQ(actual_hex, expected_hex);

    // Empty bytes
    EXPECT_EQ(Crypto::bytes_to_hex({}), "");
    // Single byte
    EXPECT_EQ(Crypto::bytes_to_hex(std::vector<std::byte>{std::byte{0}}), "00");
}

TEST(CryptoTest, CalculateSha256DataHash) {
    // Test with known SHA256 hash for "hello world"
    std::string data_str = "hello world";
    std::span<const std::byte> data_span(reinterpret_cast<const std::byte*>(data_str.data()), data_str.size());
    std::string expected_hash = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9";
    EXPECT_EQ(Crypto::calculate_data_hash(data_span), expected_hash);

    // Empty data
    EXPECT_EQ(Crypto::calculate_data_hash({}), "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
}

TEST(CryptoTest, CalculateSha256StringHash) {
    std::string str = "test string";
    std::string expected_hash = "d5579c46dfcc7f18207013e65b44e4cb4e2c2298f4ac457ba8f82743f31e930b";
    EXPECT_EQ(Crypto::calculate_string_hash(str), expected_hash);

    // Empty string
    EXPECT_EQ(Crypto::calculate_string_hash(""), "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
}

TEST(CryptoTest, CalculateSha1DataHash) {
    // Test with known SHA1 hash for "test data"
    std::string data_str = "test data";
    std::span<const std::byte> data_span(reinterpret_cast<const std::byte*>(data_str.data()), data_str.size());
    std::vector<std::byte> expected_hash = Crypto::hex_to_bytes("f48dd853820860816c75d54d0f584dc863327a7c");
    EXPECT_EQ(Crypto::calculate_sha1_hash_data(data_span), expected_hash);

    // Empty data
    std::vector<std::byte> empty_hash = Crypto::hex_to_bytes("da39a3ee5e6b4b0d3255bfef95601890afd80709");
    EXPECT_EQ(Crypto::calculate_sha1_hash_data({}), empty_hash);
}

// Fixture for file-related tests to handle temporary files
class CryptoFileTest : public ::testing::Test {
protected:
    std::filesystem::path temp_dir;
    std::filesystem::path test_file_path;

    void SetUp() override {
        temp_dir = std::filesystem::temp_directory_path() / "crypto_test_temp";
        std::filesystem::create_directories(temp_dir);
        test_file_path = temp_dir / "test_file.txt";
    }

    void TearDown() override {
        std::filesystem::remove_all(temp_dir);
    }

    void create_test_file(const std::string& content) {
        std::ofstream file(test_file_path, std::ios::binary);
        ASSERT_TRUE(file.is_open()) << "Failed to open test file: " << test_file_path;
        file << content;
        file.close();
    }
};

TEST_F(CryptoFileTest, CalculateSha256FileHash) {
    std::string content = "This is some file content for SHA256 hashing.";
    create_test_file(content);

    // Known SHA256 hash for the content above
    std::string expected_hash = "e75afa09c8577034c6ef39679cca0d28f60c510e77a9e3f1f4a2934acd5f2fcb";
    EXPECT_EQ(Crypto::calculate_file_hash(test_file_path.string()), expected_hash);

    // Test with an empty file
    create_test_file("");
    EXPECT_EQ(Crypto::calculate_file_hash(test_file_path.string()), "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
}

TEST_F(CryptoFileTest, CalculateSha256FileHashNonExistent) {
    std::filesystem::remove(test_file_path); // Ensure file doesn't exist
    EXPECT_THROW(Crypto::calculate_file_hash(test_file_path.string()), std::runtime_error);
}

TEST_F(CryptoFileTest, CalculateSha1FileHash) {
    std::string content = "Another file content for SHA1.";
    create_test_file(content);

    // Known SHA1 hash for the content above
    std::vector<std::byte> expected_hash = Crypto::hex_to_bytes("f62b4c0b6cf1e619827552431ff86e910b9d2040");
    EXPECT_EQ(Crypto::calculate_sha1_hash_file(test_file_path.string()), expected_hash);

    // Test with an empty file
    create_test_file("");
    std::vector<std::byte> empty_hash = Crypto::hex_to_bytes("da39a3ee5e6b4b0d3255bfef95601890afd80709");
    EXPECT_EQ(Crypto::calculate_sha1_hash_file(test_file_path.string()), empty_hash);
}

TEST_F(CryptoFileTest, CalculateSha1FileHashNonExistent) {
    std::filesystem::remove(test_file_path); // Ensure file doesn't exist
    EXPECT_THROW(Crypto::calculate_sha1_hash_file(test_file_path.string()), std::runtime_error);
}
