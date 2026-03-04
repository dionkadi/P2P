#include "gtest/gtest.h"
#include "Utils.hpp"
#include <vector>
#include <cstdint>
#include <string_view>
#include <stdexcept>
#include <cstring> // For std::memcpy

// POD struct for testing
struct TestPOD {
    uint32_t a;
    int16_t b;
    char c;
};

TEST(BufferWriterTest, WritePOD) {
    std::vector<std::byte> buffer;
    BufferWriter writer(buffer);

    TestPOD data = {0xDEADBEEF, -1234, 'X'};
    writer.write(data);

    ASSERT_EQ(buffer.size(), sizeof(TestPOD));

    // Verify raw bytes (endianness dependent, but consistent for testing)
    const std::byte* expected_bytes = reinterpret_cast<const std::byte*>(&data);
    for (size_t i = 0; i < sizeof(TestPOD); ++i) {
        EXPECT_EQ(buffer[i], expected_bytes[i]);
    }

    uint8_t val8 = 0xFF;
    writer.write(val8);
    ASSERT_EQ(buffer.size(), sizeof(TestPOD) + sizeof(uint8_t));
    EXPECT_EQ(buffer[sizeof(TestPOD)], static_cast<std::byte>(0xFF));
}

TEST(BufferWriterTest, WriteRawStringView) {
    std::vector<std::byte> buffer;
    BufferWriter writer(buffer);

    std::string_view sv = "Hello, World!";
    writer.write_raw(sv);

    ASSERT_EQ(buffer.size(), sv.length());
    for (size_t i = 0; i < sv.length(); ++i) {
        EXPECT_EQ(buffer[i], static_cast<std::byte>(sv[i]));
    }
}

TEST(BufferWriterTest, WriteBytes) {
    std::vector<std::byte> buffer;
    BufferWriter writer(buffer);

    std::vector<std::byte> source_bytes = {
        static_cast<std::byte>(0x01), static_cast<std::byte>(0x02), 
        static_cast<std::byte>(0x03), static_cast<std::byte>(0x04)
    };
    writer.write_bytes(source_bytes);

    ASSERT_EQ(buffer.size(), source_bytes.size());
    for (size_t i = 0; i < source_bytes.size(); ++i) {
        EXPECT_EQ(buffer[i], source_bytes[i]);
    }
}

TEST(BufferWriterTest, ChainedWrites) {
    std::vector<std::byte> buffer;
    BufferWriter writer(buffer);

    writer.write(static_cast<uint8_t>(0xAA));
    writer.write_raw("BB");
    writer.write(static_cast<uint16_t>(0xCCDD));

    ASSERT_EQ(buffer.size(), 1 + 2 + 2); // 1 byte + 2 chars + 2 bytes
    EXPECT_EQ(buffer[0], static_cast<std::byte>(0xAA));
    EXPECT_EQ(buffer[1], static_cast<std::byte>('B'));
    EXPECT_EQ(buffer[2], static_cast<std::byte>('B'));
    // Endianness of 0xCCDD depends on system, but should be correctly written.
    // We expect it to be written exactly as the memory representation of the uint16_t.
    uint16_t expected_u16 = 0xCCDD;
    const std::byte* expected_u16_bytes = reinterpret_cast<const std::byte*>(&expected_u16);
    EXPECT_EQ(buffer[3], expected_u16_bytes[0]);
    EXPECT_EQ(buffer[4], expected_u16_bytes[1]);
}

TEST(BufferReaderTest, ReadPOD) {
    TestPOD original_data = {0x11223344, 5678, 'Z'};
    std::vector<std::byte> buffer(sizeof(TestPOD));
    std::memcpy(buffer.data(), &original_data, sizeof(TestPOD));

    BufferReader reader(buffer);
    TestPOD read_data = reader.read<TestPOD>();

    EXPECT_EQ(read_data.a, original_data.a);
    EXPECT_EQ(read_data.b, original_data.b);
    EXPECT_EQ(read_data.c, original_data.c);
    EXPECT_EQ(reader.remaining(), 0);

    uint8_t val8 = 0x55;
    std::vector<std::byte> buffer8(1);
    std::memcpy(buffer8.data(), &val8, 1);
    BufferReader reader8(buffer8);
    uint8_t read8 = reader8.read<uint8_t>();
    EXPECT_EQ(read8, val8);
}

TEST(BufferReaderTest, ReadBytes) {
    std::vector<std::byte> buffer = {
        static_cast<std::byte>(0xAA), static_cast<std::byte>(0xBB), 
        static_cast<std::byte>(0xCC), static_cast<std::byte>(0xDD)
    };
    BufferReader reader(buffer);

    std::span<const std::byte> read_span = reader.read_bytes(2);
    ASSERT_EQ(read_span.size(), 2);
    EXPECT_EQ(read_span[0], static_cast<std::byte>(0xAA));
    EXPECT_EQ(read_span[1], static_cast<std::byte>(0xBB));
    EXPECT_EQ(reader.remaining(), 2);

    read_span = reader.read_all();
    ASSERT_EQ(read_span.size(), 2);
    EXPECT_EQ(read_span[0], static_cast<std::byte>(0xCC));
    EXPECT_EQ(read_span[1], static_cast<std::byte>(0xDD));
    EXPECT_EQ(reader.remaining(), 0);
}

TEST(BufferReaderTest, ReadTooMuch) {
    std::vector<std::byte> buffer = {static_cast<std::byte>(0x01)}; // Only 1 byte
    BufferReader reader(buffer);

    // Try to read a larger POD type (e.g., uint32_t)
    EXPECT_THROW(reader.read<uint32_t>(), std::runtime_error);

    // Try to read more bytes than available
    EXPECT_THROW(reader.read_bytes(2), std::runtime_error);
}

TEST(BufferReaderTest, EmptyBuffer) {
    std::vector<std::byte> buffer;
    BufferReader reader(buffer);

    EXPECT_EQ(reader.remaining(), 0);
    EXPECT_THROW(reader.read<uint8_t>(), std::runtime_error);
    EXPECT_THROW(reader.read_bytes(1), std::runtime_error);
    
    // Reading all from an empty buffer should return an empty span, not throw
    std::span<const std::byte> empty_span = reader.read_all();
    EXPECT_TRUE(empty_span.empty());
}
