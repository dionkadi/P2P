#pragma once

#include <array>
#include <cstddef>
#include <stdexcept>
#include <string>
#include <span>
#include "picosha2.h"
#include "Utils/sha1.hpp"
#include <fstream>
#include <ios>
#include <vector>

namespace Crypto {

inline std::string calculate_file_hash(const std::string& file_path) {
    std::ifstream file(file_path, std::ios::binary);
    if (!file.is_open()) {
        throw std::runtime_error(std::format("Failed to open file for hashing: {}", file_path));
    }

    picosha2::hash256_one_by_one hasher;
    std::vector<char> buffer(4096);

    while (file) {
        file.read(buffer.data(), buffer.size());
        std::streamsize bytes_read = file.gcount();
        if (bytes_read > 0) {
            hasher.process(buffer.begin(), buffer.begin() + bytes_read);
        }
    }

    hasher.finish();

    return picosha2::get_hash_hex_string(hasher);
}

inline std::string calculate_data_hash(std::span<const std::byte> data) {
    auto char_data_begin = reinterpret_cast<const unsigned char*>(data.data());
    auto char_data_end = char_data_begin + data.size();
    return picosha2::hash256_hex_string(char_data_begin, char_data_end);
}

inline std::string calculate_string_hash(const std::string& str) {
    return calculate_data_hash({reinterpret_cast<const std::byte*>(str.data()), str.size()});
}

inline std::vector<std::byte> hex_to_bytes(const std::string &hex) {
    if (hex.length() % 2 != 0) {
        throw std::invalid_argument("Hex string must have an even number of characters");
    }

    std::vector<std::byte> bytes;
    bytes.reserve(hex.length() / 2);
    for (size_t i = 0; i < hex.length(); i += 2) {
        try {
            std::string byteString = hex.substr(i, 2);
            bytes.push_back(static_cast<std::byte>(std::stoi(byteString, nullptr, 16)));
        } catch (const std::exception&) {
            throw std::invalid_argument("Invalid hex character sequence");
        }
    }
    return bytes;
}

inline std::string bytes_to_hex(std::span<const std::byte> data) {
    std::stringstream ss;
    ss << std::hex << std::setfill('0');
    for (const auto& byte : data) {
        ss << std::setw(2) << static_cast<unsigned>(byte);
    }
    return ss.str();
}

inline std::vector<std::byte> calculate_sha1_hash_data(std::span<const std::byte> data) {
    SHA1 checksum;
    checksum.update(std::string(reinterpret_cast<const char*>(data.data()), data.size()));
    return hex_to_bytes(checksum.final());
}

inline std::vector<std::byte> calculate_sha1_hash_file(const std::string& file_path) {
    std::ifstream file(file_path, std::ios::binary);
    if (!file.is_open()) {
        throw std::runtime_error(std::format("Failed to open file for SHA1 hashing: {}", file_path));
    }
    
    SHA1 checksum;
    std::array<std::byte, 4096> buffer;
    while (file) {
        file.read(reinterpret_cast<char*>(buffer.data()), buffer.size());
        checksum.update(std::string(reinterpret_cast<const char*>(buffer.data()), file.gcount()));
    }
 
    return hex_to_bytes(checksum.final());
}

}