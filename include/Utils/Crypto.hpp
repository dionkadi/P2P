#pragma once

#include <string>
#include <span>
#include "Utils/Logger.hpp"
#include "picosha2.h"
#include "Utils/sha1.hpp"
#include <fstream>
#include <ios>
#include <vector>

namespace Crypto {

inline std::string calculate_file_hash(const std::string& file_path) {
    std::ifstream file(file_path, std::ios::binary);
    if (!file.is_open()) {
        LOGERR("Failed to open file for hashing: {}", file_path);
        return "";
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

inline std::string calculate_data_hash(std::span<const char> data) {
    return picosha2::hash256_hex_string(data.begin(), data.end());
}

inline std::string calculate_string_hash(const std::string& str) {
    return calculate_data_hash(str);
}

inline std::string hex_to_bytes(const std::string &hex) {
    if (hex.length() % 2 != 0) {
        throw std::invalid_argument("Hex string must have an even number of characters");
    }
    std::string bytes;
    bytes.reserve(hex.length() / 2);
    for (size_t i = 0; i < hex.length(); i += 2) {
        try {
            std::string byteString = hex.substr(i, 2);
            char byte = static_cast<char>(std::stoi(byteString, nullptr, 16));
            bytes.push_back(byte);
        } catch (const std::exception&) {
            throw std::invalid_argument("Invalid hex character sequence");
        }
    }
    return bytes;
}

inline std::string bytes_to_hex(std::span<const char> data) {
    std::stringstream ss;
    ss << std::hex << std::setfill('0');
    for (const auto& byte : data) {
        ss << std::setw(2) << static_cast<unsigned>(static_cast<unsigned char>(byte));
    }
    return ss.str();
}

inline std::string calculate_sha1_hash_data(std::span<const char> data) {
    SHA1 checksum;
    checksum.update(std::string(data.begin(), data.end()));
    return hex_to_bytes(checksum.final());
}

inline std::string calculate_sha1_hash_file(const std::string& file_path) {
    std::ifstream file(file_path, std::ios::binary);
    if (!file.is_open()) {
        LOGERR("Failed to open file for SHA1 hashing: {}", file_path);
        return "";
    }
    
    SHA1 checksum;
    char buffer[4096];
    while (file) {
        file.read(buffer, sizeof(buffer));
        checksum.update(std::string(buffer, file.gcount()));
    }
 
    return hex_to_bytes(checksum.final());
}

}