#pragma once

#include <string>
#include <span>

namespace Crypto {

    std::string calculate_file_hash(const std::string& file_path);
    std::string calculate_data_hash(std::span<const char> data);
    std::string calculate_string_hash(const std::string& str);

    std::string hex_to_bytes(const std::string& hex);
    std::string bytes_to_hex(std::span<const char> data);
}