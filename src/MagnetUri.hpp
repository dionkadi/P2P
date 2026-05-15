#pragma once

#include "Utils.hpp"

#include <algorithm>
#include <array>
#include <cctype>
#include <charconv>
#include <format>
#include <span>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

// BEP-9 / BEP-10: Magnet URI and metadata exchange

struct MagnetLink {
    InfoHash info_hash{};
    std::string display_name;
    std::vector<std::string> tracker_urls;
    std::vector<std::string> source_urls;
    bool parsed_{false};

    bool valid() const noexcept { return parsed_; }
};

inline std::string url_decode(const std::string& encoded) {
    std::string result;
    result.reserve(encoded.size());
    for (size_t i = 0; i < encoded.size(); ++i) {
        if (encoded[i] == '%' && i + 2 < encoded.size()) {
            unsigned int hex_val;
            auto [ptr, ec] = std::from_chars(
                encoded.data() + i + 1, encoded.data() + i + 3, hex_val, 16);
            if (ec == std::errc() && ptr == encoded.data() + i + 3) {
                result.push_back(static_cast<char>(hex_val));
                i += 2;
            } else {
                result.push_back(encoded[i]);
            }
        } else if (encoded[i] == '+') {
            result.push_back(' ');
        } else {
            result.push_back(encoded[i]);
        }
    }
    return result;
}

// Decode a hex string (40 chars -> 20 bytes)
inline InfoHash decode_hex_info_hash(std::string_view hex) {
    if (hex.size() != 40) {
        throw std::runtime_error("Invalid hex info hash length: expected 40, got " + std::to_string(hex.size()));
    }
    InfoHash hash{};
    for (size_t i = 0; i < 20; ++i) {
        unsigned int byte_val;
        auto [ptr, ec] = std::from_chars(hex.data() + i * 2, hex.data() + i * 2 + 2, byte_val, 16);
        if (ec != std::errc() || ptr != hex.data() + i * 2 + 2) {
            throw std::runtime_error("Invalid hex character in info hash");
        }
        hash[i] = static_cast<std::byte>(byte_val);
    }
    return hash;
}

// Base32 decoding per RFC 4648
static constexpr std::string_view BASE32_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";

inline int8_t char_to_base32(char c) {
    for (int i = 0; i < 32; ++i) {
        if (BASE32_ALPHABET[i] == c || std::tolower(BASE32_ALPHABET[i]) == std::tolower(c)) {
            return i;
        }
    }
    return -1;
}

inline InfoHash decode_base32_info_hash(std::string_view b32) {
    if (b32.size() != 32) {
        throw std::runtime_error("Invalid base32 info hash length: expected 32, got " + std::to_string(b32.size()));
    }

    InfoHash hash{};
    for (size_t group = 0; group < 4; ++group) {
        uint64_t buffer = 0;
        for (int j = 0; j < 8; ++j) {
            char c = b32[group * 8 + j];
            int8_t val = char_to_base32(c);
            if (val < 0) {
                throw std::runtime_error(std::format("Invalid base32 character '{}' at position {}", c, group * 8 + j));
            }
            buffer = (buffer << 5) | static_cast<uint64_t>(val);
        }
        for (int j = 0; j < 5; ++j) {
            hash[group * 5 + j] = static_cast<std::byte>((buffer >> (32 - j * 8)) & 0xFF);
        }
    }
    return hash;
}

// BEP-9: Magnet URI parsing
// magnet:?xt=urn:btih:<info_hash>&dn=<name>&tr=<tracker>&tr=<tracker>&xs=<source>&...
// info_hash can be hex (40 chars) or base32 (32 chars), with urn:btih: prefix
inline MagnetLink parse_magnet_uri(const std::string& uri) {
    MagnetLink link;

    if (uri.substr(0, 8) != "magnet:?") {
        throw std::runtime_error("Invalid magnet URI: missing 'magnet:?' prefix");
    }

    // Parse query parameters
    std::string query = uri.substr(8); // skip "magnet:?"
    std::stringstream ss(query);
    std::string param;

    while (std::getline(ss, param, '&')) {
        if (param.empty()) continue;

        auto eq_pos = param.find('=');
        if (eq_pos == std::string::npos) continue;

        std::string key = param.substr(0, eq_pos);
        std::string value = url_decode(param.substr(eq_pos + 1));

        if (key == "xt") {
            if (value.substr(0, 9) == "urn:btih:") {
                std::string hash_str = value.substr(9);
                if (hash_str.size() == 40) {
                    link.info_hash = decode_hex_info_hash(hash_str);
                    link.parsed_ = true;
                } else if (hash_str.size() == 32) {
                    link.info_hash = decode_base32_info_hash(hash_str);
                    link.parsed_ = true;
                } else {
                    LOGWARN("Magnet URI: unknown info hash encoding (length={})", hash_str.size());
                }
            }
        } else if (key == "dn") {
            link.display_name = value;
        } else if (key == "tr") {
            if (!value.empty()) {
                link.tracker_urls.push_back(value);
            }
        } else if (key == "xs") {
            if (!value.empty()) {
                link.source_urls.push_back(value);
            }
        }
    }

    if (!link.valid()) {
        throw std::runtime_error("Magnet URI: no valid info_hash found (xt=urn:btih:<hash>)");
    }

    return link;
}

// Serialize a MagnetLink back to a magnet URI (for roundtrip testing)
inline std::string to_magnet_uri(const MagnetLink& link) {
    std::string uri = "magnet:?xt=urn:btih:";
    // Encode info_hash as hex
    for (const auto& b : link.info_hash) {
        uri += std::format("{:02x}", static_cast<unsigned int>(b));
    }
    if (!link.display_name.empty()) {
        uri += "&dn=" + link.display_name;
    }
    for (const auto& tr : link.tracker_urls) {
        uri += "&tr=" + tr;
    }
    for (const auto& xs : link.source_urls) {
        uri += "&xs=" + xs;
    }
    return uri;
}
