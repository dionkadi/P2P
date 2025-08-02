#include "MetaInfo.hpp"
#include "Crypto.hpp"
#include "Logger.hpp"
#include "nlohmann/json.hpp"
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <ios>
#include <sstream>
#include <string>
#include <vector>

using json = nlohmann::json;

bool MetaInfo::load_from_file(const std::string& file_path) {
    std::ifstream f(file_path);
    if (!f) {
        LOGERR("Could not open file: {}", file_path);
        return false;
    }

    try {
        json data = json::parse(f);
        info_.file_name = data.at("file_name").get<std::string>();
        info_.file_size = data.at("file_size").get<uint64_t>();
        info_.piece_size = data.at("piece_size").get<uint32_t>();
        info_.piece_hashes = data.at("pieces").get<std::vector<std::string>>();

        info_hash_ = get_info_hash();
        return true;
    } catch (const json::exception& e) {
        LOGERR("Failed to parse .mitorrent file {}: {}", file_path, e.what());
        return false;
    }
}

std::string MetaInfo::get_info_hash() const {
    std::stringstream ss;
    for (const auto& hash : info_.piece_hashes) {
        ss << hash;
    }
    return Crypto::calculate_string_hash(ss.str());
}

bool MetaInfo::create_from_file(const std::string &file_path, const std::string &torrent_path, uint32_t piece_size) {
    std::ifstream source_file(file_path);
    if (!source_file) {
        LOGERR("Failed to open source file for creating torrent: {}", file_path);
        return false;
    }

    uint64_t file_size = std::filesystem::file_size(file_path);

    TorrentInfo temp_info{};
    temp_info.file_name = std::filesystem::path(file_path).filename().string();
    temp_info.file_size = file_size;
    temp_info.piece_size = piece_size;

    std::vector<char> buffer(piece_size);
    while (source_file) {
        source_file.read(buffer.data(), piece_size);
        std::streamsize bytes_read = source_file.gcount();
        if (bytes_read > 0) {
            std::string piece_hash = Crypto::calculate_data_hash({buffer.data(), static_cast<size_t>(bytes_read)});
            temp_info.piece_hashes.push_back(piece_hash);
        }
    }

    json torrent_json;
    torrent_json["file_name"] = temp_info.file_name;
    torrent_json["file_size"] = temp_info.file_size;
    torrent_json["piece_size"] = temp_info.piece_size;
    torrent_json["pieces"] = temp_info.piece_hashes;

    std::ofstream torrent_file(torrent_path);
    if (!torrent_file) {
        LOGERR("Failed to open {} for writing.", torrent_path);
        return false;
    }

    torrent_file << torrent_json.dump(4);
    LOGINFO("Successfully created torrent file at {}", torrent_path);
    return true;
}
