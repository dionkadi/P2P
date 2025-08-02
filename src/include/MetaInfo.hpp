#pragma once

#include <cstdint>
#include <string>
#include <vector>

struct TorrentInfo {
    std::string file_name;
    uint64_t file_size;
    uint32_t piece_size;
    std::vector<std::string> piece_hashes;
};

class MetaInfo {
public:
    bool load_from_file(const std::string& file_path);
    static bool create_from_file(const std::string& file_path, const std::string& torrent_path, uint32_t piece_size = 262144);
    std::string get_info_hash() const;
    const TorrentInfo& get_torrent_info() const { return info_; }

private:
    TorrentInfo info_;
    std::string info_hash_;
};