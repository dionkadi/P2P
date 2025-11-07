#pragma once

#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

struct FileInfo {
    std::filesystem::path path;
    uint64_t size;
};

struct TorrentInfo {
    std::string name;
    uint64_t total_size = 0;
    uint32_t piece_size = 0;
    std::string pieces;
    std::vector<FileInfo> files;
};

class MetaInfo {
public:
    bool load_from_file(const std::string& file_path, std::vector<std::vector<std::string>>& out_tracker_tiers);
    static bool create_from_file(const std::filesystem::path& source_path, const std::filesystem::path& torrent_path, const std::vector<std::string>& tracker_urls, uint32_t piece_size = 262144);
    
    const std::string& get_info_hash() const { return info_hash_bytes_; }
    const TorrentInfo& get_torrent_info() const { return info_; }

private:
    TorrentInfo info_;
    std::string info_hash_bytes_;
};