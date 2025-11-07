#include "Protocols/MetaInfo.hpp"
#include "Utils/Crypto.hpp"
#include "Utils/Logger.hpp"
#include "Utils/Bencode.hpp"

#include <fstream>
#include <memory>
#include <string>
#include <variant>
#include <vector>

void gather_files(const std::filesystem::path& base_path, const std::filesystem::path& current_path, std::vector<FileInfo>& files, uint64_t& total_size) {
    for (const auto& entry : std::filesystem::directory_iterator(current_path)) {
        auto relative_path = std::filesystem::relative(entry.path(), base_path);
        if (entry.is_directory()) {
            gather_files(base_path, entry.path(), files, total_size);
        } else if (entry.is_regular_file()) {
            uint64_t file_size = std::filesystem::file_size(entry.path());
            files.push_back({relative_path, file_size});
            total_size += file_size;
        }
    }
}

bool MetaInfo::load_from_file(const std::string& file_path, std::vector<std::vector<std::string>>& out_tracker_tiers) {
    std::ifstream f(file_path);
    if (!f) {
        LOGERR("Could not open torrent file: {}", file_path);
        return false;
    }

    std::vector<char> bencoded_data(std::istreambuf_iterator<char>(f), {});

    try {
        Value metainfo_val = decode({bencoded_data.data(), bencoded_data.size()});
        const auto *metainfo_variant_ptr = &metainfo_val.get_variant();

        const auto *metainfo_dict_ptr = std::get_if<std::unique_ptr<Dict>>(metainfo_variant_ptr);
        if (!metainfo_dict_ptr || !(*metainfo_dict_ptr)) {
            throw std::runtime_error("Torrent file is not a dictionary.");
        }

        const Dict& metainfo_dict = **metainfo_dict_ptr;

        out_tracker_tiers.clear();
        if (metainfo_dict.count("announce-list")) {
            const List& announce_list = *std::get<std::unique_ptr<List>>(metainfo_dict.at("announce-list").get_variant());
            for (const auto& tier_val : announce_list) {
                const List& tier_list = *std::get<std::unique_ptr<List>>(tier_val.get_variant());
                std::vector<std::string> current_tier;
                for (const auto& url_val : tier_list) {
                    current_tier.push_back(std::get<String>(url_val.get_variant()));
                }
                if (!current_tier.empty()) {
                    out_tracker_tiers.push_back(std::move(current_tier));
                }
            }
        }

        if (out_tracker_tiers.empty() && metainfo_dict.count("announce")) {
            const auto& announce_val = metainfo_dict.at("announce");
            const String *announce_str = std::get_if<String>(&announce_val.get_variant());
            if (!announce_str) {
                throw std::runtime_error("Missing or invalid 'announce' key.");
            }
            out_tracker_tiers.push_back({*announce_str});
        }

        if (out_tracker_tiers.empty()) {
            throw std::runtime_error("No valid 'announce' or 'announce-list' trackers found in torrent file.");
        }

        const auto& info_val = metainfo_dict.at("info");
        const auto* info_dict_ptr = std::get_if<std::unique_ptr<Dict>>(&info_val.get_variant());
        if (!info_dict_ptr || !(*info_dict_ptr)) {
            throw std::runtime_error("Missing or invalid 'info' dictionary.");
        }

        const auto& info_dict = **info_dict_ptr;

        std::vector<char> info_bencoded = encode(Value(info_dict));
        info_hash_bytes_ = Crypto::calculate_sha1_hash_data({info_bencoded.data(), info_bencoded.size()});

        info_.name = std::get<String>(info_dict.at("name").get_variant());
        info_.piece_size = std::get<Integer>(info_dict.at("piece length").get_variant());
        info_.pieces = std::get<String>(info_dict.at("pieces").get_variant());

        if (info_dict.count("length")) {
            info_.total_size = std::get<Integer>(info_dict.at("length").get_variant());
            info_.files.push_back({std::filesystem::path(info_.name), info_.total_size});
        } else {
            const List* file_list = std::get_if<std::unique_ptr<List>>(&info_dict.at("files").get_variant())->get();
            for (const auto& file_val : *file_list) {
                const Dict* file_dict = std::get_if<std::unique_ptr<Dict>>(&file_val.get_variant())->get();
                uint64_t length = std::get<Integer>(file_dict->at("length").get_variant());
                const List* path_list = std::get_if<std::unique_ptr<List>>(&file_dict->at("path").get_variant())->get();

                std::filesystem::path file_path;
                for (const auto& part_val : *path_list) {
                    file_path /= std::get<String>(part_val.get_variant());
                }

                info_.files.push_back({file_path, length});
                info_.total_size += length;
            }
        }

        return true;

    } catch (const std::exception& e) {
        LOGERR("Failed to parse .torrent file {}: {}", file_path, e.what());
        return false;
    }
}


bool MetaInfo::create_from_file(const std::filesystem::path& source_path, const std::filesystem::path& torrent_path, const std::vector<std::string>& tracker_urls, uint32_t piece_size) {
    if (!std::filesystem::exists(source_path)) {
        LOGCRITICAL("Source path for torrent creation does not exist: {}", source_path.string());
        return false;
    }
    
    Dict info_dict;
    TorrentInfo temp_info;
    temp_info.piece_size = piece_size;
    temp_info.name = source_path.filename().string();

    bool is_single_file = !std::filesystem::is_directory(source_path);

    if (is_single_file) {
        uint64_t file_size = std::filesystem::file_size(source_path);
        temp_info.files.push_back({source_path.filename(), file_size});
        temp_info.total_size = file_size;
        info_dict["length"] = Value(static_cast<Integer>(file_size));
    } else {
        List file_list;
        uint64_t total_size = 0;
        gather_files(source_path, source_path, temp_info.files, total_size);
        temp_info.total_size = total_size;

        for (const auto& file_info : temp_info.files) {
            List path_list;
            for (const auto& part : file_info.path) {
                path_list.push_back(Value(part.string()));
            }
            file_list.push_back(Value(Dict{
                {"length", Value(static_cast<Integer>(file_info.size))},
                {"path", Value(path_list)}
            }));
        }

        info_dict["files"] = Value(file_list);
    }

    info_dict["name"] = Value(temp_info.name);
    info_dict["piece length"] = Value(static_cast<Integer>(piece_size));

    std::string all_piece_hashes;
    std::vector<char> piece_buffer(piece_size);
    uint32_t buffer_fill = 0;

    for (const auto& file_info : temp_info.files) {
        std::filesystem::path current_file_path = is_single_file ? source_path : source_path / file_info.path;
        std::ifstream file(current_file_path, std::ios::binary);
        if (!file) {
            LOGCRITICAL("Could not open file {} for hashing.", current_file_path.string());
            return false;
        }

        while (file) {
            file.read(piece_buffer.data() + buffer_fill, piece_size - buffer_fill);
            buffer_fill += file.gcount();
            if (buffer_fill == piece_size) {
                all_piece_hashes += Crypto::calculate_sha1_hash_data({piece_buffer.data(), piece_size});
                buffer_fill = 0;
            }
        }
    }

    if (buffer_fill > 0) {
        all_piece_hashes += Crypto::calculate_sha1_hash_data({piece_buffer.data(), buffer_fill});
    }

    info_dict["pieces"] = Value(all_piece_hashes);

    Dict metainfo_dict;
    metainfo_dict["announce"] = Value(tracker_urls[0]);
    List announce_list_tiers;
    for (const auto& url : tracker_urls) {
        List tier;
        tier.push_back(Value(url));
        announce_list_tiers.push_back(Value(std::move(tier)));
    }
    metainfo_dict["announce-list"] = Value(std::move(announce_list_tiers));
    metainfo_dict["info"] = Value(info_dict);

    std::vector<char> bencoded_data = encode(Value(metainfo_dict));
    std::ofstream out_file(torrent_path, std::ios::binary);
    if (!out_file) {
        LOGERR("Failed to open torrent file for writing: {}", torrent_path.string());
        return false;
    }
    out_file.write(bencoded_data.data(), bencoded_data.size());

    LOGINFO("Successfully created torrent file: {}", torrent_path.string());
    return true;
}
