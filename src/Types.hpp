#pragma once

#include <cstddef>
#include <cstdint>
#include <array>
#include <vector>
#include <string_view>
#include <endian.h>
#include <filesystem>
#include <string>
#include <map>

#include "Bencode.hpp"

inline constexpr std::string_view PROTOCOL_STRING = "MIT-P2P-V1.0";
inline constexpr size_t HASH_SIZE = 20;
inline constexpr size_t PEER_ID_SIZE = 20;
inline constexpr size_t HANDSHAKE_RESERVED_BYTES = 8;
inline constexpr size_t HANDSHAKE_BASE_LEN = 1 + PROTOCOL_STRING.size() + HANDSHAKE_RESERVED_BYTES + HASH_SIZE + PEER_ID_SIZE;
inline constexpr size_t BLOCK_SIZE = 16384;

using PeerId = std::array<std::byte, 20>;
using InfoHash = std::vector<std::byte>;

enum class MessageType: uint8_t {
    Choke = 0,
    Unchoke = 1,
    Interested = 2,
    NotInterested = 3,
    Have = 4,
    Bitfield = 5,
    Request = 6,
    Piece = 7,
    Cancel = 8,
    Port = 9,
    ExtendedMessage = 20,
};

enum class ExtendedMessageType: uint8_t {
    Handshake = 0,
    ut_pex = 1,
    ut_metadata = 3,
    UNKNOWN = 255,
};

#pragma pack(push, 1)

struct UdpConnectRequest {
    uint64_t protocol_id = htobe64(0x41727101980);
    uint32_t action = htobe32(0);
    uint32_t transaction_id;
};

struct UdpConnectResponse {
    uint32_t action;
    uint32_t transaction_id;
    uint64_t connection_id;
};

struct UdpAnnounceRequest {
    uint64_t connection_id;
    uint32_t action = htobe32(1); // 1 for announce
    uint32_t transaction_id;
    std::array<std::byte, 20> info_hash;
    std::array<std::byte, 20> peer_id;
    uint64_t downloaded;
    uint64_t left;
    uint64_t uploaded;
    uint32_t event = htobe32(0); // 0: none; 1: completed; 2: started; 3: stopped
    uint32_t ip_address = 0; // 0 for sender's IP
    uint32_t key = 0; // optional
    int32_t num_want = htobe32(-1); // default
    uint16_t port;
};

struct UdpAnnounceResponse {
    uint32_t action;
    uint32_t transaction_id;
    uint32_t interval;
    uint32_t leechers;
    uint32_t seeders;
};

struct UdpPeerInfo {
    uint32_t ip;
    uint16_t port;
};

#pragma pack(pop)

struct FileInfo {
    std::filesystem::path path;
    uint64_t size;
    bool download{true};
};

struct TorrentInfo {
    std::string name;
    uint64_t total_size = 0;
    uint32_t piece_size = 0;
    std::vector<std::byte> pieces;  // SHA1 value of each piece
    std::vector<FileInfo> files;
};


enum class PieceStatus { 
    Needed, 
    InProgress, 
    Have,
    Skipped,
};

struct PieceFileOverlap {
    size_t file_index;             // Index into meta_info_.get_torrent_info().files
    uint64_t offset_in_file;    // Where this piece's data starts writing in the file
    uint32_t offset_in_piece;   // Where in the piece data the file's content starts
    uint32_t length;            // How many bytes from this piece belong to this file
};


struct AnnounceRequestParams {
    std::vector<std::byte> info_hash_bytes;
    PeerId peer_id;
    std::string event;
    uint16_t port;
    uint64_t uploaded;
    uint64_t downloaded;
    uint64_t left;
};

struct TrackerAnnounceResult {
    int interval_seconds;
    std::vector<std::string> peers;
};

struct InProgressPiece {
    std::vector<std::byte> data;
    std::vector<bool> blocks_received;
    uint32_t received_count = 0;
    uint32_t total_blocks = 0;

    std::vector<std::vector<PeerId>> outstanding_requests;

    InProgressPiece(uint64_t piece_size): data(piece_size) {
        total_blocks = (piece_size + BLOCK_SIZE - 1) / BLOCK_SIZE;
        blocks_received.resize(total_blocks, false);
        outstanding_requests.resize(total_blocks);
    }
};

enum class Mode {
    Seed,
    Leech
};

struct ResumeData {
    std::string have_bitfield;  // Bitfield as string
    std::map<std::string, int64_t> file_mtimes;  // path -> mtime
    uint64_t total_uploaded = 0;
    uint64_t total_downloaded = 0;
    std::map<std::string, std::string> in_progress_pieces;  // piece_index_str -> block bitfield

    std::vector<std::byte> serialize() const {
        Dict files_metadata_dict;
        for (const auto& [path, mtime] : file_mtimes) {
            Dict this_file_dict;
            this_file_dict["mtime"] = Value(static_cast<Integer>(mtime));
            files_metadata_dict[path] = Value(std::move(this_file_dict));
        }
        
        Dict stats_dict;
        stats_dict["uploaded"] = Value(static_cast<Integer>(total_uploaded));
        stats_dict["downloaded"] = Value(static_cast<Integer>(total_downloaded));
        
        Dict in_progress_dict;
        for (const auto& [piece_idx_str, block_bitfield] : in_progress_pieces) {
            in_progress_dict[piece_idx_str] = Value(block_bitfield);
        }

        Dict resume_dict;
        resume_dict["have_bitfield"] = Value(have_bitfield);
        resume_dict["files_metadata"] = Value(std::move(files_metadata_dict));
        resume_dict["stats"] = Value(std::move(stats_dict));
        resume_dict["in_progress"] = Value(std::move(in_progress_dict));

        return encode(Value(std::move(resume_dict)));
    }

    static ResumeData deserialize(std::span<const std::byte> data) {
        auto decoded = decode(data);
        const auto *resume_dict_ptr = std::get_if<std::unique_ptr<Dict>>(&decoded.get_variant());
        if (!resume_dict_ptr) {
            throw std::runtime_error("Resume file is not a dictionary");
        }
        const Dict& resume_dict = **resume_dict_ptr;

        if (!resume_dict.count("have_bitfield")) {
            throw std::runtime_error("Resume file missing 'have_bitfield'");
        }

        ResumeData resume_data;
        resume_data.have_bitfield = std::get<String>(resume_dict.at("have_bitfield").get_variant());

        const Dict* files_metadata_dict = nullptr;
        if (resume_dict.count("files_metadata")) {
            const auto* files_metadata_ptr = std::get_if<std::unique_ptr<Dict>>(&resume_dict.at("files_metadata").get_variant());
            if (files_metadata_ptr) {
                files_metadata_dict = files_metadata_ptr->get();
            }
        }

        const Dict* stats_dict = nullptr;
        if (resume_dict.count("stats")) {
            const auto* stats_ptr = std::get_if<std::unique_ptr<Dict>>(&resume_dict.at("stats").get_variant());
            if (stats_ptr) {
                stats_dict = stats_ptr->get();
            }
        }

        const Dict* in_progress_dict = nullptr;
        if (resume_dict.count("in_progress")) {
            const auto* in_progress_ptr = std::get_if<std::unique_ptr<Dict>>(&resume_dict.at("in_progress").get_variant());
            if (in_progress_ptr) {
                in_progress_dict = in_progress_ptr->get();
            }
        }

        if (files_metadata_dict) {
            for (const auto& [k, v] : *files_metadata_dict) {
                const auto *this_file_dict_ptr = std::get_if<std::unique_ptr<Dict>>(&(files_metadata_dict->at(k).get_variant()));
                if (!this_file_dict_ptr) {
                    continue;
                }
                const Dict& this_file_dict = **this_file_dict_ptr;
                Integer saved_mtime = std::get<Integer>(this_file_dict.at("mtime").get_variant());
                resume_data.file_mtimes[k] = saved_mtime;
            }
        }

        if (stats_dict) {
            resume_data.total_uploaded = std::get<Integer>(stats_dict->at("uploaded").get_variant());
            resume_data.total_downloaded = std::get<Integer>(stats_dict->at("downloaded").get_variant());
        }

        if (in_progress_dict) {
            for (const auto& [piece_idx_str, block_bitfield] : *in_progress_dict) {
                resume_data.in_progress_pieces[piece_idx_str] = std::get<String>(block_bitfield.get_variant());
            }
        }

        return resume_data;
    } 
};