#include "Logger.hpp"
#include "TorrentFile.hpp"
#include "TorrentSession.hpp"

#include <boost/asio/awaitable.hpp>
#include <csignal>
#include <exception>
#include <filesystem>
#include <iostream>
#include <random>
#include <string>

PeerId generate_peer_id() {
    const std::string prefix = "-MI0001-"; // 8 bytes
    static constexpr char alphanum[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

    static std::mt19937 rng = []{
        std::random_device rd;
        return std::mt19937(rd());
    }();

    std::uniform_int_distribution<int> distrib(0, sizeof(alphanum) - 2);

    PeerId peer_id{};
    std::transform(prefix.begin(), prefix.end(), peer_id.begin(), 
        [](char c) { return static_cast<std::byte>(c); });
    // Fill the remaining 12 bytes with random characters
    for (size_t i = 0; i < 12; ++i) {
        peer_id[prefix.size() + i] = static_cast<std::byte>(alphanum[distrib(rng)]);
    }
    return peer_id;
}

// PeerId get_or_create_peer_id() {
//     const std::filesystem::path id_path = "peer.id";
    
//     if (std::filesystem::exists(id_path)) {
//         std::ifstream id_file(id_path, std::ios::binary);
//         if (id_file.is_open()) {
//             PeerId peer_id{};
//             // fstream functions work with char*, so we must reinterpret_cast
//             id_file.read(reinterpret_cast<char*>(peer_id.data()), peer_id.size());
//             if (id_file.gcount() == peer_id.size()) {
//                 LOGINFO("Loaded existing peer ID from {}", id_path.string());
//                 return peer_id;
//             }
//         }
//         LOGWARN("Could not read existing peer.id file. A new one will be generated.");
//     }
//     // If file doesn't exist or was invalid, generate a new one and save it.
//     PeerId new_peer_id = generate_peer_id();
//     LOGINFO("Generated new peer ID. Saving to {}", id_path.string());
//     std::ofstream id_file(id_path, std::ios::binary);
//     if (id_file.is_open()) {
//         id_file.write(reinterpret_cast<const char*>(new_peer_id.data()), new_peer_id.size());
//     } else {
//         LOGERR("Failed to save new peer ID to file!");
//     }
//     return new_peer_id;
// }

std::vector<std::string> split(const std::string& s, char delimiter) {
    std::vector<std::string> tokens;
    std::string token;
    std::istringstream tokenStream(s);
    while (std::getline(tokenStream, token, delimiter)) {
        tokens.push_back(token);
    }
    return tokens;
}

void print_usage() {
    std::cerr << "Usage:\n"
              << "  ./client create <file_to_share> <output_torrent_file> <tracker_url>\n" 
              << "  ./client seed <torrent_file> <content_directory> <peer_port>\n" 
              << "  ./client download <torrent_file> <save_path> <peer_port>\n";
}

bool parse_address(const std::string& addr_str, std::string& host, int& port) {
    try {
        size_t colon_pos = addr_str.find(':');
        if (colon_pos == std::string::npos) {
            return false;
        }
        host = addr_str.substr(0, colon_pos);
        port = std::stoi(addr_str.substr(colon_pos + 1));
        return true;
    } catch (const std::exception&) {
        return false;
    }
}

int main(int argc, char* argv[]) {
    Logger::get();
 
    if (argc < 4) {
        print_usage();
        return 1;
    }
 
    try {
        asio::io_context io_context;

        asio::signal_set signals(io_context, SIGINT, SIGTERM);
        signals.async_wait([&io_context](auto, auto) {
            LOGINFO("Signal received, initiating shutdown...");
            io_context.stop();
        });

        PeerId my_peer_id = generate_peer_id();
        LOGINFO("Client starting with Peer ID: {}", my_peer_id);

        std::string command = argv[1];

        if (command == "create" && argc == 5) {
            std::string source_path = argv[2];
            std::string torrent_path = argv[3];
            std::string tracker_urls_str = argv[4];

            std::vector<std::string> tracker_urls = split(tracker_urls_str, ',');
            if (!MetaInfo::create_from_file(source_path, torrent_path, tracker_urls)) {
                LOGCRITICAL("Failed to create torrent file.");
                return 1;
            }
            LOGINFO("Torrent file created successfully.");
            return 0;
        }

        if (command == "seed") {
            if (argc != 5) { 
                print_usage(); 
                return 1; 
            }
            std::filesystem::path file_path = argv[2];
            std::filesystem::path content_dir = argv[3];
            int peer_port = std::stoi(argv[4]);
            
            LOGINFO("Seeding from torrent '{}', content in '{}'. Listening on port {}", file_path.string(), content_dir.string(), peer_port);

            asio::co_spawn(io_context, 
                [&io_context, peer_id = std::move(my_peer_id), file_path, content_dir, peer_port]() -> asio::awaitable<void> {
                    try {
                        auto seeder = TorrentSession(io_context, peer_id, file_path, content_dir, peer_port, Mode::Seed);
                        co_await seeder.run();
                    } catch (const std::exception& e) {
                        LOGCRITICAL("Seed coroutine threw an exception: {}", e.what());
                    }
                }, 
                asio::detached
            );
        }
        else if (command == "download") {
            std::filesystem::path torrent_path = argv[2];
            std::filesystem::path save_path = argv[3];
            int peer_port = std::stoi(argv[4]);

            asio::co_spawn(io_context, 
                [&io_context, my_peer_id, torrent_path, save_path, peer_port]() mutable -> asio::awaitable<void> 
                {
                    try {
                        auto leecher = TorrentSession(io_context, my_peer_id, torrent_path, save_path, peer_port, Mode::Leech);
                        co_await leecher.run();
                    } catch (const std::exception& ex) {
                        LOGCRITICAL("Download coroutine threw an exception: {}", ex.what());
                    }
                    
                    if (!io_context.stopped()) {
                        LOGINFO("Download finished, stopping application.");
                        io_context.stop();
                    }
                },
                asio::detached
            );
        }
        else {
            print_usage();
            return 1;
        }

        io_context.run();
    
    } catch (const std::exception& e) {
        LOGCRITICAL("Error: {}", e.what());
        return 1;
    } 

    LOGINFO("Client finished");
    return 0;
}