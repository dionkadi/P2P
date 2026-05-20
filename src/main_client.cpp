#include "ClientApp.hpp"
#include "ClientConfig.hpp"
#include "TorrentFile.hpp"
#include "Utils.hpp"

#include <csignal>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

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
              << "  ./client seed <torrent_file> <content_directory> [peer_port]\n"
              << "  ./client download <torrent_file> <save_path> [peer_port]\n"
              << "\nConfig flags (can appear anywhere):\n"
              << "  --port <port>                       Peer listening port (default: 6881)\n"
              << "  --upload-rate <bytes_per_sec>       Upload rate limit (default: 524288)\n"
              << "  --download-rate <bytes_per_sec>     Download rate limit (default: 2097152)\n"
              << "  --max-connections <num>             Max peer connections (default: 200)\n"
              << "  --max-connections-per-ip <num>      Max connections per IP (default: 2)\n"
              << "  --max-half-open <num>               Max half-open connections (default: 40)\n"
              << "  --block-timeout <seconds>           Block request timeout (default: 30)\n"
              << "  --download-dir <path>               Default download directory\n"
              << "  --config <path>                     Config file path\n"
              << "  --save-config                       Save current config and exit\n"
              << "  --no-dht                            Disable DHT\n"
              << "  --no-lsd                            Disable local peer discovery\n"
              << "  --no-pex                            Disable peer exchange\n";
}

static std::vector<std::string> collect_positional(int argc, char* argv[]) {
    std::vector<std::string> positional;
    for (int i = 1; i < argc; ++i) {
        if (!argv[i]) continue;
        std::string arg(argv[i]);
        if (arg.size() >= 2 && arg[0] == '-' && arg[1] == '-') {
            if (arg == "--config" || arg == "--port" || arg == "--upload-rate" ||
                arg == "--download-rate" || arg == "--max-connections" ||
                arg == "--max-connections-per-ip" || arg == "--max-half-open" ||
                arg == "--block-timeout" || arg == "--download-dir") {
                ++i;
            }
        } else {
            positional.push_back(arg);
        }
    }
    return positional;
}

int main(int argc, char* argv[]) {
    Logger::get();

    ClientConfig cfg = ClientConfig::from_cli(argc, argv);

    bool save_config = false;
    for (int i = 1; i < argc; ++i) {
        if (argv[i] && std::string(argv[i]) == "--save-config") {
            save_config = true;
            break;
        }
    }

    if (save_config) {
        std::string save_path;
        for (int i = 1; i < argc - 1; ++i) {
            if (argv[i] && std::string(argv[i]) == "--config") {
                save_path = argv[i + 1];
                break;
            }
        }
        if (save_path.empty()) {
            save_path = ClientConfig::find_config_path();
            if (save_path.empty()) save_path = "./p2p.conf";
        }
        cfg.save(save_path);
        LOGINFO("Configuration saved to {}", save_path);
        return 0;
    }

    std::vector<std::string> positional = collect_positional(argc, argv);

    if (positional.size() < 1) {
        print_usage();
        return 1;
    }

    try {
        std::string command = positional[0];
        if (command == "create") {
            if (positional.size() < 4) {
                print_usage();
                return 1;
            }
            std::string source_path = positional[1];
            std::string torrent_path = positional[2];
            std::string tracker_urls_str = positional[3];

            std::vector<std::string> tracker_urls = split(tracker_urls_str, ',');
            if (!MetaInfo::create_from_file(source_path, torrent_path, tracker_urls)) {
                LOGCRITICAL("Failed to create torrent file.");
                return 1;
            }
            LOGINFO("Torrent file created successfully.");
            return 0;
        }

        if (command == "seed" || command == "download") {
            if (positional.size() < 3) {
                print_usage();
                return 1;
            }

            std::filesystem::path torrent_path = positional[1];
            std::filesystem::path dest_path = positional[2];

            uint16_t port = cfg.peer_port;
            if (positional.size() >= 4) {
                port = static_cast<uint16_t>(std::stoi(positional[3]));
            }

            Mode mode = (command == "seed") ? Mode::Seed : Mode::Leech;

            PeerId my_peer_id = generate_id(PEER_ID_PREFIX);
            LOGINFO("Client starting with Peer ID: {}", my_peer_id);
            LOGINFO("Using port: {} | upload rate: {} | download rate: {}",
                    port, cfg.upload_rate_limit, cfg.download_rate_limit);

            ClientApp app(cfg);
            app.add_torrent(mode, torrent_path, dest_path, port);

            return app.run();
        }
        else {
            print_usage();
            return 1;
        }

    } catch (const std::exception& e) {
        LOGCRITICAL("Error: {}", e.what());
        return 1;
    }
}
