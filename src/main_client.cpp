#include "ClientConfig.hpp"
#include "Utils.hpp"
#include "TorrentFile.hpp"
#include "TorrentSession.hpp"

#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <csignal>
#include <cstddef>
#include <exception>
#include <filesystem>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
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
        asio::io_context io_context;

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
            std::filesystem::path content_dir = positional[2];

            if (positional.size() >= 4) {
                cfg.peer_port = static_cast<uint16_t>(std::stoi(positional[3]));
            }

            PeerId my_peer_id = generate_id();
            LOGINFO("Client starting with Peer ID: {}", my_peer_id);
            LOGINFO("Using port: {} | upload rate: {} | download rate: {}",
                    cfg.peer_port, cfg.upload_rate_limit, cfg.download_rate_limit);

            std::shared_ptr<TorrentSession> session_ptr = std::make_shared<TorrentSession>(
                io_context, my_peer_id, torrent_path, content_dir, cfg.peer_port,
                (command == "seed" ? Mode::Seed : Mode::Leech),
                cfg.upload_rate_limit, cfg.download_rate_limit
            );
            if (!session_ptr) {
                LOGERR("Failed to create torrent session.");
                return 1;
            }

            asio::signal_set signals(io_context, SIGINT, SIGTERM);
            signals.async_wait([&io_context, session_ptr] (auto, auto signal_number) mutable {
                LOGINFO("Signal {} received, initiating shutdown...", signal_number);
                if (session_ptr) {
                    asio::co_spawn(io_context, session_ptr->stop(), asio::detached);
                } else {
                    io_context.stop();
                }
            });

            asio::co_spawn(io_context,
                [&io_context, session_ptr]() mutable -> asio::awaitable<void>
                {
                    try {
                        co_await session_ptr->run();
                    } catch (const boost::system::system_error& ex) {
                        if (ex.code() != asio::error::operation_aborted) {
                            LOGCRITICAL("TorrentSession run() coroutine threw an unexpected exception: {}", ex.what());
                        } else {
                            LOGDBG("TorrentSession run() coroutine aborted gracefully.");
                        }
                    } catch (const std::exception& ex) {
                        LOGCRITICAL("TorrentSession run() coroutine threw a general exception: {}", ex.what());
                    }

                    if (!io_context.stopped()) {
                        LOGINFO("TorrentSession run() finished, stopping application.");
                        co_await session_ptr->stop();
                    }
                },
                asio::detached
            );

            session_ptr.reset();
            const int num_io_threads = std::thread::hardware_concurrency();
            std::vector<std::jthread> io_threads;
            LOGINFO("Running io_context on {} threads...", num_io_threads);
            for (int i = 0; i < num_io_threads; ++i) {
                io_threads.emplace_back([&io_context] {
                    io_context.run();
                });
            }
        }
        else {
            print_usage();
            return 1;
        }

    } catch (const std::exception& e) {
        LOGCRITICAL("Error: {}", e.what());
        return 1;
    }

    LOGINFO("Client finished");
    return 0;
}
