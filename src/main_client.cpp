#include "Peer.hpp"
#include "Logger.hpp"
#include "MetaInfo.hpp"
#include <chrono>
#include <csignal>
#include <exception>
#include <filesystem>
#include <iostream>
#include <random>
#include <string>

std::string generate_peer_id() {
    static const char alphanum[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

    std::string peer_id = "-MI0001-";
    std::mt19937 rng(std::chrono::high_resolution_clock::now().time_since_epoch().count());
    std::uniform_int_distribution<int> distrib(0, sizeof(alphanum) - 2);

    for (int i = 0; i < 12; ++i) {
        peer_id += alphanum[distrib(rng)];
    }

    return peer_id;
}

void print_usage() {
    std::cerr << "Usage:\n"
              << "  ./client create <file_to_share> <output_torrent_file>\n"
              << "  ./client seed <torrent_file> <peer_port> <tracker_addr>:<tracker_port>\n"
              << "  ./client download <torrent_file> <save_path> <tracker_addr>:<tracker_port>\n";
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
        std::string tracker_host;
        int tracker_port;
        int tracker_arg_index = -1;

        asio::io_context io_context;
        auto work_guard = asio::make_work_guard(io_context);

        std::string my_peer_id = generate_peer_id();
        LOGINFO("Client starting with Peer ID: {}", my_peer_id);

        std::string command = argv[1];

        if (command == "create" && argc == 4) {
            std::string source_path = argv[2];
            std::string torrent_path = argv[3];
            if (!MetaInfo::create_from_file(source_path, torrent_path)) {
                LOGCRITICAL("Failed to create torrent file.");
                return 1;
            }
            LOGINFO("Torrent file created successfully.");
            return 0;
        }

        if (command == "seed" && argc == 5) {
            tracker_arg_index = 4;
        } else if (command == "download" && argc == 5) {
            tracker_arg_index = 4;
        } else {
            print_usage();
            return 1;
        }
    
        if (!parse_address(argv[tracker_arg_index], tracker_host, tracker_port)) {
            std::cerr << "Invalid tracker address format. Use host:port (e.g., 127.0.0.1:3333).\n";
            return 1;
        }

        if (command == "seed") {
            if (argc != 5) { 
                print_usage(); 
                return 1; 
            }
            std::filesystem::path file_path = argv[2];
            int peer_port = std::stoi(argv[3]);

            auto content_dir = std::filesystem::path(file_path).parent_path();
            
            LOGINFO("Seeding from torrent '{}', content in '{}'. Listening on port {}", file_path.string(), content_dir.string(), peer_port);

            auto seeder = std::make_shared<Seeder>(io_context, my_peer_id, file_path, content_dir, peer_port);
            asio::co_spawn(io_context, 
                [seeder, tracker_host, tracker_port]() {
                    return seeder->run(tracker_host, tracker_port);
                }, 
                asio::detached
            );
        }
        else if (command == "download") {
            std::filesystem::path torrent_path = argv[2];
            std::filesystem::path save_path = argv[3];

            auto leecher = std::make_shared<Leecher>(io_context, my_peer_id, torrent_path, save_path);

            asio::co_spawn(io_context, 
                [leecher, tracker_host, tracker_port, work_guard = std::move(work_guard)]() mutable -> asio::awaitable<void> {
                    try {
                        bool success = co_await leecher->run(tracker_host, tracker_port);
                        if (success) {
                            LOGINFO("Download process completed successfully.");
                        } else {
                            LOGWARN("Download process finished with errors.");
                        }
                    } catch (const std::exception& ex) {
                        LOGERR("Download coroutine threw an exception: {}", ex.what());
                    }
                    work_guard.reset(); // Stop the io_context
                },
                asio::detached
            );
        }
        else {
            print_usage();
            return 1;
        }

        asio::signal_set signals(io_context, SIGINT, SIGTERM);
        signals.async_wait([&](auto, auto) { io_context.stop(); });
        io_context.run();
    
    } catch (const std::exception& e) {
        LOGCRITICAL("Error: {}", e.what());
        return 1;
    } 

    LOGINFO("Client finished");
    return 0;
}