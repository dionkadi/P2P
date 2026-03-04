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
#include <string>

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

        if (command == "seed" || command == "download") {
            if (argc != 5) { 
                print_usage(); 
                return 1; 
            }
            std::filesystem::path torrent_path = argv[2];
            std::filesystem::path content_dir = argv[3];
            int peer_port = std::stoi(argv[4]);
            PeerId my_peer_id = generate_peer_id();
            LOGINFO("Client starting with Peer ID: {}", my_peer_id);
            std::shared_ptr<TorrentSession> session_ptr = std::make_shared<TorrentSession>(
                io_context, my_peer_id, torrent_path, content_dir, peer_port, 
                (command == "seed" ? Mode::Seed : Mode::Leech)
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
                    
                    // If the run() coroutine completes without the io_context being stopped (e.g., download finished naturally),
                    // ensure proper shutdown is initiated.
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