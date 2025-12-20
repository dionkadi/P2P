#include "Tracker.hpp"
#include "Logger.hpp"
#include <csignal>


int main(
    // int argc, char* argv[]
) {
    Logger::get();
    int port = 3333;

    // if (argc != 2) {
    //     std::cerr << "Usage: ./tracker <port>\n";
    //     return 1;
    // }
 
    // try {
    //     port = std::stoi(argv[1]);
    //     if (port <= 1024 || port > 65535) {
    //          std::cerr << "Error: Port must be between 1025 and 65535.\n";
    //          return 1;
    //     }
    // } catch (const std::exception& e) {
    //     std::cerr << "Error: Invalid port number.\n";
    //     return 1;
    // }

    try {
        Tracker tracker;

        asio::signal_set signals(tracker.get_io_context(), SIGINT, SIGTERM);
        signals.async_wait([&] (auto, auto) {
            tracker.get_io_context().stop();
        });

        tracker.listen_http(port);
        tracker.listen_udp(port);
        tracker.run();
    } catch (const std::exception& e) {
        LOGCRITICAL("Tracker failed to initialize: {}", e.what());
        return 1;
    }
    
    return 0;
}