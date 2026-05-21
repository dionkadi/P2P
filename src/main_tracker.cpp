#include "Tracker.hpp"
#include "Utils.hpp"
#include <csignal>
#include <iostream>
#include <span>
#include <string_view>

int main(int argc, char* argv[]) {
    Logger::get();

    int http_port = 3333;
    int udp_port = 3333;
    std::filesystem::path data_dir = "./tracker_data";
    int port = 0;

    for (int i = 1; i < argc; ++i) {
        std::string_view arg = argv[i];
        if (arg == "--port" && i + 1 < argc) {
            port = std::stoi(argv[++i]);
        } else if (arg == "--http-port" && i + 1 < argc) {
            http_port = std::stoi(argv[++i]);
        } else if (arg == "--udp-port" && i + 1 < argc) {
            udp_port = std::stoi(argv[++i]);
        } else if (arg == "--data-dir" && i + 1 < argc) {
            data_dir = argv[++i];
        } else if (arg == "--help") {
            std::cout << "Usage: " << (argc > 0 ? argv[0] : "tracker") << " [options]\n"
                      << "  --port <port>        Set both HTTP and UDP port (default: 3333)\n"
                      << "  --http-port <port>   Set HTTP port (default: 3333)\n"
                      << "  --udp-port <port>    Set UDP port (default: 3333)\n"
                      << "  --data-dir <dir>     Data directory for peer state persistence (default: ./tracker_data)\n"
                      << "  --help               Show this help\n";
            return 0;
        }
    }

    if (port > 0) {
        http_port = port;
        udp_port = port;
    }

    try {
        asio::io_context ioc;
        Tracker tracker(ioc);

        asio::signal_set signals(ioc, SIGINT, SIGTERM);
        signals.async_wait([&](auto, auto) {
            LOGINFO("Tracker shutting down...");
            ioc.stop();
        });

        tracker.load_state(data_dir);

        tracker.listen_http(http_port);
        tracker.listen_udp(udp_port);

        tracker.start_background_tasks(data_dir);

        LOGINFO("Tracker started. HTTP on {}, UDP on {}, data dir: {}", http_port, udp_port, data_dir.string());

        ioc.run();
    } catch (const std::exception& e) {
        LOGCRITICAL("Tracker failed to initialize: {}", e.what());
        return 1;
    }

    return 0;
}
