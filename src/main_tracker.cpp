#include "Tracker.hpp"
#include "Utils.hpp"

#include <argparse.hpp>
#include <progress_bar.hpp>

#include <csignal>
#include <functional>
#include <iostream>
#include <span>
#include <string_view>

int main(int argc, char* argv[]) {
    using namespace progressbar;

    // -----------------------------------------------------------------------
    // argparse setup
    // -----------------------------------------------------------------------
    argparse::ArgumentParser prog("tracker", "1.1");
    prog.add_description("BitTorrent tracker server");

    prog.add_argument("--http-port").help("HTTP listen port").scan<'d', int>().default_value(3333);
    prog.add_argument("--udp-port").help("UDP listen port").scan<'d', int>().default_value(3333);
    prog.add_argument("--port").help("Set both HTTP and UDP port").scan<'d', int>().default_value(0);
    prog.add_argument("--data-dir").help("Data directory for peer state").default_value(std::string{"./tracker_data"});

    try {
        prog.parse_args(argc, argv);
    } catch (const std::exception& e) {
        std::cerr << e.what() << "\n" << prog;
        return 1;
    }

    Logger::init("tracker");

    int http_port = prog.get<int>("--http-port");
    int udp_port = prog.get<int>("--udp-port");
    int port = prog.get<int>("--port");
    std::filesystem::path data_dir = prog.get<std::string>("--data-dir");

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

        // ── Stats cache: snapshot tracker state on the strand for the UI ──
        asio::steady_timer stats_timer(ioc);
        std::atomic<size_t> cached_swarms{0};
        std::atomic<size_t> cached_peers{0};

        std::function<void(boost::system::error_code)> refresh_stats;
        refresh_stats = [&](boost::system::error_code ec) {
            if (ec) return;
            asio::post(tracker.get_strand(), [&]() {
                size_t s = 0, p = 0;
                for (const auto& [hash, pm] : tracker.get_peers()) {
                    ++s;
                    p += pm.size();
                }
                cached_swarms.store(s, std::memory_order_relaxed);
                cached_peers.store(p, std::memory_order_relaxed);
            });
            stats_timer.expires_after(std::chrono::seconds(2));
            stats_timer.async_wait(refresh_stats);
        };
        stats_timer.expires_after(std::chrono::seconds(1));
        stats_timer.async_wait(refresh_stats);

        // ── LiveDisplay UI ──
        LiveDisplay display(LiveDisplay::Config{std::chrono::milliseconds(200), false, true});

        display.add_slot([&]() -> std::string {
            size_t term_w = Terminal::width();
            if (term_w < 40) term_w = 40;

            uint64_t announces = tracker.announce_count_.load(std::memory_order_relaxed);
            size_t swarms = cached_swarms.load(std::memory_order_relaxed);
            size_t peers = cached_peers.load(std::memory_order_relaxed);

            std::string out;

            // Header panel
            out += Panel(Text{" P2P Tracker v1.1 "}.bold()).render(term_w) + "\n";

            // Connection info
            out += "  ";
            out += Text{std::format("HTTP: {}  UDP: {}  Data: {}", http_port, udp_port, data_dir.string())}
                        .color(style::bright_black).str() + "\n";

            // Rule separator
            out += "  " + Rule{" Stats ", '-'}.color(style::dim).render(term_w - 4) + "\n";

            // Stats line
            out += "  ";
            out += Text{std::format("Swarms: {}  Peers: {}  Announces: {}", swarms, peers, announces)}
                        .color(style::cyan).str() + "\n";

            return out;
        });

        display.start();

        LOGINFO("Tracker started. HTTP on {}, UDP on {}, data dir: {}",
                http_port, udp_port, data_dir.string());

        {
            CTRACK;
            ioc.run();
        }

        display.stop();

        ctrack::result_print();

    } catch (const std::exception& e) {
        LOGCRITICAL("Tracker failed to initialize: {}", e.what());
        return 1;
    }

    return 0;
}
