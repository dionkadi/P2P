#include "ClientApp.hpp"
#include "ClientConfig.hpp"
#include "TorrentFile.hpp"
#include "Utils.hpp"

#include <argparse.hpp>
#include <progress_bar.hpp>

#include <csignal>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

static std::vector<std::string> split(const std::string& s, char delimiter) {
    std::vector<std::string> tokens;
    std::string token;
    std::istringstream tokenStream(s);
    while (std::getline(tokenStream, token, delimiter)) {
        tokens.push_back(token);
    }
    return tokens;
}

int main(int argc, char* argv[]) {
    using namespace progressbar;

    // Handle --save-config / --config before argparse to keep things simple
    bool save_config = false;
    std::string cli_config_path;
    for (int i = 1; i < argc; ++i) {
        if (!argv[i]) continue;
        std::string a(argv[i]);
        if (a == "--save-config") save_config = true;
        else if (a == "--config" && i + 1 < argc) cli_config_path = argv[++i];
    }

    // -----------------------------------------------------------------------
    // argparse setup
    // -----------------------------------------------------------------------
    argparse::ArgumentParser prog("client", "1.1");

    argparse::ArgumentParser cmd_create("create");
    cmd_create.add_description("Create a torrent file");
    cmd_create.add_argument("file").help("File or directory to share").required();
    cmd_create.add_argument("output").help("Output .torrent file path").required();
    cmd_create.add_argument("tracker").help("Tracker announce URL(s), comma-separated").required();

    argparse::ArgumentParser cmd_seed("seed");
    cmd_seed.add_description("Seed a torrent");
    cmd_seed.add_argument("torrent").help("Path to .torrent file").required();
    cmd_seed.add_argument("content").help("Content directory path").required();
    cmd_seed.add_argument("--port").help("Peer listening port").scan<'d', int>().default_value(6881);
    cmd_seed.add_argument("--upload-rate").help("Upload rate limit (bytes/s)").scan<'u', uint64_t>().default_value(uint64_t{512 * 1024});
    cmd_seed.add_argument("--download-rate").help("Download rate limit (bytes/s)").scan<'u', uint64_t>().default_value(uint64_t{2 * 1024 * 1024});
    cmd_seed.add_argument("--max-connections").help("Max peer connections").scan<'u', uint32_t>().default_value(uint32_t{200});
    cmd_seed.add_argument("--max-connections-per-ip").help("Max connections per IP").scan<'u', uint32_t>().default_value(uint32_t{2});
    cmd_seed.add_argument("--max-half-open").help("Max half-open connections").scan<'u', uint32_t>().default_value(uint32_t{40});
    cmd_seed.add_argument("--block-timeout").help("Block request timeout (seconds)").scan<'u', uint32_t>().default_value(uint32_t{30});
    cmd_seed.add_argument("--download-dir").help("Default download directory").default_value(std::string{"./downloads"});
    cmd_seed.add_argument("--no-dht").help("Disable DHT").flag();
    cmd_seed.add_argument("--no-lsd").help("Disable local peer discovery").flag();
    cmd_seed.add_argument("--no-pex").help("Disable peer exchange").flag();

    argparse::ArgumentParser cmd_download("download");
    cmd_download.add_description("Download a torrent");
    cmd_download.add_argument("torrent").help("Path to .torrent file").required();
    cmd_download.add_argument("save_path").help("Destination directory").required();
    cmd_download.add_argument("--port").help("Peer listening port").scan<'d', int>().default_value(6881);
    cmd_download.add_argument("--upload-rate").help("Upload rate limit (bytes/s)").scan<'u', uint64_t>().default_value(uint64_t{512 * 1024});
    cmd_download.add_argument("--download-rate").help("Download rate limit (bytes/s)").scan<'u', uint64_t>().default_value(uint64_t{2 * 1024 * 1024});
    cmd_download.add_argument("--max-connections").help("Max peer connections").scan<'u', uint32_t>().default_value(uint32_t{200});
    cmd_download.add_argument("--max-connections-per-ip").help("Max connections per IP").scan<'u', uint32_t>().default_value(uint32_t{2});
    cmd_download.add_argument("--max-half-open").help("Max half-open connections").scan<'u', uint32_t>().default_value(uint32_t{40});
    cmd_download.add_argument("--block-timeout").help("Block request timeout (seconds)").scan<'u', uint32_t>().default_value(uint32_t{30});
    cmd_download.add_argument("--download-dir").help("Default download directory").default_value(std::string{"./downloads"});
    cmd_download.add_argument("--no-dht").help("Disable DHT").flag();
    cmd_download.add_argument("--no-lsd").help("Disable local peer discovery").flag();
    cmd_download.add_argument("--no-pex").help("Disable peer exchange").flag();

    prog.add_subparser(cmd_create);
    prog.add_subparser(cmd_seed);
    prog.add_subparser(cmd_download);

    // Parse
    try {
        prog.parse_args(argc, argv);
    } catch (const std::exception& e) {
        std::cerr << e.what() << "\n" << prog;
        return 1;
    }

    Logger::init("client");

    // -------------------------------------------------------------------
    // Handle --save-config
    // -------------------------------------------------------------------
    if (save_config) {
        ClientConfig cfg;
        if (!cli_config_path.empty()) {
            cfg = ClientConfig::load(cli_config_path);
        } else {
            std::string found = ClientConfig::find_config_path();
            if (!found.empty()) cfg = ClientConfig::load(found);
        }
        std::string out_path = cli_config_path.empty()
            ? (ClientConfig::find_config_path().empty() ? "./p2p.conf" : ClientConfig::find_config_path())
            : cli_config_path;
        cfg.save(out_path);
        LOGINFO("Configuration saved to {}", out_path);
        std::cout << Text{"✓ Configuration saved to "}.color(style::green).str() << out_path << std::endl;
        return 0;
    }

    // -------------------------------------------------------------------
    // Determine command
    // -------------------------------------------------------------------
    std::string command;
    if (prog.is_subcommand_used("create"))
        command = "create";
    else if (prog.is_subcommand_used("seed"))
        command = "seed";
    else if (prog.is_subcommand_used("download"))
        command = "download";
    else {
        std::cerr << "Error: expected one of create, seed, download\n" << prog;
        return 1;
    }

    // Helper: build ClientConfig from parsed subparser args
    auto make_cfg = [](const argparse::ArgumentParser& p) -> ClientConfig {
        ClientConfig c;
        c.peer_port = static_cast<uint16_t>(p.get<int>("--port"));
        c.upload_rate_limit = p.get<uint64_t>("--upload-rate");
        c.download_rate_limit = p.get<uint64_t>("--download-rate");
        c.max_connections = p.get<uint32_t>("--max-connections");
        c.max_connections_per_ip = p.get<uint32_t>("--max-connections-per-ip");
        c.max_half_open = p.get<uint32_t>("--max-half-open");
        c.block_request_timeout_seconds = p.get<uint32_t>("--block-timeout");
        c.download_dir = p.get<std::string>("--download-dir");
        c.enable_dht = !p.is_used("--no-dht");
        c.enable_lsd = !p.is_used("--no-lsd");
        c.enable_pex = !p.is_used("--no-pex");
        return c;
    };

    // -------------------------------------------------------------------
    // CREATE
    // -------------------------------------------------------------------
    if (command == "create") {
        std::string source_path = cmd_create.get<std::string>("file");
        std::string torrent_path = cmd_create.get<std::string>("output");
        std::string tracker_urls_str = cmd_create.get<std::string>("tracker");

        if (!MetaInfo::create_from_file(source_path, torrent_path, split(tracker_urls_str, ','))) {
            LOGCRITICAL("Failed to create torrent file.");
            return 1;
        }
        std::cout << Text{"✓ Torrent file created: "}.color(style::green).bold().str()
                  << torrent_path << std::endl;
        return 0;
    }

    // -------------------------------------------------------------------
    // SEED / DOWNLOAD — use the matching subparser
    // -------------------------------------------------------------------
    const auto& cmd = (command == "seed")
        ? static_cast<const argparse::ArgumentParser&>(cmd_seed)
        : static_cast<const argparse::ArgumentParser&>(cmd_download);

    std::filesystem::path torrent_path = cmd.get<std::string>("torrent");
    std::filesystem::path dest_path = cmd.get<std::string>(command == "seed" ? "content" : "save_path");

    uint16_t port = static_cast<uint16_t>(cmd.get<int>("--port"));
    Mode mode = (command == "seed") ? Mode::Seed : Mode::Leech;

    // Merge config file with CLI overrides
    ClientConfig cfg;
    std::string found_cfg = cli_config_path.empty() ? ClientConfig::find_config_path() : cli_config_path;
    if (!found_cfg.empty()) {
        try { cfg = ClientConfig::load(found_cfg); } catch (...) {}
    }
    ClientConfig cli_overrides = make_cfg(cmd);
    cfg.peer_port = cli_overrides.peer_port;
    cfg.upload_rate_limit = cli_overrides.upload_rate_limit;
    cfg.download_rate_limit = cli_overrides.download_rate_limit;
    cfg.max_connections = cli_overrides.max_connections;
    cfg.max_connections_per_ip = cli_overrides.max_connections_per_ip;
    cfg.max_half_open = cli_overrides.max_half_open;
    cfg.block_request_timeout_seconds = cli_overrides.block_request_timeout_seconds;
    cfg.download_dir = cli_overrides.download_dir;
    cfg.enable_dht = cli_overrides.enable_dht;
    cfg.enable_lsd = cli_overrides.enable_lsd;
    cfg.enable_pex = cli_overrides.enable_pex;

    PeerId my_peer_id = generate_id(PEER_ID_PREFIX);

    ClientApp app(cfg);
    app.add_torrent(mode, torrent_path, dest_path, port);

    // -------------------------------------------------------------------
    // LiveDisplay UI — runs alongside the io_context threads
    // -------------------------------------------------------------------
    LiveDisplay display(LiveDisplay::Config{std::chrono::milliseconds(333), false, false});

    // Speed tracking with exponential moving average smoothing
    static constexpr double kEmaAlpha = 0.35;
    struct SpeedState {
        std::chrono::steady_clock::time_point last_time;
        uint64_t last_down{0};
        uint64_t last_up{0};
        double down_speed{0};
        double up_speed{0};
        bool first_sample{true};
    };
    auto speed = std::make_shared<SpeedState>();

    display.add_slot([&app, speed, my_peer_id, port]() -> std::string {
        auto now = std::chrono::steady_clock::now();
        std::string out;

        size_t term_w = Terminal::width();
        if (term_w < 40) term_w = 40;

        // ── Header ──
        std::string hdr = std::format(" P2P Client v1.1  Peer: {}  Port: {}",
            std::string_view(reinterpret_cast<const char*>(my_peer_id.data()), 8),
            port);
        out += Panel(Text{hdr}.bold()).render(term_w) + "\n";

        // ── Per-torrent progress ──
        for (const auto& [hash, session] : app.torrents()) {
            auto state = session->get_state();
            if (!state) continue;

            std::string name = session->get_display_name();
            Mode tm = session->get_mode();
            size_t completed = state->completed_pieces();
            size_t total = state->num_pieces();
            uint64_t downloaded = state->total_bytes_downloaded();
            uint64_t uploaded = state->total_bytes_uploaded();
            size_t peers = session->peer_manager()->connection_count();
            double progress = total > 0 ? (100.0 * completed / total) : 0.0;

            // Speed tracking with EMA smoothing
            auto elapsed = std::chrono::duration_cast<std::chrono::duration<double>>(now - speed->last_time).count();
            if (elapsed >= 0.5) {
                if (speed->first_sample) {
                    speed->down_speed = 0;
                    speed->up_speed = 0;
                    speed->first_sample = false;
                } else {
                    double raw_down = (downloaded - speed->last_down) / elapsed;
                    double raw_up = (uploaded - speed->last_up) / elapsed;
                    speed->down_speed = kEmaAlpha * raw_down + (1.0 - kEmaAlpha) * speed->down_speed;
                    speed->up_speed = kEmaAlpha * raw_up + (1.0 - kEmaAlpha) * speed->up_speed;
                }
                speed->last_time = now;
                speed->last_down = downloaded;
                speed->last_up = uploaded;
            }

            // Mode badge
            if (tm == Mode::Seed) {
                out += Text{" ● SEED "}.color(style::green).bold().str();
            } else {
                out += Text{" ▓ LEECH "}.color(style::cyan).bold().str();
            }

            // Torrent name (truncated to fit)
            size_t max_name = term_w > 48 ? term_w - 48 : 20;
            std::string ndisp = name;
            if (count_visible_characters(ndisp) > max_name) {
                ndisp = truncate(ndisp, max_name > 3 ? max_name - 3 : 0) + "...";
            }
            out += " " + Text{ndisp}.bold().str() + "\n";

            // ── Progress bar ──
            size_t bar_w = std::min<size_t>(term_w - 42, 40);
            if (bar_w < 8) bar_w = 8;
            size_t filled = static_cast<size_t>(bar_w * progress / 100.0);
            if (filled > bar_w) filled = bar_w;

            out += "  [";
            for (size_t i = 0; i < bar_w; ++i) {
                if (i < filled) {
                    auto c = Gradient{{RGB{220,50,50}, RGB{220,180,30}, RGB{50,200,50}}}
                        .at(bar_w > 1 ? static_cast<double>(i) / (bar_w - 1) : 1.0);
                    out += c.to_ansi_foreground() + "█" + std::string(style::reset);
                } else {
                    out += Text{"░"}.color(style::bright_black).str();
                }
            }
            out += "] ";

            out += Text{std::format("{:5.1f}%", progress)}.color(style::yellow).str() + "  ";
            out += Text{std::format("Peers: {}", peers)}.color(style::bright_black).str() + "\n";

            // ── Speed & totals ──
            out += "  ";
            out += Text{std::format("↓ {}/s", fmt_bytes(static_cast<uint64_t>(speed->down_speed)))}.color(style::blue).str() + "  ";
            out += Text{std::format("↑ {}/s", fmt_bytes(static_cast<uint64_t>(speed->up_speed)))}.color(style::magenta).str() + "  ";
            out += std::format("DL: {}  UL: {}", fmt_bytes(downloaded), fmt_bytes(uploaded));
            out += "\n";
        }

        return out;
    });

    display.start();

    int result = app.run();

    display.stop();

    return result;
}
