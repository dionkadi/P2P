#include "ClientApp.hpp"
#include "ClientConfig.hpp"
#include "TorrentFile.hpp"
#include "Utils.hpp"

#include <argparse.hpp>
#include <progress_bar.hpp>

#include <atomic>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <iostream>
#include <mutex>
#include <ranges>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

// POSIX raw terminal input
#include <poll.h>
#include <termios.h>
#include <unistd.h>

static std::vector<std::string> split(const std::string& s, char delimiter) {
    std::vector<std::string> tokens;
    std::string token;
    std::istringstream tokenStream(s);
    while (std::getline(tokenStream, token, delimiter)) {
        tokens.push_back(token);
    }
    return tokens;
}

static std::string trim(std::string s) {
    auto start = s.find_first_not_of(" \t\r\n");
    if (start == std::string::npos) return {};
    auto end = s.find_last_not_of(" \t\r\n");
    return s.substr(start, end - start + 1);
}

static std::vector<FileInfo> peek_file_list(const std::filesystem::path& torrent_path) {
    MetaInfo meta;
    std::vector<std::vector<std::string>> tiers;
    if (!meta.load_from_file(torrent_path.string(), tiers)) {
        return {};
    }
    return meta.get_torrent_info().files;
}

// RAII terminal raw mode: disables echo and canonical (line-buffered) input
// so we can read keystrokes character-by-character. Keeps ISIG so CTRL+C
// still sends SIGINT for graceful shutdown.
class ScopedRawTerminal {
    struct termios orig_;
    bool active_ = false;
public:
    void enable() {
        if (active_) return;
        if (!isatty(STDIN_FILENO)) return;
        tcgetattr(STDIN_FILENO, &orig_);
        struct termios raw = orig_;
        raw.c_lflag &= ~(ECHO | ICANON);
        raw.c_cc[VMIN] = 0;
        raw.c_cc[VTIME] = 0;
        tcsetattr(STDIN_FILENO, TCSANOW, &raw);
        active_ = true;
    }
    void disable() {
        if (!active_) return;
        tcsetattr(STDIN_FILENO, TCSANOW, &orig_);
        active_ = false;
    }
    ~ScopedRawTerminal() { disable(); }
};

int main(int argc, char* argv[]) {
    using namespace progressbar;

    bool save_config = false;
    std::string cli_config_path;
    for (int i = 1; i < argc; ++i) {
        if (!argv[i]) continue;
        std::string a(argv[i]);
        if (a == "--save-config") save_config = true;
        else if (a == "--config" && i + 1 < argc) cli_config_path = argv[++i];
    }

    argparse::ArgumentParser prog("client", "1.1");

    argparse::ArgumentParser cmd_create("create");
    cmd_create.add_description("Create a torrent file");
    cmd_create.add_argument("file").help("File or directory to share").required();
    cmd_create.add_argument("output").help("Output .torrent file path").required();
    cmd_create.add_argument("tracker").help("Tracker announce URL(s), comma-separated").required();

    argparse::ArgumentParser cmd_run("run");
    cmd_run.add_description("Run a torrent (seeds what we have, downloads what we need)");
    cmd_run.add_argument("torrent").help("Path to .torrent file").required();
    cmd_run.add_argument("dest").help("Content / destination directory").required();
    cmd_run.add_argument("--port").help("Peer listening port").scan<'d', int>().default_value(6881);
    cmd_run.add_argument("--upload-rate").help("Upload rate limit (bytes/s)").scan<'u', uint64_t>().default_value(uint64_t{512 * 1024});
    cmd_run.add_argument("--download-rate").help("Download rate limit (bytes/s)").scan<'u', uint64_t>().default_value(uint64_t{2 * 1024 * 1024});
    cmd_run.add_argument("--max-connections").help("Max peer connections").scan<'u', uint32_t>().default_value(uint32_t{200});
    cmd_run.add_argument("--max-connections-per-ip").help("Max connections per IP").scan<'u', uint32_t>().default_value(uint32_t{2});
    cmd_run.add_argument("--max-half-open").help("Max half-open connections").scan<'u', uint32_t>().default_value(uint32_t{40});
    cmd_run.add_argument("--block-timeout").help("Block request timeout (seconds)").scan<'u', uint32_t>().default_value(uint32_t{30});
    cmd_run.add_argument("--download-dir").help("Default download directory").default_value(std::string{"./downloads"});
    cmd_run.add_argument("--no-dht").help("Disable DHT").flag();
    cmd_run.add_argument("--no-lsd").help("Disable local peer discovery").flag();
    cmd_run.add_argument("--no-pex").help("Disable peer exchange").flag();
    cmd_run.add_argument("--selective").help("Prompt for file selection before download").flag();
    cmd_run.add_argument("--non-interactive").help("Disable interactive TUI commands").flag();

    prog.add_subparser(cmd_create);
    prog.add_subparser(cmd_run);

    try {
        prog.parse_args(argc, argv);
    } catch (const std::exception& e) {
        std::cerr << e.what() << "\n" << prog;
        return 1;
    }

    Logger::init("client");

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

    std::string command;
    if (prog.is_subcommand_used("create"))
        command = "create";
    else if (prog.is_subcommand_used("run"))
        command = "run";
    else {
        std::cerr << "Error: expected one of create, run\n" << prog;
        return 1;
    }

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

    // ========================================================================
    // RUN command
    // ========================================================================
    std::filesystem::path torrent_path = cmd_run.get<std::string>("torrent");
    std::filesystem::path dest_path = cmd_run.get<std::string>("dest");
    uint16_t port = static_cast<uint16_t>(cmd_run.get<int>("--port"));
    bool selective = cmd_run.is_used("--selective");
    bool interactive = !cmd_run.is_used("--non-interactive");

    std::vector<bool> file_selection;
    if (selective) {
        auto files = peek_file_list(torrent_path);
        if (files.empty()) {
            LOGCRITICAL("Failed to read torrent file for selection.");
            return 1;
        }
        std::cout << "\nFiles in torrent:\n";
        for (size_t i = 0; i < files.size(); ++i) {
            std::cout << std::format("  [{:3}] {:70} {:>10}\n",
                                     i, files[i].path.string(), fmt_bytes(files[i].size));
        }
        std::cout << "\nSelect files to download (comma/space-separated numbers, or 'all', or 'none'):\n> " << std::flush;
        std::string line;
        std::getline(std::cin, line);
        line = trim(line);
        file_selection.resize(files.size(), false);
        if (line == "all") {
            std::ranges::fill(file_selection, true);
        } else if (line != "none" && !line.empty()) {
            for (char& c : line) { if (c == ',') c = ' '; }
            std::istringstream ss(line);
            size_t idx;
            while (ss >> idx) {
                if (idx < files.size()) file_selection[idx] = true;
            }
        }
        size_t selected = std::ranges::count(file_selection, true);
        LOGINFO("Selective download: {} of {} files selected.", selected, files.size());
    }

    ClientConfig cfg;
    std::string found_cfg = cli_config_path.empty() ? ClientConfig::find_config_path() : cli_config_path;
    if (!found_cfg.empty()) {
        try { cfg = ClientConfig::load(found_cfg); } catch (...) {}
    }
    ClientConfig cli_overrides = make_cfg(cmd_run);
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

    ClientApp app(cfg);
    if (selective && !file_selection.empty()) {
        app.add_torrent(Mode::Hybrid, torrent_path, dest_path, port, file_selection);
    } else {
        app.add_torrent(Mode::Hybrid, torrent_path, dest_path, port);
    }

    // ========================================================================
    // Interactive input with poll-based character reading
    // ========================================================================
    ScopedRawTerminal raw_tty;
    std::string input_buffer;       // Current line being typed
    std::mutex input_mutex;
    ProgressLogger command_log;     // Submitted command history
    std::atomic<bool> input_active{interactive};

    if (interactive) {
        raw_tty.enable();
        command_log.info("Interactive mode. Type 'h' for help, 'q' to quit.");

        std::thread stdin_thread([&app, &input_buffer, &input_mutex, &command_log,
                                  &input_active, port]() {
            while (input_active.load()) {
                struct pollfd pfd = {STDIN_FILENO, POLLIN, 0};
                int ret = poll(&pfd, 1, 100);
                if (ret <= 0) continue;

                char buf[64];
                ssize_t n = read(STDIN_FILENO, buf, sizeof(buf));
                if (n <= 0) {
                    if (n == 0) break; // EOF
                    continue;
                }

                std::string cmd_line;
                {
                    std::lock_guard lock(input_mutex);
                    for (ssize_t i = 0; i < n; ++i) {
                        char c = buf[i];
                        if (c == '\n' || c == '\r') {
                            cmd_line = std::move(input_buffer);
                            input_buffer.clear();
                        } else if (c == 127 || c == '\b') {
                            if (!input_buffer.empty()) input_buffer.pop_back();
                        } else if (c >= 32 && c < 127) {
                            input_buffer += c;
                        }
                    }
                }

                if (!cmd_line.empty()) {
                    cmd_line = trim(cmd_line);
                    if (cmd_line.empty()) continue;

                    if (cmd_line == "q" || cmd_line == "quit") {
                        command_log.info("Quitting...");
                        app.stop_all();
                        break;
                    } else if (cmd_line == "h" || cmd_line == "help") {
                        command_log.info("Commands: q=quit, a <torrent> [dest]=add torrent, t <url>=add tracker, h=help");
                    } else if (cmd_line[0] == 'a' && cmd_line.size() > 1) {
                        std::string rest = trim(cmd_line.substr(1));
                        auto space = rest.find(' ');
                        std::string tpath = (space != std::string::npos) ? trim(rest.substr(0, space)) : rest;
                        std::string dpath = (space != std::string::npos) ? trim(rest.substr(space + 1)) : "./downloads";
                        if (std::filesystem::exists(tpath)) {
                            app.add_torrent(Mode::Hybrid, tpath, dpath, port);
                            command_log.info("Torrent added: " + tpath);
                        } else {
                            command_log.warning("File not found: " + tpath);
                        }
                    } else if (cmd_line[0] == 't' && cmd_line.size() > 1) {
                        std::string url = trim(cmd_line.substr(1));
                        auto sessions = app.torrents();
                        if (!sessions.empty()) {
                            sessions.rbegin()->second->add_tracker_url(url);
                            command_log.info("Tracker added: " + url);
                        } else {
                            command_log.warning("No active torrent sessions.");
                        }
                    } else {
                        command_log.warning("Unknown: " + cmd_line + "  (type 'h' for help)");
                    }
                }
            }
        });
        stdin_thread.detach();
    }

    // ========================================================================
    // LiveDisplay TUI with panels
    // ========================================================================
    LiveDisplay display(LiveDisplay::Config{std::chrono::milliseconds(333), false, false});

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

    display.add_slot([&app, speed, &input_buffer, &input_mutex,
                      &command_log, &input_active]() -> std::string {
        auto now = std::chrono::steady_clock::now();
        size_t term_w = Terminal::width();
        if (term_w < 50) term_w = 50;

        // Build torrents panel content
        std::string torrents_body;
        auto torrents = app.torrents();
        for (const auto& [hash, session] : torrents) {
            auto state = session->get_state();
            if (!state) continue;

            std::string name = session->get_display_name();
            size_t completed = state->completed_pieces();
            size_t total = state->num_pieces();
            uint64_t downloaded = state->total_bytes_downloaded();
            uint64_t uploaded = state->total_bytes_uploaded();
            size_t peers = session->peer_manager()->connection_count();
            double progress = total > 0 ? (100.0 * completed / total) : 0.0;

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

            // Badge
            if (state->is_download_complete()) {
                torrents_body += Text{" ● SEED "}.color(style::green).bold().str();
            } else {
                torrents_body += Text{" ▓ DOWNLOAD "}.color(style::cyan).bold().str();
            }

            size_t max_name = term_w > 56 ? term_w - 56 : 20;
            std::string ndisp = name;
            if (count_visible_characters(ndisp) > max_name) {
                ndisp = truncate(ndisp, max_name > 3 ? max_name - 3 : 0) + "...";
            }
            torrents_body += " " + Text{ndisp}.bold().str() + "\n";

            size_t bar_w = std::min<size_t>(term_w - 46, 36);
            if (bar_w < 8) bar_w = 8;
            size_t filled = static_cast<size_t>(bar_w * progress / 100.0);
            if (filled > bar_w) filled = bar_w;

            torrents_body += "  [";
            for (size_t i = 0; i < bar_w; ++i) {
                if (i < filled) {
                    auto c = Gradient{{RGB{220,50,50}, RGB{220,180,30}, RGB{50,200,50}}}
                        .at(bar_w > 1 ? static_cast<double>(i) / (bar_w - 1) : 1.0);
                    torrents_body += c.to_ansi_foreground() + "█" + std::string(style::reset);
                } else {
                    torrents_body += Text{"░"}.color(style::bright_black).str();
                }
            }
            torrents_body += "] ";

            torrents_body += Text{std::format("{:5.1f}%", progress)}.color(style::yellow).str() + "  ";
            torrents_body += Text{std::format("Peers: {}", peers)}.color(style::bright_black).str() + "\n";

            torrents_body += "  ";
            torrents_body += Text{std::format("↓ {}/s", fmt_bytes(static_cast<uint64_t>(speed->down_speed)))}.color(style::blue).str() + "  ";
            torrents_body += Text{std::format("↑ {}/s", fmt_bytes(static_cast<uint64_t>(speed->up_speed)))}.color(style::magenta).str() + "  ";
            torrents_body += std::format("DL: {}  UL: {}", fmt_bytes(downloaded), fmt_bytes(uploaded));
            if (!state->is_download_complete()) {
                size_t needed = state->needed_pieces();
                torrents_body += std::format("  Remaining: {} pieces", needed);
            }
            torrents_body += "\n";
        }

        if (torrents.empty()) {
            torrents_body += Text{"  No active torrents."}.color(style::bright_black).str() + "\n";
        }

        std::string panel1 = Panel(Text{torrents_body})
            .title(Text{"Downloading Torrents"}.bold())
            .render(term_w);

        // Build commands panel content
        std::string cmd_body;
        if (input_active.load()) {
            // Input line
            {
                std::lock_guard lock(input_mutex);
                cmd_body += "> ";
                if (!input_buffer.empty()) {
                    cmd_body += input_buffer;
                }
                // Blinking cursor indicator (simulated as a dim underscore)
                cmd_body += Text{"_"}.color(style::bright_black).str();
            }

            // Rule separator
            cmd_body += "\n";
            cmd_body += Rule("History", '-').align(Layout::Justify::Left).color(style::bright_black).render(term_w) + "\n";

            // ProgressLogger: show last 4 submitted commands
            cmd_body += command_log.render_recent(4);
        } else {
            cmd_body += "Interactive input disabled (--non-interactive).";
        }

        std::string panel2 = Panel(Text{cmd_body})
            .title(Text{"Commands"}.bold())
            .padding(0, 1)
            .render(term_w);

        return panel1 + "\n" + panel2;
    });

    display.start();

    int result = app.run();

    display.stop();
    input_active.store(false);
    raw_tty.disable();

    return result;
}
