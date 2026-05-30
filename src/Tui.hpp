#pragma once

#include "ClientApp.hpp"
#include "TrackerClient.hpp"

#include <progress_bar.hpp>

#include <csignal>
#include <mutex>
#include <thread>

// -----------------------------------------------------------------------
// RAII terminal raw mode: disables echo and canonical (line-buffered) input
// so we can read keystrokes character-by-character. Keeps ISIG so CTRL+C
// still sends SIGINT for graceful shutdown.
// -----------------------------------------------------------------------
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

// -----------------------------------------------------------------------
// Interactive command loop and LiveDisplay integration for the client CLI.
// Owns the stdin reader thread, command history, and rendering state.
// -----------------------------------------------------------------------
class ClientTui {
public:
    ClientTui(ClientApp& app, uint16_t port, std::string default_download_dir)
        : app_(app), port_(port), default_download_dir_(std::move(default_download_dir)) {}

    void start() {
        raw_tty_.enable();
        command_log_.info("Interactive mode: q=quit, h=help");

        input_active_ = true;

        stdin_thread_ = std::thread([this]() { stdin_loop(); });

        setup_display();
        display_.start();
    }

    void stop() {
        bool expected = true;
        if (!ui_active_.compare_exchange_strong(expected, false, std::memory_order_relaxed)) {
            return;
        }
        input_active_.store(false, std::memory_order_relaxed);
        raw_tty_.disable();
        display_.stop();
        if (stdin_thread_.joinable()) {
            stdin_thread_.join();
        }
    }

private:
    // -----------------------------------------------------------------------
    // Stdin reader + command dispatch
    // -----------------------------------------------------------------------
    void stdin_loop() {
        while (input_active_.load()) {
            struct pollfd pfd = {STDIN_FILENO, POLLIN, 0};
            int ret = poll(&pfd, 1, 16);
            if (ret <= 0) continue;

            char buf[64];
            ssize_t n = read(STDIN_FILENO, buf, sizeof(buf));
            if (n <= 0) {
                if (n == 0) break;
                continue;
            }

            std::string cmd_line;
            {
                std::lock_guard lock(input_mutex_);
                for (ssize_t i = 0; i < n; ++i) {
                    char c = buf[i];
                    if (c == '\n' || c == '\r') {
                        cmd_line = std::move(input_buffer_);
                        input_buffer_.clear();
                        cursor_pos_ = 0;
                    } else if (c == 127 || c == '\b') {
                        if (cursor_pos_ > 0 && !input_buffer_.empty()) {
                            input_buffer_.erase(cursor_pos_ - 1, 1);
                            --cursor_pos_;
                        }
                    } else if (c == '\x1b' && i + 2 < n && buf[i + 1] == '[') {
                        switch (buf[i + 2]) {
                            case 'A':
                                if (!cmd_history_.empty() && history_pos_ > 0) {
                                    --history_pos_;
                                    input_buffer_ = cmd_history_[history_pos_];
                                    cursor_pos_ = input_buffer_.size();
                                }
                                break;
                            case 'B':
                                if (history_pos_ < cmd_history_.size()) {
                                    ++history_pos_;
                                    if (history_pos_ >= cmd_history_.size()) {
                                        input_buffer_.clear();
                                        cursor_pos_ = 0;
                                    } else {
                                        input_buffer_ = cmd_history_[history_pos_];
                                        cursor_pos_ = input_buffer_.size();
                                    }
                                }
                                break;
                            case 'C':
                                if (cursor_pos_ < input_buffer_.size()) ++cursor_pos_;
                                break;
                            case 'D':
                                if (cursor_pos_ > 0) --cursor_pos_;
                                break;
                        }
                        i += 2;
                    } else if (c >= 32 && c < 127) {
                        input_buffer_.insert(cursor_pos_, 1, c);
                        ++cursor_pos_;
                    }
                }
            }

            if (!cmd_line.empty()) {
                if (cmd_history_.empty() || cmd_history_.back() != cmd_line) {
                    cmd_history_.push_back(cmd_line);
                }
                history_pos_ = cmd_history_.size();
                execute_command(trim(cmd_line));
            }
        }
    }

    void execute_command(const std::string& cmd_line) {
        if (cmd_line.empty()) return;

        if (cmd_line == "q" || cmd_line == "quit") {
            command_log_.info("Quitting...");
            app_.stop_all();
            return;
        }

        if (cmd_line == "h" || cmd_line == "help") {
            command_log_.info(
                "Commands:\n"
                "  a <torrent> [dest]  - Add .torrent file\n"
                "  m <magnet> [dest]   - Add magnet link\n"
                "  d <path>            - Set default download dir\n"
                "  t <url>             - Add tracker to all torrents\n"
                "  f <url>             - Fetch trackers list from URL\n"
                "  s <idx>             - Stop torrent at index\n"
                "  r <idx>             - Remove torrent at index\n"
                "  q                   - Quit\n"
                "  h                   - This help"
            );
            return;
        }

        if (cmd_line[0] == 'a' && cmd_line.size() > 1) {
            std::string rest = trim(cmd_line.substr(1));
            auto space = rest.find(' ');
            std::string tpath = (space != std::string::npos) ? trim(rest.substr(0, space)) : rest;
            std::string dpath = (space != std::string::npos) ? trim(rest.substr(space + 1)) : default_download_dir_;
            if (std::filesystem::exists(tpath)) {
                app_.add_torrent(Mode::Hybrid, tpath, dpath, port_);
                command_log_.info("Added: " + std::filesystem::path(tpath).filename().string());
            } else {
                command_log_.warning("File not found: " + tpath);
            }
            return;
        }

        if (cmd_line[0] == 'm' && cmd_line.size() > 1) {
            std::string rest = trim(cmd_line.substr(1));
            auto space = rest.find(' ');
            std::string magnet = (space != std::string::npos) ? trim(rest.substr(0, space)) : rest;
            std::string dpath = (space != std::string::npos) ? trim(rest.substr(space + 1)) : default_download_dir_;
            try {
                app_.add_torrent_magnet(magnet, dpath, port_);
                command_log_.info("Magnet torrent added");
            } catch (const std::exception& e) {
                command_log_.warning("Bad magnet URI: " + std::string(e.what()));
            }
            return;
        }

        if (cmd_line[0] == 'd' && cmd_line.size() > 1) {
            default_download_dir_ = trim(cmd_line.substr(1));
            command_log_.info("Default download dir: " + default_download_dir_);
            return;
        }

        if (cmd_line[0] == 't' && cmd_line.size() > 1) {
            std::string url = trim(cmd_line.substr(1));
            app_.add_tracker_to_all(url);
            command_log_.info("Tracker added to all: " + url);
            return;
        }

        if (cmd_line[0] == 'f' && cmd_line.size() > 1) {
            std::string url = trim(cmd_line.substr(1));
            command_log_.info("Fetching trackers from " + url + " (background)...");
            std::thread([this, url]() {
                try {
                    auto trackers = fetch_tracker_list(url);
                    if (trackers.empty()) {
                        command_log_.warning("No trackers fetched from " + url);
                    } else {
                        app_.add_trackers_to_all(trackers);
                        command_log_.info(std::format("Fetched {} tracker(s) from {}", trackers.size(), url));
                    }
                } catch (...) {
                    command_log_.warning("Fetch threw for " + url);
                }
            }).detach();
            return;
        }

        if ((cmd_line[0] == 's' || cmd_line[0] == 'r') && cmd_line.size() > 1) {
            std::string idx_str = trim(cmd_line.substr(1));
            char* end = nullptr;
            long idx = std::strtol(idx_str.c_str(), &end, 10);
            if (end == idx_str.c_str() || idx < 0) {
                command_log_.warning("Usage: " + std::string(1, cmd_line[0]) + " <index>");
            } else {
                auto session = app_.torrent_by_index(static_cast<size_t>(idx));
                if (!session) {
                    command_log_.warning("No torrent at index " + idx_str);
                } else if (cmd_line[0] == 's') {
                    app_.stop_torrent(static_cast<size_t>(idx));
                    command_log_.info("Stopped: " + session->get_display_name());
                } else {
                    app_.remove_torrent(static_cast<size_t>(idx));
                    command_log_.info("Removed: " + session->get_display_name());
                }
            }
            return;
        }

        command_log_.warning("Unknown: '" + cmd_line + "'  (type 'h' for help)");
    }

    // -----------------------------------------------------------------------
    // LiveDisplay slot — renders torrents panel + commands panel
    // -----------------------------------------------------------------------
    void setup_display() {
        using namespace progressbar;


        static constexpr double kEmaAlpha = 0.35;
        struct SpeedState {
            std::chrono::steady_clock::time_point last_time;
            uint64_t last_down{0};
            uint64_t last_up{0};
            double down_speed{0};
            double up_speed{0};
            bool first_sample{true};
        };
        auto speeds = std::make_shared<std::map<InfoHash, SpeedState>>();
        auto frozen = std::make_shared<std::string>();

        display_.add_slot([this, speeds, frozen]() -> std::string {
            if (!ui_active_.load(std::memory_order_relaxed)) {
                return *frozen;
            }
            auto now = std::chrono::steady_clock::now();
            size_t term_w = Terminal::width();
            if (term_w < 50) term_w = 50;

            std::string torrents_body;
            auto torrents = app_.torrents();
            size_t tor_idx = 0;
            for (const auto& [hash, session] : torrents) {
                auto state = session->get_state();
                if (!state) continue;

                std::string name = session->get_display_name();
                size_t completed = state->completed_pieces();
                size_t total = state->num_pieces();
                uint64_t downloaded = state->total_bytes_downloaded();
                uint64_t uploaded = state->total_bytes_uploaded();
                size_t peers = session->peer_manager()->connection_count();
                size_t trackers = session->connected_tracker_count();
                double progress = total > 0 ? (100.0 * completed / total) : 0.0;

                auto& speed = (*speeds)[hash];
                if (speed.last_time == std::chrono::steady_clock::time_point{}) {
                    speed.last_time = now;
                    speed.last_down = downloaded;
                    speed.last_up = uploaded;
                }

                auto elapsed = std::chrono::duration_cast<std::chrono::duration<double>>(now - speed.last_time).count();
                if (elapsed >= 0.5) {
                    if (speed.first_sample) {
                        speed.down_speed = 0;
                        speed.up_speed = 0;
                        speed.first_sample = false;
                    } else {
                        double raw_down = (downloaded - speed.last_down) / elapsed;
                        double raw_up = (uploaded - speed.last_up) / elapsed;
                        speed.down_speed = kEmaAlpha * raw_down + (1.0 - kEmaAlpha) * speed.down_speed;
                        speed.up_speed = kEmaAlpha * raw_up + (1.0 - kEmaAlpha) * speed.up_speed;
                    }
                    speed.last_time = now;
                    speed.last_down = downloaded;
                    speed.last_up = uploaded;
                }

                if (state->is_download_complete()) {
                    torrents_body += Text{" ● SEED "}.color(style::green).bold().str();
                } else {
                    torrents_body += Text{" ▓ DOWNLOAD "}.color(style::cyan).bold().str();
                }

                size_t max_name = term_w > 60 ? term_w - 60 : 20;
                std::string ndisp = std::format("[{}] {}", tor_idx, name);
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
                torrents_body += Text{std::format("Peers: {}  Trackers: {}", peers, trackers)}.color(style::bright_black).str() + " |";

                torrents_body += "  ";
                torrents_body += Text{std::format("↓ {}/s", fmt_bytes(static_cast<uint64_t>(speed.down_speed)))}.color(style::blue).str() + "  ";
                torrents_body += Text{std::format("↑ {}/s", fmt_bytes(static_cast<uint64_t>(speed.up_speed)))}.color(style::magenta).str() + "  ";
                torrents_body += std::format("DL: {}  UL: {}", fmt_bytes(downloaded), fmt_bytes(uploaded));
                if (!state->is_download_complete()) {
                    size_t needed = state->needed_pieces();
                    torrents_body += std::format("  Remaining: {} pieces", needed);
                }
                torrents_body += "\n\n";
                ++tor_idx;
            }

            if (torrents.empty()) {
                torrents_body += Text{"  No active torrents."}.color(style::bright_black).str() + "\n";
            }

            std::string panel1 = Panel(Text{torrents_body})
                .title(Text{"Downloading Torrents"}.bold())
                .render(term_w);

            std::string cmd_body;
            if (input_active_.load()) {
                {
                    std::lock_guard lock(input_mutex_);
                    cmd_body += "> ";
                    cmd_body += input_buffer_.substr(0, cursor_pos_);
                    cmd_body += Text{"_"}.color(style::bright_black).str();
                    cmd_body += input_buffer_.substr(cursor_pos_);
                }
                cmd_body += "\n";
                cmd_body += Rule("History", '-').align(Layout::Justify::Left).color(style::bright_black).render(term_w) + "\n";
                cmd_body += command_log_.render_recent(4);
            } else {
                cmd_body += "Interactive input disabled (--non-interactive).";
            }

            std::string panel2 = Panel(Text{cmd_body})
                .title(Text{"Commands"}.bold())
                .padding(0, 1)
                .render(term_w);

            *frozen = panel1 + "\n" + panel2;
            return *frozen;
        });
    }

    ClientApp& app_;
    uint16_t port_;
    std::string default_download_dir_;

    ScopedRawTerminal raw_tty_;
    progressbar::LiveDisplay display_{progressbar::LiveDisplay::Config{std::chrono::milliseconds(50), false, false}};

    std::thread stdin_thread_;
    std::atomic<bool> input_active_{false};
    std::atomic<bool> ui_active_{true};

    std::string input_buffer_;
    mutable std::mutex input_mutex_;
    size_t cursor_pos_{0};
    std::vector<std::string> cmd_history_;
    size_t history_pos_{0};

    progressbar::ProgressLogger command_log_;
};
