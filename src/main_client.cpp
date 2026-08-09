#include "ClientApp.hpp"
#include "ClientConfig.hpp"
#include "MagnetUri.hpp"
#include "TorrentFile.hpp"
#include "Utils.hpp"

#include <argparse.hpp>
#include <progress_bar.hpp>

#include <boost/asio.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/url.hpp>

#include <atomic>
#include <chrono>
#include <clocale>
#include <csignal>
#include <cstddef>
#include <cstdlib>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <mutex>
#include <optional>
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

// Template helper: performs HTTP GET and parses tracker lines from response body.
// Works with both beast::tcp_stream and beast::ssl_stream<beast::tcp_stream>.
template <typename Stream>
static std::vector<std::string> http_get_trackers(Stream& stream, const std::string& host, const std::string& target) {
    http::request<http::string_body> req{http::verb::get, target, 11};
    req.set(http::field::host, host);
    req.set(http::field::user_agent, "qBittorrent/5.2.3");
    http::write(stream, req);

    beast::flat_buffer buffer;
    http::response<http::string_body> res;
    http::read(stream, buffer, res);

    if (res.result() != http::status::ok) {
        LOGWARN("Fetch trackers returned HTTP {}", static_cast<int>(res.result()));
        return {};
    }

    std::vector<std::string> trackers;
    std::istringstream body_stream(res.body());
    std::string line;
    while (std::getline(body_stream, line)) {
        line = trim(line);
        if (line.empty() || line[0] == '#' || line[0] == '/') continue;
        if (line.back() == '\r') line.pop_back();
        trackers.push_back(line);
    }
    return trackers;
}

// Returns the HTTP proxy (host, port) to use for `host`, honoring
// no_proxy/NO_PROXY (exact host or dot-suffix match) and the scheme-specific
// http(s)_proxy / HTTP(S)_PROXY, falling back to all_proxy/ALL_PROXY.
// Only plain http:// proxies are supported — SOCKS entries are ignored.
//
// This is used ONLY for the tracker-list fetch (`f` command, ngosang.github.io):
// that host is RST'd on some networks (incl. this one) and requires the local
// proxy. Tracker ANNOUNCES, peer connections, and DHT stay direct — they are
// deliberately not proxied (see announce_proxy_for in TrackerClient.hpp).
static std::optional<std::pair<std::string, std::string>> get_http_proxy(
    const std::string& scheme, const std::string& host) {
    const char* no_proxy_env = std::getenv("NO_PROXY");
    if (!no_proxy_env) no_proxy_env = std::getenv("no_proxy");
    if (no_proxy_env) {
        std::istringstream ss(no_proxy_env);
        std::string entry;
        while (std::getline(ss, entry, ',')) {
            entry = trim(entry);
            if (entry.empty()) continue;
            std::string suffix = entry;
            if (!suffix.empty() && suffix.front() == '*') suffix.erase(0, 1);
            if (!suffix.empty() && suffix.front() == '.') suffix.erase(0, 1);
            if (host == suffix ||
                (host.size() > suffix.size() && host.ends_with("." + suffix))) {
                return std::nullopt;
            }
        }
    }

    const char* proxy = std::getenv((scheme + "_proxy").c_str());
    if (!proxy) proxy = std::getenv((scheme + "_PROXY").c_str());
    if (!proxy) proxy = std::getenv("all_proxy");
    if (!proxy) proxy = std::getenv("ALL_PROXY");
    if (!proxy) return std::nullopt;

    std::string proxy_str(proxy);
    if (proxy_str.rfind("socks", 0) == 0 || proxy_str.rfind("SOCKS", 0) == 0) {
        return std::nullopt;  // SOCKS proxying not implemented
    }
    try {
        boost::urls::url u(proxy_str);
        if (u.scheme() != "http") {
            return std::nullopt;
        }
        return std::make_pair(u.host(), u.has_port() ? std::string(u.port()) : "80");
    } catch (const std::exception&) {
        return std::nullopt;
    }
}

static std::vector<std::string> fetch_tracker_list(const std::string& url_str) {
    LOGINFO("Fetching trackers...");
    try {
        using namespace boost::beast;
        using tcp = boost::asio::ip::tcp;

        boost::urls::url u(url_str);
        std::string host = u.host();
        std::string port = u.has_port() ? std::string(u.port()) : (u.scheme() == "https" ? "443" : "80");
        std::string target = u.path() + (u.has_query() ? "?" + u.query() : "");

        boost::asio::io_context ioc;
        tcp::resolver resolver(ioc);

        constexpr auto op_timeout = 5s;
        auto proxy = get_http_proxy(u.scheme() == "https" ? "https" : "http", host);

        if (u.scheme() == "https") {
            boost::asio::ssl::context ssl_ctx(boost::asio::ssl::context::tlsv12_client);
            ssl_ctx.set_default_verify_paths();

            if (proxy) {
                // HTTP proxy CONNECT tunnel: plain TCP to the proxy, CONNECT
                // request, then TLS inside the tunnel (SNI still targets the
                // real host).
                tcp::resolver proxy_resolver(ioc);
                auto const proxy_results = proxy_resolver.resolve(proxy->first, proxy->second);
                beast::tcp_stream stream(ioc);
                stream.expires_after(op_timeout);
                stream.connect(proxy_results);

                http::request<http::empty_body> connect_req{http::verb::connect, host + ":" + port, 11};
                connect_req.set(http::field::host, host + ":" + port);
                stream.expires_after(op_timeout);
                http::write(stream, connect_req);
                beast::flat_buffer connect_buffer;
                // read_header (via a parser), not read: a 2xx CONNECT response
                // has no body and no Content-Length/Transfer-Encoding, so
                // http::read cannot frame it and blocks until the connection
                // closes (observed: hang on every proxy fetch). The header is
                // the whole message; the proxy sends nothing else until our
                // ClientHello, so no stray bytes are lost.
                http::response_parser<http::empty_body> connect_parser;
                stream.expires_after(op_timeout);
                http::read_header(stream, connect_buffer, connect_parser);
                auto connect_res = connect_parser.release();
                if (connect_res.result() != http::status::ok) {
                    LOGWARN("Proxy CONNECT to {}:{} failed: {}", host, port, static_cast<int>(connect_res.result()));
                    return {};
                }

                stream.expires_after(op_timeout);
                beast::ssl_stream<beast::tcp_stream> stream_tls(std::move(stream), ssl_ctx);
                if (!SSL_set_tlsext_host_name(stream_tls.native_handle(), host.c_str())) {
                    beast::error_code ec{static_cast<int>(::ERR_get_error()), asio::error::get_ssl_category()};
                    LOGWARN("HTTPS SNI failure for {}: {}", host, ec.message());
                    return {};
                }
                stream_tls.handshake(asio::ssl::stream_base::client);
                beast::get_lowest_layer(stream_tls).expires_after(op_timeout);
                auto trackers = http_get_trackers(stream_tls, host, target);
                beast::error_code ec;
                stream_tls.shutdown(ec);
                LOGINFO("Fetching done (via proxy {}:{})...", proxy->first, proxy->second);
                return trackers;
            }

            auto const results = resolver.resolve(host, port);
            beast::ssl_stream<beast::tcp_stream> stream(ioc, ssl_ctx);

            if (!SSL_set_tlsext_host_name(stream.native_handle(), host.c_str())) {
                beast::error_code ec{static_cast<int>(::ERR_get_error()), asio::error::get_ssl_category()};
                LOGWARN("HTTPS SNI failure for {}: {}", host, ec.message());
                return {};
            }
            beast::get_lowest_layer(stream).expires_after(op_timeout);
            beast::get_lowest_layer(stream).connect(results);
            beast::get_lowest_layer(stream).expires_after(op_timeout);
            stream.handshake(asio::ssl::stream_base::client);
            beast::get_lowest_layer(stream).expires_after(op_timeout);
            auto trackers = http_get_trackers(stream, host, target);
            beast::error_code ec;
            stream.shutdown(ec);
            LOGINFO("Fetching done...");
            return trackers;
        } else {
            beast::tcp_stream stream(ioc);
            if (proxy) {
                // Plain HTTP through the proxy: request with absolute-form
                // target (http://host/path) per RFC 7230.
                tcp::resolver proxy_resolver(ioc);
                auto const proxy_results = proxy_resolver.resolve(proxy->first, proxy->second);
                stream.expires_after(op_timeout);
                stream.connect(proxy_results);
                stream.expires_after(op_timeout);
                auto trackers = http_get_trackers(stream, host, "http://" + host + target);
                beast::error_code ec;
                stream.socket().shutdown(tcp::socket::shutdown_both, ec);
                LOGINFO("Fetching done (via proxy {}:{})...", proxy->first, proxy->second);
                return trackers;
            }

            auto const results = resolver.resolve(host, port);
            stream.expires_after(op_timeout);
            stream.connect(results);
            stream.expires_after(op_timeout);
            auto trackers = http_get_trackers(stream, host, target);
            beast::error_code ec;
            stream.socket().shutdown(tcp::socket::shutdown_both, ec);
            LOGINFO("Fetching done...");
            return trackers;
        }
    } catch (const std::exception& e) {
        LOGWARN("Failed to fetch tracker list from {}: {}", url_str, e.what());
        return {};
    }
}

int main(int argc, char* argv[]) {
    using namespace progressbar;

    // wcwidth() must see the user's locale (not the default "C") so the TUI's
    // character-width computation matches how the terminal lays out columns.
    // Without this, wide glyphs like CJK would be miscounted and the panel
    // borders would misalign under any non-C locale.
    std::setlocale(LC_ALL, "");

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
    cmd_run.add_argument("torrent").help("Path to .torrent file").default_value(std::string{});
    cmd_run.add_argument("dest").help("Content / destination directory").default_value(std::string{});
    cmd_run.add_argument("--port").help("Peer listening port").scan<'d', int>().default_value(6881);
    cmd_run.add_argument("--upload-rate").help("Upload rate limit (bytes/s)").scan<'u', uint64_t>().default_value(uint64_t{0});
    cmd_run.add_argument("--download-rate").help("Download rate limit (bytes/s), 0 = unlimited").scan<'u', uint64_t>().default_value(uint64_t{0});
    cmd_run.add_argument("--max-connections").help("Max peer connections").scan<'u', uint32_t>().default_value(uint32_t{500});
    cmd_run.add_argument("--max-connections-per-ip").help("Max connections per IP").scan<'u', uint32_t>().default_value(uint32_t{4});
    cmd_run.add_argument("--max-half-open").help("Max half-open connections").scan<'u', uint32_t>().default_value(uint32_t{500});
    cmd_run.add_argument("--block-timeout").help("Block request timeout (seconds)").scan<'u', uint32_t>().default_value(uint32_t{30});
    cmd_run.add_argument("--download-dir").help("Default download directory").default_value(std::string{"./downloads"});
    cmd_run.add_argument("--no-dht").help("Disable DHT").flag();
    cmd_run.add_argument("--no-lsd").help("Disable local peer discovery").flag();
    cmd_run.add_argument("--no-pex").help("Disable peer exchange").flag();
    cmd_run.add_argument("--no-encryption").help("Disable protocol encryption (MSE)").flag();
    cmd_run.add_argument("--selective").help("Prompt for file selection before download").flag();
    cmd_run.add_argument("--non-interactive").help("Disable interactive TUI commands").flag();
    cmd_run.add_argument("--profile").help("Enable CTRACK function profiling and print results on exit").flag();

    prog.add_subparser(cmd_create);
    prog.add_subparser(cmd_run);

    try {
        prog.parse_args(argc, argv);
    } catch (const std::exception& e) {
        std::cerr << e.what() << "\n" << prog;
        return 1;
    }

    // Profiling is opt-in: without --profile, CTRACK/CTRACK_ASYNC are runtime
    // no-ops and no profiling report is printed at exit.
    if (!(prog.is_subcommand_used("run") && cmd_run.is_used("--profile")))
        ctrack::set_profiling_enabled(false);

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
        c.enable_encryption = !p.is_used("--no-encryption");
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
    std::string torrent_arg = cmd_run.get<std::string>("torrent");
    std::string dest_arg = cmd_run.get<std::string>("dest");
    uint16_t port = static_cast<uint16_t>(cmd_run.get<int>("--port"));
    bool interactive = !cmd_run.is_used("--non-interactive");

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
    cfg.enable_encryption = cli_overrides.enable_encryption;

    ClientApp app(cfg);
    std::string default_download_dir = cfg.download_dir;

    const char* home_dir = std::getenv("HOME");
    auto state_path = std::filesystem::path(home_dir ? home_dir : ".") / ".config" / "p2p" / "client_state.bencode";

    if (!torrent_arg.empty()) {
        std::string initial_dest = dest_arg.empty() ? default_download_dir : dest_arg;
        app.add_torrent(Mode::Hybrid, torrent_arg, initial_dest, port);
        LOGINFO("Initial torrent: {} -> {}", torrent_arg, initial_dest);
    } else if (std::filesystem::exists(state_path)) {
        LOGINFO("Loading client state from {}...", state_path.string());
        app.load_state(state_path, port);
        default_download_dir = app.config().download_dir;
    }

    // ========================================================================
    // Interactive input with poll-based character reading
    // ========================================================================
    ScopedRawTerminal raw_tty;
    std::string input_buffer;
    std::mutex input_mutex;
    ProgressLogger command_log;
    auto input_active = std::make_shared<std::atomic<bool>>(interactive);
    size_t cursor_pos = 0;
    std::vector<std::string> cmd_history;
    size_t history_pos = 0;

    if (interactive) {
        raw_tty.enable();
        command_log.info("Interactive mode: q=quit, h=help");

        std::thread stdin_thread([&app, &input_buffer, &input_mutex, &command_log,
                                  input_active, port, &default_download_dir,
                                  &cursor_pos, &cmd_history, &history_pos,
                                  &state_path]() {
            while (input_active->load()) {
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
                    std::lock_guard lock(input_mutex);
                    for (ssize_t i = 0; i < n; ++i) {
                        char c = buf[i];
                        if (c == '\n' || c == '\r') {
                            cmd_line = std::move(input_buffer);
                            input_buffer.clear();
                            cursor_pos = 0;
                        } else if (c == 127 || c == '\b') {
                            if (cursor_pos > 0 && !input_buffer.empty()) {
                                input_buffer.erase(cursor_pos - 1, 1);
                                --cursor_pos;
                            }
                        } else if (c == '\x1b' && i + 2 < n && buf[i + 1] == '[') {
                            // Escape sequence: \x1b[<char>
                            switch (buf[i + 2]) {
                                case 'A': // Up — history back
                                    if (!cmd_history.empty()) {
                                        if (history_pos > 0) {
                                            --history_pos;
                                            input_buffer = cmd_history[history_pos];
                                            cursor_pos = input_buffer.size();
                                        }
                                    }
                                    break;
                                case 'B': // Down — history forward
                                    if (history_pos < cmd_history.size()) {
                                        ++history_pos;
                                        if (history_pos >= cmd_history.size()) {
                                            input_buffer.clear();
                                            cursor_pos = 0;
                                        } else {
                                            input_buffer = cmd_history[history_pos];
                                            cursor_pos = input_buffer.size();
                                        }
                                    }
                                    break;
                                case 'C': // Right
                                    if (cursor_pos < input_buffer.size()) ++cursor_pos;
                                    break;
                                case 'D': // Left
                                    if (cursor_pos > 0) --cursor_pos;
                                    break;
                            }
                            i += 2;
                        } else if (c >= 32 && c < 127) {
                            input_buffer.insert(cursor_pos, 1, c);
                            ++cursor_pos;
                        }
                    }
                }

                if (!cmd_line.empty()) {
                    if (cmd_history.empty() || cmd_history.back() != cmd_line) {
                        cmd_history.push_back(cmd_line);
                    }
                    history_pos = cmd_history.size();

                    cmd_line = trim(cmd_line);
                    if (cmd_line.empty()) continue;

                    if (cmd_line == "q" || cmd_line == "quit") {
                        command_log.info("Quitting...");
                        app.stop_all();
                        break;
                    } else if (cmd_line == "h" || cmd_line == "help") {
                        command_log.info(
                            "Commands:\n"
                            "  a <torrent> [dest]  - Add .torrent file\n"
                            "  m <magnet> [dest]   - Add magnet link\n"
                            "  d <path>            - Set default download dir\n"
                            "  t <url>             - Add tracker to all torrents\n"
                            "  f <url>             - Fetch trackers list from URL\n"
                            "  s <idx>             - Stop torrent at index\n"
                            "  p <idx>             - Resume stopped torrent\n"
                            "  r <idx>             - Remove torrent at index\n"
                            "  q                   - Quit\n"
                            "  h                   - This help"
                        );
                    } else if (cmd_line[0] == 'a' && cmd_line.size() > 1) {
                        std::string rest = trim(cmd_line.substr(1));
                        auto space = rest.find(' ');
                        std::string tpath = (space != std::string::npos) ? trim(rest.substr(0, space)) : rest;
                        std::string dpath = (space != std::string::npos) ? trim(rest.substr(space + 1)) : default_download_dir;
                        if (std::filesystem::exists(tpath)) {
                            app.add_torrent(Mode::Hybrid, tpath, dpath, port);
                            command_log.info("Added: " + std::filesystem::path(tpath).filename().string());
                        } else {
                            command_log.warning("File not found: " + tpath);
                        }
                    } else if (cmd_line[0] == 'm' && cmd_line.size() > 1) {
                        std::string rest = trim(cmd_line.substr(1));
                        auto space = rest.find(' ');
                        std::string magnet = (space != std::string::npos) ? trim(rest.substr(0, space)) : rest;
                        std::string dpath = (space != std::string::npos) ? trim(rest.substr(space + 1)) : default_download_dir;
                        try {
                            app.add_torrent_magnet(magnet, dpath, port);
                            command_log.info("Magnet torrent added");
                        } catch (const std::exception& e) {
                            command_log.warning("Bad magnet URI: " + std::string(e.what()));
                        }
                    } else if (cmd_line[0] == 'd' && cmd_line.size() > 1) {
                        default_download_dir = trim(cmd_line.substr(1));
                        command_log.info("Default download dir: " + default_download_dir);
                    } else if (cmd_line[0] == 't' && cmd_line.size() > 1) {
                        std::string url = trim(cmd_line.substr(1));
                        app.add_tracker_to_all(url);
                        command_log.info("Tracker added to all: " + url);
                    } else if (cmd_line[0] == 'f' && cmd_line.size() > 1) {
                        std::string url = trim(cmd_line.substr(1));
                        command_log.info("Fetching trackers from " + url + " (background)...");
                        std::thread([url, &command_log, &app]() {
                            try {
                                auto trackers = fetch_tracker_list(url);
                                if (trackers.empty()) {
                                    command_log.warning("No trackers fetched from " + url);
                                } else {
                                    app.add_trackers_to_all(trackers);
                                    command_log.info(std::format("Fetched {} tracker(s) from {}", trackers.size(), url));
                                }
                            } catch (...) {
                                command_log.warning("Fetch threw for " + url);
                            }
                        }).detach();
                    } else if (cmd_line[0] == 'p' && cmd_line.size() > 1) {
                        std::string idx_str = trim(cmd_line.substr(1));
                        char* end = nullptr;
                        long idx = std::strtol(idx_str.c_str(), &end, 10);
                        if (end == idx_str.c_str() || idx < 0) {
                            command_log.warning("Usage: p <index>");
                        } else {
                            app.resume_torrent(static_cast<size_t>(idx));
                            command_log.info("Resumed torrent at index " + idx_str);
                        }
                    } else if ((cmd_line[0] == 's' || cmd_line[0] == 'r') && cmd_line.size() > 1) {
                        std::string idx_str = trim(cmd_line.substr(1));
                        char* end = nullptr;
                        long idx = std::strtol(idx_str.c_str(), &end, 10);
                        if (end == idx_str.c_str() || idx < 0) {
                            command_log.warning("Usage: " + std::string(1, cmd_line[0]) + " <index>");
                        } else {
                            auto session = app.torrent_by_index(static_cast<size_t>(idx));
                            if (!session) {
                                command_log.warning("No torrent at index " + idx_str);
                            } else if (cmd_line[0] == 's') {
                                app.stop_torrent(static_cast<size_t>(idx));
                                command_log.info("Stopped: " + session->get_display_name());
                            } else {
                                app.remove_torrent(static_cast<size_t>(idx));
                                command_log.info("Removed: " + session->get_display_name());
                                // Persist the removal immediately so
                                // client_state.bencode no longer lists the
                                // torrent, even if the client is later killed
                                // before the shutdown save.
                                app.save_state(state_path);
                            }
                        }
                    } else {
                        command_log.warning("Unknown: '" + cmd_line + "'  (type 'h' for help)");
                    }
                }
            }
        });
        stdin_thread.detach();
    }

    // ========================================================================
    // LiveDisplay TUI with panels
    // ========================================================================
    LiveDisplay display(LiveDisplay::Config{std::chrono::milliseconds(50), false, false});

    static constexpr double kEmaAlpha = 0.35;
    struct SpeedState {
        std::chrono::steady_clock::time_point last_time;
        uint64_t last_down{0};
        uint64_t last_up{0};
        double down_speed{0};
        double up_speed{0};
        bool first_sample{true};
    };
    // Per-torrent speed tracking keyed by InfoHash
    std::map<InfoHash, SpeedState> speeds;

    display.add_slot([&app, &speeds, &input_buffer, &input_mutex,
                      &command_log, input_active, &cursor_pos]() -> std::string {
        auto now = std::chrono::steady_clock::now();
        size_t term_w = Terminal::width();
        if (term_w < 50) term_w = 50;

        // Build torrents panel content
        std::string torrents_body;
        auto torrents = app.torrents();
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
            double progress = total > 0 ? (100.0 * completed / total) : 0.0;

            auto& sp = speeds[hash];
            // Detect session replacement (stop + resume creates a new session
            // with the same InfoHash but fresh byte counters).  Reset speed
            // tracking to avoid a huge negative raw rate.
            if (!sp.first_sample && downloaded < sp.last_down) {
                sp = SpeedState{};
            }
            auto elapsed = std::chrono::duration_cast<std::chrono::duration<double>>(now - sp.last_time).count();
            if (elapsed >= 0.5) {
                if (sp.first_sample) {
                    sp.down_speed = 0;
                    sp.up_speed = 0;
                    sp.first_sample = false;
                } else {
                    double raw_down = (downloaded - sp.last_down) / elapsed;
                    double raw_up = (uploaded - sp.last_up) / elapsed;
                    sp.down_speed = kEmaAlpha * raw_down + (1.0 - kEmaAlpha) * sp.down_speed;
                    sp.up_speed = kEmaAlpha * raw_up + (1.0 - kEmaAlpha) * sp.up_speed;
                }
                sp.last_time = now;
                sp.last_down = downloaded;
                sp.last_up = uploaded;
            }

            // Badge (reads persisted stopped flag, not runtime state)
            // if (app.is_stopping()) {
            //     // Global shutdown in progress — don't flip to stopped
            //     if (state->is_download_complete()) {
            //         torrents_body += Text{" ✅ SEED "}.color(style::green).bold().str();
            //     } else if (app.is_torrent_stopped(hash)) {
            //         torrents_body += Text{" ⏸ STOPPED "}.color(style::yellow).bold().str();
            //     } else {
            //         torrents_body += Text{" ⬇ DOWNLOAD "}.color(style::cyan).bold().str();
            //     }
            // } else 
            if (app.is_torrent_stopped(hash)) {
                torrents_body += Text{" ⏸ STOPPED "}.color(style::yellow).bold().str();
            } else if (state->is_download_complete()) {
                torrents_body += Text{" ✅ SEED "}.color(style::green).bold().str();
            } else {
                torrents_body += Text{" ⬇ DOWNLOAD "}.color(style::cyan).bold().str();
            }

            size_t max_name = term_w > 60 ? term_w - 60 : 20;
            std::string ndisp = std::format("[{}] {}", tor_idx, name);
            if (count_visible_characters(ndisp) > max_name) {
                ndisp = truncate(ndisp, max_name > 3 ? max_name - 3 : 0) + "...";
            }
            torrents_body += " " + Text{ndisp}.bold().str() + "\n";

            // The bar is budgeted in terminal COLUMNS, not cells: the fill
            // glyph "😎" is 2 columns wide, so counting it as one cell makes
            // the rendered bar grow with progress.  Budget columns and pack
            // pairs of columns into emoji, with a 1-column half block for an
            // odd remainder, so "]" always sits at the same position.
            size_t bar_cols = std::min<size_t>(term_w - 46, 36);
            if (bar_cols < 8) bar_cols = 8;
            size_t filled_cols = static_cast<size_t>(bar_cols * progress / 100.0);
            if (filled_cols > bar_cols) filled_cols = bar_cols;
            size_t emoji_count = filled_cols / 2;   // 😎 = 2 columns each
            size_t half_col = filled_cols % 2;      // 1 leftover column
            size_t empty_cols = bar_cols - filled_cols;

            torrents_body += "[";
            for (size_t i = 0; i < emoji_count; ++i) {
                auto c = Gradient{{RGB{220,50,50}, RGB{220,180,30}, RGB{50,200,50}}}
                    .at(bar_cols > 1 ? static_cast<double>(i * 2) / (bar_cols - 1) : 1.0);
                torrents_body += c.to_ansi_foreground() + "😎" + std::string(style::reset);
            }
            if (half_col) {
                auto c = Gradient{{RGB{220,50,50}, RGB{220,180,30}, RGB{50,200,50}}}
                    .at(bar_cols > 1 ? static_cast<double>(filled_cols - 1) / (bar_cols - 1) : 1.0);
                torrents_body += c.to_ansi_foreground() + " " + std::string(style::reset);
            }
            for (size_t i = 0; i < empty_cols; ++i) {
                torrents_body += Text{" "}.color(style::bright_black).str();
            }
            torrents_body += "]";

            torrents_body += Text{std::format(" {:5.1f}%", progress)}.color(style::yellow).str();
            if (app.is_torrent_stopped(hash)) {
                torrents_body += "\n\n";
                ++tor_idx;
                continue;
            }
            torrents_body += "  |  ";
            size_t trackers = session->connected_tracker_count();
            torrents_body += Text{std::format("Peers: {}  Trackers: {}", peers, trackers)}.color(style::bright_black).str() + " | ";

            torrents_body += "  ";
            torrents_body += Text{std::format("↓ {}/s", fmt_bytes(static_cast<uint64_t>(sp.down_speed)))}.color(style::blue).str() + "  ";
            torrents_body += Text{std::format("↑ {}/s", fmt_bytes(static_cast<uint64_t>(sp.up_speed)))}.color(style::magenta).str() + "  ";
            torrents_body += std::format("DL: {}  UL: {}", fmt_bytes(downloaded), fmt_bytes(uploaded));
            if (!state->is_download_complete()) {
                if (total == 0) {
                    // Metadata-download mode (magnet link): pieces are not
                    // known until a peer supplies the torrent metadata.
                    torrents_body += std::format("  Awaiting metadata ({} peers connected)", peers);
                } else {
                    size_t needed = state->needed_pieces();
                    torrents_body += std::format("  Remaining: {} pieces", needed);
                }
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

        // Build commands panel content
        std::string cmd_body;
        if (input_active->load()) {
            {
                std::lock_guard lock(input_mutex);
                cmd_body += "> ";
                cmd_body += input_buffer.substr(0, cursor_pos);
                cmd_body += Text{"_"}.color(style::bright_black).str();
                cmd_body += input_buffer.substr(cursor_pos);
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

    // Per-second speed logger: appends "unix_ts download_bps upload_bps"
    // to logs/speed.txt (aggregate across all torrents), so speed changes
    // can be inspected over time instead of only in the live TUI.
    // Logs while the app runs, regardless of interactive mode (input_active
    // is false under --non-interactive, which would otherwise starve it).
    auto speed_log_active = std::make_shared<std::atomic<bool>>(true);
    std::thread speed_logger([&app, speed_log_active]() {
        std::filesystem::create_directories("logs");
        std::ofstream out("logs/speed.txt", std::ios::app);
        if (!out) return;
        auto last = std::chrono::steady_clock::now();
        uint64_t last_down = 0, last_up = 0;
        bool first = true;
        while (speed_log_active->load()) {
            // Sleep in small increments so the join after shutdown returns
            // promptly instead of waiting out the full 1s tick.
            const auto wake_at = std::chrono::steady_clock::now() + std::chrono::seconds(1);
            while (speed_log_active->load() && std::chrono::steady_clock::now() < wake_at) {
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
            }
            if (!speed_log_active->load()) break;
            uint64_t down = 0, up = 0;
            for (const auto& [hash, session] : app.torrents()) {
                auto st = session->get_state();
                if (!st) continue;
                down += st->total_bytes_downloaded();
                up += st->total_bytes_uploaded();
            }
            auto now = std::chrono::steady_clock::now();
            double dt = std::chrono::duration<double>(now - last).count();
            if (!first && dt > 0 && down >= last_down) {
                auto now_s = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();
                out << now_s << " "
                    << static_cast<uint64_t>((down - last_down) / dt) << " "
                    << static_cast<uint64_t>((up - last_up) / dt) << "\n";
                out.flush();
            }
            first = false;
            last = now;
            last_down = down;
            last_up = up;
        }
    });

    int result;
    {
        CTRACK;
        result = app.run();
    }

    display.stop();
    speed_log_active->store(false);
    input_active->store(false);
    if (speed_logger.joinable()) {
        speed_logger.join();
    }
    raw_tty.disable();

    app.save_state(state_path);

    if (ctrack::profiling_is_enabled())
        ctrack::result_print();

    // Skip normal teardown: destroying the io_context joins the resolver
    // thread pool, which waits for the one in-flight getaddrinfo (up to
    // the system DNS timeout — tens of seconds when the ~250-tracker
    // fan-out storms the resolver; these ops cannot be cancelled). The
    // state was just saved and the logger flushes synchronously on info,
    // so nothing is lost by exiting directly; the OS reclaims any
    // coroutine frames still suspended in DNS.
    std::_Exit(result);
}
