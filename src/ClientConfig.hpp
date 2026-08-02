#pragma once

#include "Bencode.hpp"

#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

struct ClientConfig {
    uint16_t peer_port = 6881;
    // 0 means unlimited. A low upload cap hurts download speed under BitTorrent's
    // tit-for-tat: peers only unchoke us in proportion to what we upload to them,
    // so capping upload directly throttles the number of peers willing to send us data.
    uint64_t upload_rate_limit = 0;
    uint64_t download_rate_limit = 0;
    uint32_t max_connections = 200;
    uint32_t max_connections_per_ip = 2;
    uint32_t max_half_open = 40;
    uint32_t block_request_timeout_seconds = 30;
    uint32_t peer_ban_corrupt_threshold = 3;
    uint32_t peer_ban_duration_minutes = 60;
    std::string download_dir = "./downloads";
    bool enable_dht = true;
    bool enable_lsd = true;
    bool enable_pex = true;
    std::vector<std::string> dht_bootstrap_nodes = {
        "router.bittorrent.com:6881",
        "dht.transmissionbt.com:6881",
        "router.utorrent.com:6881"
    };

    Dict to_dict() const {
        Dict d;
        d["peer_port"] = Value(static_cast<Integer>(peer_port));
        d["upload_rate_limit"] = Value(static_cast<Integer>(upload_rate_limit));
        d["download_rate_limit"] = Value(static_cast<Integer>(download_rate_limit));
        d["max_connections"] = Value(static_cast<Integer>(max_connections));
        d["max_connections_per_ip"] = Value(static_cast<Integer>(max_connections_per_ip));
        d["max_half_open"] = Value(static_cast<Integer>(max_half_open));
        d["block_request_timeout_seconds"] = Value(static_cast<Integer>(block_request_timeout_seconds));
        d["peer_ban_corrupt_threshold"] = Value(static_cast<Integer>(peer_ban_corrupt_threshold));
        d["peer_ban_duration_minutes"] = Value(static_cast<Integer>(peer_ban_duration_minutes));
        d["download_dir"] = Value(String(download_dir));
        d["enable_dht"] = Value(static_cast<Integer>(enable_dht ? 1 : 0));
        d["enable_lsd"] = Value(static_cast<Integer>(enable_lsd ? 1 : 0));
        d["enable_pex"] = Value(static_cast<Integer>(enable_pex ? 1 : 0));

        List nodes;
        for (const auto& n : dht_bootstrap_nodes) {
            nodes.push_back(Value(String(n)));
        }
        d["dht_bootstrap_nodes"] = Value(std::move(nodes));

        return d;
    }

    static ClientConfig from_dict(const Dict& d) {
        ClientConfig cfg;

        auto get_int = [&](const std::string& key, auto& field) {
            auto it = d.find(key);
            if (it != d.end() && std::holds_alternative<Integer>(it->second.get_variant())) {
                field = static_cast<std::decay_t<decltype(field)>>(std::get<Integer>(it->second.get_variant()));
            }
        };

        auto get_str = [&](const std::string& key, std::string& field) {
            auto it = d.find(key);
            if (it != d.end() && std::holds_alternative<String>(it->second.get_variant())) {
                field = std::get<String>(it->second.get_variant());
            }
        };

        auto get_bool = [&](const std::string& key, bool& field) {
            auto it = d.find(key);
            if (it != d.end() && std::holds_alternative<Integer>(it->second.get_variant())) {
                field = (std::get<Integer>(it->second.get_variant()) != 0);
            }
        };

        auto get_list = [&](const std::string& key, std::vector<std::string>& field) {
            auto it = d.find(key);
            if (it != d.end() && std::holds_alternative<std::unique_ptr<List>>(it->second.get_variant())) {
                const List& lst = *std::get<std::unique_ptr<List>>(it->second.get_variant());
                field.clear();
                for (const auto& v : lst) {
                    if (std::holds_alternative<String>(v.get_variant())) {
                        field.push_back(std::get<String>(v.get_variant()));
                    }
                }
            }
        };

        get_int("peer_port", cfg.peer_port);
        get_int("upload_rate_limit", cfg.upload_rate_limit);
        get_int("download_rate_limit", cfg.download_rate_limit);
        get_int("max_connections", cfg.max_connections);
        get_int("max_connections_per_ip", cfg.max_connections_per_ip);
        get_int("max_half_open", cfg.max_half_open);
        get_int("block_request_timeout_seconds", cfg.block_request_timeout_seconds);
        get_int("peer_ban_corrupt_threshold", cfg.peer_ban_corrupt_threshold);
        get_int("peer_ban_duration_minutes", cfg.peer_ban_duration_minutes);
        get_str("download_dir", cfg.download_dir);
        get_bool("enable_dht", cfg.enable_dht);
        get_bool("enable_lsd", cfg.enable_lsd);
        get_bool("enable_pex", cfg.enable_pex);
        get_list("dht_bootstrap_nodes", cfg.dht_bootstrap_nodes);

        return cfg;
    }

    static ClientConfig load(const std::string& path = "") {
        std::string actual_path = path.empty() ? find_config_path() : path;
        if (actual_path.empty()) return ClientConfig{};

        std::ifstream file(actual_path, std::ios::binary);
        if (!file.is_open()) return ClientConfig{};

        try {
            std::string raw((std::istreambuf_iterator<char>(file)),
                            std::istreambuf_iterator<char>());
            std::vector<std::byte> data(raw.size());
            for (size_t i = 0; i < raw.size(); ++i) {
                data[i] = static_cast<std::byte>(raw[i]);
            }
            Value decoded = decode(data);
            if (std::holds_alternative<std::unique_ptr<Dict>>(decoded.get_variant())) {
                return from_dict(*std::get<std::unique_ptr<Dict>>(decoded.get_variant()));
            }
        } catch (const std::exception&) {
        }

        return ClientConfig{};
    }

    void save(const std::string& path) const {
        std::filesystem::path fs_path(path);
        std::filesystem::create_directories(fs_path.parent_path());

        Dict d = to_dict();
        Value v(d);
        auto encoded = encode(v);

        std::ofstream file(path, std::ios::binary);
        if (!file.is_open()) {
            throw std::runtime_error("Failed to open config file for writing: " + path);
        }
        file.write(reinterpret_cast<const char*>(encoded.data()), encoded.size());
        if (!file.good()) {
            throw std::runtime_error("Failed to write config file: " + path);
        }
    }

    static ClientConfig from_cli(int argc, char* argv[]) {
        ClientConfig cfg;

        std::string config_path;
        for (int i = 1; i < argc - 1; ++i) {
            if (argv[i] && std::string(argv[i]) == "--config") {
                config_path = argv[i + 1];
                break;
            }
        }

        std::string found_path = config_path.empty() ? find_config_path() : config_path;
        if (!found_path.empty()) {
            try {
                cfg = load(found_path);
            } catch (const std::exception&) {
            }
        }

        for (int i = 1; i < argc; ++i) {
            if (!argv[i]) continue;
            std::string arg(argv[i]);

            auto consume = [&]() -> bool {
                if (i + 1 < argc && argv[i + 1]) { ++i; return true; }
                return false;
            };

            if (arg == "--port" && consume()) {
                cfg.peer_port = static_cast<uint16_t>(std::stoul(argv[i]));
            } else if (arg == "--upload-rate" && consume()) {
                cfg.upload_rate_limit = std::stoull(argv[i]);
            } else if (arg == "--download-rate" && consume()) {
                cfg.download_rate_limit = std::stoull(argv[i]);
            } else if (arg == "--max-connections" && consume()) {
                cfg.max_connections = std::stoul(argv[i]);
            } else if (arg == "--max-connections-per-ip" && consume()) {
                cfg.max_connections_per_ip = std::stoul(argv[i]);
            } else if (arg == "--max-half-open" && consume()) {
                cfg.max_half_open = std::stoul(argv[i]);
            } else if (arg == "--block-timeout" && consume()) {
                cfg.block_request_timeout_seconds = std::stoul(argv[i]);
            } else if (arg == "--download-dir" && consume()) {
                cfg.download_dir = argv[i];
            } else if (arg == "--config" && consume()) {
            } else if (arg == "--no-dht") {
                cfg.enable_dht = false;
            } else if (arg == "--no-lsd") {
                cfg.enable_lsd = false;
            } else if (arg == "--no-pex") {
                cfg.enable_pex = false;
            }
        }

        return cfg;
    }

    static std::string find_config_path(const std::string& cli_path = "") {
        if (!cli_path.empty()) {
            if (std::filesystem::exists(cli_path)) return cli_path;
            return "";
        }

        std::filesystem::path local = "./p2p.conf";
        if (std::filesystem::exists(local)) {
            return std::filesystem::absolute(local).string();
        }

        const char* home = std::getenv("HOME");
        if (home) {
            std::filesystem::path config_path = std::filesystem::path(home) / ".config" / "p2p" / "p2p.conf";
            if (std::filesystem::exists(config_path)) {
                return config_path.string();
            }
        }

        return "";
    }
};
