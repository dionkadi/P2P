#include "ClientConfig.hpp"
#include "Bencode.hpp"

#include <gtest/gtest.h>

#include <cstdio>
#include <cstring>
#include <fstream>
#include <vector>

static std::vector<std::vector<char>> arg_storage;
static std::vector<char*> make_argv(std::initializer_list<const char*> args) {
    arg_storage.clear();
    std::vector<char*> result;
    for (const char* s : args) {
        arg_storage.emplace_back(s, s + strlen(s) + 1);
        result.push_back(arg_storage.back().data());
    }
    return result;
}

TEST(ClientConfigTest, DefaultValues) {
    ClientConfig cfg;
    EXPECT_EQ(cfg.peer_port, 6881);
    EXPECT_EQ(cfg.upload_rate_limit, 0u);
    EXPECT_EQ(cfg.download_rate_limit, 0u);
    EXPECT_EQ(cfg.max_connections, 200u);
    EXPECT_EQ(cfg.max_connections_per_ip, 2u);
    EXPECT_EQ(cfg.max_half_open, 100u);
    EXPECT_EQ(cfg.block_request_timeout_seconds, 30u);
    EXPECT_EQ(cfg.peer_ban_corrupt_threshold, 3u);
    EXPECT_EQ(cfg.peer_ban_duration_minutes, 60u);
    EXPECT_EQ(cfg.download_dir, "./downloads");
    EXPECT_TRUE(cfg.enable_dht);
    EXPECT_TRUE(cfg.enable_lsd);
    EXPECT_TRUE(cfg.enable_pex);
    ASSERT_EQ(cfg.dht_bootstrap_nodes.size(), 3);
    EXPECT_EQ(cfg.dht_bootstrap_nodes[0], "router.bittorrent.com:6881");
    EXPECT_EQ(cfg.dht_bootstrap_nodes[1], "dht.transmissionbt.com:6881");
    EXPECT_EQ(cfg.dht_bootstrap_nodes[2], "router.utorrent.com:6881");
}

TEST(ClientConfigTest, SerializeDeserializeRoundtrip) {
    ClientConfig cfg;
    cfg.peer_port = 6999;
    cfg.upload_rate_limit = 1024 * 1024;
    cfg.download_rate_limit = 10 * 1024 * 1024;
    cfg.max_connections = 500;
    cfg.max_connections_per_ip = 5;
    cfg.max_half_open = 80;
    cfg.block_request_timeout_seconds = 60;
    cfg.peer_ban_corrupt_threshold = 10;
    cfg.peer_ban_duration_minutes = 120;
    cfg.download_dir = "/tmp/p2p_downloads";
    cfg.enable_dht = false;
    cfg.enable_lsd = false;
    cfg.enable_pex = false;
    cfg.dht_bootstrap_nodes = {"test1.example.com:6881", "test2.example.com:6881"};

    Dict d = cfg.to_dict();
    ClientConfig loaded = ClientConfig::from_dict(d);

    EXPECT_EQ(loaded.peer_port, cfg.peer_port);
    EXPECT_EQ(loaded.upload_rate_limit, cfg.upload_rate_limit);
    EXPECT_EQ(loaded.download_rate_limit, cfg.download_rate_limit);
    EXPECT_EQ(loaded.max_connections, cfg.max_connections);
    EXPECT_EQ(loaded.max_connections_per_ip, cfg.max_connections_per_ip);
    EXPECT_EQ(loaded.max_half_open, cfg.max_half_open);
    EXPECT_EQ(loaded.block_request_timeout_seconds, cfg.block_request_timeout_seconds);
    EXPECT_EQ(loaded.peer_ban_corrupt_threshold, cfg.peer_ban_corrupt_threshold);
    EXPECT_EQ(loaded.peer_ban_duration_minutes, cfg.peer_ban_duration_minutes);
    EXPECT_EQ(loaded.download_dir, cfg.download_dir);
    EXPECT_EQ(loaded.enable_dht, cfg.enable_dht);
    EXPECT_EQ(loaded.enable_lsd, cfg.enable_lsd);
    EXPECT_EQ(loaded.enable_pex, cfg.enable_pex);
    ASSERT_EQ(loaded.dht_bootstrap_nodes.size(), cfg.dht_bootstrap_nodes.size());
    EXPECT_EQ(loaded.dht_bootstrap_nodes[0], cfg.dht_bootstrap_nodes[0]);
    EXPECT_EQ(loaded.dht_bootstrap_nodes[1], cfg.dht_bootstrap_nodes[1]);
}

TEST(ClientConfigTest, DefaultRoundtrip) {
    ClientConfig cfg;
    Dict d = cfg.to_dict();
    ClientConfig loaded = ClientConfig::from_dict(d);

    EXPECT_EQ(loaded.peer_port, cfg.peer_port);
    EXPECT_EQ(loaded.enable_dht, cfg.enable_dht);
    EXPECT_EQ(loaded.dht_bootstrap_nodes.size(), cfg.dht_bootstrap_nodes.size());
}

TEST(ClientConfigTest, CLIFlagParsingPort) {
    auto argv = make_argv({"client", "--port", "6999", "seed"});
    ClientConfig cfg = ClientConfig::from_cli(static_cast<int>(argv.size()), argv.data());
    EXPECT_EQ(cfg.peer_port, 6999);
}

TEST(ClientConfigTest, CLIFlagParsingUploadRate) {
    auto argv = make_argv({"client", "--upload-rate", "1048576"});
    ClientConfig cfg = ClientConfig::from_cli(static_cast<int>(argv.size()), argv.data());
    EXPECT_EQ(cfg.upload_rate_limit, 1048576ULL);
}

TEST(ClientConfigTest, CLIFlagParsingDownloadRate) {
    auto argv = make_argv({"client", "--download-rate", "4194304"});
    ClientConfig cfg = ClientConfig::from_cli(static_cast<int>(argv.size()), argv.data());
    EXPECT_EQ(cfg.download_rate_limit, 4194304ULL);
}

TEST(ClientConfigTest, CLIFlagParsingMaxConnections) {
    auto argv = make_argv({"client", "--max-connections", "500"});
    ClientConfig cfg = ClientConfig::from_cli(static_cast<int>(argv.size()), argv.data());
    EXPECT_EQ(cfg.max_connections, 500u);
}

TEST(ClientConfigTest, CLIFlagParsingMaxConnectionsPerIp) {
    auto argv = make_argv({"client", "--max-connections-per-ip", "5"});
    ClientConfig cfg = ClientConfig::from_cli(static_cast<int>(argv.size()), argv.data());
    EXPECT_EQ(cfg.max_connections_per_ip, 5u);
}

TEST(ClientConfigTest, CLIFlagParsingMaxHalfOpen) {
    auto argv = make_argv({"client", "--max-half-open", "80"});
    ClientConfig cfg = ClientConfig::from_cli(static_cast<int>(argv.size()), argv.data());
    EXPECT_EQ(cfg.max_half_open, 80u);
}

TEST(ClientConfigTest, CLIFlagParsingBlockTimeout) {
    auto argv = make_argv({"client", "--block-timeout", "60"});
    ClientConfig cfg = ClientConfig::from_cli(static_cast<int>(argv.size()), argv.data());
    EXPECT_EQ(cfg.block_request_timeout_seconds, 60u);
}

TEST(ClientConfigTest, CLIFlagParsingDownloadDir) {
    auto argv = make_argv({"client", "--download-dir", "/tmp/mydownloads"});
    ClientConfig cfg = ClientConfig::from_cli(static_cast<int>(argv.size()), argv.data());
    EXPECT_EQ(cfg.download_dir, "/tmp/mydownloads");
}

TEST(ClientConfigTest, CLIFlagParsingNoDht) {
    auto argv = make_argv({"client", "--no-dht"});
    ClientConfig cfg = ClientConfig::from_cli(static_cast<int>(argv.size()), argv.data());
    EXPECT_FALSE(cfg.enable_dht);
    EXPECT_TRUE(cfg.enable_lsd);
    EXPECT_TRUE(cfg.enable_pex);
}

TEST(ClientConfigTest, CLIFlagParsingNoLsd) {
    auto argv = make_argv({"client", "--no-lsd"});
    ClientConfig cfg = ClientConfig::from_cli(static_cast<int>(argv.size()), argv.data());
    EXPECT_TRUE(cfg.enable_dht);
    EXPECT_FALSE(cfg.enable_lsd);
    EXPECT_TRUE(cfg.enable_pex);
}

TEST(ClientConfigTest, CLIFlagParsingNoPex) {
    auto argv = make_argv({"client", "--no-pex"});
    ClientConfig cfg = ClientConfig::from_cli(static_cast<int>(argv.size()), argv.data());
    EXPECT_TRUE(cfg.enable_dht);
    EXPECT_TRUE(cfg.enable_lsd);
    EXPECT_FALSE(cfg.enable_pex);
}

TEST(ClientConfigTest, CLIFlagParsingMultipleFlags) {
    auto argv = make_argv({"client", "--port", "7000", "--no-dht", "--upload-rate", "2097152", "seed", "file.torrent"});
    ClientConfig cfg = ClientConfig::from_cli(static_cast<int>(argv.size()), argv.data());
    EXPECT_EQ(cfg.peer_port, 7000);
    EXPECT_FALSE(cfg.enable_dht);
    EXPECT_EQ(cfg.upload_rate_limit, 2097152ULL);
}

TEST(ClientConfigTest, CLIFlagParsingDefaultsPreserved) {
    auto argv = make_argv({"client", "--port", "7000"});
    ClientConfig cfg = ClientConfig::from_cli(static_cast<int>(argv.size()), argv.data());
    EXPECT_EQ(cfg.peer_port, 7000);
    EXPECT_EQ(cfg.max_connections, 200u);
    EXPECT_TRUE(cfg.enable_dht);
}

TEST(ClientConfigTest, ConfigFileSaveAndLoad) {
    std::string tmp_path = "/tmp/p2p_config_test.conf";
    std::remove(tmp_path.c_str());

    ClientConfig cfg;
    cfg.peer_port = 7777;
    cfg.enable_dht = false;
    cfg.download_dir = "/tmp/p2p_test";

    cfg.save(tmp_path);

    EXPECT_TRUE(std::filesystem::exists(tmp_path));

    ClientConfig loaded = ClientConfig::load(tmp_path);
    EXPECT_EQ(loaded.peer_port, 7777);
    EXPECT_FALSE(loaded.enable_dht);
    EXPECT_EQ(loaded.download_dir, "/tmp/p2p_test");
    EXPECT_TRUE(loaded.enable_lsd);
    EXPECT_TRUE(loaded.enable_pex);
    EXPECT_EQ(loaded.max_connections, 200u);

    std::remove(tmp_path.c_str());
}

TEST(ClientConfigTest, ConfigFileBencodeRoundtrip) {
    std::string tmp_path = "/tmp/p2p_config_bencode.conf";
    std::remove(tmp_path.c_str());

    ClientConfig cfg;
    cfg.peer_port = 6882;
    cfg.upload_rate_limit = 1024 * 1024;
    cfg.download_rate_limit = 5 * 1024 * 1024;
    cfg.max_connections = 100;
    cfg.enable_dht = false;
    cfg.enable_lsd = true;
    cfg.enable_pex = false;
    cfg.dht_bootstrap_nodes = {"node1.example.com:6881"};

    cfg.save(tmp_path);

    std::ifstream file(tmp_path, std::ios::binary);
    ASSERT_TRUE(file.is_open());
    std::string raw_str((std::istreambuf_iterator<char>(file)),
                         std::istreambuf_iterator<char>());
    file.close();
    std::vector<std::byte> raw_data(raw_str.size());
    for (size_t i = 0; i < raw_str.size(); ++i) {
        raw_data[i] = static_cast<std::byte>(raw_str[i]);
    }
    Value decoded = decode(raw_data);
    ASSERT_TRUE(std::holds_alternative<std::unique_ptr<Dict>>(decoded.get_variant()));
    ClientConfig loaded = ClientConfig::load(tmp_path);
    EXPECT_EQ(loaded.peer_port, cfg.peer_port);
    EXPECT_EQ(loaded.upload_rate_limit, cfg.upload_rate_limit);
    EXPECT_EQ(loaded.download_rate_limit, cfg.download_rate_limit);
    EXPECT_EQ(loaded.max_connections, cfg.max_connections);
    EXPECT_EQ(loaded.enable_dht, cfg.enable_dht);
    EXPECT_EQ(loaded.enable_lsd, cfg.enable_lsd);
    EXPECT_EQ(loaded.enable_pex, cfg.enable_pex);
    ASSERT_EQ(loaded.dht_bootstrap_nodes.size(), 1);
    EXPECT_EQ(loaded.dht_bootstrap_nodes[0], "node1.example.com:6881");

    std::remove(tmp_path.c_str());
}

TEST(ClientConfigTest, LoadNonexistentFileReturnsDefaults) {
    ClientConfig cfg = ClientConfig::load("/nonexistent/path/p2p.conf");
    EXPECT_EQ(cfg.peer_port, 6881);
    EXPECT_TRUE(cfg.enable_dht);
}

TEST(ClientConfigTest, ConfigFileCreatesParentDirectories) {
    std::string tmp_dir = "/tmp/p2p_config_nested_dir_test";
    std::string tmp_path = tmp_dir + "/subdir/p2p.conf";
    std::filesystem::remove_all(tmp_dir);

    ClientConfig cfg;
    cfg.peer_port = 8888;
    cfg.save(tmp_path);

    EXPECT_TRUE(std::filesystem::exists(tmp_path));

    ClientConfig loaded = ClientConfig::load(tmp_path);
    EXPECT_EQ(loaded.peer_port, 8888);

    std::filesystem::remove_all(tmp_dir);
}
