#include "helper.hpp"

class TestFileManager : public FileManager {
public:
    using PromptMock = std::function<asio::awaitable<int>(const std::string&)>;
    static PromptMock& mock() {
        static PromptMock instance;
        return instance;
    }

    TestFileManager(std::shared_ptr<SessionState> state)
        : FileManager(state) {
        auto_approve_all_ = false;
    }

protected:
    // Override the async_prompt method
    asio::awaitable<int> async_prompt(const std::string& question) override {
        if (mock()) {
            co_return co_await mock()(question);
        }
        co_return 1; // default Yes
    }
};

// Fixture for FileManager tests
class FileManagerTest : public ::testing::Test {
protected:
    asio::io_context io_context;
    std::filesystem::path temp_dir;
    std::filesystem::path torrent_path;
    std::filesystem::path save_path_base;
    std::shared_ptr<SessionState> session_state;
    std::unique_ptr<TestFileManager> file_manager;

    void SetUp() override {
        temp_dir = std::filesystem::temp_directory_path() / "filemanager_test_temp";
        std::filesystem::create_directories(temp_dir);
        torrent_path = temp_dir / "test.torrent";
        save_path_base = temp_dir / "download_output";
        std::filesystem::create_directories(save_path_base);
    }

    void TearDown() override {
        std::filesystem::remove_all(temp_dir);
    }

    void create_mock_torrent_file(const std::string& name, uint64_t total_size, uint32_t piece_length, bool is_multi_file = false, const std::vector<std::string>& files_paths = {}) {
        Dict info_dict;
        info_dict["name"] = Value(name);
        info_dict["piece length"] = Value(static_cast<Integer>(piece_length));
        
        size_t num_pieces = (total_size + piece_length - 1) / piece_length;
        std::string all_piece_hashes_str(num_pieces * 20, 'A'); // Dummy hashes
        info_dict["pieces"] = Value(all_piece_hashes_str);

        if (is_multi_file) {
            List files_list;
            uint64_t current_offset = 0;
            for (const auto& p : files_paths) {
                uint64_t file_size = (files_paths.size() == 1) ? total_size : (total_size / files_paths.size()); // Simple split
                if (current_offset + file_size > total_size) file_size = total_size - current_offset;
                
                List path_parts;
                for (const auto& part : std::filesystem::path(p)) {
                    path_parts.push_back(Value(part.string()));
                }
                files_list.push_back(Value(Dict{
                    {"length", Value(static_cast<Integer>(file_size))},
                    {"path", Value(path_parts)}
                }));
                current_offset += file_size;
            }
            info_dict["files"] = Value(files_list);
        } else {
            info_dict["length"] = Value(static_cast<Integer>(total_size));
        }

        Dict metainfo_dict;
        metainfo_dict["announce"] = Value(String("http://testtracker.com/announce"));
        metainfo_dict["info"] = Value(info_dict);
        
        std::ofstream file(torrent_path, std::ios::binary);
        ASSERT_TRUE(file.is_open());
        file.write(bytes_to_string(encode(Value(metainfo_dict))).data(), bytes_to_string(encode(Value(metainfo_dict))).size());
        file.close();
    }

    void init_session(const std::string& name, uint64_t total_size, uint32_t piece_length, bool is_multi_file = false, const std::vector<std::string>& files_paths = {}) {
        create_mock_torrent_file(name, total_size, piece_length, is_multi_file, files_paths);
        session_state = std::make_shared<SessionState>(torrent_path, save_path_base);
        file_manager = std::make_unique<TestFileManager>(session_state);
        TestFileManager::mock() = nullptr;
    }

    // Helper to create dummy data for a file
    std::vector<std::byte> create_dummy_data(size_t size, char start_char = 'A') {
        std::vector<std::byte> data(size);
        std::ranges::transform(
            std::views::iota(0uz, size),
            data.begin(),
            [start = static_cast<unsigned char>(start_char)](size_t i) {
                return static_cast<std::byte>(start + i);
            });
        return data;
    }

    // Write content to a specific file, simulating external data for seeding
    void write_file_content(const std::filesystem::path& file_path, std::span<const std::byte> data) {
        std::filesystem::create_directories(file_path.parent_path());
        std::ofstream ofs(file_path, std::ios::binary);
        ASSERT_TRUE(ofs.is_open()) << "Failed to open " << file_path << " for writing.";
        ofs.write(reinterpret_cast<const char*>(data.data()), data.size());
        ofs.close();
    }

    // Read file content for verification
    std::vector<std::byte> read_file_content(const std::filesystem::path& file_path) {
        std::ifstream ifs(file_path, std::ios::binary | std::ios::ate);
        if (!ifs.is_open()) return {};
        std::streamsize size = ifs.tellg();
        ifs.seekg(0, std::ios::beg);
        std::vector<std::byte> buffer(size);
        ifs.read(reinterpret_cast<char*>(buffer.data()), size);
        return buffer;
    }
};

TEST_F(FileManagerTest, PreallocateFilesSingleFileYesAll) {
    std::string torrent_name = "single_file";
    uint64_t total_size = 50000;
    uint32_t piece_length = 16384;
    init_session(torrent_name, total_size, piece_length);

    // Mock prompt to always return "Yes for all" (3)
    TestFileManager::mock() = [&](const std::string& q) -> asio::awaitable<int> {
        EXPECT_TRUE(q.find(session_state->torrent_info().name) != std::string::npos);
        co_return 3; // Yes for all
    };

    bool success = false;
    RunAsync(io_context, [&]() -> asio::awaitable<void> {
        success = co_await file_manager->preallocate_files();
    });

    EXPECT_TRUE(success);

    // Verify the file was created and has the correct size
    std::filesystem::path expected_path = save_path_base / torrent_name;
    EXPECT_TRUE(std::filesystem::exists(expected_path));
    EXPECT_EQ(std::filesystem::file_size(expected_path), total_size);

    // Verify piece status: all should be Needed as it's a new download
    ASSERT_EQ(session_state->num_pieces(), (total_size + piece_length - 1) / piece_length);
    for (size_t i = 0; i < session_state->num_pieces(); ++i) {
        EXPECT_EQ(session_state->piece_status(i), PieceStatus::Needed);
    }
}

TEST_F(FileManagerTest, PreallocateFilesMultiFileYesAll) {
    std::string torrent_name = "multi_file_dir";
    uint64_t total_size = 50000;
    uint32_t piece_length = 16384;
    std::vector<std::string> files_in_torrent = {"file1.txt", "subdir/file2.txt"};
    init_session(torrent_name, total_size, piece_length, true, files_in_torrent);

    TestFileManager::mock() = [](const std::string& q) -> asio::awaitable<int> {
        EXPECT_TRUE(q.find("file1.txt") != std::string::npos);
        co_return 3; // Yes for all
    };
    
    bool success = false;
    RunAsync(io_context, [&]() -> asio::awaitable<void> {
        success = co_await file_manager->preallocate_files();
    });

    EXPECT_TRUE(success);

    // Verify directories and files were created
    std::filesystem::path base_path = save_path_base / torrent_name;
    EXPECT_TRUE(std::filesystem::exists(base_path / files_in_torrent[0]));
    EXPECT_TRUE(std::filesystem::exists(base_path / files_in_torrent[1]));

    // Verify file sizes (simple split logic in create_mock_torrent_file)
    EXPECT_EQ(std::filesystem::file_size(base_path / files_in_torrent[0]), total_size / 2);
    EXPECT_EQ(std::filesystem::file_size(base_path / files_in_torrent[1]), total_size - (total_size / 2));

    // All pieces should be Needed
    for (size_t i = 0; i < session_state->num_pieces(); ++i) {
        EXPECT_EQ(session_state->piece_status(i), PieceStatus::Needed);
    }
}

TEST_F(FileManagerTest, PreallocateFilesMultiFileNoAll) {
    std::string torrent_name = "multi_file_dir_no";
    uint64_t total_size = 50000;
    uint32_t piece_length = 16384;
    std::vector<std::string> files_in_torrent = {"file1.txt", "subdir/file2.txt"};
    init_session(torrent_name, total_size, piece_length, true, files_in_torrent);

    TestFileManager::mock() = [](const std::string& q) -> asio::awaitable<int> {
        EXPECT_TRUE(q.find("file1.txt") != std::string::npos);
        co_return 4; // No for all
    };

    bool success = false;
    RunAsync(io_context, [&]() -> asio::awaitable<void> {
        success = co_await file_manager->preallocate_files();
    });

    EXPECT_TRUE(success);

    // Verify no files were created
    std::filesystem::path base_path = save_path_base / torrent_name;
    EXPECT_FALSE(std::filesystem::exists(base_path / files_in_torrent[0]));
    EXPECT_FALSE(std::filesystem::exists(base_path / files_in_torrent[1]));

    // All pieces should be Skipped
    for (size_t i = 0; i < session_state->num_pieces(); ++i) {
        EXPECT_EQ(session_state->piece_status(i), PieceStatus::Skipped);
    }
}

TEST_F(FileManagerTest, PreallocateFilesMultiFileMixedDecision) {
    std::string torrent_name = "multi_file_mixed";
    uint64_t total_size = 60000;
    uint32_t piece_length = 20000; // 3 pieces total
    std::vector<std::string> files_in_torrent = {"fileA.dat", "folder/fileB.dat", "fileC.dat"};
    init_session(torrent_name, total_size, piece_length, true, files_in_torrent);

    size_t prompt_count = 0;
    TestFileManager::mock() = [&](const std::string& q) -> asio::awaitable<int> {
        prompt_count++;
        if (q.find("fileA.dat") != std::string::npos) {
            co_return 1; // Yes
        } else if (q.find("fileB.dat") != std::string::npos) {
            co_return 2; // No
        } else if (q.find("fileC.dat") != std::string::npos) {
            co_return 1; // Yes
        }
        co_return -1; // Should not happen
    };

    bool success = false;
    RunAsync(io_context, [&]() -> asio::awaitable<void> {
        success = co_await file_manager->preallocate_files();
    });

    EXPECT_TRUE(success);
    EXPECT_EQ(prompt_count, 3); // All files should be prompted

    std::filesystem::path base_path = save_path_base / torrent_name;
    EXPECT_TRUE(std::filesystem::exists(base_path / files_in_torrent[0])); // fileA.dat downloaded
    EXPECT_FALSE(std::filesystem::exists(base_path / files_in_torrent[1])); // fileB.dat skipped
    EXPECT_TRUE(std::filesystem::exists(base_path / files_in_torrent[2])); // fileC.dat downloaded

    // Check file sizes
    EXPECT_EQ(std::filesystem::file_size(base_path / files_in_torrent[0]), total_size / 3);
    EXPECT_EQ(std::filesystem::file_size(base_path / files_in_torrent[2]), total_size / 3);

    // Verify piece statuses:
    // Pieces overlapping with fileB.dat should be skipped if fileB is not downloaded.
    // This requires detailed knowledge of piece-file mapping, which is complex.
    // For this test, we can check a few simple cases:
    // All pieces covering fileA and fileC should be Needed.
    // If a piece ONLY covers fileB, it should be Skipped.
    // If a piece covers fileA AND fileB, it should be Needed.

    // Assuming pieces 0, 1, 2 for simplicity
    // Let's say:
    // P0: overlaps fileA.dat (needed)
    // P1: overlaps fileB.dat (skipped)
    // P2: overlaps fileC.dat (needed)
    // This test's mock files are too simple to accurately check cross-piece boundaries easily.
    // A more robust test would explicitly build the `piece_to_files_map_` and `file_to_pieces_map_`
    // in `init_session` for deterministic outcomes.

    // Given the simple division (20k/file, 20k/piece):
    // Piece 0 -> fileA.dat (downloaded) -> Needed
    // Piece 1 -> fileB.dat (skipped) -> Skipped
    // Piece 2 -> fileC.dat (downloaded) -> Needed
    EXPECT_EQ(session_state->piece_status(0), PieceStatus::Needed);
    EXPECT_EQ(session_state->piece_status(1), PieceStatus::Skipped);
    EXPECT_EQ(session_state->piece_status(2), PieceStatus::Needed);
}

TEST_F(FileManagerTest, WriteAndReadBlockSingleFile) {
    std::string torrent_name = "rw_block_file";
    uint64_t total_size = 50000;
    uint32_t piece_length = 16384; // ~3 pieces
    init_session(torrent_name, total_size, piece_length);

    // Preallocate (assume Yes for all)
        TestFileManager::mock() = [](const std::string& q) -> asio::awaitable<int> { co_return 3; };
    RunAsync(io_context, [&]() -> asio::awaitable<void> {
        EXPECT_TRUE(co_await file_manager->preallocate_files());
    });

    // Generate some data for piece 0, block 0
    size_t piece_index = 0;
    uint32_t begin = 0;
    uint32_t length = BLOCK_SIZE;
    std::vector<std::byte> test_block_data = create_dummy_data(length, 'B');

    RunAsync(io_context, [&]() -> asio::awaitable<void> {
        // Write the block
        co_await file_manager->write_piece(piece_index, test_block_data); // write_piece expects full piece data, here we simulate one block as a piece

        // Read the block back
        std::vector<std::byte> read_data = co_await file_manager->read_block(piece_index, begin, length);
        EXPECT_EQ(read_data, test_block_data);
    });
}

TEST_F(FileManagerTest, WriteAndReadBlockMultiFile) {
    std::string torrent_name = "rw_block_multi";
    uint64_t total_size = 60000; // 3 files, 3 pieces
    uint32_t piece_length = 20000;
    std::vector<std::string> files_in_torrent = {"file1", "file2", "file3"}; // Each file is one piece
    init_session(torrent_name, total_size, piece_length, true, files_in_torrent);

    TestFileManager::mock() = [](const std::string& q) -> asio::awaitable<int> { co_return 3; };
    RunAsync(io_context, [&]() -> asio::awaitable<void> {
        EXPECT_TRUE(co_await file_manager->preallocate_files());
    });

    // Data for piece 1 (which maps to file2)
    size_t piece_index = 1;
    uint32_t begin = 0;
    uint32_t length = piece_length; // Entire piece
    std::vector<std::byte> test_piece_data = create_dummy_data(length, 'C');

    RunAsync(io_context, [&]() -> asio::awaitable<void> {
        // Write the piece
        co_await file_manager->write_piece(piece_index, test_piece_data);

        // Read the piece back
        std::vector<std::byte> read_data = co_await file_manager->read_block(piece_index, begin, length);
        EXPECT_EQ(read_data, test_piece_data);

        // Verify content in the actual file on disk
        std::filesystem::path expected_file_path = save_path_base / torrent_name / "file2";
        std::vector<std::byte> file_on_disk = read_file_content(expected_file_path);
        EXPECT_EQ(file_on_disk, test_piece_data);
    });
}

TEST_F(FileManagerTest, VerifyPieceAndSeedData) {
    std::string torrent_name = "verify_seed_data";
    uint64_t total_size = BLOCK_SIZE * 2; // 2 blocks, 1 piece
    uint32_t piece_length = BLOCK_SIZE * 2;
    init_session(torrent_name, total_size, piece_length);

    std::vector<std::byte> piece_data = create_dummy_data(piece_length, 'D');
    
    // Write data to the actual file path that the FileManager expects for seeding
    std::filesystem::path seed_file_path = save_path_base / torrent_name;
    write_file_content(seed_file_path, piece_data);

    // Update SessionState's piece hash to match our test data
    std::vector<std::byte> actual_hash = Crypto::calculate_sha1_hash_data(piece_data);
    session_state->torrent_info().pieces.assign(actual_hash.begin(), actual_hash.end());

    bool verified = false;
    bool seed_verified = false;
    RunAsync(io_context, [&]() -> asio::awaitable<void> {
        verified = co_await file_manager->verify_piece(0);    
        seed_verified = co_await file_manager->verify_seed_data();
    });

    EXPECT_TRUE(verified);
    EXPECT_TRUE(seed_verified);
}

TEST_F(FileManagerTest, VerifySeedDataMismatchSize) {
    std::string torrent_name = "verify_seed_mismatch";
    uint64_t total_size = BLOCK_SIZE * 2;
    uint32_t piece_length = BLOCK_SIZE * 2;
    init_session(torrent_name, total_size, piece_length);

    // Create a file with incorrect size
    std::vector<std::byte> piece_data = create_dummy_data(piece_length / 2, 'E'); // Half size
    std::filesystem::path seed_file_path = save_path_base / torrent_name;
    write_file_content(seed_file_path, piece_data);

    bool seed_verified = true;
    RunAsync(io_context, [&]() -> asio::awaitable<void> {
        seed_verified = co_await file_manager->verify_seed_data();
    });

    EXPECT_FALSE(seed_verified);
}

TEST_F(FileManagerTest, SaveAndLoadResumeData) {
    std::string torrent_name = "resume_test";
    uint64_t total_size = 50000;
    uint32_t piece_length = 16384;
    init_session(torrent_name, total_size, piece_length);

    // Create some mock resume data
    ResumeData original_resume;
    original_resume.have_bitfield = "\x80\x00\x00"; // Piece 0 is "Have" (assuming 3 pieces total, first byte MSB is piece 0)
    original_resume.total_downloaded = 1024;
    original_resume.total_uploaded = 512;
    original_resume.in_progress_pieces["1"] = "\x80\x00"; // Piece 1, block 0 received
    original_resume.file_mtimes[session_state->torrent_info().name] = 1234567890; // Example mtime

    std::vector<std::byte> serialized_resume = original_resume.serialize();

    RunAsync(io_context, [&]() -> asio::awaitable<void> {
        // Save the data
        co_await file_manager->save_resume_data(serialized_resume);

        // Load it back
        auto loaded_resume_opt = co_await file_manager->load_resume_data();
        EXPECT_TRUE(loaded_resume_opt.has_value());

        ResumeData loaded_resume = ResumeData::deserialize(loaded_resume_opt.value());

        // Verify loaded data matches original
        EXPECT_EQ(loaded_resume.have_bitfield, original_resume.have_bitfield);
        EXPECT_EQ(loaded_resume.total_downloaded, original_resume.total_downloaded);
        EXPECT_EQ(loaded_resume.total_uploaded, original_resume.total_uploaded);
        EXPECT_EQ(loaded_resume.in_progress_pieces.size(), original_resume.in_progress_pieces.size());
        EXPECT_EQ(loaded_resume.in_progress_pieces.at("1"), original_resume.in_progress_pieces.at("1"));
        EXPECT_EQ(loaded_resume.file_mtimes.size(), original_resume.file_mtimes.size());
        EXPECT_EQ(loaded_resume.file_mtimes.at(session_state->torrent_info().name), original_resume.file_mtimes.at(session_state->torrent_info().name));
    });
}

TEST_F(FileManagerTest, LoadResumeDataNonExistent) {
    std::string torrent_name = "no_resume";
    uint64_t total_size = 1000;
    uint32_t piece_length = 100;
    init_session(torrent_name, total_size, piece_length);

    RunAsync(io_context, [&]() -> asio::awaitable<void> {
        auto loaded_resume_opt = co_await file_manager->load_resume_data();
        EXPECT_FALSE(loaded_resume_opt.has_value());
    });
}

TEST_F(FileManagerTest, CacheHitOnReRead) {
    std::string torrent_name = "cache_hit";
    uint64_t total_size = BLOCK_SIZE;
    uint32_t piece_length = BLOCK_SIZE;
    init_session(torrent_name, total_size, piece_length);

    TestFileManager::mock() = [](const std::string& q) -> asio::awaitable<int> { co_return 3; };
    bool prealloc_ok = false;
    RunAsync(io_context, [&]() -> asio::awaitable<void> {
        prealloc_ok = co_await file_manager->preallocate_files();
    });
    ASSERT_TRUE(prealloc_ok);

    std::vector<std::byte> original_data = create_dummy_data(BLOCK_SIZE, 'X');
    {
        std::filesystem::path file_path = save_path_base / torrent_name;
        std::ofstream ofs(file_path, std::ios::binary);
        ASSERT_TRUE(ofs.is_open());
        ofs.write(reinterpret_cast<const char*>(original_data.data()), original_data.size());
        ofs.close();
    }

    RunAsync(io_context, [&]() -> asio::awaitable<void> {
        auto first_read = co_await file_manager->read_block(0, 0, BLOCK_SIZE);
        EXPECT_EQ(first_read, original_data);
    });

    std::vector<std::byte> modified_data = create_dummy_data(BLOCK_SIZE, 'Z');
    {
        std::filesystem::path file_path = save_path_base / torrent_name;
        std::ofstream ofs(file_path, std::ios::binary);
        ASSERT_TRUE(ofs.is_open());
        ofs.write(reinterpret_cast<const char*>(modified_data.data()), modified_data.size());
        ofs.close();
    }

    RunAsync(io_context, [&]() -> asio::awaitable<void> {
        auto second_read = co_await file_manager->read_block(0, 0, BLOCK_SIZE);
        EXPECT_EQ(second_read, original_data);
        EXPECT_NE(second_read, modified_data);
    });
}

TEST_F(FileManagerTest, WriteThenReadFromCache) {
    std::string torrent_name = "write_read_cache";
    uint64_t total_size = BLOCK_SIZE;
    uint32_t piece_length = BLOCK_SIZE;
    init_session(torrent_name, total_size, piece_length);

    TestFileManager::mock() = [](const std::string& q) -> asio::awaitable<int> { co_return 3; };
    bool prealloc_ok = false;
    RunAsync(io_context, [&]() -> asio::awaitable<void> {
        prealloc_ok = co_await file_manager->preallocate_files();
    });
    ASSERT_TRUE(prealloc_ok);

    std::vector<std::byte> piece_data = create_dummy_data(BLOCK_SIZE, 'D');

    RunAsync(io_context, [&]() -> asio::awaitable<void> {
        co_await file_manager->write_piece(0, piece_data);
    });

    std::filesystem::path file_path = save_path_base / torrent_name;
    std::vector<std::byte> disk_before = read_file_content(file_path);
    bool all_zeros = std::ranges::all_of(disk_before, [](std::byte b) { return b == std::byte{0}; });
    EXPECT_TRUE(all_zeros) << "Data should still be in cache, not on disk";

    RunAsync(io_context, [&]() -> asio::awaitable<void> {
        auto read_data = co_await file_manager->read_block(0, 0, BLOCK_SIZE);
        EXPECT_EQ(read_data, piece_data);
    });
}

TEST_F(FileManagerTest, LruEvictionUnderLoad) {
    std::string torrent_name = "lru_eviction";
    uint32_t piece_length = BLOCK_SIZE;
    uint64_t total_size = static_cast<uint64_t>(BLOCK_SIZE) * 2500;
    init_session(torrent_name, total_size, piece_length);

    TestFileManager::mock() = [](const std::string& q) -> asio::awaitable<int> { co_return 3; };
    bool prealloc_ok = false;
    RunAsync(io_context, [&]() -> asio::awaitable<void> {
        prealloc_ok = co_await file_manager->preallocate_files();
    });
    ASSERT_TRUE(prealloc_ok);

    size_t num_pieces = session_state->num_pieces();
    ASSERT_EQ(num_pieces, 2500);

    for (size_t i = 0; i < num_pieces; ++i) {
        std::vector<std::byte> piece_data = create_dummy_data(BLOCK_SIZE, static_cast<char>('A' + (i % 26)));
        RunAsync(io_context, [i, piece_data, this]() -> asio::awaitable<void> {
            co_await file_manager->write_piece(i, piece_data);
        });
    }

    for (size_t i = 0; i < num_pieces; ++i) {
        std::vector<std::byte> expected_data = create_dummy_data(BLOCK_SIZE, static_cast<char>('A' + (i % 26)));
        RunAsync(io_context, [i, expected_data, this]() -> asio::awaitable<void> {
            auto read_data = co_await file_manager->read_block(i, 0, BLOCK_SIZE);
            EXPECT_EQ(read_data, expected_data) << "Mismatch for piece " << i;
        });
    }
}
