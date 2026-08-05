#include "FileManager.hpp"
#include <iostream>
#include <fstream>
#include <boost/asio/experimental/awaitable_operators.hpp>

FileManager::FileManager(std::shared_ptr<SessionState> state)
    : state_(state),
      file_io_pool_(4),
      file_locker_(std::make_shared<FileLocker>())
{
    const auto& info = state_->torrent_info();
    const size_t num_pieces = state_->num_pieces();

    piece_to_files_map_.reserve(num_pieces);
    for (size_t i = 0; i < num_pieces; ++i) {
        piece_to_files_map_.push_back(std::make_shared<std::vector<PieceFileOverlap>>());
    }
    file_to_pieces_map_.reserve(info.files.size());
    for (size_t i = 0; i < info.files.size(); ++i) {
        file_to_pieces_map_.push_back(std::make_shared<std::vector<size_t>>());
    }

    build_maps();
}

FileManager::~FileManager() {
    shutting_down_.store(true);
    io_cancelled_->store(true);
    if (flush_timer_) {
        flush_timer_->cancel();
    }
    sync_flush_all_dirty();
}

asio::awaitable<std::vector<std::byte>> FileManager::read_block(size_t piece_index, uint32_t begin, uint32_t length) {
    CTRACK_ASYNC("FileManager::read_block");
    CacheKey key(piece_index, begin);

    // Check cache first
    {
        std::lock_guard lock(cache_mutex_);
        auto it = cache_.find(key);
        if (it != cache_.end() && it->second.size() >= length) {
            touch_cache(key);
            co_return std::vector<std::byte>(it->second.begin(), it->second.begin() + length);
        }
    }

    // Cache miss — read from disk
    const auto& info = state_->torrent_info();
    std::vector<std::byte> block_data(length);
    uint64_t global_offset = static_cast<uint64_t>(piece_index) * info.piece_size + begin;
    uint64_t current_file_offset = 0;
    uint32_t read_for_block = 0;

    for (const auto& file_info : info.files) {
        if (!file_info.download) {
            current_file_offset += file_info.size;
            continue;
        }
        
        uint64_t file_start = current_file_offset;
        uint64_t file_end = current_file_offset + file_info.size;
        uint64_t read_start = global_offset + read_for_block;

        if (read_start < file_end) {  // overlap
            uint64_t read_pos_in_file = read_start - file_start;
            uint32_t bytes_to_read_from_this_file = std::min(static_cast<uint64_t>(length - read_for_block), file_info.size - read_pos_in_file);
            if (bytes_to_read_from_this_file > 0) {
                auto full_file_path = get_full_path_for_file(file_info);
                auto file_data_part = co_await async_read_from_file(full_file_path, read_pos_in_file, bytes_to_read_from_this_file);
                std::ranges::copy(file_data_part, block_data.begin() + read_for_block);
                read_for_block += bytes_to_read_from_this_file;
            }
        }

        current_file_offset += file_info.size;
        if (read_for_block == length) {
            break;
        }
    }

    if (read_for_block != length) {
        throw std::runtime_error("Failed to read full block from storage.");
    }

    // Populate cache
    {
        std::lock_guard lock(cache_mutex_);
        evict_if_needed();
        cache_[key] = block_data;
        touch_cache(key);
        cache_current_size_ += block_data.size();
        dirty_blocks_.erase(key);  // Fresh read — not dirty
    }

    co_return block_data;
}

asio::awaitable<void> FileManager::write_piece(size_t piece_index, std::span<const std::byte> piece_data) {
    CTRACK_ASYNC("FileManager::write_piece");
    uint32_t offset = 0;
    while (offset < piece_data.size()) {
        uint32_t block_size = std::min(static_cast<uint32_t>(BLOCK_SIZE), static_cast<uint32_t>(piece_data.size() - offset));
        CacheKey key(piece_index, offset);
        std::vector<std::byte> block_data(piece_data.subspan(offset, block_size).begin(),
                                           piece_data.subspan(offset, block_size).end());

        {
            std::lock_guard lock(cache_mutex_);
            evict_if_needed();
            auto existing = cache_.find(key);
            if (existing != cache_.end()) {
                cache_current_size_ -= existing->second.size();
            }
            cache_[key] = std::move(block_data);
            touch_cache(key);
            cache_current_size_ += block_size;
            dirty_blocks_.insert(key);
        }

        offset += BLOCK_SIZE;
    }

    if (!flush_timer_started_) {
        flush_timer_started_ = true;
        auto executor = co_await asio::this_coro::executor;
        flush_timer_ = std::make_unique<asio::steady_timer>(executor);
        asio::co_spawn(executor, periodic_flush(), asio::detached);
    }
}

void FileManager::build_maps() {
    const auto& info = state_->torrent_info();
    const size_t num_pieces = state_->num_pieces();
    const uint32_t piece_size = info.piece_size;
    const uint64_t total_torrent_size = info.total_size;

    // Clear and rebuild maps with proper shared_ptrs (not null).
    piece_to_files_map_.clear();
    piece_to_files_map_.reserve(num_pieces);
    for (size_t i = 0; i < num_pieces; ++i) {
        piece_to_files_map_.push_back(std::make_shared<std::vector<PieceFileOverlap>>());
    }
    file_to_pieces_map_.clear();
    file_to_pieces_map_.reserve(info.files.size());
    for (size_t i = 0; i < info.files.size(); ++i) {
        file_to_pieces_map_.push_back(std::make_shared<std::vector<size_t>>());
    }

    uint64_t current_total_offset = 0;
    for (size_t file_idx = 0; file_idx < info.files.size(); ++file_idx) {
        const auto& file = info.files[file_idx];
        uint64_t file_start_offset = current_total_offset; // Absolute start in total torrent data
        uint64_t file_end_offset = file_start_offset + file.size; // Absolute end in total torrent data

        uint32_t start_piece = file_start_offset / piece_size;
        uint32_t end_piece = (file.size == 0) ? start_piece : (file_end_offset - 1) / piece_size;
        // Ensure end_piece_idx doesn't exceed the total number of pieces
        if (end_piece >= num_pieces) {
            end_piece = num_pieces - 1;
        }
        for (uint32_t piece_idx = start_piece; piece_idx <= end_piece; ++piece_idx) {
            uint64_t piece_start_offset = static_cast<uint64_t>(piece_idx) * piece_size;
            
            uint64_t actual_piece_size;
            if (piece_idx == num_pieces - 1) { // This is the last piece
                if (total_torrent_size == 0) {
                    actual_piece_size = 0; // Empty torrent
                } else {
                    uint64_t remainder = total_torrent_size % piece_size;
                    // If remainder is 0 and total_torrent_size > 0, it means the last piece is a full piece_size.
                    actual_piece_size = (remainder == 0) ? piece_size : remainder;
                }
            } else { // Not the last piece, so it's a full piece_size
                actual_piece_size = piece_size;
            }

            uint64_t piece_end_offset = piece_start_offset + actual_piece_size;
            uint64_t overlap_start = std::max(file_start_offset, piece_start_offset);
            uint64_t overlap_end = std::min(file_end_offset, piece_end_offset);

            if (overlap_end > overlap_start) {
                PieceFileOverlap overlap;
                overlap.file_index = file_idx;
                overlap.offset_in_file = overlap_start - file_start_offset;
                overlap.offset_in_piece = overlap_start - piece_start_offset;
                overlap.length = overlap_end - overlap_start;
                piece_to_files_map_[piece_idx]->push_back(std::move(overlap));
                file_to_pieces_map_[file_idx]->push_back(piece_idx);
            }
        }

        current_total_offset += file.size;
    }
}

std::filesystem::path FileManager::get_full_path_for_file(const FileInfo& file_info) const {
    const auto& info = state_->torrent_info();
    std::filesystem::path full_path = state_->save_path();
    if (info.files.size() > 1) { // Multi-file torrent
        full_path /= info.name;
        full_path /= file_info.path;
    } else { // Single-file torrent
        full_path /= info.name;
    }
    return full_path;
}

std::filesystem::path FileManager::get_resume_file_path() const {
    const auto& info = state_->torrent_info();
    const auto& data_file_path = state_->save_path();
    // Use the info_hash hex to make the resume file unique per torrent,
    // avoiding races when multiple single-file torrents use the same directory.
    // The file is hidden (leading '.') and placed inside the torrent's own
    // download folder, so it is CWD-independent and stays with the data.
    std::string hash_hex = Crypto::bytes_to_hex(state_->info_hash());
    std::filesystem::path p;
    if (info.files.size() > 1) {
        p = data_file_path / info.name / ("." + hash_hex + ".resume");
    } else {
        p = data_file_path / ("." + hash_hex + ".resume");
    }
    return p;
}

asio::awaitable<void> FileManager::async_write_to_file(const std::filesystem::path& path, uint64_t offset, std::span<const std::byte> data) {
    co_await asio::async_initiate<void(std::error_code)>(
        [this, path, offset, data, cancelled = io_cancelled_] (auto&& completion_handler) {
            file_io_pool_.enqueue(
                [path, offset, data, cancelled, locker = file_locker_, handler = std::move(completion_handler)]
                () mutable {
                    if (cancelled->load()) {
                        std::move(handler)(make_error_code(std::errc::operation_canceled));
                        return;
                    }
                    try {
                        std::lock_guard lock(locker->get_lock(path));

                        std::filesystem::path parent_dir = path.parent_path();
                        if (!parent_dir.empty()) { // Only try to create if there is a parent directory
                            std::error_code dir_ec;
                            std::filesystem::create_directories(parent_dir, dir_ec);
                            if (dir_ec) {
                                throw std::runtime_error(std::format("Failed to create parent directories for {}: {}", path.string(), dir_ec.message()));
                            }
                        }

                        std::fstream output_file(path, std::ios::binary | std::ios::in | std::ios::out);
                        if (!output_file.is_open()) {
                            throw std::runtime_error(std::format("Failed to open preallocated file for writing: {}. Error: {}", path.string(), std::strerror(errno)));
                        }
                        output_file.clear();  // Clear any error flags if stream was used before or created with errors

                        output_file.seekp(offset, std::ios::beg);
                        if (output_file.fail()) {
                            throw std::runtime_error(std::format("Failed to seek to offset {} in file {}. Error: {}", offset, path.string(), std::strerror(errno)));
                        }

                        output_file.write(reinterpret_cast<const char*>(data.data()), data.size());
                        if (!output_file) {
                            throw std::runtime_error("Failed to write data to file: " + path.string());
                        }
                        output_file.flush();

                        if (cancelled->load()) {
                            std::move(handler)(make_error_code(std::errc::operation_canceled));
                            return;
                        }

                        std::move(handler)(std::error_code{});
                    } catch (const std::exception& e) {
                        LOGERR("File write error for {}: {}", path.string(), e.what());
                        std::move(handler)(make_error_code(std::errc::io_error));
                    }
                }
            );
        },
        asio::use_awaitable
    );
}

asio::awaitable<std::vector<std::byte>> FileManager::async_read_from_file(const std::filesystem::path& path, uint64_t offset, uint32_t size) {
    auto [ec, block_data] = co_await asio::async_initiate<void(std::error_code, std::vector<std::byte>)>(
        [this, path, offset, size, cancelled = io_cancelled_] (auto&& completion_handler) mutable {
            file_io_pool_.enqueue(
                [path, offset, size, cancelled, handler = std::move(completion_handler)]
                () mutable {
                    if (cancelled->load()) {
                        std::move(handler)(make_error_code(std::errc::operation_canceled), std::vector<std::byte>{});
                        return;
                    }
                    try {
                        std::ifstream data_file(path, std::ios::binary);
                        if (!data_file) {
                            LOGERR("FileManager: [FileIO Pool] Failed to open file for reading: {}", path.string());
                            throw std::runtime_error("Failed to open file for reading: " + path.string());
                        }
                        
                        uint64_t actual_file_size = std::filesystem::file_size(path);
                        if (offset + size > actual_file_size) {
                            LOGERR("FileManager: [FileIO Pool] Requested read ({}+{}) exceeds actual file size ({}) for file: {}", 
                                offset, size, actual_file_size, path.string());
                            throw std::runtime_error(std::format("Requested read ({}+{}) exceeds actual file size ({}) for file: {}", 
                                offset, size, actual_file_size, path.string()));
                        }

                        if (size == 0) {  // read full file if given size is 0
                            size = std::filesystem::file_size(path);
                        }

                        std::vector<std::byte> buffer(size);
                        data_file.seekg(offset, std::ios::beg);
                        if (data_file.fail()) {
                            LOGERR("FileManager: [FileIO Pool] Failed to seek to offset {} in file {}. Error: {}", offset, path.string(), std::strerror(errno));
                            throw std::runtime_error("Failed to seek in file: " + path.string());
                        }
                        data_file.read(reinterpret_cast<char *>(buffer.data()), size);
                        if (static_cast<std::size_t>(data_file.gcount()) != size) {
                            throw std::runtime_error("Incomplete read from file: " + path.string());
                        }
                        std::streamsize bytes_read_actual = data_file.gcount();
                        if (static_cast<uint32_t>(bytes_read_actual) != size) {
                            LOGERR("FileManager: [FileIO Pool] Incomplete read from file '{}'. Read {} bytes, requested {}. EOF={}, Fail={}", 
                                path.string(), bytes_read_actual, size, data_file.eof(), data_file.fail());
                            throw std::runtime_error("Incomplete read from file: " + path.string());
                        }

                        if (cancelled->load()) {
                            std::move(handler)(make_error_code(std::errc::operation_canceled), std::vector<std::byte>{});
                            return;
                        }
                        
                        std::move(handler)(std::error_code{}, std::move(buffer));
                    } catch (const std::exception& e) {
                        LOGERR("File read error: {}", e.what());
                        std::move(handler)(make_error_code(std::errc::io_error), std::vector<std::byte>{});
                    }
                }
            );
        }, 
        asio::as_tuple(asio::use_awaitable)
    );

    if (ec) {
        throw boost::system::system_error(ec, "async_read_from_file");
    }

    co_return block_data;
}

asio::awaitable<bool> FileManager::verify_piece(size_t piece_index) {
    CTRACK_ASYNC("FileManager::verify_piece");
    const auto& info = state_->torrent_info();
    const size_t num_pieces = state_->num_pieces();

    uint32_t piece_size_to_read = (piece_index == num_pieces - 1) ? (info.total_size % info.piece_size ? info.total_size % info.piece_size : info.piece_size) : info.piece_size;
    auto piece_data = co_await read_block(piece_index, 0, piece_size_to_read);
    auto actual_hash = Crypto::calculate_sha1_hash_data(piece_data);
    auto expected_hash_start = info.pieces.begin() + piece_index * 20;
    std::vector<std::byte> expected_hash(expected_hash_start, expected_hash_start + 20);

    co_return actual_hash == expected_hash;
}

asio::awaitable<void> FileManager::verify_pieces() {
    CTRACK_ASYNC("FileManager::verify_pieces");
    const size_t num_pieces = state_->num_pieces();

    for (size_t i : std::views::iota(0UL, num_pieces) 
                       | std::views::filter([&](size_t idx) { 
                            return state_->piece_status(idx) == PieceStatus::Have; 
                        })
    )  {
        if (!co_await verify_piece(i)) {
            LOGCRITICAL("Hash mismatch for piece {} during verification. Marking as Needed.", i);
            state_->piece_status(i, PieceStatus::Needed);
            state_->add_completed_pieces(-1);
        } else {
            state_->piece_status(i, PieceStatus::Have);
            state_->add_completed_pieces(1);
        }
    }

    if (size_t pieces_done_count = state_->completed_pieces(); pieces_done_count > 0) {
        float progress = (static_cast<float>(pieces_done_count) / num_pieces) * 100.0f;
        LOGINFO("Verification complete. Found {}/{} valid pieces ({:.2f}% progress).", 
                pieces_done_count, num_pieces, progress
        );
    }
}

asio::awaitable<bool> FileManager::verify_seed_data() {
    CTRACK_ASYNC("FileManager::verify_seed_data");
    const auto& info = state_->torrent_info();
    const size_t num_pieces = state_->num_pieces();
    
    LOGINFO("Verifying seed data for '{}'...", info.name);
    
    auto save_path = state_->save_path();
    if (info.files.size() > 1 && !std::filesystem::is_directory(save_path)) {
        LOGERR("Content path for multi-file torrent must be a directory: " + save_path.string());
        co_return false;
    } else if (info.files.size() == 1 && !std::filesystem::is_regular_file(save_path / info.files[0].path)) {
        save_path /= info.files[0].path;
        if (!std::filesystem::exists(save_path)) {
            LOGERR("Data file not found: " + save_path.string());
            co_return false;
        }
    }

    uint64_t discovered_total_size = 0;
    for (const auto& file_info : info.files) {
        std::filesystem::path full_file_path = get_full_path_for_file(file_info);
        if (!std::filesystem::exists(full_file_path)) {
            LOGCRITICAL("Seeder file missing: {}", full_file_path.string());
            co_return false;
        }

        discovered_total_size += std::filesystem::file_size(full_file_path);
    }

    if (discovered_total_size != info.total_size) {
        LOGCRITICAL("Seed file size mismatch. Expected {}, found {}.", 
                    info.total_size, discovered_total_size);
        co_return false;
    }

    for (size_t i : std::views::iota(0UL, num_pieces)) {
        if (!co_await verify_piece(i)) {
            LOGCRITICAL("Hash mismatch for piece {}! File is corrupt. Aborting.", i);
            co_return false;
        }
    }  

    LOGINFO("Seed data verified successfully.");
    co_return true;
}

asio::awaitable<void> FileManager::save_resume_data(std::span<const std::byte> data) {
    CTRACK_ASYNC("FileManager::save_resume_data");
    std::filesystem::path p = get_resume_file_path();
    std::filesystem::path temp_path = p;
    temp_path += ".tmp";

    try {
        std::filesystem::path parent_dir = p.parent_path();
        if (!parent_dir.empty()) {
            std::error_code ec;
            std::filesystem::create_directories(parent_dir, ec);
            if (ec) {
                throw std::runtime_error(std::format("Failed to create parent directories for resume file {}: {}", parent_dir.string(), ec.message()));
            }
        }

        if (!std::filesystem::exists(temp_path)) {
            std::ofstream temp_file(temp_path, std::ios::binary | std::ios::in | std::ios::out | std::ios::trunc);
            if (!temp_file) {
                throw std::runtime_error(std::format("Failed to create/open file: {}. Error: {}", temp_path.string(), std::strerror(errno)));
            }
            temp_file.close();
        }

        co_await async_write_to_file(temp_path, 0, data);

        std::error_code ec;
        std::filesystem::rename(temp_path, p, ec);
        if (ec) {
            throw std::runtime_error(std::format("filesystem error: cannot rename: {} [{}] [{}]", ec.message(), temp_path.string(), p.string()));
        }
        
        LOGINFO("Progress saved successfully to {}", p.string());
    } catch(const std::exception& e) {
        LOGERR("Failed to save resume file: {}", e.what());
        
        std::error_code ec;
        std::filesystem::remove(temp_path, ec);
        if (ec) {
            LOGWARN("Could not remove temporary resume file {}: {}", temp_path.string(), ec.message());
        }
        throw;
    }
    
}

asio::awaitable<std::optional<std::vector<std::byte>>> FileManager::load_resume_data() {
    auto p = get_resume_file_path();
    if (!std::filesystem::exists(p) || std::filesystem::file_size(p) == 0) {
        co_return std::nullopt;
    }

    co_return co_await async_read_from_file(p, 0);
}

asio::awaitable<bool> FileManager::preallocate_files() {
    CTRACK_ASYNC("FileManager::preallocate_files");
    const auto& info = state_->torrent_info();
    const size_t num_pieces = state_->num_pieces();

    // Rebuild piece-to-file maps (needed after magnet metadata load,
    // where the constructor built them with num_pieces=0).
    build_maps();

    try {
        auto base_save_path = state_->save_path();

        // For a multi-file torrent, base_save_path is the parent directory.
        // We create a subdirectory named after the torrent.
        if (state_->is_multi_file()) {
            base_save_path /= info.name;
        }

        std::error_code ec;
        std::filesystem::create_directories(base_save_path, ec);
        if (ec) {
            LOGCRITICAL("Failed to create base torrent directory {}: {}", base_save_path.string(), ec.message());
            co_return false;
        }

        // selective downloading
        std::optional<bool> download_all_decision;
        if (auto_approve_all_) {
            download_all_decision.emplace(true);
        }

        for (size_t i = 0; i < info.files.size(); ++i) {
            const auto& file = info.files[i];
            std::filesystem::path full_path = get_full_path_for_file(file);
            if (download_all_decision.has_value()) {
                state_->update_file_stat(i, download_all_decision.value());
            } else {
                bool skip_this_file = false;
                int ans = co_await async_prompt(
                    std::format("Download file {}?\n    1 - Yes\n    2 - No\n    3 - Yes for all\n    4 - No for all\n:", 
                    full_path.filename().string())
                );
                if (ans == 1) { // Yes
                    state_->update_file_stat(i, true);
                } else if (ans == 2) { // No
                    skip_this_file = true;
                } else if (ans == 3) { // Yes for all
                    state_->update_file_stat(i, true); // Current file is downloaded
                    download_all_decision = true; // Set global decision for subsequent files
                } else if (ans == 4) { // No for all
                    skip_this_file = true; // Current file is skipped
                    download_all_decision = false; // Set global decision for subsequent files
                } else {
                    LOGWARN("Unknown input '{}', defaulting to Yes", ans);
                    state_->update_file_stat(i, true); // Default to download
                }

                if (skip_this_file) {
                    state_->update_file_stat(i, false);
                }
            }
            
            if (!file.download) {
                continue;
            }

            // Only create/truncate the file if it DOES NOT exist
            if (!std::filesystem::exists(full_path)) {
                LOGINFO("File {} does not exist. Pre-allocating...", full_path.string());
                std::filesystem::path parent_dir = full_path.parent_path();
                if (!parent_dir.empty()) {
                    std::error_code ec;
                    std::filesystem::create_directories(parent_dir, ec);
                    if (ec) {
                        LOGCRITICAL("Failed to create parent directories for file {}: {}", full_path.string(), ec.message());
                        co_return false; // Critical failure, abort preallocation
                    }
                }
                // Now, truncating is safe because the file is new
                std::ofstream output_file(full_path, std::ios::binary | std::ios::in | std::ios::out | std::ios::trunc);
                if (!output_file.is_open()) {
                    throw std::runtime_error(std::format("Failed to create/open file for preallocation: {}. Error: {}", full_path.string(), std::strerror(errno)));
                }
                if (file.size > 0) {
                    output_file.seekp(file.size - 1);
                    output_file.write("", 1);
                    if (output_file.fail()) {
                        throw std::runtime_error(std::format("Failed to preallocate file size for {}. Error: {}", full_path.string(), std::strerror(errno)));
                    }
                }
                output_file.close();
                
                if (!std::filesystem::exists(full_path)) {
                    throw std::runtime_error(std::format("File {} was not found on disk after successful preallocation attempt.", full_path.string()));
                }
            }
        }

        for (size_t piece_idx : std::views::iota(0UL, num_pieces)) {
            bool is_needed = std::ranges::any_of(*get_piece_to_files_map(piece_idx), 
                                                 [this](const auto& overlap) {
                                                    return state_->torrent_info().files[overlap.file_index].download;
                                                 });
            PieceStatus status = is_needed ? PieceStatus::Needed : PieceStatus::Skipped;
            state_->piece_status(piece_idx, status);
        }
        co_return true;
    } catch (const std::exception& e) {
        LOGCRITICAL("Failed to preallocate files: {}", e.what());
        co_return false;
    }
}

asio::awaitable<int> FileManager::async_prompt(const std::string& question) {
    using namespace boost::asio::experimental::awaitable_operators;
    std::cout << question << std::flush;
    
    auto context = co_await asio::this_coro::executor;
    asio::steady_timer timer{context};
    timer.expires_after(std::chrono::seconds(300)); // 5 minute timeout
    
    std::string input;
    asio::streambuf buffer;
    asio::posix::stream_descriptor in(context, STDIN_FILENO);
    
    auto read_op = asio::async_read_until(
        in, buffer, '\n', asio::use_awaitable
    );
    auto timer_op = timer.async_wait(asio::use_awaitable);
    
    // Wait for either input or timeout/shutdown
    auto result = co_await (std::move(read_op) || std::move(timer_op));
    
    if (result.index() == 0) {
        // Input received
        std::istream is(&buffer);
        std::getline(is, input);
        
        try {
            co_return std::stoi(input);
        } catch (const std::exception&) {
            co_return -1; // Invalid input
        }
    } else {
        // Timeout or interrupted
        // if (shutting_down_.load()) {
        //     throw std::runtime_error("Shutdown during prompt");
        // } else {
            throw std::runtime_error("Input timeout");
        // }
    }
}

asio::awaitable<std::map<std::string, int64_t>> FileManager::async_get_file_mtimes() {
    const auto& files_to_check = state_->torrent_info().files; 
    auto [ec, mtimes_map] = co_await asio::async_initiate<void(std::error_code, std::map<std::string, int64_t>)>(
        [this, files_to_check](auto&& completion_handler) {
            file_io_pool_.enqueue(
                [this, files_to_check, h = std::move(completion_handler)]() mutable {
                    std::map<std::string, int64_t> mtimes;
                    for (const auto& file_info : files_to_check) {
                        auto full_path = get_full_path_for_file(file_info);
                        try {
                            if (std::filesystem::exists(full_path)) {
                                mtimes[file_info.path.string()] = std::filesystem::last_write_time(full_path).time_since_epoch().count();
                            }
                        } catch (const std::exception& e) {
                            LOGWARN("Could not get mtime for {}: {}", full_path.string(), e.what());
                            // Decide how to handle this error. For now, log and skip.
                        }
                    }
                    std::move(h)(boost::system::error_code{}, std::move(mtimes));
                }
            );
        },
        asio::as_tuple(asio::use_awaitable)
    );
    if (ec) {
        throw boost::system::system_error(ec, "async_get_file_mtimes");
    }
    co_return mtimes_map;
}

// -- Disk cache helpers --

void FileManager::touch_cache(const CacheKey& key) {
    auto it = lru_index_.find(key);
    if (it != lru_index_.end()) {
        lru_order_.erase(it->second);
    }
    lru_order_.push_front(key);
    lru_index_[key] = lru_order_.begin();
}

void FileManager::evict_if_needed() {
    while (cache_current_size_ > DISK_CACHE_SIZE && !lru_order_.empty()) {
        CacheKey evict_key = lru_order_.back();
        lru_order_.pop_back();
        lru_index_.erase(evict_key);

        auto cache_it = cache_.find(evict_key);
        if (cache_it == cache_.end()) {
            dirty_blocks_.erase(evict_key);
            continue;
        }

        if (dirty_blocks_.contains(evict_key)) {
            sync_write_block(evict_key, cache_it->second);
            dirty_blocks_.erase(evict_key);
        }

        cache_current_size_ -= cache_it->second.size();
        cache_.erase(cache_it);
    }
}

void FileManager::sync_write_block(const CacheKey& key, const std::vector<std::byte>& data) {
    auto [piece_index, offset] = key;
    if (data.empty()) return;
    try {
        auto overlaps_ptr = get_piece_to_files_map(piece_index);
        const auto& overlaps = *overlaps_ptr;
        uint32_t block_end = offset + static_cast<uint32_t>(data.size());

        for (const auto& overlap : overlaps) {
            const auto& file_info = state_->torrent_info().files.at(overlap.file_index);
            if (!file_info.download) continue;

            uint32_t overlap_start = overlap.offset_in_piece;
            uint32_t overlap_end = overlap_start + overlap.length;

            uint32_t intersect_start = std::max(offset, overlap_start);
            uint32_t intersect_end = std::min(block_end, overlap_end);

            if (intersect_end > intersect_start) {
                uint32_t data_offset = intersect_start - offset;
                uint32_t file_off_in_overlap = intersect_start - overlap_start;
                uint64_t file_offset = overlap.offset_in_file + file_off_in_overlap;
                uint32_t write_len = intersect_end - intersect_start;

                auto full_path = get_full_path_for_file(file_info);
                std::filesystem::create_directories(full_path.parent_path());

                std::lock_guard file_lock(file_locker_->get_lock(full_path));
                std::fstream output_file(full_path, std::ios::binary | std::ios::in | std::ios::out);
                if (!output_file.is_open()) {
                    throw std::runtime_error("Failed to open file for sync write: " + full_path.string());
                }
                output_file.clear();
                output_file.seekp(static_cast<std::streamoff>(file_offset), std::ios::beg);
                if (output_file.fail()) {
                    throw std::runtime_error("Failed to seek in file: " + full_path.string());
                }
                output_file.write(reinterpret_cast<const char*>(data.data() + data_offset),
                                  static_cast<std::streamsize>(write_len));
                if (!output_file) {
                    throw std::runtime_error("Failed to sync write to file: " + full_path.string());
                }
                output_file.flush();
            }
        }
    } catch (const std::exception& e) {
        LOGERR("Sync write-back error for piece {} offset {}: {}", piece_index, offset, e.what());
    }
}

void FileManager::sync_flush_all_dirty() {
    std::vector<std::pair<CacheKey, std::vector<std::byte>>> to_flush;
    {
        std::lock_guard lock(cache_mutex_);
        for (const auto& key : dirty_blocks_) {
            auto it = cache_.find(key);
            if (it != cache_.end()) {
                to_flush.emplace_back(key, it->second);
            }
        }
        dirty_blocks_.clear();
    }

    for (auto& [key, data] : to_flush) {
        sync_write_block(key, data);
    }
}

asio::awaitable<void> FileManager::flush() {
    co_await flush_all_dirty();
}

asio::awaitable<void> FileManager::flush_all_dirty() {
    std::vector<std::pair<CacheKey, std::vector<std::byte>>> to_flush;
    {
        std::lock_guard lock(cache_mutex_);
        for (const auto& key : dirty_blocks_) {
            auto it = cache_.find(key);
            if (it != cache_.end()) {
                to_flush.emplace_back(key, it->second);
            }
        }
        dirty_blocks_.clear();
    }

    for (auto& [key, data] : to_flush) {
        auto [piece_index, offset] = key;
        try {
            const auto& overlaps = *get_piece_to_files_map(piece_index);
            uint32_t block_end = offset + static_cast<uint32_t>(data.size());

            for (const auto& overlap : overlaps) {
                const auto& file_info = state_->torrent_info().files.at(overlap.file_index);
                if (!file_info.download) continue;

                uint32_t overlap_start = overlap.offset_in_piece;
                uint32_t overlap_end = overlap_start + overlap.length;

                uint32_t intersect_start = std::max(offset, overlap_start);
                uint32_t intersect_end = std::min(block_end, overlap_end);

                if (intersect_end > intersect_start) {
                    uint32_t data_offset = intersect_start - offset;
                    uint32_t file_off_in_overlap = intersect_start - overlap_start;
                    uint64_t file_offset = overlap.offset_in_file + file_off_in_overlap;
                    uint32_t write_len = intersect_end - intersect_start;

                    auto full_path = get_full_path_for_file(file_info);
                    co_await async_write_to_file(full_path, file_offset,
                        std::span<const std::byte>(data.data() + data_offset, write_len));
                }
            }
        } catch (const std::exception& e) {
            LOGERR("Async flush error for piece {} offset {}: {}", piece_index, offset, e.what());
        }
    }
}

asio::awaitable<void> FileManager::periodic_flush() {
    while (!shutting_down_.load()) {
        flush_timer_->expires_after(std::chrono::seconds(5));
        boost::system::error_code ec;
        co_await flush_timer_->async_wait(asio::redirect_error(asio::use_awaitable, ec));
        if (ec == asio::error::operation_aborted || shutting_down_.load()) {
            co_return;
        }
        co_await flush_all_dirty();
    }
}
