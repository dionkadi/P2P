#include "FileManager.hpp"
#include <algorithm>
#include <boost/asio/async_result.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/system/detail/error_code.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <exception>
#include <filesystem>
#include <fstream>
#include <ios>
#include <memory>
#include <mutex>
#include <optional>
#include <system_error>
#include <vector>

FileManager::FileManager(std::shared_ptr<SessionState> state)
    : state_(state),
      file_io_pool_(get_file_io_pool()),
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

asio::awaitable<std::vector<std::byte>> FileManager::read_block(size_t piece_index, uint32_t begin, uint32_t length) {
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

    co_return block_data;
}

asio::awaitable<void> FileManager::write_piece(size_t piece_index, std::span<const std::byte> piece_data) {
    const auto& overlaps = *get_piece_to_files_map(piece_index);
    for (const auto& overlap : overlaps) {
        const auto& file_info = state_->torrent_info().files.at(overlap.file_index);
        if (file_info.download) {
            auto full_path = get_full_path_for_file(file_info);
            auto data_to_write = piece_data.subspan(overlap.offset_in_piece, overlap.length);
            co_await async_write_to_file(full_path, overlap.offset_in_file, data_to_write);
        }
    }
}

ThreadPool& FileManager::get_file_io_pool() {
    static ThreadPool instance(4);
    return instance;
}

void FileManager::build_maps() {
    const auto& info = state_->torrent_info();
    const size_t num_pieces = state_->num_pieces();
    const uint32_t piece_size = info.piece_size;
    const uint64_t total_torrent_size = info.total_size;

    piece_to_files_map_.resize(num_pieces);
    file_to_pieces_map_.resize(info.files.size());

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
            if (piece_idx == num_pieces - 1) { // Last piece
                actual_piece_size = total_torrent_size - piece_start_offset;
                // If total_torrent_size is a perfect multiple of piece_size, 
                // the last piece will have piece_size.
                // If total_torrent_size is 0, actual_piece_size should be 0.
                if (total_torrent_size == 0 && num_pieces == 0) actual_piece_size = 0;
                else if (actual_piece_size == 0 && total_torrent_size > 0 && num_pieces > 0) { // e.g. 10MB total, 2MB piece, last piece starts at 8MB, actual size 2MB
                    actual_piece_size = piece_size;
                }
            } else {
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
    std::filesystem::path p;
    if (info.files.size() > 1) {
        p = data_file_path / info.name / ".resume";
    } else {
        // For single file, place .resume alongside the file
        p = data_file_path.parent_path() / (data_file_path.filename().string() + ".resume");
    }
    return p;
}

asio::awaitable<void> FileManager::async_write_to_file(const std::filesystem::path& path, uint64_t offset, std::span<const std::byte> data) {
    co_await asio::async_initiate<void(std::error_code)>(
        [this, &path, offset, data] (auto&& completion_handler) {
            file_io_pool_.enqueue(
                [path, offset, data, locker = file_locker_, handler = std::move(completion_handler)]
                () mutable {
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
        [this, &path, offset, size] (auto&& completion_handler) {
            file_io_pool_.enqueue(
                [&path, offset, size, handler = std::move(completion_handler)]
                () mutable {
                    try {
                        // LOGDBG("FileManager: [FileIO Pool] Attempting to open '{}'", path.string());
                        std::ifstream data_file(path, std::ios::binary);
                        if (!data_file) {
                            LOGERR("FileManager: [FileIO Pool] Failed to open file for reading: {}", path.string());
                            throw std::runtime_error("Failed to open file for reading: " + path.string());
                        }
                        
                        uint64_t actual_file_size = std::filesystem::file_size(path);
                        // LOGDBG("FileManager: [FileIO Pool] Reading from '{}', requested offset={}, size={}, actual file size={}", 
                        //     path.string(), offset, size, actual_file_size);
                        // Validate read range against actual file size
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
                        // LOGDBG("FileManager: [FileIO Pool] Read {} bytes, requested {}. EOF={}, Fail={}", bytes_read_actual, size, data_file.eof(), data_file.fail());
                        if (static_cast<uint32_t>(bytes_read_actual) != size) {
                            LOGERR("FileManager: [FileIO Pool] Incomplete read from file '{}'. Read {} bytes, requested {}. EOF={}, Fail={}", 
                                path.string(), bytes_read_actual, size, data_file.eof(), data_file.fail());
                            throw std::runtime_error("Incomplete read from file: " + path.string());
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
        
        LOGDBG("Progress saved successfully to {}", p.string());
    } catch(const std::exception& e) {
        LOGERR("Failed to save resume file: {}", e.what());
        
        std::error_code ec;
        std::filesystem::remove(temp_path, ec);
        if (ec) {
            LOGDBG("Could not remove temporary resume file {}: {}", temp_path.string(), ec.message());
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
    const auto& info = state_->torrent_info();
    const size_t num_pieces = state_->num_pieces();

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
                                mtimes[full_path.string()] = std::filesystem::last_write_time(full_path).time_since_epoch().count();
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