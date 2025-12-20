#include "FileManager.hpp"
#include <algorithm>
#include <boost/asio/async_result.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/system/detail/error_code.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>
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

asio::awaitable<void> FileManager::write_piece(size_t piece_index, const std::vector<std::byte>& piece_data) {
    const auto& overlaps = piece_to_files_map_.at(piece_index);
    for (const auto& overlap : overlaps) {
        const auto& file_info = state_->torrent_info().files.at(overlap.file_index);
        if (file_info.download) {
            auto full_path = get_full_path_for_file(file_info);
            std::span<const std::byte> data_to_write(piece_data.data() + overlap.offset_in_piece, overlap.length);
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

    piece_to_files_map_.resize(num_pieces);
    file_to_pieces_map_.resize(info.files.size());

    uint64_t current_total_offset = 0;
    for (size_t file_idx = 0; file_idx < info.files.size(); ++file_idx) {
        const auto& file = info.files[file_idx];
        uint64_t file_start_offset = current_total_offset;
        uint64_t file_end_offset = file_start_offset + file.size;

        uint32_t start_piece = file_start_offset / piece_size;
        uint32_t end_piece = (file_end_offset - 1) / piece_size;
        for (uint32_t piece_idx = start_piece; piece_idx <= end_piece; ++piece_idx) {
            uint64_t piece_start_offset = static_cast<uint64_t>(piece_idx) * piece_size;

            uint64_t overlap_start = std::max(file_start_offset, piece_start_offset);
            uint64_t overlap_end = std::min(file_end_offset, piece_start_offset + piece_size);

            if (overlap_end > overlap_start) {
                PieceFileOverlap overlap;
                overlap.file_index = file_idx;
                overlap.offset_in_file = overlap_start - file_start_offset;
                overlap.offset_in_piece = overlap_start - piece_start_offset;
                overlap.length = overlap_end - overlap_start;
                piece_to_files_map_[piece_idx].push_back(std::move(overlap));
                file_to_pieces_map_[file_idx].push_back(piece_idx);
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
                [&path, offset, data, locker = file_locker_, handler = std::move(completion_handler)]
                () mutable {
                    try {
                        std::lock_guard lock(locker->get_lock(path));

                        if (!path.has_parent_path()) {
                            std::filesystem::create_directories(path.parent_path());
                        }

                        std::fstream output_file(path, std::ios::binary | std::ios::in | std::ios::out);
                        if (!output_file) {
                            // File doesn't exist, create it.
                            output_file.open(path, std::ios::binary | std::ios::out);
                            if (!output_file) {
                                throw std::runtime_error("Failed to open file for writing: " + path.string());
                            }
                        }

                        output_file.seekp(offset, std::ios::beg);
                        output_file.write(reinterpret_cast<const char*>(data.data()), data.size());

                        if (!output_file) {
                            throw std::runtime_error("Failed to write data to file: " + path.string());
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
        [this, &path, offset, size] (auto&& completion_handler) {
            file_io_pool_.enqueue(
                [&path, offset, size, handler = std::move(completion_handler)]
                () mutable {
                    try {
                        std::ifstream data_file(path, std::ios::binary);
                        if (!data_file) {
                            throw std::runtime_error("Failed to open file for reading: " + path.string());
                        }

                        if (size == 0) {
                            size = std::filesystem::file_size(path);
                        }

                        std::vector<std::byte> buffer(size);
                        data_file.seekg(offset);
                        data_file.read(reinterpret_cast<char *>(buffer.data()), size);
                        if (static_cast<std::size_t>(data_file.gcount()) != size) {
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

    for (size_t i = 0; i < num_pieces; ++i) {
        if (state_->piece_status(i) != PieceStatus::Have) continue;

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

    for (size_t i = 0; i < num_pieces; ++i) {
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
        std::filesystem::create_directories(p.parent_path());
        co_await async_write_to_file(temp_path, 0, data);
        std::filesystem::rename(temp_path, p);
        LOGDBG("Progress saved successfully");
    } catch(const std::exception& e) {
        LOGERR("Failed to save resume file: {}", e.what());
        
        std::error_code ec;
        std::filesystem::remove(temp_path, ec);
        if (ec) {
            LOGDBG("Could not remove temporary file: {}", ec.message());
        }
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
    auto& info = state_->torrent_info();
    const size_t num_pieces = state_->num_pieces();

    try {
        auto base_save_path = state_->save_path();

        // For a multi-file torrent, base_save_path is the parent directory.
        // We create a subdirectory named after the torrent.
        if (info.files.size() > 1) {
            // Check if the user gave a file path instead of a directory
            if (base_save_path.has_extension() || !std::filesystem::is_directory(base_save_path.parent_path())) {
                base_save_path = base_save_path.parent_path();
            }
        }

        // selective downloading
        bool skip_all = false;
        for (size_t i = 0; i < info.files.size(); ++i) {
            auto& file = info.files[i];
            std::filesystem::path full_path = get_full_path_for_file(file);

            bool skip_this_file = false;
            int ans = co_await async_prompt(
                std::format("Download file {}?\n    1 - Yes\n    2 - No\n    3 - Yes for all\n    4 - No for all\n:", 
                full_path.filename().string())
            );

            if (ans == 1) {
                
            } else if (ans == 2) {
                skip_this_file = true;
            } else if (ans == 3) {
                skip_all = true;
            } else if (ans == 4) {
                skip_this_file = true;
                skip_all = true;
            } else {
                LOGWARN("Unknown input '{}', defaulting to Yes", ans);
            }

            if (skip_this_file) {
                file.download = false;
                continue;
            }

            if (skip_this_file && skip_all) {
                for (size_t j = i; j < info.files.size(); ++j) {
                    info.files[j].download = false;
                }
                break;
            }

            // Only create/truncate the file if it DOES NOT exist
            if (!std::filesystem::exists(full_path) && file.download) {
                LOGINFO("File {} does not exist. Pre-allocating...", full_path.string());
                if (full_path.has_parent_path()) {
                    std::filesystem::create_directories(full_path.parent_path());
                }
                // Now, truncating is safe because the file is new
                std::ofstream output_file(full_path, std::ios::binary | std::ios::trunc);
                if (file.size > 0) {
                    output_file.seekp(file.size - 1);
                    output_file.write("", 1);
                }
            }

            if (skip_all) {
                break;
            }
        }

        for (size_t piece_idx = 0; piece_idx < num_pieces; ++piece_idx) {
            bool is_needed = false;
            for (const auto& overlap : piece_to_files_map_[piece_idx]) {
                if (state_->torrent_info().files[overlap.file_index].download) {
                    is_needed = true;
                    break;
                }
            }
            PieceStatus status = is_needed ? PieceStatus::Needed : PieceStatus::Skipped;
            state_->piece_status(piece_idx, status);
        }
    } catch (const std::exception& e) {
        LOGCRITICAL("Failed to preallocate files: {}", e.what());
        co_return false;
    }

    co_return true;
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