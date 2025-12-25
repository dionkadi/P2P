#include <type_traits>
#include <vector>
#include <span>
#include <string_view>
#include <cstring>
#include <stdexcept>

template<typename T>
concept POD = std::is_standard_layout_v<T> && std::is_trivial_v<T>;

class BufferWriter {
public:
    explicit BufferWriter(std::vector<std::byte>& buffer) noexcept
        : buffer_(buffer) {}

    template<POD T>
    void write(const T& value) {
        const std::byte* begin = reinterpret_cast<const std::byte*>(&value);
        buffer_.insert(buffer_.end(), begin, begin + sizeof(T));
    }

    void write_raw(std::string_view sv) {
        buffer_.insert(buffer_.end(), reinterpret_cast<const std::byte*>(sv.data()), reinterpret_cast<const std::byte*>(sv.data()) + sv.size());
    }

    void write_bytes(std::span<const std::byte> data) {
        buffer_.insert(buffer_.end(), data.begin(), data.end());
    }

private:
    std::vector<std::byte>& buffer_;
};

class BufferReader {
public:
    explicit BufferReader(std::span<const std::byte> buffer) noexcept
        : view_(buffer) {}

    template<POD T>
    T read() {
        if (view_.size() < sizeof(T)) {
            throw std::runtime_error("Not enough data in buffer to read value.");
        }

        T value;
        std::memcpy(&value, view_.data(), sizeof(T));
        view_ = view_.subspan(sizeof(T));
        return value;
    }

    std::span<const std::byte> read_bytes(size_t size) {
        if (view_.size() < size) {
            throw std::runtime_error("Not enough data in buffer to read bytes.");
        }

        auto result = view_.subspan(0, size);
        view_ = view_.subspan(size);
        return result;
    }

    std::span<const std::byte> read_all() {
        return read_bytes(remaining());
    }

    size_t remaining() const noexcept { return view_.size(); }
    bool empty() const noexcept { return view_.empty(); }

private:
    std::span<const std::byte> view_;
};