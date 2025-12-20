#include "Bencode.hpp"
#include <charconv>
#include <cstddef>
#include <format>
#include <iterator>
#include <memory>
#include <span>
#include <string>
#include <system_error>
#include <variant>
#include <algorithm>
#include <vector>

template<typename... Ts>
struct overloaded : Ts... {
    using Ts::operator()...;
};

template<typename... Ts>
overloaded(Ts...) -> overloaded<Ts...>;

Value::Value(const Value& other) {
    data_ = std::visit(overloaded {
        [](Integer i) -> BencodeVariant {return i; },
        [](const String& s) -> BencodeVariant { return s; },
        [](const std::unique_ptr<List>& l) -> BencodeVariant {
            return std::make_unique<List>(*l);
        },
        [](const std::unique_ptr<Dict>& d) -> BencodeVariant {
            return std::make_unique<Dict>(*d);
        }
    }, other.data_);
}

Value& Value::operator=(const Value& other) {
    if (this == &other) {
        return *this;
    }

    data_ = std::visit(overloaded {
        [](Integer i) -> BencodeVariant { return i; },
        [](const String& s) -> BencodeVariant { return s; },
        [](const std::unique_ptr<List>& l) -> BencodeVariant {
            return std::make_unique<List>(*l);
        },
        [](const std::unique_ptr<Dict>& d) -> BencodeVariant {
            return std::make_unique<Dict>(*d);
        }
    }, other.data_);

    return *this;
}

Value::Value(Integer i) : data_(i) {}
Value::Value(const String& s) : data_(s) {}
Value::Value(String&& s) : data_(std::move(s)) {}
Value::Value(const List& l) : data_(std::make_unique<List>(l)) {}
Value::Value(List&& l) : data_(std::make_unique<List>(std::move(l))) {}
Value::Value(const Dict& d) : data_(std::make_unique<Dict>(d)) {}
Value::Value(Dict&& d) : data_(std::make_unique<Dict>(std::move(d))) {}

Value decode_value(std::span<const std::byte>& data);

String decode_string(std::span<const std::byte>& data) {
    auto colon_it = std::find(data.begin(), data.end(), static_cast<std::byte>(':'));
    if (colon_it == data.end()) {
        throw std::runtime_error("Bencode: Invalid string format, missing colon.");
    }
    size_t colon_pos = std::distance(data.begin(), colon_it);

    long long len;
    auto [ptr, ec] = std::from_chars(reinterpret_cast<const char *>(data.data()), reinterpret_cast<const char *>(data.data()) + colon_pos, len);
    if (ec != std::errc()) {
        throw std::runtime_error("Bencode: Invalid string length.");
    }
    if (len < 0) {
        throw std::runtime_error("Bencode: String length cannot be negative.");
    }

    data = data.subspan(colon_pos + 1);

    if (static_cast<size_t>(len) > data.size()) {
        throw std::runtime_error("Bencode: String length exceeds buffer size.");
    }

    String result(reinterpret_cast<const char *>(data.data()), len);
    data = data.subspan(len);
    return result;
}

Integer decode_integer(std::span<const std::byte>& data) {
    if (data.empty() || data.front() != static_cast<std::byte>('i')) {
        throw std::runtime_error("Bencode: Invalid integer format, missing 'i'.");
    }

    data = data.subspan(1);

    auto end_it = std::find(data.begin(), data.end(), static_cast<std::byte>('e'));
    if (end_it == data.end()) {
        throw std::runtime_error("Bencode: Invalid integer format, missing 'e'.");
    }
    size_t end_pos = std::distance(data.begin(), end_it);

    Integer result;
    auto [ptr, ec] = std::from_chars(reinterpret_cast<const char *>(data.data()), reinterpret_cast<const char *>(data.data()) + end_pos, result);
    if (ec != std::errc() || ptr != reinterpret_cast<const char *>(data.data()) + end_pos) {
        throw std::runtime_error("Bencode: Failed to parse integer value.");
    }
    
    data = data.subspan(end_pos + 1);
    return result;
}

List decode_list(std::span<const std::byte>& data) {
    if (data.empty() || data.front() != static_cast<std::byte>('l')) {
        throw std::runtime_error("Bencode: Invalid list format, missing 'l'.");
    }
    data = data.subspan(1);

    List result;
    while (!data.empty() && data.front() != static_cast<std::byte>('e')) {
        result.push_back(decode_value(data));
    }

    if (data.empty() || data.front() != static_cast<std::byte>('e')) {
        throw std::runtime_error("Bencode: Invalid list format, missing 'e'.");
    }

    data = data.subspan(1);
    return result;
}

Dict decode_dict(std::span<const std::byte>& data) {
    if (data.empty() || data.front() != static_cast<std::byte>('d')) {
        throw std::runtime_error("Bencode: Invalid dictionary format, missing 'd'.");
    }
    data = data.subspan(1);

    Dict result;
    while (!data.empty() && data.front() != static_cast<std::byte>('e')) {
        String key = decode_string(data);
        Value value = decode_value(data);
        result.emplace(std::move(key), std::move(value));
    }

    if (data.empty() || data.front() != static_cast<std::byte>('e')) {
        throw std::runtime_error("Bencode: Invalid dictionary format, missing 'e'.");
    }
    
    data = data.subspan(1);
    return result;
}

Value decode_value(std::span<const std::byte>& data) {
    if (data.empty()) {
        throw std::runtime_error("Bencode: Unexpected end of data.");
    }

    if (data.front() >= static_cast<std::byte>('0') && data.front() <= static_cast<std::byte>('9')) {
        return Value(decode_string(data));
    }

    switch (static_cast<char>(data.front())) {
        case 'i': return Value(decode_integer(data));
        case 'l': return Value(decode_list(data));
        case 'd': return Value(decode_dict(data));
        default: throw std::runtime_error("Bencode: Unknown type specifier.");
    }
}

Value decode(std::span<const std::byte> data) {
    auto view = data;
    Value result = decode_value(view);
    if (!view.empty()) {
        throw std::runtime_error("Bencode: Trailing data left after decoding.");
    }
    return result;
}

std::vector<std::byte> encode(const Value &value) {
    std::string encode_str;
    std::visit(overloaded {
        [&](Integer i) {
            encode_str += std::format("i{}e", i);
        },
        [&](const String& s) {
            encode_str += std::format("{}:{}", s.length(), s);
        },
        [&](const std::unique_ptr<List>& l) {
            encode_str += 'l';
            for (const auto& item : *l) {
                auto encoded_item = encode(item);
                encode_str.append(encoded_item.begin(), encoded_item.end());
            }
            encode_str += 'e';
        },
        [&](const std::unique_ptr<Dict>& d) {
            encode_str += 'd';
            for (const auto& [key, val] : *d) {
                encode_str += std::format("{}:{}", key.length(), key);
                auto encoded_val = encode(val);
                encode_str.append(encoded_val.begin(), encoded_val.end());
            }
            encode_str += 'e';
        }
    }, value.get_variant());

    return {reinterpret_cast<std::byte*>(encode_str.data()), reinterpret_cast<std::byte*>(encode_str.data()) + encode_str.size()};
}