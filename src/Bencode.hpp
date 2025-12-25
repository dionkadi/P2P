#pragma once

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <span>
#include <string>
#include <variant>
#include <vector>

class Value;

using Integer = int64_t;
using String = std::string;
using List = std::vector<Value>;
using Dict = std::map<String, Value>;

class Value {
public:
    using BencodeVariant = std::variant<Integer, String, std::unique_ptr<List>, std::unique_ptr<Dict>>;

    Value() noexcept = default;
    Value(const Value& other);
    Value& operator=(const Value& other);

    Value(Value&& other) noexcept = default;
    Value& operator=(Value&& other) noexcept = default;

    Value(Integer i) noexcept : data_(i) {}
    Value(const String& s) noexcept : data_(s) {}
    Value(String&& s) noexcept : data_(std::move(s)) {}
    Value(const List& l) : data_(std::make_unique<List>(l)) {}
    Value(List&& l) noexcept : data_(std::make_unique<List>(std::move(l))) {}
    Value(const Dict& d) : data_(std::make_unique<Dict>(d)) {}
    Value(Dict&& d) noexcept : data_(std::make_unique<Dict>(std::move(d))) {}

    const BencodeVariant& get_variant() const noexcept { return data_; }

private:
    BencodeVariant data_;
};


Value decode(std::span<const std::byte> data);
std::vector<std::byte> encode(const Value& value);