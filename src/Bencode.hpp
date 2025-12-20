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

    Value() = default;
    Value(const Value& other);
    Value& operator=(const Value& other);

    Value(Value&& other) = default;
    Value& operator=(Value&& other) = default;

    Value(Integer i);
    Value(const String& s);
    Value(String&& s);
    Value(const List& l);
    Value(List&& l);
    Value(const Dict& d);
    Value(Dict&& d);

    const BencodeVariant& get_variant() const { return data_; }

private:
    BencodeVariant data_;
};


Value decode(std::span<const std::byte> data);
std::vector<std::byte> encode(const Value& value);