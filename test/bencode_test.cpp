#include "helper.hpp"

TEST(BencodeTest, IntegerEncodingDecoding) {
    // Positive integer
    Value val1(static_cast<Integer>(123));
    std::vector<std::byte> encoded1 = encode(val1);
    EXPECT_EQ(bytes_to_string(encoded1), "i123e");
    Value decoded1 = decode(encoded1);
    ASSERT_TRUE(std::holds_alternative<Integer>(decoded1.get_variant()));
    EXPECT_EQ(std::get<Integer>(decoded1.get_variant()), 123);

    // Negative integer
    Value val2(static_cast<Integer>(-456));
    std::vector<std::byte> encoded2 = encode(val2);
    EXPECT_EQ(bytes_to_string(encoded2), "i-456e");
    Value decoded2 = decode(encoded2);
    ASSERT_TRUE(std::holds_alternative<Integer>(decoded2.get_variant()));
    EXPECT_EQ(std::get<Integer>(decoded2.get_variant()), -456);

    // Zero
    Value val3(static_cast<Integer>(0));
    std::vector<std::byte> encoded3 = encode(val3);
    EXPECT_EQ(bytes_to_string(encoded3), "i0e");
    Value decoded3 = decode(encoded3);
    ASSERT_TRUE(std::holds_alternative<Integer>(decoded3.get_variant()));
    EXPECT_EQ(std::get<Integer>(decoded3.get_variant()), 0);

    // Large integer
    Value val4(static_cast<Integer>(9223372036854775807LL)); // Max long long
    std::vector<std::byte> encoded4 = encode(val4);
    EXPECT_EQ(bytes_to_string(encoded4), "i9223372036854775807e");
    Value decoded4 = decode(encoded4);
    ASSERT_TRUE(std::holds_alternative<Integer>(decoded4.get_variant()));
    EXPECT_EQ(std::get<Integer>(decoded4.get_variant()), 9223372036854775807LL);
}

TEST(BencodeTest, StringEncodingDecoding) {
    // Simple string
    Value val1(String("hello"));
    std::vector<std::byte> encoded1 = encode(val1);
    EXPECT_EQ(bytes_to_string(encoded1), "5:hello");
    Value decoded1 = decode(encoded1);
    ASSERT_TRUE(std::holds_alternative<String>(decoded1.get_variant()));
    EXPECT_EQ(std::get<String>(decoded1.get_variant()), "hello");

    // Empty string
    Value val2(String(""));
    std::vector<std::byte> encoded2 = encode(val2);
    EXPECT_EQ(bytes_to_string(encoded2), "0:");
    Value decoded2 = decode(encoded2);
    ASSERT_TRUE(std::holds_alternative<String>(decoded2.get_variant()));
    EXPECT_EQ(std::get<String>(decoded2.get_variant()), "");

    // String with special characters
    Value val3(String("key:value"));
    std::vector<std::byte> encoded3 = encode(val3);
    EXPECT_EQ(bytes_to_string(encoded3), "9:key:value");
    Value decoded3 = decode(encoded3);
    ASSERT_TRUE(std::holds_alternative<String>(decoded3.get_variant()));
    EXPECT_EQ(std::get<String>(decoded3.get_variant()), "key:value");
}

TEST(BencodeTest, ListEncodingDecoding) {
    // List of integers and strings
    List l1;
    l1.push_back(Value(static_cast<Integer>(1)));
    l1.push_back(Value(String("a")));
    l1.push_back(Value(static_cast<Integer>(2)));
    Value val1(l1);
    std::vector<std::byte> encoded1 = encode(val1);
    EXPECT_EQ(bytes_to_string(encoded1), "li1e1:ai2ee");
    Value decoded1 = decode(encoded1);
    ASSERT_TRUE(std::holds_alternative<std::unique_ptr<List>>(decoded1.get_variant()));
    const List& list1 = *std::get<std::unique_ptr<List>>(decoded1.get_variant());
    EXPECT_EQ(list1.size(), 3);
    EXPECT_EQ(std::get<Integer>(list1[0].get_variant()), 1);
    EXPECT_EQ(std::get<String>(list1[1].get_variant()), "a");
    EXPECT_EQ(std::get<Integer>(list1[2].get_variant()), 2);

    // Empty list
    List l2;
    Value val2(l2);
    std::vector<std::byte> encoded2 = encode(val2);
    EXPECT_EQ(bytes_to_string(encoded2), "le");
    Value decoded2 = decode(encoded2);
    ASSERT_TRUE(std::holds_alternative<std::unique_ptr<List>>(decoded2.get_variant()));
    const List& list2 = *std::get<std::unique_ptr<List>>(decoded2.get_variant());
    EXPECT_TRUE(list2.empty());

    // Nested list
    List nested_l3;
    nested_l3.push_back(Value(static_cast<Integer>(100)));
    List l3;
    l3.push_back(Value(String("outer")));
    l3.push_back(Value(nested_l3));
    Value val3(l3);
    std::vector<std::byte> encoded3 = encode(val3);
    EXPECT_EQ(bytes_to_string(encoded3), "l5:outerli100eee");
    Value decoded3 = decode(encoded3);
    ASSERT_TRUE(std::holds_alternative<std::unique_ptr<List>>(decoded3.get_variant()));
    const List& list3 = *std::get<std::unique_ptr<List>>(decoded3.get_variant());
    EXPECT_EQ(list3.size(), 2);
    EXPECT_EQ(std::get<String>(list3[0].get_variant()), "outer");
    ASSERT_TRUE(std::holds_alternative<std::unique_ptr<List>>(list3[1].get_variant()));
    const List& inner_list3 = *std::get<std::unique_ptr<List>>(list3[1].get_variant());
    EXPECT_EQ(inner_list3.size(), 1);
    EXPECT_EQ(std::get<Integer>(inner_list3[0].get_variant()), 100);
}

TEST(BencodeTest, DictEncodingDecoding) {
    // Simple dictionary
    Dict d1;
    d1["foo"] = Value(String("bar"));
    d1["num"] = Value(static_cast<Integer>(42));
    Value val1(d1);
    std::vector<std::byte> encoded1 = encode(val1);
    // Dictionary keys are sorted lexicographically
    EXPECT_EQ(bytes_to_string(encoded1), "d3:foo3:bar3:numi42ee");
    Value decoded1 = decode(encoded1);
    ASSERT_TRUE(std::holds_alternative<std::unique_ptr<Dict>>(decoded1.get_variant()));
    const Dict& dict1 = *std::get<std::unique_ptr<Dict>>(decoded1.get_variant());
    EXPECT_EQ(dict1.size(), 2);
    EXPECT_EQ(std::get<String>(dict1.at("foo").get_variant()), "bar");
    EXPECT_EQ(std::get<Integer>(dict1.at("num").get_variant()), 42);

    // Empty dictionary
    Dict d2;
    Value val2(d2);
    std::vector<std::byte> encoded2 = encode(val2);
    EXPECT_EQ(bytes_to_string(encoded2), "de");
    Value decoded2 = decode(encoded2);
    ASSERT_TRUE(std::holds_alternative<std::unique_ptr<Dict>>(decoded2.get_variant()));
    const Dict& dict2 = *std::get<std::unique_ptr<Dict>>(decoded2.get_variant());
    EXPECT_TRUE(dict2.empty());

    // Nested dictionary and list
    Dict inner_d3;
    inner_d3["inner_key"] = Value(String("inner_value"));
    List l3;
    l3.push_back(Value(static_cast<Integer>(1)));
    Dict d3;
    d3["my_dict"] = Value(inner_d3);
    d3["my_list"] = Value(l3);
    Value val3(d3);
    std::vector<std::byte> encoded3 = encode(val3);
    EXPECT_EQ(bytes_to_string(encoded3), "d7:my_dictd9:inner_key11:inner_valuee7:my_listli1eee");
    Value decoded3 = decode(encoded3);
    ASSERT_TRUE(std::holds_alternative<std::unique_ptr<Dict>>(decoded3.get_variant()));
    const Dict& dict3 = *std::get<std::unique_ptr<Dict>>(decoded3.get_variant());
    EXPECT_EQ(dict3.size(), 2);
    ASSERT_TRUE(std::holds_alternative<std::unique_ptr<Dict>>(dict3.at("my_dict").get_variant()));
    const Dict& inner_dict3 = *std::get<std::unique_ptr<Dict>>(dict3.at("my_dict").get_variant());
    EXPECT_EQ(std::get<String>(inner_dict3.at("inner_key").get_variant()), "inner_value");
    ASSERT_TRUE(std::holds_alternative<std::unique_ptr<List>>(dict3.at("my_list").get_variant()));
    const List& list3 = *std::get<std::unique_ptr<List>>(dict3.at("my_list").get_variant());
    EXPECT_EQ(std::get<Integer>(list3[0].get_variant()), 1);
}

TEST(BencodeTest, InvalidBencodeStrings) {
    // Missing 'e' for integer
    EXPECT_THROW(decode(string_to_bytes("i123")), std::runtime_error);
    // Invalid integer format
    EXPECT_THROW(decode(string_to_bytes("i123ae")), std::runtime_error);
    // Missing colon for string
    EXPECT_THROW(decode(string_to_bytes("5hello")), std::runtime_error);
    // String length mismatch (too short)
    EXPECT_THROW(decode(string_to_bytes("5:hell")), std::runtime_error);
    // String length mismatch (too long)
    EXPECT_THROW(decode(string_to_bytes("5:helloX")), std::runtime_error);
    // Missing 'e' for list
    EXPECT_THROW(decode(string_to_bytes("li1e")), std::runtime_error);
    // Missing 'e' for dictionary
    EXPECT_THROW(decode(string_to_bytes("d3:foo3:bar")), std::runtime_error);
    // Dictionary key not a string
    EXPECT_THROW(decode(string_to_bytes("di1e3:bar:e")), std::runtime_error);
    // Trailing data
    EXPECT_THROW(decode(string_to_bytes("i123eX")), std::runtime_error);
    // Unknown type specifier
    EXPECT_THROW(decode(string_to_bytes("x123e")), std::runtime_error);
    // Empty data
    EXPECT_THROW(decode(string_to_bytes("")), std::runtime_error);
}

// Test Value copy constructor and assignment operator
TEST(BencodeTest, ValueCopySemantics) {
    Dict original_dict;
    original_dict["key1"] = Value(String("value1"));
    original_dict["key2"] = Value(static_cast<Integer>(100));
    Value original_value(original_dict);

    // Test copy constructor
    Value copied_value = original_value;
    ASSERT_TRUE(std::holds_alternative<std::unique_ptr<Dict>>(copied_value.get_variant()));
    const Dict& copied_dict = *std::get<std::unique_ptr<Dict>>(copied_value.get_variant());
    EXPECT_EQ(std::get<String>(copied_dict.at("key1").get_variant()), "value1");
    EXPECT_EQ(std::get<Integer>(copied_dict.at("key2").get_variant()), 100);

    // Ensure deep copy for unique_ptr variants
    // Modify original and check if copy is unchanged
    std::get<std::unique_ptr<Dict>>(original_value.get_variant())->at("key1") = Value(String("modified"));
    EXPECT_EQ(std::get<String>(copied_dict.at("key1").get_variant()), "value1"); // Should still be "value1"

    // Test assignment operator
    Dict another_dict;
    another_dict["another_key"] = Value(static_cast<Integer>(200));
    Value assigned_value(another_dict);
    assigned_value = original_value; // Assign original to assigned
    ASSERT_TRUE(std::holds_alternative<std::unique_ptr<Dict>>(assigned_value.get_variant()));
    const Dict& assigned_dict = *std::get<std::unique_ptr<Dict>>(assigned_value.get_variant());
    EXPECT_EQ(std::get<String>(assigned_dict.at("key1").get_variant()), "modified"); // Now expects "modified" from modified original
    EXPECT_EQ(std::get<Integer>(assigned_dict.at("key2").get_variant()), 100);

    // // Self-assignment
    // Value self_assigned = original_value;
    // self_assigned = self_assigned;
    // ASSERT_TRUE(std::holds_alternative<std::unique_ptr<Dict>>(self_assigned.get_variant()));
    // const Dict& self_assigned_dict = *std::get<std::unique_ptr<Dict>>(self_assigned.get_variant());
    // EXPECT_EQ(std::get<String>(self_assigned_dict.at("key1").get_variant()), "modified");
}

TEST(BencodeTest, ValueMoveSemantics) {
    Dict original_dict;
    original_dict["key1"] = Value(String("value1"));
    Value original_value(original_dict);

    // Test move constructor
    Value moved_value = std::move(original_value);
    ASSERT_TRUE(std::holds_alternative<std::unique_ptr<Dict>>(moved_value.get_variant()));
    const Dict& moved_dict = *std::get<std::unique_ptr<Dict>>(moved_value.get_variant());
    EXPECT_EQ(std::get<String>(moved_dict.at("key1").get_variant()), "value1");
    
    // original_value.data_ should now hold a unique_ptr that is empty.
    ASSERT_TRUE(std::holds_alternative<std::unique_ptr<Dict>>(original_value.get_variant()));
    EXPECT_EQ(std::get<std::unique_ptr<Dict>>(original_value.get_variant()), nullptr);
    
    // Test move assignment
    Dict another_dict;
    another_dict["another_key"] = Value(static_cast<Integer>(200));
    Value assigned_value(another_dict);
    Value another_original_value(Dict{{"move_me", Value(String("data"))}});
    assigned_value = std::move(another_original_value);
    ASSERT_TRUE(std::holds_alternative<std::unique_ptr<Dict>>(assigned_value.get_variant()));
    const Dict& assigned_dict = *std::get<std::unique_ptr<Dict>>(assigned_value.get_variant());
    EXPECT_EQ(std::get<String>(assigned_dict.at("move_me").get_variant()), "data");
    
    ASSERT_TRUE(std::holds_alternative<std::unique_ptr<Dict>>(another_original_value.get_variant()));
    EXPECT_EQ(std::get<std::unique_ptr<Dict>>(another_original_value.get_variant()), nullptr);
}
