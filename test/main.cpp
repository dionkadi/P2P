#include <gtest/gtest.h>
#include "Utils.hpp"

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    Logger::init("test");
    return RUN_ALL_TESTS();
}