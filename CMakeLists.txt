cmake_minimum_required(VERSION 4.0)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

project(P2PFileSharing
        VERSION 1.0
        DESCRIPTION "A P2P file sharing"
        LANGUAGES C CXX
)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
        message(STATUS "Setting build type to `Debug` as none was specified.")
        set(CMAKE_BUILD_TYPE "Debug")
endif()

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -Wall -Wextra -Werror")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -g -fsanitize=address -fno-omit-frame-pointer -fno-optimize-sibling-calls")
set(CMAKE_POSITION_INDEPENDENT_CODE ON)


find_package(Threads REQUIRED)

add_subdirectory(third_party/json)
add_subdirectory(third_party/picosha2)
add_subdirectory(third_party/spdlog)
add_subdirectory(third_party/asio/asio)

add_library(p2p_common
    src/AsyncSocket.cpp
    src/Logger.cpp
    src/Peer.cpp
    src/Tracker.cpp
    src/Crypto.cpp
    src/MetaInfo.cpp
    src/Protocol.cpp
    src/ThreadPool.cpp
)

target_include_directories(p2p_common PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}/src/include"
    third_party/asio/asio/include
    third_party/picosha2/
    third_party/json/include/nlohmann
    third_party/spdlog/include/spdlog
)

set(INCLUDEDIRS
        src/include
        third_party/asio/asio/include
        third_party/picosha2
)

target_link_libraries(p2p_common PUBLIC
    spdlog
    Threads::Threads
    picosha2
    asio
)

add_executable(tracker src/main_tracker.cpp)
add_executable(client src/main_client.cpp)

target_link_libraries(tracker PRIVATE p2p_common)

target_link_libraries(client PRIVATE p2p_common)