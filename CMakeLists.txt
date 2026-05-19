cmake_minimum_required(VERSION 4.0)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

project(P2PFileSharing
        VERSION 1.1
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
find_package(Boost REQUIRED COMPONENTS url)
find_package(GTest REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(spdlog REQUIRED)


add_library(p2p_common
    src/Bencode.cpp  
    src/FileManager.cpp  
    src/Kademlia.cpp
    src/PeerConnection.cpp  
    src/PeerManager.cpp  
    src/PieceManager.cpp  
    src/TorrentSession.cpp
)

target_link_libraries(p2p_common PUBLIC
    spdlog::spdlog
    Threads::Threads
    Boost::headers
    Boost::url
    OpenSSL::SSL
    OpenSSL::Crypto
)

add_executable(tracker src/main_tracker.cpp)
add_executable(client src/main_client.cpp)

target_link_libraries(tracker PRIVATE p2p_common)
target_link_libraries(client PRIVATE p2p_common)

add_executable(p2p_test
    test/main.cpp
    test/bencode_test.cpp
    test/buffer_test.cpp
    test/crypto_test.cpp
    test/dht_test.cpp
    test/magnet_test.cpp
    test/protocol_test.cpp
    test/torrentfile_test.cpp
    test/sessionstate_test.cpp
    test/asyncratelimiter_test.cpp
    test/filemanager_test.cpp
    test/integration_test.cpp
)

target_include_directories(p2p_test PUBLIC
    src
)

target_link_libraries(p2p_test
  PRIVATE
    p2p_common
    GTest::gtest
    GTest::gtest_main
    Boost::headers    
)
