cmake_minimum_required(VERSION 3.14)
project(RaftConsensus CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Threads REQUIRED)

add_executable(raft_node src/main.cpp src/raft_node.cpp src/network.cpp)
target_link_libraries(raft_node PRIVATE Threads::Threads)

if(WIN32)
    target_link_libraries(raft_node PRIVATE ws2_32)
endif()
