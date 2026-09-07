cmake_minimum_required(VERSION 3.10)
project(OriginCCL VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED on)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -g")

find_package(Threads REQUIRED)

# fmt is a system dependency, managed the same way as OpenMPI: it must be
# pre-installed and provide its CMake package (fmt >= 9 required, 10.2.1 verified).
find_package(fmt REQUIRED)

set(ORIGIN_CCL_SOURCES
    src/utils.cpp
    src/transport_tcp.cpp
    src/topology_ring.cpp
    src/bootstrap.cpp
    src/planner.cpp
    src/multi_thread_executor.cpp
    src/communicator.cpp
)

add_library(originccl SHARED ${ORIGIN_CCL_SOURCES})

target_include_directories(originccl
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
)

target_link_libraries(originccl
    PRIVATE
        Threads::Threads
        fmt::fmt
)

add_subdirectory(tests)
