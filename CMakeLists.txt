cmake_minimum_required(VERSION 3.10)
project(OriginCCL VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED on)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -g")

find_package(Threads REQUIRED)

# fmt is vendored inside the project (see third_party/fmt). This keeps the
# dependency self-contained: anyone who clones this repo can build it offline.
# Fall back to a system-installed fmt only if the vendored copy is missing.
if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/third_party/fmt/CMakeLists.txt")
    message(STATUS "Using vendored fmt: ${CMAKE_CURRENT_SOURCE_DIR}/third_party/fmt")
    add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/third_party/fmt ${CMAKE_CURRENT_BINARY_DIR}/third_party/fmt EXCLUDE_FROM_ALL)
else()
    message(STATUS "Vendored fmt not found, falling back to find_package(fmt)")
    find_package(fmt REQUIRED)
endif()

set(ORIGIN_CCL_SOURCES
    src/utils.cpp
    src/transport_tcp.cpp
    src/topology_ring.cpp
    src/bootstrap.cpp
    src/planner.cpp
    src/ring_executor.cpp
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
