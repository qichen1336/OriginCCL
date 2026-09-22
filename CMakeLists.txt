cmake_minimum_required(VERSION 3.10)
project(OriginCCL VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED on)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -g")

# gcov instrumentation for the library and the tests. `--coverage` has to reach the
# compile step, the shared-library link, and the executable link, otherwise the
# counters are either absent or never flushed.
# -fprofile-update=atomic is not optional here: MultiThreadExecutor runs one worker
# thread per channel inside a single process and they all write the same .gcda.
option(OCCL_ENABLE_COVERAGE "Build with gcov coverage instrumentation" OFF)
if(OCCL_ENABLE_COVERAGE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} --coverage -fprofile-update=atomic")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} --coverage")
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} --coverage")
endif()

find_package(Threads REQUIRED)

# fmt is a system dependency, managed the same way as OpenMPI: it must be
# pre-installed and provide its CMake package (fmt >= 9 required, 10.2.1 verified).
find_package(fmt REQUIRED)

# RDMA is a hard dependency: libibverbs and librdmacm must be pre-installed with their
# development headers. The RDMA transport is always compiled in; whether it is used at
# runtime is decided by device probing, not by the build.
find_path(OCCL_RDMA_INCLUDE_DIR rdma/rdma_cma.h)
find_library(OCCL_IBVERBS_LIBRARY NAMES ibverbs)
find_library(OCCL_RDMACM_LIBRARY NAMES rdmacm)
if(NOT OCCL_RDMA_INCLUDE_DIR OR NOT OCCL_IBVERBS_LIBRARY OR NOT OCCL_RDMACM_LIBRARY)
    message(FATAL_ERROR
        "OriginCCL requires the RDMA development files (rdma/rdma_cma.h, libibverbs, librdmacm). "
        "Install librdmacm-dev and libibverbs-dev, or a provider package that ships them.")
endif()
message(STATUS "OriginCCL RDMA: ${OCCL_IBVERBS_LIBRARY} ${OCCL_RDMACM_LIBRARY}")

set(ORIGIN_CCL_SOURCES
    src/utils.cpp
    src/transport/transport_tcp.cpp
    src/transport/transport_shm.cpp
    src/transport/transport_rdma.cpp
    src/topology_ring.cpp
    src/bootstrap.cpp
    src/planner.cpp
    src/executor/multi_thread_executor.cpp
    src/executor/epoll_executor.cpp
    src/executor/polling_executor.cpp
    src/executor/reactor_executor.cpp
    src/communicator.cpp
)

add_library(originccl SHARED ${ORIGIN_CCL_SOURCES})

# Compile-time executor selection. Exactly one strategy is baked into a library build;
# scripts/run_all_executors.sh builds all of them to cover the full matrix.
#   cmake -DOCCL_EXECUTOR=epoll|polling|reactor|multi_thread
set(OCCL_EXECUTOR "multi_thread" CACHE STRING "Executor strategy: multi_thread, epoll, polling, or reactor")
set_property(CACHE OCCL_EXECUTOR PROPERTY STRINGS multi_thread epoll polling reactor)
if(OCCL_EXECUTOR STREQUAL "epoll")
    target_compile_definitions(originccl PRIVATE OCCL_EXECUTOR_EPOLL)
elseif(OCCL_EXECUTOR STREQUAL "polling")
    target_compile_definitions(originccl PRIVATE OCCL_EXECUTOR_POLLING)
elseif(OCCL_EXECUTOR STREQUAL "reactor")
    target_compile_definitions(originccl PRIVATE OCCL_EXECUTOR_REACTOR)
elseif(NOT OCCL_EXECUTOR STREQUAL "multi_thread")
    message(FATAL_ERROR
        "Unknown OCCL_EXECUTOR '${OCCL_EXECUTOR}' (expected multi_thread, epoll, polling, or reactor)")
endif()
message(STATUS "OriginCCL executor: ${OCCL_EXECUTOR}")

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
        ${OCCL_IBVERBS_LIBRARY}
        ${OCCL_RDMACM_LIBRARY}
)

target_include_directories(originccl PRIVATE ${OCCL_RDMA_INCLUDE_DIR})

add_subdirectory(tests)
