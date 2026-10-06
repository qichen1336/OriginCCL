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
    src/transport/transport_rdma_zc.cpp
    src/topology/topology_ring.cpp
    src/topology/topology_tree.cpp
    src/topology/topology_p2p.cpp
    src/bootstrap.cpp
    src/planner.cpp
    src/executor/multi_thread_executor.cpp
    src/executor/preempt_multi_thread_executor.cpp
    src/executor/epoll_executor.cpp
    src/executor/polling_executor.cpp
    src/executor/reactor_executor.cpp
    src/communicator.cpp
)

add_library(originccl SHARED ${ORIGIN_CCL_SOURCES})

# The executor is picked at runtime by the communicator, not by the build.

# Low-resource runs: scale the planner's chunk granularity (and, in tests/, the collective
# counts) down by 256x so the suite fits a small VM. The units_total / channel split, and
# therefore the planner code path exercised, is unchanged. Real machines build with it OFF;
# scripts/run_tests.sh passes -DOCCL_SMALL_TESTS=ON unless --full-data is given.
option(OCCL_SMALL_TESTS "Scale the chunk granularity and test counts down for low-resource runs" OFF)
if(OCCL_SMALL_TESTS)
    target_compile_definitions(originccl PRIVATE OCCL_SMALL_TESTS)
endif()
message(STATUS "OriginCCL small tests: ${OCCL_SMALL_TESTS}")

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
