# Rank, world size and the bootstrap UniqueId all travel over MPI: rank 0 generates the id and
# every rank receives it with MPI_Bcast, so every rank-based run must go through an MPI
# launcher. A process started on its own is an MPI singleton (rank 0, world size 1). Use
# scripts/run_tests.sh for the full matrix; the executables below are the five entries it drives.
find_package(MPI REQUIRED COMPONENTS CXX)

# The collective suites share the case generator, the independent oracle and the result
# reporting. The transport suite shares one semantics driver across the three transports.
add_library(occl_test_support STATIC test_common.cpp)
target_link_libraries(occl_test_support PUBLIC originccl fmt::fmt MPI::MPI_CXX)
target_include_directories(occl_test_support PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

add_library(occl_transport_support STATIC transport_check.cpp)
target_link_libraries(occl_transport_support PUBLIC originccl fmt::fmt MPI::MPI_CXX)
target_include_directories(occl_transport_support PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

# The collective suites scale their counts with the library's chunk granularity, so the macro
# has to reach the test sources as well.
if(OCCL_SMALL_TESTS)
    target_compile_definitions(occl_test_support PRIVATE OCCL_SMALL_TESTS)
endif()

# Single-machine and multi-machine collective suites. The multi-machine suite injects a logical
# hostname through CommConfig::get_hostname, so "several machines" never leaves one host.
add_executable(test_single_machine test_single_machine.cpp)
target_link_libraries(test_single_machine occl_test_support)

add_executable(test_multi_machine test_multi_machine.cpp)
target_link_libraries(test_multi_machine occl_test_support)

add_executable(test_p2p test_p2p.cpp)
target_link_libraries(test_p2p occl_test_support)

# Transport suites. TCP and shared memory need two processes and no device; RDMA needs a device
# with an active port and exits with 2 when there is none.
add_executable(test_transport_tcp test_transport_tcp.cpp)
target_link_libraries(test_transport_tcp occl_transport_support)

add_executable(test_transport_shm test_transport_shm.cpp)
target_link_libraries(test_transport_shm occl_transport_support)

add_executable(test_transport_rdma test_transport_rdma.cpp)
target_link_libraries(test_transport_rdma occl_transport_support)

add_executable(test_transport_rdma_zc test_transport_rdma_zc.cpp)
target_link_libraries(test_transport_rdma_zc occl_transport_support)

add_executable(test_rdma_zc_benchmark test_rdma_zc_benchmark.cpp)
target_link_libraries(test_rdma_zc_benchmark occl_transport_support)
