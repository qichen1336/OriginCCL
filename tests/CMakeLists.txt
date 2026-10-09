# Rank, world size and the bootstrap UniqueId all travel over MPI: rank 0 generates the id and
# every rank receives it with MPI_Bcast, so every rank-based run must go through an MPI
# launcher. A process started on its own is an MPI singleton (rank 0, world size 1). Use
# scripts/run_tests.sh for the full matrix; it drives the regression/ executables.
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

# The suites live in two subdirectories: regression/ holds the functional entries that
# scripts/run_tests.sh drives, benchmark/ holds the standalone performance programs. Both are
# added after the support libraries above so they can link them.
add_subdirectory(regression)
add_subdirectory(benchmark)
