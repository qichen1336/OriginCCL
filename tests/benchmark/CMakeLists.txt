# Standalone performance programs, run by hand with mpirun. They are not part of the
# scripts/run_tests.sh regression matrix.
add_executable(test_rdma_zc_benchmark test_rdma_zc_benchmark.cpp)
target_link_libraries(test_rdma_zc_benchmark occl_transport_support)

add_executable(test_allreduce_benchmark test_allreduce_benchmark.cpp)
target_link_libraries(test_allreduce_benchmark occl_test_support)
target_compile_definitions(test_allreduce_benchmark PRIVATE "OCCL_BENCH_BUILD_TYPE=\"$<CONFIG>\"")
if(OCCL_SMALL_TESTS)
    target_compile_definitions(test_allreduce_benchmark PRIVATE OCCL_SMALL_TESTS)
endif()
