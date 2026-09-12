add_executable(test_allreduce test_allreduce.cpp)
target_link_libraries(test_allreduce
    originccl
    fmt::fmt
)
target_include_directories(test_allreduce
    PRIVATE
        ${CMAKE_SOURCE_DIR}/include
)

# Rank/world_size come from Open MPI (OMPI_COMM_WORLD_RANK / OMPI_COMM_WORLD_SIZE).
#   mpirun -np 2 ./test_allreduce
# Or pass them manually:
#   ./test_allreduce <rank> <world_size>
#
# This target is the 2-rank tier only. The ranks are different code paths, so use
# scripts/run_tests.sh when you want the full matrix (1, 2 and 4 ranks).
find_program(MPIRUN_EXECUTABLE NAMES mpirun)
if(MPIRUN_EXECUTABLE)
    add_custom_target(run_test_allreduce
        COMMAND ${MPIRUN_EXECUTABLE} -np 2 $<TARGET_FILE:test_allreduce>
        DEPENDS test_allreduce originccl
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
        COMMENT "Run AllReduce test with Open MPI (2-rank tier only; run_tests.sh covers 1/2/4)"
    )
endif()
