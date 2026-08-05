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
find_program(MPIRUN_EXECUTABLE NAMES mpirun)
if(MPIRUN_EXECUTABLE)
    add_custom_target(run_test_allreduce
        COMMAND ${MPIRUN_EXECUTABLE} -np 2 $<TARGET_FILE:test_allreduce>
        DEPENDS test_allreduce originccl
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
        COMMENT "Run AllReduce test with Open MPI (mpirun -np 2)"
    )
endif()
