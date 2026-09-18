# Rank, world size and the bootstrap UniqueId all travel over MPI: rank 0 generates the id
# and every rank receives it with MPI_Bcast, so a rank-based run must be launched by an MPI
# launcher
#   mpirun -np 2 ./test_allreduce
# A process started on its own is an MPI singleton (rank 0, world size 1), which is what the
# single-rank tier needs. MPI development files are therefore a hard requirement: the
# launcher is not only used to start the process pair, it also carries the unique id.
find_package(MPI REQUIRED COMPONENTS CXX)

add_executable(test_allreduce test_allreduce.cpp)
target_link_libraries(test_allreduce
    originccl
    fmt::fmt
    MPI::MPI_CXX
)
target_include_directories(test_allreduce
    PRIVATE
        ${CMAKE_SOURCE_DIR}/include
)

# Single-host assertions on the local rank view (local_rank / local_size / local_ranks /
# single_machine). Multi-machine correctness is only meaningful on real multi-host setups.
add_executable(test_local_info test_local_info.cpp)
target_link_libraries(test_local_info
    originccl
    fmt::fmt
    MPI::MPI_CXX
)
target_include_directories(test_local_info
    PRIVATE
        ${CMAKE_SOURCE_DIR}/include
)

# The shared-memory transport is exercised by a forked process pair, so it needs no
# mpirun and no communicator. It covers rendezvous, descriptor transfer, the control
# handshake, blocking and non-blocking transfer, wrap/backpressure, direction rejection
# and endpoint teardown.
add_executable(test_transport_shm test_transport_shm.cpp)
target_link_libraries(test_transport_shm
    originccl
    fmt::fmt
)
target_include_directories(test_transport_shm
    PRIVATE
        ${CMAKE_SOURCE_DIR}/include
)

# The targets below are the 2-rank tier only. The ranks are different code paths, so use
# scripts/run_tests.sh when you want the full matrix (1, 2 and 4 ranks).
find_program(MPIRUN_EXECUTABLE NAMES mpirun)
if(MPIRUN_EXECUTABLE)
    add_custom_target(run_test_allreduce
        COMMAND ${MPIRUN_EXECUTABLE} -np 2 $<TARGET_FILE:test_allreduce>
        DEPENDS test_allreduce originccl
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
        COMMENT "Run AllReduce test with Open MPI (2-rank tier only; run_tests.sh covers 1/2/4)"
    )
    add_custom_target(run_test_local_info
        COMMAND ${MPIRUN_EXECUTABLE} -np 2 $<TARGET_FILE:test_local_info>
        DEPENDS test_local_info originccl
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
        COMMENT "Run local-rank info test with Open MPI (single-host assertions)"
    )
endif()
