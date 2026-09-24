# Rank, world size and the bootstrap UniqueId all travel over MPI: rank 0 generates the id
# and every rank receives it with MPI_Bcast, so a rank-based run must be launched by an MPI
# launcher
#   mpirun -np 2 ./test_allreduce
# A process started on its own is an MPI singleton (rank 0, world size 1), which is what the
# single-rank tier needs. MPI development files are therefore a hard requirement: the
# launcher is not only used to start the process pair, it also carries the unique id.
find_package(MPI REQUIRED COMPONENTS CXX)

add_executable(test_collectives test_collectives.cpp collective_cases.cpp)
target_link_libraries(test_collectives originccl fmt::fmt MPI::MPI_CXX)

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

# The RDMA transport needs two independent processes: a verbs context initialized and then
# forked without exec makes every ibv_post_send fail with EPERM, so the endpoints must be
# launched by mpirun rather than fork. It covers the CM handshake, the RC queue pair, the
# RDMA write path, ring slot recycling through credits, and multi-megabyte transfers.
add_executable(test_transport_rdma test_transport_rdma.cpp)
target_link_libraries(test_transport_rdma
    originccl
    fmt::fmt
    MPI::MPI_CXX
)
target_include_directories(test_transport_rdma
    PRIVATE
        ${CMAKE_SOURCE_DIR}/include
)

# A single host is made to look like several machines by injecting a fake hostname through
# CommConfig::get_hostname, so cross-machine edges (TCP) and the local rank view can be
# asserted without leaving one node. The layout (how many ranks share a machine) is
# hardcoded inside the test by world size.
add_executable(test_multi_machine test_multi_machine.cpp collective_cases.cpp)
target_link_libraries(test_multi_machine
    originccl
    fmt::fmt
    MPI::MPI_CXX
)
target_include_directories(test_multi_machine
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
    add_custom_target(run_test_multi_machine
        COMMAND ${MPIRUN_EXECUTABLE} -np 2 $<TARGET_FILE:test_multi_machine>
        DEPENDS test_multi_machine originccl
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
        COMMENT "Run multi-machine simulation test with Open MPI (all-distinct hostnames)"
    )
endif()
