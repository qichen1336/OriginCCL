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
