# OriginCCL

C++ 集合通信库。

## 传输选路

数据面按 channel edge 自动选路：

- 两端 **hostname 相同**：使用共享内存 ring（`transport_shm.cpp`）。
- 两端 **hostname 不同**：使用 TCP（`transport_tcp.cpp`）。

bootstrap（节点信息交换）始终使用 TCP。设置 `OCCL_DISABLE_SHM=1` 可让所有 edge 强制走 TCP；
该变量按进程读取，**必须整作业一致**，混用会让两端选路不同而初始化失败。

共享内存 edge 的资源由其被动端（rank 较大一側）创建，fd 经临时 abstract Unix socket 传给对端，
之后该 socket 关闭，数据只走共享内存；每个 edge 固定 1 MiB，任意大小的消息都通过环形缓冲流式推进。
Linux 专用（`memfd_create`、`eventfd`、`SCM_RIGHTS`、abstract `AF_UNIX`）。

## 构建

```bash
cmake -S . -B build
cmake --build build -j"$(nproc)"
```

## 运行测试

多进程启动只支持 **Open MPI** 的 `mpirun`。测试进程从 `OMPI_COMM_WORLD_RANK` / `OMPI_COMM_WORLD_SIZE` 读取 rank 和 world size，不依赖 MPICH、PMI 或 Slurm。

```bash
build/tests/test_transport_shm          # 共享内存传输（fork 一对进程，不需要 MPI）
mpirun -np 2 build/tests/test_allreduce
mpirun -np 4 build/tests/test_allreduce

OCCL_DISABLE_SHM=1 mpirun -np 4 build/tests/test_allreduce   # 强制 TCP
```

完整矩阵（1/2/4 rank × 自动选路/强制 TCP，并校验日志中的实际选路）：

```bash
scripts/run_tests.sh
scripts/run_all_executors.sh    # 再叠加四种 executor
```

也可以通过构建目标一次完成编译与运行：

```bash
cmake --build build --target run_test_allreduce
```

单进程手动指定 rank（需自行同时拉起各进程）：

```bash
build/tests/test_allreduce <rank> <world_size>
```
