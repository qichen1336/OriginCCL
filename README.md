# OriginCCL

C++ 集合通信库。

## 构建

```bash
cmake -S . -B build
cmake --build build -j"$(nproc)"
```

## 运行测试

多进程启动只支持 **Open MPI** 的 `mpirun`。rank / world size 都由 MPI 给出，bootstrap 地址也不再从环境变量读：rank0 调 `Communicator::GetUniqueId` 挑一个空闲端口、连同自己的 IP 生成 `UniqueId`，再用 `MPI_Bcast` 发给其余 rank，所以测试必须链接并启动 MPI（不依赖 MPICH、PMI 或 Slurm）。

```bash
mpirun -np 2 build/tests/test_allreduce
mpirun -np 4 build/tests/test_allreduce
```

也可以通过构建目标一次完成编译与运行：

```bash
cmake --build build --target run_test_allreduce
```

单进程直接运行就是 MPI singleton（rank 0、world size 1，走无数据面路径）：

```bash
build/tests/test_allreduce
```
