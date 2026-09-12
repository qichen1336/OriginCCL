# OriginCCL

C++ 集合通信库。

## 构建

```bash
cmake -S . -B build
cmake --build build -j"$(nproc)"
```

## 运行测试

多进程启动只支持 **Open MPI** 的 `mpirun`。测试进程从 `OMPI_COMM_WORLD_RANK` / `OMPI_COMM_WORLD_SIZE` 读取 rank 和 world size，不依赖 MPICH、PMI 或 Slurm。

```bash
mpirun -np 2 build/tests/test_allreduce
mpirun -np 4 build/tests/test_allreduce
```

也可以通过构建目标一次完成编译与运行：

```bash
cmake --build build --target run_test_allreduce
```

单进程手动指定 rank（需自行同时拉起各进程）：

```bash
build/tests/test_allreduce <rank> <world_size>
```
