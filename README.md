# OriginCCL

C++ 集合通信库。

## 构建

```bash
mkdir -p build && cd build
cmake ..
make
```

## 运行测试

多进程启动只支持 **Open MPI** 的 `mpirun`。测试进程从 `OMPI_COMM_WORLD_RANK` / `OMPI_COMM_WORLD_SIZE` 读取 rank 和 world size，不依赖 MPICH、PMI 或 Slurm。

```bash
cd build
mpirun -np 2 ./tests/test_allreduce
```

或在构建目录执行：

```bash
make run_test_allreduce
```

单进程手动指定 rank（需自行同时拉起各进程）：

```bash
./tests/test_allreduce <rank> <world_size>
```
