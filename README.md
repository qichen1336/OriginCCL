# OriginCCL

C++ 集合通信库。

支持 AllReduce、Broadcast、Reduce、AllGather、ReduceScatter。
接口参数和缓冲区布局见 [Communicator 层文档](docs/layers/communicator.md)。四种 executor 均通过统一的三阶段接口驱动集合操作。

## 构建

```bash
cmake -S . -B build
cmake --build build -j"$(nproc)"
```

## 运行测试

多进程启动只支持 **Open MPI** 的 `mpirun`。rank / world size 都由 MPI 给出，bootstrap 地址也不再从环境变量读：rank0 调 `Communicator::GetUniqueId` 挑一个空闲端口、连同自己的 IP 生成 `UniqueId`，再用 `MPI_Bcast` 发给其余 rank，所以测试必须链接并启动 MPI（不依赖 MPICH、PMI 或 Slurm）。

测试入口只有一个脚本，`-h` 有完整说明：

```bash
scripts/run_tests.sh -h
# 本机可跑的最小档（单机 4 rank、模拟多机 4×1、三种传输、集合用例）：
scripts/run_tests.sh --level 0 --executor all -j 2 --oversubscribe
```

等级决定进程布局与数据量：

| 等级 | 单机 | 模拟多机 | 每 rank 的 count | 组合强度 |
| --- | --- | --- | --- | --- |
| 0 | 4 rank | 4 机 × 1 rank | 32768、131072 | 每个接口遍历全部 dtype / op / count |
| 1 | 8 rank | 4 机 × 2 rank | 65536、262144 | 补齐 dtype/op/count 两两组合 |
| 2 | 32 rank | 8 机 × 4 rank | 262144、1048576 | 完整合法核心集 |

```bash
# 需要足够核数，或加 --oversubscribe：
scripts/run_tests.sh --level 1 --executor all -j 2 --oversubscribe
scripts/run_tests.sh --level 2 --executor all -j 2 --oversubscribe
```

- 只覆盖 **polling** 与 **epoll** 两个 executor（`--executor all` 即这两个）。
- **多机是单机模拟**：通过 `CommConfig::get_hostname` 注入逻辑 hostname，验证分组与传输选择，不代表真实跨主机。
- `OCCL_DISABLE_SHM` / `OCCL_DISABLE_RDMA` 不作矩阵维度；集合用例按自动选择走 SHM / RDMA / TCP，测试只做黑盒接口断言，不检查实际路径。
- 无可用 RDMA 设备时 RDMA 档位记为 **SKIP**，不算通过；退出码 `0` 全过 / `1` 失败或超时 / `2` 只剩 SKIP / `4` 参数非法。

查看某一等级的完整用例矩阵（需先构建一次）：

```bash
scripts/run_tests.sh --level 0 --list-cases --no-build
```

五个测试入口：`test_single_machine`、`test_multi_machine`、`test_transport_tcp`、`test_transport_shm`、`test_transport_rdma`。用例生成与结果校验在 `tests/test_common.*`，三种传输共用 `tests/transport_check.*`。报告落在 `test-reports/`（`summary.txt` 汇总）。

集合用例的 payload 会逐元素精确比对，传输用例覆盖边界尺寸与就绪活性；这些比对无法被任何单一动态检查替代，因此始终是必跑项。
