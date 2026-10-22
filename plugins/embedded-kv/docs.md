# embedded-kv 元数据引擎

**钩子**：meta_engine（唯一型，altitude 0）　**热路径**：否
**fail point**：`plugin.embedded-kv.meta_engine`（包在 `read` / `txn` 外层；触发 = 这次读或写直接得到配置的错误码）
**指标**：`wmfs_raft_term`、`wmfs_raft_is_leader`、`wmfs_raft_commit_index`、`wmfs_raft_applied_index`、`wmfs_raft_persist_seconds`、
`wmfs_raft_msg_dropped_total`、`wmfs_meta_batch_ops`、`wmfs_meta_batch_bytes`、`wmfs_meta_commit_seconds`、
`wmfs_meta_batch_stale_total`（应恒为 0）、`wmfs_meta_read_gate_seconds`、`wmfs_meta_ops_total{op,code}`

代码在 core（`core/src/meta/raft/` 与 `core/src/meta/engine/`）：元数据 Raft 是契约层（design.md 3.4.1，ADR-0019），
这个目录只放 manifest 与这份说明。注册表（`cli/src/registry.rs`）把它登记为唯一的 meta_engine。

## 它做什么

所有元数据（目录树、属性、会话、集群配置……）存在每个元数据节点的一个文件 `<data_dir>/meta/meta.redb` 里：
raft 日志、raft 的硬状态和"状态机"（键值表）在同一个 redb 数据库，**应用修改与"已应用到第几条"在同一个事务里落盘**，
所以重启时不用从头重放日志。

- **写**：leader 上只有一个执行器线程，把排队的请求一个接一个执行完，得到一份"写集"（要改哪些键、改成什么），
  整批交给 raft。同一时刻 raft 里只有一个批次在等提交（"单飞批次"）：一批提交要一次 fsync（本机约 3 ms），
  这期间新来的请求排队，下一批一起带走——负载越高，每批越大（`wmfs debug kv bench` 会打印平均批大小）。
  一批最多 256 个操作、写集最多约 768 KiB。
- **每个写都在多数派落盘、应用之后才回复**；结果不确定（例如 leader 换了）时回复 `NOT_LEADER` / `RETRY`，
  客户端用同一个请求序号重试，去重表保证恰好执行一次。
- **读**：线性一致读——先确认自己仍是 leader（ReadIndex，单节点时立即确认），再读本地已应用的数据。
- **安全网**：写集带 `expect_version`；每个副本应用前比对，对不上就整批作废（不会写坏状态）。正常运行中永远不会发生，
  `wmfs_meta_batch_stale_total` 不是 0 就说明有 bug。
- **fail-stop**：raft 线程或执行器线程上 redb 出错、或任何内部不变量被破坏，进程立即退出（写一行错误日志后 abort）——
  状态机写到一半继续运行比崩溃更危险；重启后由 raft 日志恢复。
- **日志压缩**：每应用 1 万条检查一次，删掉"已落盘的已应用位置"之前 1 万条以外的旧日志。

## 配置

没有部署参数。

## 本机能验证到哪一步

- 单节点：`kill -9`、在落盘的任一步 abort（fail point `raft.persist.before_commit` / `raft.persist.after_commit` /
  `raft.apply.before`）之后重启，确认过的写一个不丢（`scripts/accept.sh M3`）；
- 确定性模拟：在模拟掉电的内存存储上，随机在任意一次 fsync 处"断电"再重启，确认过的写一个不丢（1000 个随机种子）。
- **真实设备上的掉电**本机无法验证（需要 root 的 dm-log-writes 或 LazyFS 环境，ADR-0015）；三副本、成员变更与快照在 M8。
