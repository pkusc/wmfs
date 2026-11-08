# prometheus-exporter 插件（wmfs mgr 内）

**钩子**：mgmt_ext（进程外，`wmfs mgr` 里）　**fail point**：`plugin.prometheus-exporter.mgmt_ext`（包在每 5 s 一次的 tick 外层）
**指标**（它自己算的集群级指标）：`wmfs_quota_used_bytes{dir}`、`wmfs_quota_limit_bytes{dir,kind}`（kind = hard / soft；按类的配额另带
`class` 标签）、`wmfs_declared_mismatch_files{attr}`
**消费的目录属性**：`quota`、`quota.<存储类>`（只读：找出设了配额的目录）

## 它做什么

mgr 的 `GET /metrics`（免 token）= 抓取所有节点的 `/metrics`（集群里全部没有移除的节点，按 `GET /api/v1/cluster`；并发地问、每个节点
最多 3 s，一个冻住的节点不拖住整个抓取）、给每个样本加 `node="<管理地址>"` 标签、合并输出（同一个指标的 HELP/TYPE
只写一次、样本排在一起），最后是 mgr 自己的指标（`node="mgr"`：五个插件的 `wmfs_hook_*`、下面这些集群级指标、`wmfs_mgr_*`）。Prometheus
只需配一个抓取目标（design.md 4.14）。

集群级指标（mgr 经管理 API 算，core 里没有常驻的全树扫描；只有 mgr 一份，换 leader 不会留下旧值）：

| 指标 | 怎么算 | 多久一次 |
|---|---|---|
| `wmfs_quota_used_bytes{dir}`、`wmfs_quota_limit_bytes{dir,kind}` | `GET /api/v1/attr?path=/&recursive=1` 找出设了 quota 的目录（最多 1000 个），逐个 `GET /api/v1/usage`；删掉了的配额归 0 | 30 s |
| `wmfs_declared_mismatch_files{attr}` | `GET /api/v1/status?path=/` 里每个属性的"已有文件里与声明不同的个数"（status 的上限照旧：最多看 100 万个 inode，受后台限速）；没算成（换 leader、超时）就保留上次的值，30 s 后再试 | 5 min |

配额目录的"用量/上限比"在 Prometheus 里算：`wmfs_quota_used_bytes / on(dir) wmfs_quota_limit_bytes{kind="hard"}`。链健康（`wmfs_chains_degraded`）、
任务积压（`wmfs_task_backlog`）、离线的 target（`wmfs_targets_offline`）、每个 target 的用量（`wmfs_target_used_bytes{target}`）在节点自己的
`/metrics` 里，合并之后一样能查。

这些管理 API 调用在 exporter 自己的线程上做（请求最多等 300 s：大树上的 status 要扫很久），每 5 s 的 tick 只"踢"它一下（它还在忙就跳过）：
告警的求值与 dashboard 不会被一次长扫描卡住。fail point `plugin.prometheus-exporter.mgmt_ext` 挡在 tick 外层，触发时这一下不踢。
