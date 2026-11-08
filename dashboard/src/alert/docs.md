# alert 插件（wmfs mgr 内）

**钩子**：mgmt_ext 与 event_sink（进程外，`wmfs mgr` 里；同一个插件挂两个钩子，共用状态）
**fail point**：`plugin.alert.mgmt_ext`（每 5 s 一次的求值）、`plugin.alert.event_sink`（事件投递；失败的那一批稍后整批重投）
**指标**：`wmfs_alerts_firing`（正在 firing 的规则数），宿主的 `wmfs_hook_*{plugin="alert"}`；告警状态在 mgr 的 `GET /alerts`（JSON）

## 它做什么

按规则判断集群有没有问题，状态机 inactive → pending → firing → resolved（design.md 4.14）：

- **指标型**：`expr = "指标{标签} 比较符 阈值"`——任一节点的样本满足就成立；也可以写成 `sum(…)`（全部节点之和）或 `rate(…)`（用于计数器：
  按序列——节点 + 标签——分别算相邻两轮的增量再相加，只算两轮都抓到的序列，变小的按 0：一个节点一轮没抓到再回来，不会被当成一次暴涨）；
  比较符 `> >= < <= == !=`。每 5 s 抓一次全部节点的 `/metrics`（连同 mgr 自己的指标）求值：条件成立 → pending，持续 `for` → firing；
  不成立 → pending 回到 inactive、firing 变 resolved（resolved 一直保持到下一次条件成立）。时长的写法同属性（`500ms`、`60s`、`5m`、`1h`、`7d`）。
- **`grows(…)`**（"在 `for` 里持续增长"，协调器积压用；ADR-0045 第 10 条）：值是全部节点之和。满足比较（例如积压 > 0）→ pending，并记下
  这时的值；之后每满一个 `for` 比一次——比窗口开始时大 → firing，不大 → 从现在（用现值）重新计时；firing 之后同样每满一个 `for` 比一次，
  不再比上一次大（这一个 `for` 里没有再增长）→ resolved。不满足比较（积压清零）时照常立即回到 inactive / resolved。所以大目录的正常
  迁移（积压很大但一直在减）、涨了又回落、持平都不告警；只有协调器真的跟不上（积压一个 `for` 比一个多）才告警。
- **事件型**：`event = "操作名[|操作名…]"`（changelog 的 op，按操作码表核对、大小写不敏感，不认识的名字 mgr 拒绝启动），可选 `kind = "…"`（只看 extra 里 kind 等于它的）：收到就 firing，之后 `for`
  （默认 5 min）内没有新的就 resolved（实现注记（M14）：规格只写了"任何 PolicyViolation 事件"这类规则，状态机取这个最小方案）。
- **动作**：mgr 的日志一行（"告警 X → firing：原因"）；规则写了 `webhook = "http(s)://…"` 的另外 POST `{rule, state, message, at_ms}`
  （与 webhook 插件同一套 HTTP 设置：mgr.toml 的 `[webhook] proxy`，单次 3 s 超时；失败只写日志）。

内置规则（`default_rules.toml`，时长都按 4.14 的默认值）：

| 规则 | 条件 |
|---|---|
| `target_offline` | `wmfs_targets_offline > 0` 持续 60 s（target 租约 8 s 过期才算离线，所以杀掉一个节点后约 70 s 触发） |
| `chain_degraded` / `chain_unavailable` | 有降级的链持续 5 min / 有链没有 Serving 成员 |
| `no_meta_leader` | `sum(wmfs_raft_is_leader) < 1` 持续 10 s |
| `quota_soft_exceeded` / `quota_denied` | `PolicyViolation`（kind=quota_soft）/ `rate(wmfs_quota_denied_total) > 0`（硬限拒绝：create、truncate、fallocate 被拒，以及写到没有余量——客户端来要额度却一点也没有，随后报 `EDQUOT`） |
| `scrub_corrupt` | `ChecksumRepaired` 或 `DataLossDetected` 事件 |
| `reconciler_backlog` | `grows(wmfs_task_backlog{task="reconciler"}) > 0`、`for` 1 h：协调器积压 1 h 内持续增长（一直在减的大迁移不算；ADR-0045 第 10 条） |
| `bufpool_wait` / `plugin_over_budget` | 内存池等待每秒 > 1 次 / 插件超预算每秒 > 10 次，持续 60 s |
| `meta_batch_stale` | `wmfs_meta_batch_stale_total > 0` |
| `audit_stalled` | `wmfs_mgr_sink_stalled_seconds{sink="audit-log"} >= 600`（审计消费者落后、又 10 分钟没有前进；mgr 自己的指标） |
| `policy_violation` | 任何 `PolicyViolation` 事件（配额软限、QoS 保底未满足、p99 超过 latency_target、隔离降级、迁移因写打开被跳过超过 24 h……） |
| `changelog_gap` | mgr 落后超出保留范围、changelog 的一段已被修剪（410） |
| `clock_skew` | `wmfs_clock_skew_seconds > 1` 持续 60 s（3.11：数据节点的墙钟与元数据 leader 偏差超过 1 s） |

## 用法

```bash
wmfs dev wait alert target_offline firing --timeout 120     # 按条件等某条规则到某个状态
curl --noproxy '*' "http://127.0.0.1:19090/alerts?token=$(cat $WMFS_MGR_DIR/token)"
```

补充或覆盖规则：mgr.toml 里 `[alert] rules = "<文件>"`，文件格式同 `default_rules.toml`（同名的替换内置规则，别的追加；写错了 mgr 拒绝启动）。
