# dashboard 插件（wmfs mgr 内）

**钩子**：mgmt_ext（进程外，`wmfs mgr` 里）　**fail point**：`plugin.dashboard.mgmt_ext`（宿主包在每 5 s 一次的 tick 外层）
**指标**：`wmfs_dashboard_proxy_total`（代理到管理 API 的请求数；宿主另有 `wmfs_hook_*{hook="mgmt_ext",plugin="dashboard"}`）

## 它做什么

一个单页 HTML/JS（`index.html`，无外部库，编进二进制），在浏览器里看整个集群（design.md 4.14）：

| 面板 | 数据从哪来 |
|---|---|
| 集群 | `GET /api/v1/cluster`（节点、leader、元数据成员、target 与它们的状态——不在线的标红、会话数）与 `/api/v1/chains`（降级 / 离线的链标红）；集群警告（短链、共享设备、故障域重叠）在最上面 |
| 容量与配额 | 每个节点的 `GET /api/v1/node`（target 的已用 / 容量、物理盘）；设了 quota 的目录（`/api/v1/attr?path=/&recursive=1`）逐个 `/api/v1/usage`：用量条、软限计时 |
| 声明 vs 实际 | `GET /api/v1/status?path=<输入的路径>`：**逐个属性**显示声明、来源、实际（新文件、已有文件里不一致的个数、配额、回收站、审计落后、保底欠额……）、是否一致（不一致标红） |
| QoS 与存储类 | 每个节点的 `GET /api/v1/qos`（每个队列的排队数、派发速率、保底欠额、命中上限次数）；等待 p99 由页面按 mgr 的 `/metrics` 里 `wmfs_qos_wait_seconds` 的直方图算；`GET /api/v1/classes`：每个存储类的标签、隔离、按实际部署算出的承诺（`promise` 照原文显示，design.md 6.3），隔离降级的标红 |
| 后台任务 | leader 上的任务来自 `GET /api/v1/tasks`，节点任务（scrub、gc-local）来自每个节点的 `GET /api/v1/node`（各节点的真实进度）：状态、已处理、积压（协调器是目录数）、这一轮的进度、最近出错 |
| 事件与告警 | mgr 的 `GET /alerts`：不是 inactive 的告警，与 mgr 最近收到的 30 条 changelog 记录（策略违约标红） |

页面每 5 s 刷新一次（"声明 vs 实际"要扫子树，每分钟一次，或点"查看"）。

## 用法

```bash
wmfs dev mgr        # 开发集群：最后一行是带 token 的网址，例如 http://127.0.0.1:19090/?token=…
```

生产环境见 `docs/ops.md`（systemd、`--listen`）。浏览器访问要带 token（`?token=`：mgr 的启动日志与 `<dir>/token` 里有）；默认只监听 127.0.0.1。

## 实现说明

- 页面经两个只读代理取数据：`GET /dash/api?path=/api/v1/…`（按 leader 跟随转给管理 API，用 mgr 的 key 签名）与
  `GET /dash/each?path=/api/v1/…`（节点本地的端点，并发地问集群里的每个节点——节点列表来自 `GET /api/v1/cluster`，之后 join 的节点也在；
  每个节点最多 3 s，回 `[{addr, reply} | {addr, error}]`）。只转发 GET、只转发 `/api/v1/` 开头的路径：mgr 持有的是 admin key，页面也只能读。
- 不另存任何状态；声明 vs 实际、配额、任务都是管理 API 的原样数据，dashboard 与 `wmfs status` 等命令看到的是同一份。
