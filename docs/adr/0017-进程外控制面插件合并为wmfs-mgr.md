# ADR-0017 五个进程外控制面插件合并在一个 `wmfs mgr` 进程（独立 crate，不依赖 core）

- 状态：accepted（人类于 2026-10-20 批准）
- 日期：2026-10-20
- 偏离条款：理念文档 4.2"进程外（控制面）……完全隔离，可独立升级与重启"；10.3"`plugins/<name>/` 每个插件一个目录""`dashboard/` 进程外 dashboard"
- 是否触及契约层：否
- 批准类别：需确认；确认批次 0（B 栏：知悉即可）
- 相关章节：design.md 2.1、3.7.3、4.13、4.14、7.4

## 背景

首发的进程外插件有 dashboard、prometheus-exporter、alert（mgmt_ext）与 audit-log、webhook（event_sink）。五个独立进程对小集群的运维负担偏重。理念文档 4.3 的规则（manifest 必填字段、fail point 覆盖每个钩子、指标注册）对进程外插件同样适用，而 fail point 与指标注册表原本在 core 里，mgr 又不能依赖 core。

## 决定

1. 五个插件编进同一个 `wmfs mgr` 进程；代码放在 `dashboard/` 目录下的 `wmfs-mgr` crate，每个插件一个子目录（含 `manifest.toml`、`docs.md`、测试）。
2. `wmfs-mgr` **不依赖 `wmfs-core`**，只依赖 `wmfs-api`（管理 API 的 JSON 类型、指标注册表、fail point——这三样从 core 抽出来放在这个只依赖 serde 的小 crate，core 也依赖它）与 ureq/tiny_http/serde/serde_json/toml/hmac/sha2——"进程外插件只能通过管理 API、指标流、事件流接触系统"由编译期保证。CLI 的 HTTP 客户端 `ApiClient` 也放在这个 crate（M1 就有）。
3. mgr 宿主：启动时校验每个插件 `manifest.toml` 的 7 个必填字段与 `abi_version == MGR_ABI_VERSION`，不符合就拒绝启动；每次 `deliver/tick` 先过 `fail_point("plugin.<name>.<hook>")` 并计时计数；只有一个 changelog 长轮询循环，按插件分发，各插件各自提交游标。
4. **mgr 只手动启动**：server 不拉起它，也没有"跟着 leader 漂移"的逻辑；生产用 systemd（`docs/ops.md` 给 unit 示例），开发用 `wmfs dev mgr`（`wmfs dev up --profile nas-safe` 自动调用）。同一集群只运行一个 mgr；游标存在本体（`CURSOR`），mgr 自身无持久状态，重启后从游标继续。

## 备选方案

- 五个独立二进制/进程：隔离更细，运维更重。需要时可以拆（每个插件已是独立目录）。
- 放在 `plugins/` 下：会诱使它们依赖 core 的内部接口。否决。
- server 以子进程拉起 mgr、只有与 leader 同节点的实例活跃：约 100 行，多一类故障模式，理念文档也没有要求。否决。

## 后果

mgr 崩溃会同时停掉五个插件，但它们都不在数据路径上，重启后从游标继续（至少一次投递）。
