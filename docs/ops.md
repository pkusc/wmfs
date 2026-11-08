# wmfs 运维说明

> 状态：**M14 先建了这个文件，只放 `wmfs mgr` 的两条**（design.md 7.4）：systemd unit 示例与"同一集群只运行一个 mgr"。其余运维内容
> （部署、升级、备份、监控抓取、可信网络的前提等）在 M15 定稿。

## wmfs mgr：用 systemd 运行（同一集群只运行一个）

`wmfs mgr` 是进程外控制面插件的宿主：dashboard、prometheus-exporter（`/metrics` 汇总全部节点）、alert（`/alerts`）、audit-log（审计日志）、
webhook。它只通过管理 API、`/metrics` 与 changelog 接触集群，崩溃或停掉都不影响文件读写。`wmfs server` **不会**拉起它：生产环境用 systemd
跑**一个**实例（开发集群用 `wmfs dev mgr`，`wmfs dev up --profile nas-safe` 会自动调用）。

**同一集群只运行一个 mgr**：每个 event_sink（audit-log、webhook、alert）用插件名当 changelog 游标名。两个 mgr 会把同一批事件各投递一遍
（审计日志重复、webhook 重复 POST），并且互相覆盖对方的游标。mgr 所在的机器坏了，就在另一台机器上用同一份配置启动它：游标在集群的
元数据里，新的 mgr 从游标接着投（至少一次投递，事件带 seq，下游按 seq 去重）；mgr 停着的期间，记录按游标保留（最多 7 天 / 1000 万条），
超出的部分被修剪时 mgr 启动后显式告警（`changelog_gap`），不是静默丢失。mgr 停着的时候没有任何告警（告警是 mgr 自己发的），只有
`wmfs status` 与 `/api/v1/health` 显示"审计消费者落后 N 条"——所以用 systemd 的 `Restart=on-failure` 让它一直在运行。

`/etc/systemd/system/wmfs-mgr.service`：

```ini
[Unit]
Description=wmfs mgr（dashboard、Prometheus 汇总、告警、审计日志、webhook）
After=network-online.target
Wants=network-online.target

[Service]
User=wmfs
# systemd 建 /var/lib/wmfs-mgr 并交给 User（wmfs 用户自己建不了它：建不出 --dir，mgr 拒绝启动、Restart 会一直循环）
StateDirectory=wmfs-mgr
# --cluster 写几个节点的管理地址就行（连不上就换下一个，自动跟随元数据 leader）：抓指标、逐节点的面板问的是集群里的全部节点
# （按 GET /api/v1/cluster，之后加入的节点也在），不必每加一个节点就改这里；--dir 放审计日志（audit/）与 dashboard 的 token
ExecStart=/usr/local/bin/wmfs mgr --cluster 10.0.0.1:9380,10.0.0.2:9380,10.0.0.3:9380 \
    --listen 127.0.0.1:9500 --dir /var/lib/wmfs-mgr --key-file /etc/wmfs/admin.key --config /etc/wmfs/mgr.toml
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

- `--listen` 默认只在本机（127.0.0.1）；浏览器访问 dashboard 要带 token（mgr 启动日志里打印、也写在 `<dir>/token`，重启沿用）。Prometheus
  抓 `http://<listen>/metrics`（免 token，一个抓取目标就有全部节点的指标，每个样本带 `node` 标签）。
- `--key-file` 是管理 API 的密钥：dashboard 只读，但 audit-log 等要提交游标（写），所以用 admin key。
- `mgr.toml`（可选，不认识的键 = 错误）：

```toml
[webhook]
url = "https://hooks.example.com/wmfs"   # 不写 = 不启用 webhook（也就没有它的游标）
proxy = ""                               # 走哪个 HTTP 代理；不写 = 不走代理

[alert]
rules = "/etc/wmfs/alert-rules.toml"     # 可选：补充或覆盖内置告警规则（格式见 dashboard/src/alert/default_rules.toml）
```
