# wmfs 运维说明

> 状态：**首发定稿（M15，2026-11-08）**。只写已经实现的操作与命令（部署、扩容、滚动升级、备份、监控抓取都在开发机上用非开发端口照着
> 跑过一遍）。对外的语义承诺见 [semantics.md](semantics.md)，设计细节见 [design.md](design.md)。下面的地址 `10.0.0.1–3` 与路径是示例，
> 换成你自己的；`/etc/systemd/system/` 里的 unit 要管理员（root）放进去。

## 0. 先读：部署前提

- **只在可信网络中使用**（ADR-0009）：首发没有 TLS，线上的数据与元数据都不加密；管理 API 的写请求带签名与时间戳，**5 分钟之内被截获的
  请求可以被原样重放**。客户端机器凭共享密钥证明身份，它声明的 uid 只在它的密钥允许的范围内被相信——被攻破的客户端机器能冒充这个范围里的
  任何用户。RDMA 下服务端之间互相信任。
- **管理端口（HTTP，默认 9380）只对可信网络开放**（ADR-0030）：同一网络里的恶意程序可以用不发完的请求占住它的处理线程，让管理命令变慢或
  无响应（不会让节点崩溃、绕不过签名；数据读写走 rpc 端口，不受影响）。防火墙只放行管理员机器与 mgr 所在的机器。
- **时钟**：所有节点跑 NTP，节点之间墙钟误差要 < 5 分钟（mtime 用元数据 leader 的时钟、令牌过期按墙钟判断）；误差超过 1 s 时 mgr 告警
  `clock_skew`。租约只用单调时钟。
- **文件系统**：target 目录放在 XFS 上（推荐）或 ext4 上（也可以，ADR-0008）；每块盘一个 target。
- **体检**：每台机器先跑 `wmfs doctor`：内核、`/dev/fuse`、fusermount3、memlock、根分区、RDMA、残留挂载……逐项 ✓/!/✗。
- **生产不要打开调试开关**：`wmfs init` 生成的 node.toml 里 `failpoints_api = false`、`debug_ops = false`（`wmfs dev` 的开发集群才打开）；
  不要改成 true——故障注入接口与调试键空间只给测试用。
- **端口**：rpc 默认 9300（客户端与其他节点连它）、管理 API 默认 9380（`wmfs` 命令行、mgr、Prometheus 连它）；`--addr` 是这两个端口监听的地址。

## 1. 部署

### 1.1 第一个节点

以运行 server 的用户（下面 unit 里的 `wmfs`）执行 init 与 join：数据目录里的密钥与配置都是 0600、属于运行 init 的用户，文件系统根目录的
属主也是他。数据目录与 target 目录事先建好、交给这个用户（或者是空的、他能建）。

```bash
wmfs init --data-dir /var/lib/wmfs --addr 10.0.0.1 --target /data/nvme0 --no-start
```

- `--target PATH[:标签,…]` 每块盘给一次；不给就没有地方放文件数据（新建文件报 `ENOSPC`）。容量上限写在 node.toml 的 `[[target]] capacity`
  （`"0"` = 不限）。
- `--profile` 选部署模板（design.md 3.4.3）：`default`（`/home replicas=3`、`/scratch replicas=2`）、`minimal`（单节点、1 副本、不启用配额与
  mclock）、`nas-safe`（`/home replicas=3,trash=7d,audit=on`）、`hpc-rdma`（**首发未验证**，见第 6 节：default 再加 RDMA——文件数据走
  rdma、元数据照旧走 tcp，rdma 监听 `<--addr>:<rpc 端口 + 1>`，`--addr` 要是 RoCE 网口的 IPv4）。
- 数据目录里：`cluster.key`（**集群主密钥，唯一的秘密根**）、`node.toml`（本节点配置）、`keys/admin.key` 与 `keys/client.key`、`meta/`
  （元数据库）、`logs/`。单节点时 init 会警告"只有 1 个故障域：新文件实际为 1 副本"，加入节点后自动补齐。
- 用 systemd 运行（`/etc/systemd/system/wmfs.service`；SIGTERM 时元数据 leader 先把 leader 让出去，再退出）：

```ini
[Unit]
Description=wmfs server
After=network-online.target
Wants=network-online.target

[Service]
User=wmfs
ExecStart=/usr/local/bin/wmfs server --data-dir /var/lib/wmfs
Restart=on-failure
RestartSec=5
LimitNOFILE=65536
# hpc-rdma：RDMA 要把内存池注册给网卡，再加一行 LimitMEMLOCK=infinity

[Install]
WantedBy=multi-user.target
```

### 1.2 加节点（扩容）

1. 把第一个节点的 `cluster.key` **自己拷**到新机器的当前目录（`scp`；wmfs 从不经网络传输秘密），权限 0600。
2. 在新机器上：

```bash
wmfs join 10.0.0.1:9300 --cluster-key ./cluster.key --data-dir /var/lib/wmfs --addr 10.0.0.2 --target /data/nvme0 --no-start
systemctl enable --now wmfs
```

- 种子写集群里任何一个节点的 rpc 地址（不是 leader 也行）。前 3 个节点自动成为元数据成员（先当 learner，追上之后一次变成 3 个 voter）；
  第 4 个起默认只做数据节点（也可以明确写 `--meta none`）。
- 新节点的插件配置必须与集群一致（`wmfs cluster plugins show`），不一致会被拒绝加入（退出码 3）；集群里有动态库插件时要带 `--dylib`。
- hpc-rdma 集群（首发未验证）新加入的节点加 `--rdma`（同 init 的 `--profile hpc-rdma`）：rdma 地址在加入时公布进集群图，之后再改
  node.toml 不会更新它。
- 检查：`wmfs node list` 里新节点是 `active`、元数据成员都"追平"；`wmfs chain list` 里每条链的成员都是 `Serving`。

### 1.3 管理员与客户端

- 管理员机器：把 `keys/admin.key` 拷到 `~/.config/wmfs/admin.key`（0600），再
  `export WMFS_CLUSTER=10.0.0.1:9380,10.0.0.2:9380,10.0.0.3:9380`（写几个节点，连不上就换下一个、自动跟随 leader）。之后 `wmfs status`、
  `wmfs set/get/usage`、`wmfs node list` 等都直接可用。
- 客户端机器：把 client key 拷到 `~/.config/wmfs/client.key`（0600），然后

```bash
wmfs mount 10.0.0.1:9300,10.0.0.2:9300,10.0.0.3:9300 /mnt/wmfs     # 前台运行；SIGTERM 时写出全部数据再卸载
wmfs umount /mnt/wmfs
```

- 每台客户端机器（或每个用户组）发一把自己的 key，限制它能声明的 uid 与能访问的子树；不再用的 key 随时撤销（立即生效，它的会话被驱逐、
  连接被关闭）：

```bash
mkdir /mnt/wmfs/home/alice                                   # --path 指的子树要先存在
wmfs key create host-a --uids 1000-1999 --root-squash --path /home/alice --out host-a.key
wmfs key list
wmfs key revoke <id>
```

### 1.4 一个存储类一个只做数据的进程（设备出口隔离，ADR-0011）

首发不做 cgroup io 与 NVMe 硬件队列的隔离，同盘的两类只承诺份额。要让一类盘（例如 HDD）的 IO 受 cgroup 限制，就在那台机器上另起一个
**只做数据**的 `wmfs server`，只挂那一类的 target，再用 systemd 的 IO 控制给它限额（要 root 部署；本机没有可委派的 cgroup io，未实测
IO 限额本身）。

1. **先**建这一类的存储类，并声明独占（`dedicated = true`：打了 `hdd` 标签的 target 从此只归这个类用，default 等别的类不会把链建到这些
   盘上）。要在这些盘加入集群**之前**建：别的类已经用上的盘，旧链与旧数据会一直留着（semantics.md 第 6 节）。存储类的 TOML 格式见
   design.md 6.1：

```bash
printf 'name = "cold"\n[devices]\nlabel = "hdd"\ndedicated = true\n[redundancy]\nreplica = 1\n' > cold.toml
wmfs class create cold --file cold.toml
```

2. 在那台机器上加入只做数据的进程（另用一组端口与数据目录）：

```bash
wmfs join 10.0.0.1:9300 --cluster-key ./cluster.key --data-dir /var/lib/wmfs-hdd --addr 10.0.0.2 \
    --rpc-port 9310 --mgmt-port 9390 --meta none --target /data/hdd0:hdd --no-start
```

   `/etc/systemd/system/wmfs-hdd.service` 与 1.1 的 unit 相同，只改 `ExecStart=… --data-dir /var/lib/wmfs-hdd`，并在 `[Service]` 里加：

```ini
IOWeight=50
IOReadBandwidthMax=/dev/sdb 200M
IOWriteBandwidthMax=/dev/sdb 200M
```

3. 它起来、`wmfs node list` 里是 `active` 之后，给目录设上这个类：

```bash
mkdir /mnt/wmfs/scratch/archive
wmfs set /mnt/wmfs/scratch/archive class=cold,replicas=1
```

- 同一台机器上的两个进程是两个节点，却只是一个故障域：`wmfs status` 第一行会一直警告"故障域重叠：2 个节点在同一台机器上（machine_id
  相同）"。链只保证同一条链的副本不在同一个**节点**上：这台机器坏了，cold 类（1 副本）的数据就不可用，直到它修好回来；default 类不用
  这些 hdd 盘（dedicated），它的副本照旧分布在不同的机器上。

## 2. 日常检查与监控

- `wmfs status`：第一行起是集群警告（链降级、等待最后的副本、故障域重叠、审计消费者落后……），之后逐个属性的"声明 vs 实际"。
- `wmfs node list`（谁是 leader、各节点是否追平）、`wmfs chain list`（每条链的成员与状态）、`wmfs task list`（后台任务的进度、速率与积压）。
- `GET /api/v1/health`（免认证）：节点是否在、它的元数据角色与 leader 是谁——适合给负载均衡或存活探针用。
- **Prometheus**：每个节点的 `http://<节点>:9380/metrics`（免认证）；更省事的是只抓 mgr 的 `http://<mgr 的 --listen>/metrics`——一个抓取目标就有
  全部节点的指标，每个样本带 `node` 标签。指标名都以 `wmfs_` 开头（design.md 7.3）。
- **告警**：mgr 的 `GET /alerts`（带 dashboard 的 token）列出每条规则的状态；内置规则（`target_offline`、`chain_degraded`、`chain_unavailable`、
  `no_meta_leader`、`quota_soft_exceeded`、`quota_denied`、`scrub_corrupt`、`reconciler_backlog`、`clock_skew`、`audit_stalled`、`changelog_gap`
  等）见 `dashboard/src/alert/default_rules.toml`，`mgr.toml` 的 `[alert] rules` 可以补充或覆盖；告警与 changelog 事件可以经 webhook 推出去。

### 2.1 wmfs mgr：用 systemd 运行（同一集群只运行一个）

`wmfs mgr` 是进程外控制面插件的宿主：dashboard、prometheus-exporter（`/metrics` 汇总全部节点）、alert（`/alerts`）、audit-log（审计日志）、
webhook。它只通过管理 API、`/metrics` 与 changelog 接触集群，崩溃或停掉都不影响文件读写。`wmfs server` **不会**拉起它：生产环境用 systemd
跑**一个**实例（开发集群用 `wmfs dev mgr`，`wmfs dev up --profile nas-safe` 会自动调用）。

**同一集群只运行一个 mgr**：每个 event_sink（audit-log、webhook、alert）用插件名当 changelog 游标名。两个 mgr 会把同一批事件各投递一遍
（审计日志重复、webhook 重复 POST），并且互相覆盖对方的游标。mgr 所在的机器坏了，就在另一台机器上用同一份配置启动它：游标在集群的
元数据里，新的 mgr 从游标接着投（至少一次投递，事件带 seq，下游按 seq 去重）；mgr 停着的期间，记录按游标保留（最多 7 天 / 1000 万条），
超出的部分被修剪时 mgr 启动后显式告警（`changelog_gap`），不是静默丢失。mgr 停着的时候没有任何告警（告警是 mgr 自己发的），只有
`wmfs status` 与 `/api/v1/health` 显示"审计消费者落后 N 条"——所以用 systemd 的 `Restart=on-failure` 让它一直在运行。

先把 admin key 放到 mgr 所在的机器上（要 root；mgr 在第一个节点上时就从它的数据目录拷）：

```bash
install -d -m 0755 /etc/wmfs
install -o wmfs -m 0600 /var/lib/wmfs/keys/admin.key /etc/wmfs/admin.key
```

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
# 写了 /etc/wmfs/mgr.toml（见下）才在末尾加 --config /etc/wmfs/mgr.toml：给了却读不到，mgr 拒绝启动、Restart 会一直循环
ExecStart=/usr/local/bin/wmfs mgr --cluster 10.0.0.1:9380,10.0.0.2:9380,10.0.0.3:9380 \
    --listen 127.0.0.1:9500 --dir /var/lib/wmfs-mgr --key-file /etc/wmfs/admin.key
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

- `--listen` 默认只在本机（127.0.0.1）；浏览器访问 dashboard 要带 token（mgr 启动日志里打印、也写在 `<dir>/token`，重启沿用）。Prometheus
  抓 `http://<listen>/metrics`（免 token，一个抓取目标就有全部节点的指标，每个样本带 `node` 标签）。
- `--key-file` 是管理 API 的密钥：dashboard 只读，但 audit-log 等要提交游标（写），所以用 admin key。
- `mgr.toml`（可选：不要 webhook、也不改告警规则就不用写；不认识的键 = 错误）：

```toml
[webhook]
url = "https://hooks.example.com/wmfs"   # 不写 = 不启用 webhook（也就没有它的游标）
proxy = ""                               # 走哪个 HTTP 代理；不写 = 不走代理

[alert]
# rules = "/etc/wmfs/alert-rules.toml"   # 补充或覆盖内置告警规则（格式见 dashboard/src/alert/default_rules.toml）；写了这一行，
                                         # 这个文件就必须在，不然 mgr 起不来
```

## 3. 滚动升级

一次只升级一个节点，每个节点：

```bash
systemctl stop wmfs          # SIGTERM：是元数据 leader 就先让出 leader，再退出
cp wmfs-新版本 /usr/local/bin/wmfs
systemctl start wmfs
wmfs node list               # 它回来了、追平了
wmfs chain list              # 每条链的成员都回到 Serving（重启过的节点先 Syncing，同步完才 Serving），再升级下一个
```

- 节点停着的时候读写照常（3 副本的链少一个副本，`wmfs status` 第一行警告"链降级"，节点回来后自动补齐）。
- 能滚动升级的前提：线上协议主版本相同（`wmfs --version` 里的 `proto 1`）；集群插件清单只比插件名、不比版本；持久化格式带 feature 位，
  新版本需要的不兼容特性要等全部节点升级之后用 `wmfs feature enable` 打开（首发没有这样的特性，`wmfs feature list` 为空）。
- 改集群插件清单（例如加一个授权插件）不是滚动的：`wmfs cluster plugins add|remove <钩子:名字>` 之后各节点因清单不一致退出，用新配置
  重启全部节点。某个插件再也装不上时，在**全部**元数据节点上（用同一份插件配置）以 `wmfs server --data-dir D --adopt-local-plugins` 启动，
  当选的节点把清单改成本地的，之后去掉这个参数重启（design.md 3.7.2）。

## 4. 备份与恢复

- **副本不是备份**：3 副本防的是盘与节点坏掉，防不了误删、误改。首发没有快照（二期）。
- **密钥**：`cluster.key` 是唯一的秘密根（所有 key 文件都能从它重新算出来），丢了它节点就无法与集群互相认证；连同各节点的 `node.toml`
  离线保存两份，权限 0600。
- **数据**：从一个挂载点拷出来（close-to-open：拷到的是每个文件打开时已经 close 过的内容）。要读得到全部用户的文件，用 **root** 并用一把
  **不 squash** 的 client key 挂载（init 生成的默认 client key 就是：uid 不限、不 squash）。`-H` 保留硬链接、`-A` 保留 ACL、`-X` 保留扩展
  属性；目录属性（replicas、quota、trash、audit、qos、class、encrypt……）不在文件里，用 `wmfs get -r` 另存一份：

```bash
rsync -aHAX --exclude=.wmfs-trash /mnt/wmfs/ /backup/wmfs/
wmfs get -r /mnt/wmfs --json > /backup/wmfs-attrs.json      # 它的 subtree：每个显式设置了属性的目录与它的属性
```

  恢复 = 先建好用到的存储类与数据密钥（`wmfs class create`、`wmfs datakey create`），把文件拷回去（`rsync -aHAX /backup/wmfs/ /mnt/wmfs/`），
  再照着 `wmfs-attrs.json` 的 `subtree` 逐个目录 `wmfs set <目录> <键>=<值>,…`。要拷"某一时刻"的一致状态，先停掉写这些目录的应用。
- **误删**：给重要的目录开回收站（`wmfs set /mnt/wmfs/home trash=7d`），删掉的文件在 `.wmfs-trash/<uid>/` 里，`mv` 出来就恢复了。
- **变更记录**：`wmfs changelog tail -f` 是全部修改的流（外部程序可以长轮询并提交自己的游标）；开了 `audit=on` 的目录的修改由 mgr 写进
  `<mgr 目录>/audit/audit-YYYYMMDD.log`（一行一个 JSON、按 seq 去重），把这个目录也一起备份。

## 5. 常见运维操作

- **节点坏了**：什么也不用做——元数据 leader 约 1–3 s 切换、数据链约 8–10 s 重配，客户端只感到一次停顿；修好后启动它，它自己追上、重同步
  （先 Syncing 后 Serving）。一条链的最后一个副本再也回不来时才用 `wmfs chain force-serving <链> <target> --i-accept-data-loss`
  （明确接受可能的数据丢失，写进 changelog）。
- **计划内下线一个节点**：`wmfs node drain <id>`（不再当 leader、链从它上面迁走）→ 等 `wmfs chain list` 里没有它的成员 → 停掉它 →
  `wmfs node remove <id>`。
- **坏盘**：一个 target 连续 3 次提交失败就自动变成只读、被移出写路径（链上补上别的副本），读写照常。首发不支持给已有节点换 target：
  换盘 = 把这个节点下线、再作为新节点加入——`wmfs node drain <id>` → 等 `wmfs chain list` 里没有它的成员 → `systemctl stop wmfs` →
  `wmfs node remove <id>` → 换上新盘、清空数据目录与各个 target 目录 → 按 1.2 重新 `wmfs join`（它分到一个新的节点号）。
- **数据加密密钥轮换**（`plugins/aes-gcm/docs.md`）：同一把密钥用随机 nonce 的安全上限约 2^32 次加密（约 256 TiB 的 64 KiB 块），用了
  64 TiB 之后建议换新密钥：

```bash
wmfs datakey create k2                                      # 打印新密钥的数字 id
wmfs set /mnt/wmfs/home/sec encrypt=tenant:k2               # 新文件立即用 k2；已有文件由协调器在后台转换
wmfs task list                                              # 看协调器（reconciler）的进度与积压
```

- **后台任务限速**：`wmfs task set reconciler bw=100M`（scrub、gc 等同理）；`wmfs task pause|resume <任务>` 暂停与恢复。

## 6. 首发的验证范围（发布说明）

- 只承诺三个 profile 通过全套测试（`scripts/accept.sh release`）：**`minimal` 与 `nas-safe` 在本机通过**——完整的 pjdfstest（容器内，
  minimal 部署上；没有清单外的失败）、每个 profile fsx 10 万次与 fsstress、fio verify、IOR/mdtest 冒烟（容器内）、各自的必过测试
  （design.md 9.7），以及故障注入（kill -9、SIGSTOP、分区、时钟偏移）中客户端零错误、Jepsen-lite 线性一致。
- **`hpc-rdma` 未在本机验证**：本机没有可用的 RDMA 设备，人类决定不做 RDMA 实机测试（不建 Soft-RoCE），这个 profile 的测试显示 SKIP——
  **发布说明不得声称支持 `hpc-rdma`**；rdma 传输插件只有不需要设备的单元测试与假网卡上的组件测试。
- 本机验证不了、首发不承诺的：真实设备上的掉电持久性、RDMA 性能与"不低于 BeeGFS 的 80%"、IO500、xfstests/LTP（除 fsx、fsstress）、
  dm-flakey（用 fail point 代替）、elbencho（ADR-0015）。3 个"节点"跑在同一台机器上：验证的是协议与故障处理，不是机器级的容错。
