# wmfs 里程碑计划（roadmap）

> 状态：**accepted（人类于 2026-10-20 批准）**。规格见 [design.md](design.md)，对外承诺见 [semantics.md](semantics.md)，决策记录见 [adr/](adr/)。
> 写给谁：你（按里程碑跑命令验收的人）和实现 agent（按里程碑写代码的人）。
> 怎么用：每个里程碑按顺序做。开工前，如果第 2 节写着"这个里程碑之前有一批要你确认"，agent 先把它整理成**一页中文摘要**给你：A 栏是要你拍板的事（每条有推荐选项和一句话后果），B 栏是评审已经逐条核对过的技术契约（你回一句"同意"即可）。你确认后它才开始写契约层代码。做完后你只需要复制粘贴"演示命令"，最后跑一条 `scripts/accept.sh Mx`，看到 `Mx PASS` 就算通过。

---

## 0. 交付规则（每个里程碑都一样）

1. **能跑、能演示**：每个里程碑结束时 `cargo build --release` 得到的 `wmfs` 能做一件新的、看得见的事。
2. **一条命令验收**：`scripts/accept.sh Mx` 自动执行本里程碑的全部检查，逐条打印 `✓/✗ + 中文说明`，最后一行是 `Mx PASS` 或 `Mx FAIL（第 n 条失败，请看 <日志路径>）`。它只解析命令的 `--json` 输出，不解析给人看的文字；需要等待的地方一律用 `wmfs dev wait <条件>`，不用固定的 `sleep`。失败时保留 `~/wmfs-dev/` 与全部日志，方便排查。需要本机没有的硬件或权限的检查打印 `SKIP（原因）`——**SKIP 不算通过，也不算失败**，并且不阻塞后面的里程碑。
3. **每开一个新终端先执行一次**（之后的演示命令都能直接复制粘贴）：

```bash
cd /home/yzl/wmfs && cargo build --release
export PATH=/home/yzl/wmfs/target/release:$PATH
export no_proxy=127.0.0.1,localhost,10.77.0.1 NO_PROXY=127.0.0.1,localhost,10.77.0.1   # 本机设置了 http_proxy，不加这行 curl 访问本机会失败
# 想从头再来时：wmfs dev down --purge   （停掉所有开发节点、卸载开发挂载点——包括被冻住的——再删除 ~/wmfs-dev）
```

   `wmfs dev up` 启动的第 n 个节点使用端口：rpc `19n00`、RDMA 引导 `19n01`、HTTP 管理 `19n80`（例如 node1 = 19100 / 19101 / 19180）。`eval "$(wmfs dev env)"` 会设置 `WMFS_CLUSTER`（**全部节点**的管理地址，逗号分隔——所以杀掉任何一个节点，包括 leader，之后命令照样能用）、`WMFS_KEY_FILE` 与 `WMFS_MGR_DIR`，之后 `wmfs set/get/...` 就知道连哪个集群、用哪把管理员密钥。
4. **完成的定义**（理念文档 10.4）：代码 + 单元测试 + fail point 用例 + 指标 + 文档（插件 `docs.md`、`docs/semantics.md` 更新）+（有旋钮时）目录属性 +（触及契约时）已批准的 ADR，并且通过本里程碑应跑的 profile 测试。功能是一个个里程碑长出来的，所以 profile 测试**分阶段收紧**（design.md 9.7，【ADR-0015】）：M0–M5 只要求单元测试与 fail point 测试；M6 起每个里程碑都跑 `minimal`；M9–M11 再跑 `nas-safe` 阶段版（3 节点、replicas=3，只设置已有消费者的属性）；M12 起阶段版加上 `trash` 与 `audit`；M14 起跑完整的 `nas-safe`；`hpc-rdma` 从 M9 起有 Soft-RoCE 时每次都跑，没有就 SKIP。
5. **回归**：`scripts/accept.sh all` 依次重跑全部已完成里程碑（实现 agent 每次提交前跑一遍）。
6. **提交规范与节奏**（2026-10-20 你的决定："只在需要拍板或 sudo 时停"）：agent 自测通过（`scripts/accept.sh Mx` 与 `scripts/accept.sh all`）后直接提交、打 tag，然后接着做下一个里程碑，不停下来等你验收；你随时可以挑任何一个已完成里程碑的演示命令来验。每个里程碑一个或多个提交，提交信息用中文简述（例如 `M3: 单节点元数据引擎（raft-rs + redb）`），**不包含任何 AI 署名或 co-author 行**（你的明确要求）；每个里程碑完成时打 tag `m0` … `m15`；M2 开工时另打 `m2-base`（"本体零改动"检查要用）。
7. **体量控制**（落实"不要过度工程化"）：`scripts/loc.sh` 统计非测试代码行数，与第 1 节的预算对照，并按模块重新估算剩余工作。预计总量超过 30k 行时，先从第 4 节"简化储备"里删减；删完还超，就停下来和你商量砍什么，不悄悄扩大范围。
8. **需要你做的事**：全部 ADR 与推荐方案已在 2026-10-20 一次性批准（含 M6 联网构建 docker 测试镜像）；RDMA 按你的决定**不在本机做实机测试**（不建 Soft-RoCE、不需要 sudo、仓库里不放相关脚本）。所以你只需要（可选地）运行演示命令。只有出现新的产品取舍（例如超出代码量预算、需要砍功能）时 agent 才会停下来问你。**实现 agent 自己绝不运行 sudo。**

---

## 1. 依赖顺序与总表

```
M0 ─▶ M1 ─┬─▶ M2（RDMA，本机不做实机测试；可以在 M3 之后任何时候再做）
          └─▶ M3 ─▶ M4 ─▶ M5 ─▶ M6 ─▶ M7 ─▶ M8 ─▶ M9 ─▶ M10 ─▶ M11 ─▶ M12 ─▶ M13 ─▶ M14 ─▶ M15
```

| 里程碑 | 名称 | 一句话结果 | 新增行数 | 累计 |
|---|---|---|---|---|
| M0 | 骨架、体检与接口骨架 | `wmfs doctor` 告诉你这台机器能测什么；全部"格式"写成代码并拍下字节快照 | 2,900 | 2,900 |
| M1 | 传输与认证 | 两个进程用我们自己的协议互相 ping，带 4 MiB 数据块和密钥认证 | 2,540 | 5,440 |
| M2 | RDMA 传输（可选） | 同一个 ping 走 RDMA，而且 `core/` 一行没改 | 1,000 | 6,440 |
| M3 | 单节点元数据引擎 | 写进去的键值在 `kill -9` 之后还在 | 2,300 | 8,740 |
| M4 | 命名空间与会话 | 不挂载也能 mkdir/mv/ln/rm；重试不会冒出假错误 | 2,650 | 11,390 |
| M5 | 单节点数据路径 | `wmfs fs put/get` 存取大文件，坏块会被校验和发现 | 2,800 | 14,190 |
| M6 | FUSE 挂载 | 普通的 `cp/diff/dd/md5sum` 在挂载点上工作 | 2,900 | 17,090 |
| M7 | 目录属性 + 递归用量 + 管理 API | `wmfs set/get/unset/usage`，用量常量时间可得 | 2,100 | 19,190 |
| M8 | 元数据三副本 | 3 个节点；杀掉 leader 时 mkdir 循环一个错都没有 | 1,310 | 20,500 |
| M9 | 链式复制与故障恢复 | 写 2 GiB 时杀任一节点不报错、md5 一致，节点回来自动同步 | 2,150 | 22,650 |
| M10 | 授权 + 配额 + 驱逐 | 配额报 EDQUOT、ACL 生效、被冻住的客户端被安全踢出 | 1,150 | 23,800 |
| M11 | 后台任务 + 协调器 + 变换 | 改 `compress=zstd:3` 后存量自动转换、进度可见；scrub 修好坏块 | 2,100 | 25,900 |
| M12 | 锁 + strong + O_APPEND + 回收站/ttl | 两个挂载点并发追加不丢不乱；flock 跨节点互斥；rm 可恢复 | 850 | 26,750 |
| M13 | 存储类 + 放置 + QoS + 分层 | 给目录设 IOPS 上限，dd 速度立刻被限住；诚实显示隔离承诺 | 1,350 | 28,100 |
| M14 | 管理面插件 + trace + 动态库 ABI | dashboard、告警、审计日志、跨节点 trace；.so 插件加载/拒绝 | 1,190 | 29,290 |
| M15 | profile 全套与发布门槛 | `scripts/accept.sh release` 全绿 | 300 | **≈ 29,600** |

（与 design.md 2.2 按模块估算的 ≈ 29,600 行一致，目标上限 30k；测试代码另计约 12k 行。M0 比较大，是因为全部契约类型在 M0 一次写成可编译的骨架，后面的里程碑相应变小。）

---

## 2. 什么时候要你确认什么

> **2026-10-20 更新**：你已一次性同意下表全部批次（0–5）的推荐方案，所有 ADR 的状态改为 accepted，开工前不再逐批询问；下表保留作记录。之后如果出现表里没有的新取舍，agent 仍会先问你。**2026-10-21 补充**：实现中发现的纯技术缺口（不改变对外承诺、不增删功能，例如 M0 审查发现漏了 changelog 操作码表 → ADR-0029）由主管直接定、写 ADR，并在进度说明里告诉你；改变对外承诺或要砍功能的，照样先问你。

理念文档规定：契约层的 ADR 处于 `proposed` 时不得实现。做法是：**所有技术契约在 M0 开工前一次确认**（你看不了字节格式、Ready 处理顺序这种细节，评审已经逐条核对过，你回一句"同意"就行；这样 M0 就能把全部接口写成代码，之后任何里程碑都不会"先实现、后批准"）；**真正要你做产品取舍的几条，在用到它的那个里程碑开工前单独问你**，那时你对系统已经更熟。

| 批次 | 什么时候 | A 栏：要你拍板（推荐选项 → 后果） | B 栏：评审已核对，知悉即可（回复"同意"） |
|---|---|---|---|
| 0 | M0 开工前 | **0009 信任边界**：推荐"客户端机器凭密钥证明身份，它说的 uid 在密钥允许的范围内才被相信；没有 TLS，只在可信网络使用"→ 被攻破的客户端机器能冒充它范围内的用户，网络能被窃听。**0015 发布门槛与分阶段测试**：推荐"本机做不到的测试（xfstests、LTP、dm-flakey、IO500、和 BeeGFS 比性能、elbencho、真实掉电）推迟并如实标注；profile 测试按里程碑逐步收紧"→ 首发不做性能对比承诺 | 你已同意的：0001 不用 async、0002 用 raft-rs、0006 命令叫 wmfs。技术契约：0005 管理 API 与 changelog 流、0018 线上协议、0019 元数据引擎、0020 元数据磁盘格式、0021 chunk 文件格式（附带一条小偏离：已有文件开了压缩/加密后，新写入要等后台转换完成才按新设置）、0022 内存池与缓冲区所有权、0023 租约与驱逐、0024 链式复制规则、0025 插件钩子与"全集群插件清单"、0027 后台任务与可观测基座、0028 命名空间语义与缓存失效。小偏离：0003 不用 io_uring、0004 不用 FUSE passthrough、0007 存储类用 TOML 写、0008 在 ext4 上测试、0010 QoS 只在数据节点做、0011 cgroup/NVMe 硬件隔离首发不做、0017 五个控制面插件合成一个 `wmfs mgr` 进程 |
| 1 | M4 开工前 | **0014 新增的 16 条 POSIX 放松**（semantics.md 第 3.3 节）：推荐接受 → 都写进对外文档。**0016 声明满足不了时怎么办**（例如单节点上 `/home` 写着 replicas=3）：推荐"照常创建，但到处大声警告（init、server 日志、挂载输出、`wmfs status` 第一行、`wmfs get` 的 semantics 行），加节点后自动补齐"；备选"直接拒绝（ENOSPC）"→ 单节点开箱就用不了 `/home` | — |
| 2 | M6 开工前 | **是否允许联网构建 docker 测试镜像**（下载 ubuntu 基础镜像与 fio/pjdfstest/IOR 源码，几分钟）：推荐允许 → 否则 pjdfstest 等外部测试跑不了 | — |
| 3 | M7 开工前 | **0026 配额模型**：推荐"quantum 额度"——配额可能**短暂**超限，上界 = 64 MiB × 同时写入的文件数，可以逐目录调小；备选"预留模型"——永不超限，代价是多约 300 行和一张预留表、别人的预留可能让你提前收到 EDQUOT | 0026 的其余部分（目录属性继承、递归用量） |
| 4 | M12 开工前 | **0013 strong 模式也不支持跨节点共享可写 mmap**：推荐接受（直接报 ENODEV，诚实拒绝）；备选是做页级租约与回调，工作量大、风险高 | — |
| 5 | M14 开工前 | **0012 动态库插件的范围**：推荐"首发只支持授权（authz）这一种"；备选"再加压缩类"（约 +150 行，还要连带 chunk 格式的 feature 位、集群级开关与各节点一致性规则） | — |

---

## 3. 各里程碑详情

### M0 骨架、体检与接口骨架（约 2,900 行）

- **目标**：工程能编译；一条命令告诉你这台机器能测什么、哪些测试本机做不了；把 design.md 里全部"格式与接口"一次写成可编译的代码并拍下字节快照，之后各模块照它对接。
- **包含**：workspace 与全部 crate 空壳（`api`、`core`、`plugins`、`cli`、`dashboard`、`tests`、`examples/plugin-mem-transport`）；`wmfs-api`（指标注册表 + Prometheus 文本、fail point 框架与语法、错误码名字表）；`core/base`（错误码、单位与时长解析、时钟、随机数、日志、node.toml 结构、`sys.rs` 的安全封装、`devinfo.rs`）；**接口骨架**：把 design.md 3.2.7–3.2.9、3.3、3.5.1、3.6.2、3.6.4、3.7.1、3.7.3、3.7.7、3.10、3.12 的全部类型、枚举、trait 原样写成 Rust 文件（只有签名，函数体先 `unimplemented!()`），连同 `tests/golden/` 的金样（每种消息、记录、帧头、chunk 头一份固定字节）；`wmfs --version`、`wmfs doctor`；`scripts/accept.sh`、`scripts/loc.sh`、`scripts/ci.sh`。从此骨架文件是唯一真源：design.md 里对应的代码块保留、仅供阅读，每个对应小节开头加一行实现注记写明以哪个源码文件为准（原计划是把代码块改成"见 `core/src/…`"，主管于 2026-10-21 改为这种做法，方便新手读文档）。对应理念文档：第 3 章"可观测性基座"的计数器注册表与 fail points、"持久化格式与线上协议"的格式定义。
- **开工前确认**：批次 0。
- **验收标准**：`cargo build --release` 零警告；`cargo test` 通过（含全部金样）；`wmfs doctor` 输出检查表并以 0 退出；`scripts/accept.sh M0` PASS。
- **演示命令**：

```bash
wmfs --version          # 预期：wmfs 0.1.0 (proto 1, core-abi 1, c-abi 1)
wmfs doctor             # 预期：逐项 ✓/!/✗：内核 6.17 ✓、/dev/fuse ✓、fusermount3 ✓、memlock ≈15 GiB ✓、根分区 ext4（提示 XFS 更推荐）、
                        #       根分区所在物理盘 nvme0n1、http_proxy 已设置（提示已自动绕过）、RDMA：4 个 RoCE 端口链路 DOWN、
                        #       没有可用 RDMA 设备（本机不做 RDMA 实机测试）、cgroup io 未委派给用户（设备出口隔离需要 root）、FDP：不支持、docker 可用、
                        #       user_allow_other 未开启（用 --as-uid 演示多用户）、没有残留的 wmfs 挂载
cargo test --release -p wmfs-core golden            # 预期：全部金样逐字节一致
scripts/loc.sh          # 预期：各 crate 行数与 M0 预算对照
scripts/accept.sh M0    # 预期最后一行：M0 PASS
```

- **本阶段你需要理解的概念**：
  - **workspace 与 crate**：Rust 把一个项目拆成几个"包"（crate），workspace 是把它们放在一起统一编译的容器。`core` 是本体，`plugins` 是插件，`cli` 生成唯一的程序 `wmfs`，`api` 是本体和外部程序（dashboard 等）共用的一小块接口。
  - **指标（metrics）**：程序里的计数器，比如"处理了多少请求"。它们以 Prometheus 文本格式从 `/metrics` 网址读出，dashboard 和告警都靠它。
  - **fail point（故障注入点）**：代码里预先埋好的"开关"，测试时可以让某一步故意出错、变慢或让进程崩溃，用来证明出故障时系统仍然正确。平时关闭，几乎零开销。
  - **接口骨架与金样**：先把所有"格式"（网络消息长什么样、磁盘上的记录长什么样）写成代码，并给每种格式拍一张"字节快照"。以后谁不小心改了格式，测试马上报错——格式一旦有数据在用就改不了了。
  - **验收脚本**：`scripts/accept.sh` 把演示里的检查自动化，你只要看最后一行是不是 PASS。

### M1 传输与认证（约 2,540 行）

- **目标**：两个进程用我们自己的帧协议在 TCP 上通信，带共享密钥认证、能传 4 MiB 数据块；内存池与"缓冲区所有权"从第一天就按 RDMA 的要求实现；用一个独立 crate 的内存传输插件证明"新增传输插件不改本体一行"；管理 API 的骨架（HTTP + 签名）可用。
- **包含**：`proto`（帧头、握手、能力协商、角色核对与角色权限表）、`buf`（三档内存池、复制 lane、`Buf`/`FrozenBuf`/`BufSlice`）、`net`（服务端：每连接一个读线程 + 一个写线程 + 有界发送队列 + 5 s 写超时；`RpcClient`：call/start/call_cb/stream、rpc-timer、重连、按消息族选 scheme；重试）、`hooks`（Transport/Conn/Authn 的框架、Manifest、PluginSet 校验）、`plugins/tcp`、`plugins/shared-key`、`examples/plugin-mem-transport`；`mgmt` 框架（tiny_http + HMAC 认证 + `/metrics`、免认证的 `/api/v1/health`（带本节点地址）、`/api/v1/failpoints`）；`wmfs-mgr` crate 先只放 `ApiClient`（CLI 的 HTTP 客户端）；`wmfs init`（此时只生成目录、密钥与 node.toml）、`wmfs server`、`wmfs ping`、`wmfs failpoint set/clear/list`、`wmfs dev up/down/env/status/kill/start/logs`（基础版）、`wmfs debug gen-key`。对应理念文档：第 3 章"持久化格式与线上协议"（消息帧、能力协商）、"缓冲区与内存模型"、4.1 的 transport（tcp）与 authn（shared-key）。
- **开工前确认**：无新批次（批次 0 已覆盖）。
- **验收标准**：ping 1000 次全成功；4 MiB bulk 往返 200 次 CRC 全对；错误密钥被拒绝；**client key 申请 server 角色被拒**；客户端注入 30% 发送失败时（每次失败 = 连接作废）靠重连重试全部成功；一个故意不读 socket 的假客户端不会卡住服务端（5 s 写超时后它被断开，别的客户端不受影响）；mem 传输插件的测试通过；帧与握手的金样测试通过；`wmfs failpoint` 经签名的管理 API 生效。
- **演示命令**：

```bash
wmfs dev up --nodes 1 && eval "$(wmfs dev env)"     # 预期：node1 started（rpc 127.0.0.1:19100，http 127.0.0.1:19180）
wmfs ping tcp://127.0.0.1:19100 --count 1000        # 预期：1000/1000 ok，打印 p50/p99 延迟
wmfs ping tcp://127.0.0.1:19100 --bulk 4M --count 200    # 预期：bulk crc ok 200/200，吞吐若干 GB/s
wmfs debug gen-key --out /tmp/wrong.key
wmfs ping tcp://127.0.0.1:19100 --key-file /tmp/wrong.key   # 预期：AUTH_FAILED（认证被拒）
WMFS_FAILPOINTS='plugin.tcp.transport=pct(30,return(EIO))' wmfs ping tcp://127.0.0.1:19100 --count 100   # 预期：日志里有"连接断开 → 重连重试"，结果 100/100 ok
wmfs failpoint list 1                               # 预期：空列表（这条命令走带签名的管理 API；M4 会用它注入故障）
curl --noproxy '*' -s 127.0.0.1:19180/api/v1/health # 预期：JSON，含本节点的 rpc 与管理地址
curl --noproxy '*' -s 127.0.0.1:19180/metrics | grep wmfs_rpc_requests_total
cargo test --release -p plugin-mem-transport        # 预期：通过——一个独立的 crate 就加了一种传输方式
wmfs dev down
scripts/accept.sh M1
```

- **本阶段你需要理解的概念**：
  - **帧（frame）**：网络上传的一条消息 = 固定 48 字节的"信封"（帧头，写着消息类型、长度、校验码、追踪号）+ 正文 + 可选的大块数据。双方都按这个格式读写，所以 TCP 和 RDMA 能用同一种协议。
  - **握手与 HMAC 认证**：连接一建立，双方交换随机数，客户端用密钥对它们算一个"指纹"（HMAC），服务端用同一个密钥验算。密钥本身从不在网上传，偷听的人拿不到。服务端还核对"你声称的角色"和"这把密钥的角色"是否一致，客户端密钥冒充不了服务端。
  - **内存池与 Buf 所有权**：大块数据只放在启动时一次申请好的内存池里。一个 `Buf` 同一时刻只有一个主人，把它"交出去"（move）就等于把责任交给对方；用完自动还给池子。这正是 RDMA 要求的"网卡正在读写的内存不能被别人动"。
  - **读线程与写线程**：每条连接有一个专门读、一个专门写的线程；别的线程要发消息只是"放进发送队列"。所以就算某个客户端卡住不收数据，也只会卡住它自己那条连接。
  - **传输插件与注册表**：TCP、RDMA、内存传输都实现同一个 `Transport` 接口；程序里只有 `cli/src/registry.rs` 一个文件知道有哪些插件，所以加插件不用改本体。

### M2 RDMA 传输（约 1,000 行；本机不做实机测试）

- **目标**：同一个 ping 走 RDMA，大块数据用 RDMA READ/WRITE 搬运；并用 `git diff` 与 `grep` 证明这个里程碑没有改 `core/` 一行，本体里也没有任何 RDMA 专用代码（理念文档第 9 章成功标准"新增一个传输插件不改本体任何一行"）。
- **包含**：`plugins/rdma`（sideway：建 QP、内存注册、SEND/RECV、按描述符表做 RDMA READ/WRITE、完成队列轮询、信用控制、插件私有的 QP 信息交换、握手完成前不做单边操作）；`wmfs doctor` 识别 rxe 设备；`wmfs ping rdma://…`；`wmfs dev up --rdma IP`（写 `[plugin.rdma] listen` 与 `[net] data_scheme = "rdma"`）；接收槽占用的启动校验；传输一致性测试（同一套用例跑 tcp / mem / rdma）。
- **本机不做实机测试**（你的决定，2026-10-20）：不创建 Soft-RoCE 设备、不运行 sudo、仓库里不放 Soft-RoCE 脚本。rdma 插件照常实现并编译；不需要设备的部分（描述符表、信用控制、插件私有 QP 信息的编解码、握手完成前禁止单边操作的状态机）写单元测试；需要设备的检查在 `accept.sh M2` 里打印 SKIP；semantics.md 与发布说明把 RDMA 标为"未在本机验证"。
- **开工前确认**：无新批次（批次 0 已覆盖）。开工时 agent 打 tag `m2-base`（M2 可以放到 M3 之后任何时候再做，检查只比较 M2 自己的改动）。
- **验收标准**：rdma ping 带 4 MiB bulk 200 次 CRC 全对，且指标显示走了 RDMA READ/WRITE；断开重连测试通过；`git diff --stat m2-base -- core/` 为空；`core/src` 里除注释外搜不到 `rdma`；本机没有可用 RDMA 设备，所以 `accept.sh M2` 里需要设备的检查打印 `SKIP（原因：本机按决定不做 RDMA 实机测试）`，其余检查（不需设备的单元测试、`core/` 零改动、`core/src` 无 rdma）必须 ✓。
- **演示命令**：

```bash
wmfs doctor                                         # 预期：RDMA 一项显示"4 个 RoCE 端口链路 DOWN，无可用设备（本机不做 RDMA 实机测试）"
cargo test --release -p wmfs-plugins --features rdma rdma   # 预期：不需要设备的 rdma 单元测试全部通过
git diff --stat m2-base -- core/                    # 预期：没有任何输出
grep -rn --include='*.rs' -i rdma core/src | grep -v '//'   # 预期：没有任何输出
scripts/accept.sh M2                                # 预期：需要设备的检查显示 SKIP，其余 ✓，最后一行 M2 PASS
```

- **本阶段你需要理解的概念**：
  - **RDMA 与 Soft-RoCE**：RDMA 让网卡直接读写另一台机器的内存，不经过对方 CPU 拷贝，所以又快又省 CPU。本机的 RDMA 网口没插线，Soft-RoCE（rxe）用软件模拟一块 RDMA 网卡，功能一样、速度不代表真实硬件。
  - **内存注册与 rkey**：网卡只能访问"注册过"的内存。我们在启动时把整个内存池注册一次，得到一把"钥匙"（rkey）。钥匙只随每个请求交给对方，用来访问请求方指定的那几块缓冲。
  - **谁发起 READ/WRITE**：永远由"收到请求的一方"去读对方的数据或往对方的缓冲里写。服务端从不向客户端发请求，于是客户端永远拿不到服务端的钥匙，不可信的客户端碰不到服务端内存。
  - **为什么元数据不走 RDMA**：RDMA 的每条控制消息要放进对方预先准备好的小格子里（120 KiB 以内）；集群图、目录列表这些可能更大，所以只让读写数据的消息走 RDMA，其余永远走 TCP。

### M3 单节点元数据引擎（约 2,300 行）

- **目标**：单节点的 raft + redb + 串行执行器 + 读闸门全部按最终形态实现（只是只有一个 voter），写进去的数据在 `kill -9` 后还在。
- **包含**：`meta/raft`（RedbStorage、驱动循环与全部 Ready 规则、日志压缩、redb 出错即 abort 的 fail-stop 规则）、`meta/engine`（单飞批次执行器：边取边执行、≤ 256 个操作、写集 768 KiB 上限；`expect_version`、去重基础设施、读闸门、MetaEngine trait）、`wmfs init` 写 index = 1 的初始状态并自动 campaign、调试键空间 `0x2F` 与 `wmfs debug kv put/get/bench`、`wmfs dev wait`（先只有 `leader` 条件，其余条件随功能在后面的里程碑加入）、测试用的掉电模拟后端 `MemCrashBackend`。对应理念文档：第 3 章"复制、成员、选举、fencing"中的元数据 Raft，"事务与锁原语"中的元数据事务，第 7 章"元数据：内嵌 KV + Raft"。
- **开工前确认**：无新批次（批次 0 已覆盖）。
- **验收标准**：raft Storage 合约测试；**回归测试"单节点写入 → kill -9 → 重启"不 fatal**；`raft.persist.before_commit=abort` 后重启，已确认的写一个不丢；写 20 万条后日志被压缩；执行器"写集 ≥ 768 KiB 就结束本批"的单测；确定性 raft 模拟（单节点部分，内存后端模拟掉电）通过；`wmfs_meta_batch_stale_total` 恒为 0。
- **演示命令**：

```bash
wmfs dev up --nodes 1 && eval "$(wmfs dev env)"
wmfs debug kv put hello world
wmfs debug kv get hello                             # 预期：world
wmfs debug kv bench --n 100000                      # 预期：约 10–20 秒；打印写入速率与平均批大小（64 路并发，批会自然变大）
wmfs dev kill 1 && wmfs dev start 1 && wmfs dev wait leader   # kill -9 后重新启动，等它重新当上 leader
wmfs debug kv get hello                             # 预期：仍是 world
curl --noproxy '*' -s 127.0.0.1:19180/metrics | grep -E 'wmfs_raft_applied_index|wmfs_meta_batch_stale_total'
scripts/accept.sh M3
```

- **本阶段你需要理解的概念**：
  - **Raft**：让几台机器对"一串操作的顺序"达成一致的算法。有一个 leader 负责接收写入，把每条操作写进日志并复制给其他机器，**多数派**都写进磁盘后才算"提交"。leader 挂了，剩下的多数派会选出新 leader（term 加一）。
  - **状态机与写集**：leader 先把一个操作执行完，得到"要改哪些键、改成什么"的清单（写集），只把这份清单交给 Raft；其他机器照单修改即可，不需要懂文件系统逻辑。
  - **fsync 与持久化**：写文件时数据先留在内存里，`fsync` 才真正落到磁盘。我们只在必须的时候 fsync，并且一次 fsync 带走一整批操作。
  - **单飞批次**：同一时刻只有一批操作在 Raft 里等提交，下一批在这期间排队。简单、不会出错，负载高时每批自然变大；一批的总大小有上限，保证网络消息不会大得发不出去。
  - **ReadIndex**：读之前先问一圈多数派"我还是 leader 吗"，确认后再读本地数据，保证不会从一个已经被取代的旧 leader 上读到过期内容。

### M4 命名空间与会话（约 2,650 行）

- **目标**：完整的 POSIX 元数据操作（不挂载，用 `wmfs fs` 调试客户端直接发请求）；会话与去重让"重试"永远不会冒出假错误；POSIX 权限生效；每个修改都写 changelog；创建文件时就按存储类选好链。
- **包含**：`meta/schema`（KV 记录的编解码）、`meta/ns`（lookup、getattr、readdir 分页、create、mkdir、mknod、symlink、link、unlink、rmdir、rename 全部标志、setattr——含"截断到 0 按当前属性重建 layout"、xattr、孤儿标记、umask/默认 ACL 规则）、**`plugins/label-match` 基础版 + `gen_chains`**（只写 CHAIN 记录；target 来自 init 写的 TargetRecV1，此时链长 1；`ns::common::new_layout`）、集群图与集群警告（短链部分）、会话（open/heartbeat/close）、去重表、changelog 写入、`plugins/posix-mode` 与 authz 管线、`client/meta_client`（跟随 leader、seq、重试）、`wmfs fs` 的元数据命令与 `--as-uid`。属性解析此时只返回内置默认值（DIRATTR 的继承在 M7）。对应理念文档：第 3 章"命名空间语义与一致性模型"（POSIX 子集、rename 原子性）、4.1 authz 的 posix-mode 与 placement、8.2。
- **开工前确认**：批次 1（0014 放松清单、0016 降级还是拒绝）。
- **验收标准**：POSIX 元数据语义单元测试；服务端"弄丢"一次回复时，客户端重试得到原结果而不是 `EEXIST`；rename 原子性测试；重启后命名空间完好；非属主写入被拒绝；新文件的 layout 指向一条刚生成的链；server 日志里出现"有链少于声明副本数"的警告（单节点 + default 类 replica=3）。
- **演示命令**：

```bash
wmfs dev up --nodes 1 && eval "$(wmfs dev env)"     # 预期：日志与输出里有警告"default 类声明 3 副本，只有 1 个故障域……"
wmfs fs mkdir /a && wmfs fs touch /a/f1 && wmfs fs ln /a/f1 /a/f2
wmfs fs stat /a/f1                                  # 预期：nlink=2，layout：1 条链（chain 1）
wmfs fs mv /a/f1 /a/g && wmfs fs ls -l /a           # 预期：f2、g
wmfs fs rmdir /a                                    # 预期：错误 Directory not empty（ENOTEMPTY）
wmfs fs --as-uid 2000 mkdir /a/x                    # 预期：Permission denied（/a 属于你，uid 2000 没有写权限）
wmfs failpoint set 1 meta.reply.drop 'times(1,return(EIO))'   # 让服务端"弄丢"下一个元数据回复
wmfs fs mkdir /b --verbose                          # 预期：约 10 秒后（一次 RPC 超时）出现"超时重试 seq=… → 去重命中"，结果成功（没有 File exists）
wmfs dev kill 1 && wmfs dev start 1 && wmfs dev wait leader && wmfs fs ls -l /a   # 预期：仍是 f2、g
scripts/accept.sh M4
```

- **本阶段你需要理解的概念**：
  - **inode 与目录项**：inode 是"文件本身"（权限、大小、时间、数据放在哪），目录项是"名字 → inode 号"的对应关系。硬链接 = 两个名字指向同一个 inode；rename 只改目录项。
  - **会话与 seq 去重**：每个挂载点有一个会话，每个修改请求带一个递增的序号 seq。网络出问题时客户端用同一个 seq 重试；服务端记得每个 seq 的结果，第二次直接把第一次的结果返回——所以"创建目录"不会因为重试而报"已存在"。
  - **changelog**：每个修改都按顺序记一条事件（谁、什么时候、改了什么）。审计、webhook、dashboard 都从这里读。
  - **链与放置**：文件的数据将来要放在一条"链"上（一条链 = 几台机器上的几个副本）。创建文件时就选好链；现在只有一个节点，所以链只有 1 个成员——系统会如实警告"声明 3 份、实际 1 份"。
  - **POSIX 权限位**：rwx 三组（属主/属组/其他人）。服务端在每个请求上检查，客户端声明的身份不会被盲目相信。

### M5 单节点数据路径（约 2,800 行）

- **目标**：文件数据存成带校验和的 chunk 文件；能力令牌授权数据访问；大文件存取一致；坏块被发现；截断正确。
- **包含**：`data/format`（chunk 头与块表）、`data/build`（读改写与变换）、`data/state`/`write`/`read`/`truncate`/`service`（含 Corrupt 标记的规则、QoS 出队的睡眠与唤醒）、`plugins/local-fs`、`plugins/crc32c`、`plugins/replica`、`plugins/fifo`（QoS 先用先进先出）、target 心跳的最小版（含物理盘解析 `phys_devs`）、cap 签发与校验、`Open/CommitWrite/inline/uninline`（含 `len(INLINE) ≤ size` 不变式）、改 size 的 `SetAttr` 回 `OpenOut`、`client/data_client`、后台调度器骨架 + gc 任务（删除废弃 epoch 与孤儿）、`wmfs fs put/get/cat/truncate`、`wmfs debug chunk-path/chunk-header/corrupt-chunk`、测试用的内存版 `MemTargetStore`（模拟 fsync 与掉电）。对应理念文档：第 3 章"持久化格式"（chunk 头部：版本、校验、变换标记、所属 inode）、4.1 chunk_store（local-fs）/codec（replica）/transform（crc32c）、第 7 章"chunk 4 MiB、inline 64 KiB"。
- **开工前确认**：无新批次（批次 0 已覆盖 0021、0008）。
- **验收标准**：100 MiB 文件往返一致；小文件走 inline；人为损坏一个块后读返回 `EIO`（单副本无法修复，诚实报错）而不是错误数据；之后截断到 10 MiB 照常成功（被截掉的坏 chunk 是整块清空，不需要读坏块）；cap 被篡改/过期时数据节点拒绝；截断到 s > 0 与到 0 都正确；`echo hello>f; echo x>f; truncate -s 6 f` 读到 `x\n` 加 4 个 0 字节；注入"pending 文件写到一半崩溃"后重启无损坏读；内存后端下模拟掉电，fsync 过的数据不丢；废弃 epoch 在宽限期后被删除。
- **演示命令**：

```bash
wmfs dev up --nodes 1 && eval "$(wmfs dev env)"
head -c 100M /dev/urandom > /tmp/r100
wmfs fs put /tmp/r100 /big && wmfs fs get /big /tmp/r100.back && cmp /tmp/r100 /tmp/r100.back && echo SAME   # 预期：SAME
wmfs fs stat /big                                   # 预期：size=104857600，layout：chunk=4M、epoch=1、1 条链
echo hello | wmfs fs put - /small && wmfs fs stat /small        # 预期：inline=yes
wmfs debug chunk-header "$(wmfs debug chunk-path /big 3)"      # 预期：version、checksum=crc32c、logical_len=4194304
wmfs debug corrupt-chunk /big 3 --node 1 --block 0 # 人为制造一个坏块（按块表找到第 0 块的数据，翻转一个字节）
wmfs fs get /big /tmp/x                             # 预期：Input/output error（校验和不符；单副本修不了，诚实报错）
wmfs fs truncate /big 10M && wmfs fs stat /big      # 预期：size=10485760（坏掉的 chunk 3 被整块截掉，不需要读它）
scripts/accept.sh M5
```

- **本阶段你需要理解的概念**：
  - **chunk 与版本**：文件按 4 MiB 切成 chunk，每个 chunk 是数据节点上的一个文件；每改一次，chunk 的版本号加一，旧版本号永不复用（为将来的快照和副本比对打基础）。
  - **写时复制 + rename 原子提交**：修改 chunk 时不碰旧文件，而是写一个新文件，写完后一次 `rename` 替换旧文件。断电时要么是旧版本、要么是新版本，不会出现"写了一半"的文件。
  - **能力令牌（cap）**：打开文件时元数据服务给客户端一张签了名的"通行证"（哪个文件、能读还是能写、几点过期）。数据节点只验签名，不用再问元数据服务——又快又能防止客户端越权。
  - **校验和**：每个 64 KiB 块存一个指纹（crc32c）。读的时候重新算一遍，对不上就说明数据坏了，宁可报错也不返回错误数据。

### M6 FUSE 挂载（约 2,900 行）

- **目标**：把 wmfs 挂成一个普通目录，普通命令都能用；两个挂载点之间满足 close-to-open。
- **包含**：`cli/fuse`（全部 Filesystem 方法、错误码映射、TTL、打开标志、通知、`FUSE_DONT_MASK` 与 umask 规则、根目录虚拟 xattr `user.wmfs.mgmt`）、`client/files`/`writeback`（按槽位计的写回缓冲、inline、截断、readdir 句柄快照）、`client/strong.rs` 的同步写路径（这时先给 cto 下的 `O_DIRECT` 用）、`plugins/cache-memory`、`wmfs mount/umount`（接受多个地址，挂载后打印集群警告）、`wmfs dev mount/umount/client-pid`、`wmfs dev down --purge` 的安全清理顺序、`tests/docker/Dockerfile.tools` 与 `scripts/build-tools-image.sh`、`minimal` profile 冒烟。另做一个约 20 行的 spike，打印内核传来的 mode/umask，确认 8.1 的内核行为。对应理念文档：第 7 章"客户端：FUSE"、第 3 章"缓存失效协议"、8.2 cto 规则。
- **开工前确认**：批次 2（是否允许联网构建 docker 测试镜像）。
- **验收标准**：`cp -r /usr/include` 后 `diff -r` 完全一致（其中 135 个文件大于 64 KiB，走 chunk）；1 GiB 写入 + md5 在卸载重挂后不变；两个挂载点之间：写者 close 之后读者 open 一定看到（包括在 1 秒属性缓存窗口内）；`echo v > f`（O_TRUNC）之后的写直接进新 epoch（日志里没有 `STALE_LAYOUT` 重写）；`umask 027` 下新建文件的权限是 640；符号链接、硬链接、xattr 正常；被删除但仍打开的文件可以读完；`dd oflag=direct` 写入后 md5 一致；`minimal` profile 冒烟通过；（同意构建镜像后）容器内 pjdfstest 中不涉及 chown/权限的子集通过、fsx 1 万次无不一致。
- **演示命令**：

```bash
wmfs dev up --nodes 1 && eval "$(wmfs dev env)"
mkdir -p ~/wmfs-mnt ~/wmfs-mnt2 && wmfs dev mount ~/wmfs-mnt && wmfs dev mount ~/wmfs-mnt2   # 两个挂载点 = 两个"客户端"；预期：打印"实际只有 1 份"的警告
cp -r /usr/include ~/wmfs-mnt/include && diff -r /usr/include ~/wmfs-mnt/include && echo SAME   # 预期：SAME
dd if=/dev/urandom of=~/wmfs-mnt/big bs=1M count=1024 status=progress && md5sum ~/wmfs-mnt/big
echo v1 > ~/wmfs-mnt/cto.txt && cat ~/wmfs-mnt2/cto.txt          # 预期：v1（另一个客户端：写者 close 之后 open 一定看得到）
echo v2 > ~/wmfs-mnt/cto.txt && cat ~/wmfs-mnt2/cto.txt          # 预期：v2（即使还在 1 秒属性缓存窗口内）
(umask 027; touch ~/wmfs-mnt/um && stat -c %a ~/wmfs-mnt/um)     # 预期：640
ln -s cto.txt ~/wmfs-mnt/link && cat ~/wmfs-mnt/link             # 预期：v2
python3 -c "import os; p=os.path.expanduser('~/wmfs-mnt/cto.txt'); os.setxattr(p,'user.k',b'hi'); print(os.getxattr(p,'user.k'))"   # 预期：b'hi'
wmfs dev umount ~/wmfs-mnt && wmfs dev mount ~/wmfs-mnt && md5sum ~/wmfs-mnt/big   # 预期：与上面相同
scripts/build-tools-image.sh                        # 第一次需要几分钟并联网（先征得你同意）
scripts/accept.sh M6
```

- **本阶段你需要理解的概念**：
  - **FUSE**：Linux 允许一个普通程序"扮演"文件系统：内核把 `open/read/write` 等请求转给 `wmfs mount` 进程处理。所以 wmfs 不用写内核模块。
  - **页缓存与 close-to-open（cto）**：内核会把读过的文件内容缓存在内存里。cto 的约定是：别人 `close` 之后，你下一次 `open` 时我们会检查文件是否变过（change_id），变了就丢掉旧缓存——所以"写完关闭、再打开读"一定读到新内容。
  - **写回缓冲**：`write()` 先把数据放在客户端的缓冲区里马上返回，到 close/fsync 或攒够一个 chunk 时再一起发给数据节点，减少网络往返。用 `O_DIRECT` 打开的文件例外：每次写都直接同步提交。
  - **inline 小文件**：≤ 64 KiB 的文件直接存在元数据里，`open` 的回复就带着内容，读小文件只要一次往返（对 AI 训练读大量小文件很重要）。

### M7 目录属性 + 递归用量 + 管理 API（约 2,100 行）

- **目标**：`wmfs set/get/unset/usage` 可用；属性沿目录树继承、能查来源；任意目录的递归用量常量时间可得；管理 API 与 changelog 流可用。
- **包含**：`meta/attrs` 与属性注册表（存储、**服务端切分**、继承、来源、校验、缓存、"必须有插件消费"、成功但警告的情况）、`meta/rstat`（所有操作的增量）、`mgmt`（attr/usage/status/changelog/semantics 端点；请求/响应结构在 `wmfs-api`）、CLI 的挂载点路径映射、查询命令的 `--json`、`wmfs get` 的 `semantics:` 行（含"声明 / 实际"副本数）、`wmfs changelog tail`、`wmfs cluster set`（集群运行参数，例如租约时长、`epoch_gc_grace`）、`wmfs debug rstat-check`。属性键：`chunk`、`inline`、`stripe`、`class`、`replicas`、`checksum`、`consistency`（此时只接受 `cto` 与子键 `attr_ttl`；`strong` 在 M12 才能设置，之前返回 `EOPNOTSUPP`）、`inherit`。对应理念文档：第 3 章"目录属性存储与继承""递归用量计数"、第 5 章 5.1、第 7 章"管理 API""指标"。
- **开工前确认**：批次 3（0026 配额模型）。
- **验收标准**：继承、`inherit=false`、来源显示正确；未知属性名被拒绝；没有插件消费的属性被拒绝；单节点上 `stripe=2`、`replicas=3` 设置成功但给出警告；`wmfs usage` 的字节数等于挂载点下所有普通文件大小之和，且立即返回；随机操作后 `rstat-check` 通过；changelog 包含 AttrSet、Mkdir、Create；`wmfs status --json` 里每个已实现的属性都有 declared/actual。
- **演示命令**：

```bash
wmfs dev up --nodes 1 && eval "$(wmfs dev env)" && mkdir -p ~/wmfs-mnt && wmfs dev mount ~/wmfs-mnt
mkdir -p ~/wmfs-mnt/proj/a/b
wmfs set ~/wmfs-mnt/proj chunk=1M,stripe=2          # 预期：成功，并警告"只有 1 条可用的链，新文件实际只有 1 个条带"
wmfs set ~/wmfs-mnt/proj/a chunk=8M
wmfs get ~/wmfs-mnt/proj/a/b                        # 预期：chunk=8M 来自 /proj/a；stripe=2 来自 /proj；其余 (默认值)；最后一行 semantics:
wmfs unset ~/wmfs-mnt/proj/a chunk && wmfs get ~/wmfs-mnt/proj/a/b    # 预期：chunk=1M 来自 /proj
wmfs set ~/wmfs-mnt/proj chunck=4M                  # 预期：错误"未知属性 chunck"
cp -r /usr/include ~/wmfs-mnt/proj/a/
wmfs usage ~/wmfs-mnt/proj                          # 预期：立即返回字节数与文件数
find ~/wmfs-mnt/proj -type f -printf '%s\n' | awk '{s+=$1} END {print s}'   # 预期：与上一行的字节数相同
wmfs changelog tail -n 100000 | grep -E 'AttrSet|Mkdir' | tail -3   # 预期：能看到 AttrSet 与 Mkdir 事件（最后几条大多是 cp 产生的 Create）
scripts/accept.sh M7
```

- **本阶段你需要理解的概念**：
  - **目录属性继承**：在一个目录上 `wmfs set` 的策略（比如副本数）自动作用于整个子树，子目录可以覆盖；`wmfs get` 会告诉你每个值是从哪一层目录继承来的。
  - **递归用量**：每个目录都记着"我下面一共有多少字节、多少文件"，每次修改顺着父目录一路更新到根。所以查任何目录的用量只读一条记录，不用遍历。
  - **HTTP 管理 API 与 HMAC 签名**：`wmfs set` 等命令通过 HTTP 调用服务端；修改类请求带一个用密钥算出的签名和时间戳。签名防止别人伪造请求；时间戳让截获的请求**超过 5 分钟就不能重放**（5 分钟之内仍可能被重放，所以首发只在可信网络中使用）。
  - **changelog 消费者游标**：外部程序（审计、webhook）读 changelog 时记住"我读到第几条"（游标存在服务端）。程序重启后从游标继续，不会漏、最多重复一次。

### M8 元数据三副本（约 1,310 行）

- **目标**：3 个元数据 voter；杀掉 leader 时客户端不报错；被杀的节点回来后自动追上；各节点的插件集合保证一致。**本里程碑只演示元数据高可用**（数据的多副本链在 M9 实现），所以演示只做元数据操作。
- **包含**：`wmfs join`（带插件清单，不一致就拒绝）、learner 加入、一次 joint 变更提升为 3 voter、leave-joint 兜底、快照（MVCC 读事务导出、带外下载、安装、看门狗、snap-serve 共享 pin）、raft 发送线程（RaftMsg 的 protobuf 走 bulk）、客户端跟随 leader、CLI 多地址、**集群插件清单**（GLOBAL `plugins`、节点比对与退出、`wmfs cluster plugins show/add/remove`、`--adopt-local-plugins` 逃生口）、`wmfs node list/drain/remove/promote`、`wmfs dev up --nodes 3`、`wmfs dev leader`、`wmfs debug meta-hash`。对应理念文档：第 3 章"元数据 Raft（3 副本）""成员变更"、第 7 章"fs join 加入"、原则 7。
- **开工前确认**：无新批次。
- **验收标准**：3 个 voter；3000 次 mkdir 循环中 kill -9 leader → 全部成功（只停顿 1–3 秒）；被杀节点重启后追平；强制小压缩阈值触发快照安装的测试通过；连续 10 轮杀 leader；**3 节点下提交 900 KiB 写集的批次成功**（回归测试）；插件配置不同的节点 join 被拒绝并列出差异；确定性 raft 模拟全套（1000 个种子，内存后端）通过。
- **演示命令**：

```bash
wmfs dev up --nodes 3 && eval "$(wmfs dev env)"
wmfs node list                                      # 预期：3 个 voter，标出 leader
mkdir -p ~/wmfs-mnt && wmfs dev mount ~/wmfs-mnt
( for i in $(seq 1 3000); do mkdir -p ~/wmfs-mnt/m/d$i || echo FAIL $i; done; echo DONE ) &
sleep 2; L=$(wmfs dev leader); wmfs dev kill $L     # kill -9 当前 leader（sleep 2 只是让循环先跑起来）
wait                                                # 预期：只看到 DONE，没有任何 FAIL
wmfs dev wait leader && wmfs node list              # 预期：换了新 leader，term 变大（CLI 自动换到活着的节点）
ls ~/wmfs-mnt/m | wc -l                             # 预期：3000
wmfs dev start $L && wmfs dev wait leader && wmfs node list   # 预期：该节点回来当 follower 并追平
wmfs cluster plugins show                           # 预期：每个节点都必须启用的插件清单（钩子:名字）
scripts/accept.sh M8
```

- **本阶段你需要理解的概念**：
  - **成员变更（learner → voter）**：新节点先当只学习不投票的 learner，等追上进度后，再用一次"联合共识"（joint consensus）变更把 1 个 voter 直接变成 3 个，中间不会出现"2 个 voter、坏一个就不可用"的脆弱状态。
  - **快照**：日志不能无限长，旧日志会被删掉。新加入或落后太多的节点直接拷一份"某一时刻的完整状态"（快照），再从那之后的日志继续追。
  - **leader 切换与客户端重试**：leader 挂掉后约 1–2 秒选出新 leader；客户端发现旧 leader 不回应，就换一个节点重试（同一个 seq），所以应用程序感觉不到错误，只是慢了一下。
  - **集群插件清单**：授权、配额、QoS 这些策略由插件执行。如果某台机器少装了一个插件，它一当上 leader 策略就被悄悄跳过了。所以集群记着"每台都必须装哪些插件"，对不上的节点直接退出，宁可停也不悄悄放过。

### M9 链式复制与故障恢复（约 2,150 行）

- **目标**：文件数据按 `replicas=3` 存三份；写大文件时杀掉任一节点，客户端不报错、内容一致；节点回来后自动同步并恢复服务（理念文档第 9 章成功标准第一条）。
- **包含**：多成员链生成与链管理器、target 心跳/租约/boot_id/自我隔离、转发 + 逆向确认 + pending 表（不占线程）、NEED_FULL、CRAQ 脏读重定向、前滚与 chain-timer、incarnation、last_serving、重同步、`SyncChunks`/`DeleteEpoch` 沿链传播、复制 lane、服务端之间按 data_scheme 选传输（退回时警告）、`wmfs chain list/show/force-serving`、`wmfs status`（副本部分）、`wmfs dev wait chains-serving`、Jepsen-lite 初版（寄存器负载由 testkit 直接用 `DataClient` 读写单个 chunk）、`nas-safe` 阶段版 profile。对应理念文档：第 3 章"数据链式复制""租约与客户端驱逐（target 部分）""脑裂防护"、第 7 章"固定大小 chunk + 链式复制（CRAQ）"。
- **开工前确认**：无新批次（批次 0 已覆盖 0024）。
- **验收标准**：写 2 GiB 过程中 kill -9 一个节点 → 写成功、md5 一致；该节点的 target 先被移出链，回来后 Syncing → Serving，`wmfs status` 的差距归零；不变式检查器（静止后所有 Serving 副本的已提交版本与内容哈希一致）；寄存器负载的线性一致性检查通过；`chain.*`/`sync.*` fail point 测试；整链重启与唯一幸存者（incarnation）测试；`minimal` 与 `nas-safe` 阶段版通过。
- **演示命令**：

```bash
wmfs dev up --nodes 3 && eval "$(wmfs dev env)" && mkdir -p ~/wmfs-mnt && wmfs dev mount ~/wmfs-mnt
wmfs get ~/wmfs-mnt/home | grep replicas            # 预期：replicas=3（来源 /home）
wmfs chain list                                     # 预期：每条链 3 个成员，全部 Serving
head -c 2G /dev/urandom > /tmp/src2g && md5sum /tmp/src2g
( cp /tmp/src2g ~/wmfs-mnt/home/f && echo WRITE-OK ) &
sleep 5; wmfs dev kill 2                            # 写到一半时 kill -9 节点 2（sleep 5 只是让写先开始）
wait                                                # 预期：WRITE-OK（客户端只是等待并重试，没有报错）
md5sum ~/wmfs-mnt/home/f                            # 预期：与 /tmp/src2g 相同
wmfs chain list                                     # 预期：节点 2 的 target 已从各链移除（链降级为 2 个成员）
wmfs status ~/wmfs-mnt/home                         # 预期：第一行警告链降级；replicas 声明 3、实际 2 的文件数 > 0
wmfs dev start 2 && wmfs dev wait chains-serving && wmfs chain list   # 预期：节点 2 的 target 先 Syncing，后 Serving
wmfs status ~/wmfs-mnt/home                         # 预期：差距归零
scripts/accept.sh M9
```

- **本阶段你需要理解的概念**：
  - **链式复制与 CRAQ**：一个 chunk 的几个副本排成一条链。写从链头进，依次传到链尾；链尾先确认，确认再一路传回链头。读可以去任何一个副本；如果它手上有"还没确认完"的新版本，就让客户端去问链上最后一个正常服务的副本，保证不会读到"先新后旧"。
  - **pending 前滚**：链中途有节点坏了，写到一半的版本不回滚，而是在剩下的节点上继续推进完成——因为它可能已经被别人读到过，回滚会让读到的数据凭空消失。
  - **Syncing / Serving 与重同步**：重启或新加入的副本先当 Syncing（只接收新写、不对外提供读），从"最后一个正常服务的副本"拷差异，拷完才变 Serving。任何重启都要走这一步，所以重启时丢掉的半截数据永远不会被读到。
  - **租约与自我隔离（fencing）**：数据节点每秒向 leader 报到；连续 6.4 秒报不到就自己停止服务，而 leader 等满 8 秒才把它移出链。"自己先停、别人后接手"保证不会两边同时服务。
  - **incarnation**：当某条链只剩一个副本、而它又刚重启过时，给这条链换一个"新纪元"，之后的版本号都比以前大，旧副本上的数据永远不会被误认成新数据。

### M10 授权 + 配额 + 驱逐（约 1,150 行）

- **目标**：ACL 与路径限制生效；配额在写入时报 `EDQUOT`，超限有可证明的上界；被冻住的客户端被安全地驱逐。
- **包含**：`plugins/posix-acl`、`plugins/path-caps`（规则来自复制状态）、`plugins/quota` 与 core 的 quantum 额度/护栏、`acl.default` 属性（core 的继承）、root_squash/uid 范围、两阶段驱逐、`wmfs key create/list/revoke`、`wmfs session list/evict`（含 24 h 内的 EVICTED 记录）、`wmfs dev wait session-evicted`；容器内 pjdfstest 的权限相关子集。对应理念文档：4.1 authz（posix-acl、path-caps）、quota_admission（quota）、第 3 章"租约与客户端驱逐"、5.2 `quota`/`quota.<class>`/`acl.default`、8.2 配额精度。
- **开工前确认**：无新批次（批次 0、3 已覆盖）。
- **验收标准**：单个写者恰好在 100 MiB 附近得到 `EDQUOT`；8 个并发写者时总用量 ≤ 100 MiB + 8 × quantum；软限超出 → SoftWarn 事件，宽限期后按软限拒绝；`files=` 精确；ACL 授权正确；path-caps 密钥越界被拒；`kill -STOP` 客户端约 30–40 秒后会话被驱逐，数据节点拒绝它的 cap，锁在数据节点确认之后才释放（日志可见顺序），恢复后旧句柄 `EIO`、新打开正常；pjdfstest 权限子集通过。
- **演示命令**：

```bash
wmfs dev up --nodes 3 && eval "$(wmfs dev env)" && mkdir -p ~/wmfs-mnt ~/wmfs-mnt2 && wmfs dev mount ~/wmfs-mnt && wmfs dev mount ~/wmfs-mnt2
mkdir ~/wmfs-mnt/home/q && wmfs set ~/wmfs-mnt/home/q quota=100M,soft=80M,grace=1m,files=1000,quantum=1M
dd if=/dev/zero of=~/wmfs-mnt/home/q/f bs=1M count=300   # 预期：写到约 100 MiB 时报 "Disk quota exceeded"
wmfs usage ~/wmfs-mnt/home/q                        # 预期：用量 ≤ 100 MiB；软限已超，宽限剩余约 1 分钟
mkdir ~/wmfs-mnt/home/private && chmod 700 ~/wmfs-mnt/home/private && echo secret > ~/wmfs-mnt/home/private/s
wmfs fs --as-uid 2000 cat /home/private/s           # 预期：Permission denied
setfacl -m u:2000:rx ~/wmfs-mnt/home/private && setfacl -m u:2000:r ~/wmfs-mnt/home/private/s
wmfs fs --as-uid 2000 cat /home/private/s           # 预期：secret（ACL 放行了 uid 2000）
wmfs key create q-only --role client --path /home/q --out /tmp/q.key
wmfs fs --key-file /tmp/q.key ls /home/private      # 预期：Permission denied（这把 key 只能访问 /home/q）
exec 3< ~/wmfs-mnt2/home/private/s                  # 让第二个客户端打开一个文件
kill -STOP $(wmfs dev client-pid ~/wmfs-mnt2)
wmfs dev wait session-evicted ~/wmfs-mnt2 --timeout 90 && wmfs session list   # 预期：该会话显示 EVICTED
kill -CONT $(wmfs dev client-pid ~/wmfs-mnt2); cat <&3    # 预期：Input/output error（旧句柄不再可用）
cat ~/wmfs-mnt2/home/private/s                      # 预期：secret（自动建立新会话后，新打开正常）
scripts/accept.sh M10
```

- **本阶段你需要理解的概念**：
  - **ACL**：比 rwx 更细的权限表，可以单独给某个用户或组开权限（`setfacl -m u:2000:r`）。
  - **path-caps**：给一把客户端密钥加"只能进这几个目录"的限制，例如把一个实验室的机器限制在 `/proj/lab-a`。规则存在集群的复制状态里，所有节点看到的一样。
  - **配额 quantum**：客户端每次向元数据要一段写入额度（默认 64 MiB，演示里调成 1 MiB），用完再要。多个客户端同时写时，每个手里可能还有一点没用完的额度，所以用量可能**短暂**超出上限，最多超 `quantum × 同时写入的文件数`；单个写者不会超。
  - **两阶段驱逐**：客户端失联 30 秒后，先把它"拉黑"并等所有数据节点确认收到黑名单，然后才释放它持有的锁。这样它在失联前发出的写，不可能在锁已经交给别人之后才落地覆盖别人的数据。

### M11 后台任务 + 协调器 + 全部变换（约 2,100 行）

- **目标**：改目录属性后，存量数据被后台自动转换，进度与积压可见、可暂停、断点续跑；压缩与加密生效；scrub 发现并修复坏副本；`wmfs status` 对**每个**属性都给出声明与实际。
- **包含**：`bg`（调度器、限速、可续跑游标、遍历器、迁移原语——含"因写打开而跳过"的计数与 24 h 告警）、`bg/status.rs`（design.md 3.10 那张"每个属性的实际值从哪来"的表）、`plugins/reconciler`、`plugins/scrub`、`plugins/gc` 完整版、`plugins/{xxh3, blake3, lz4, zstd, aes-gcm}`、数据密钥（`wmfs datakey create` 打印 id、`DATAKEY_NAME` 索引、经集群图下发）、`wmfs set` 对已有文件的提醒、`wmfs task …`、`wmfs dev wait task-idle`。对应理念文档：第 3 章"后台任务框架"、5.3 协调器、4.1 transform 与 bg_task、5.2 `compress`/`encrypt`/`replicas`/`class` 的"协调器迁移"。
- **开工前确认**：无新批次（批次 0 已覆盖 0027）。
- **验收标准**：设置 `compress=zstd:3` 后存量全部转换、内容不变、物理占用明显下降；转换途中杀 leader → 新 leader 从断点继续；`encrypt=tenant:k1` 后 target 目录里搜不到明文；`replicas` 3 → 2 转换；人为损坏一个副本 → scrub 发现并从健康副本修复；GC 在文件仍被打开时不删除旧 epoch；一直写打开的文件被跳过并单独计数；`O_DIRECT` 写的同时触发迁移，内容不丢。
- **演示命令**：

```bash
wmfs dev up --nodes 3 && eval "$(wmfs dev env)" && mkdir -p ~/wmfs-mnt && wmfs dev mount ~/wmfs-mnt
wmfs cluster set epoch_gc_grace=0s                  # 演示用：旧 epoch 一转换完就可以删（生产保持默认 60 s）
mkdir ~/wmfs-mnt/home/data
for i in $(seq 1 20); do yes "WMFS-PLAINTEXT-MARKER $i" | head -c 8M > ~/wmfs-mnt/home/data/f$i; done
cat ~/wmfs-mnt/home/data/f* | md5sum; du -sch ~/wmfs-dev/n*/targets | tail -1   # 记下内容的 md5 与压缩前的物理占用（3 个节点合计）
wmfs task set reconciler bw=2M                      # 先把协调器限速到 2 MiB/s，方便看清进度
wmfs set ~/wmfs-mnt/home/data compress=zstd:3       # 预期：成功，并提醒"已有文件要等协调器转换完才按新设置"
wmfs status ~/wmfs-mnt/home/data                    # 预期：compress 声明 zstd:3、不一致约 20 个文件；协调器进度
wmfs task list                                      # 预期：reconciler 的进度、速率（约 2 MiB/s）、积压
L=$(wmfs dev leader); wmfs dev kill $L && wmfs dev wait leader && wmfs task list   # 预期：新 leader 从断点继续推进
wmfs dev start $L && wmfs dev wait chains-serving   # 被杀的节点回来并同步完
wmfs task set reconciler bw=200M && wmfs dev wait task-idle reconciler --timeout 300
wmfs status ~/wmfs-mnt/home/data                    # 预期：不一致 0
wmfs task run gc && wmfs dev wait task-idle gc      # 清理旧 epoch
cat ~/wmfs-mnt/home/data/f* | md5sum; du -sch ~/wmfs-dev/n*/targets | tail -1   # 预期：md5 不变；物理占用明显变小
wmfs datakey create k1                              # 预期：打印数据密钥的 id
wmfs set ~/wmfs-mnt/home/data encrypt=tenant:k1 && wmfs dev wait task-idle reconciler --timeout 300
wmfs task run gc && wmfs dev wait task-idle gc
grep -rl "WMFS-PLAINTEXT-MARKER" ~/wmfs-dev/n*/targets/ | head -1        # 预期：没有输出（落盘的是密文）
wmfs debug corrupt-chunk /home/data/f1 0 --node 2 --block 0              # 人为损坏节点 2 上的一个副本
wmfs task run scrub && wmfs dev wait task-idle scrub && wmfs changelog tail -n 50 | grep ChecksumRepaired   # 预期：发现并修复了这个坏块
scripts/accept.sh M11
```

- **本阶段你需要理解的概念**：
  - **后台任务与断点续跑**：大量数据的转换不能一口气做完，后台任务每次只做一小步，并把"做到哪了"（游标）存进集群状态。leader 换了，新 leader 从游标接着做。
  - **协调器（声明 vs 实际）**：你用 `wmfs set` 写下的是"声明"（我想要压缩），协调器不断比较每个文件的"实际"，不一致就把它迁移到新布局。`wmfs status` 随时告诉你每个属性还差多少。还在被写的文件会先跳过，等它关闭再转换。
  - **变换链**：数据写盘前依次经过 校验和 → 压缩 → 加密，读的时候反过来。顺序由本体固定，插件只负责各自那一步。
  - **scrub**：后台定期把每个副本读一遍、核对校验和；发现坏块就从另一个好副本拷回来；如果只有这一个副本也坏了，就如实告警，不假装没事。

### M12 锁 + strong + O_APPEND + 回收站 / ttl（约 850 行）

- **目标**：跨挂载点的 flock/fcntl 互斥；`consistency=strong` 的目录里写完另一边立刻可见、并发追加不丢不乱；共享可写 mmap 被诚实拒绝；回收站与 ttl 按属性生效。
- **包含**：`meta/locks`（复制的锁表；`SetLock` 永远非阻塞；"到文件尾"= `i64::MAX`）、客户端锁处理（F_SETLKW 交给 lock-wait 线程轮询，不占 FUSE 线程）、strong 模式（direct_io、读前 GetAttr、每次写同步提交、strong 目录里新文件不 inline、写打开 inline 文件前先转出）、`AppendReserve`、回收站（unlink 路径、`.wmfs-trash`、mv 恢复）、`plugins/{trash-expire, ttl}`、内置策略求值器、`audit` 标记（第一次启用时创建 `audit-log` 游标）、relatime；一个确认 F_GETLK 行为的小 spike。对应理念文档：第 3 章"事务与锁原语"（字节范围锁、跨节点原子操作）、"一致性：cto 与 strong"、5.2 `consistency`/`trash`/`ttl`/`audit`、8.2 的 mmap 与 O_APPEND 条目。（分层 `tier` 需要先能创建存储类，放到 M13。）
- **开工前确认**：批次 4（0013）。
- **验收标准**：两个挂载点在 strong 目录并发追加 2 × 1000 行 → 正好 2000 行且格式完整；strong 目录里两个挂载点并发追加同一个**小文件**也正确；strong 下写后另一挂载点立即读到；flock/fcntl 跨挂载点互斥（计数器测试结果恰好 2000）；**17 个进程同时 F_SETLKW 等同一把锁时，挂载点上的 `ls` 仍立即返回**；F_GETLK 能看到 `l_len = 0`（到文件尾）的冲突锁；strong 文件上 `mmap(MAP_SHARED)` 得到 `ENODEV`；rm 后能从回收站 mv 回来；ttl 用短时长按预期删除；在锁回调里调用内核通知不出问题。
- **演示命令**：

```bash
wmfs dev up --nodes 3 && eval "$(wmfs dev env)" && mkdir -p ~/mntA ~/mntB && wmfs dev mount ~/mntA && wmfs dev mount ~/mntB
mkdir ~/mntA/home/shared && wmfs set ~/mntA/home/shared consistency=strong
(for i in $(seq 1000); do echo "A $i" >> ~/mntA/home/shared/log; done) &
(for i in $(seq 1000); do echo "B $i" >> ~/mntB/home/shared/log; done) &
wait; wc -l < ~/mntA/home/shared/log; grep -cE '^(A|B) [0-9]+$' ~/mntA/home/shared/log   # 预期：2000 和 2000
echo v2 > ~/mntA/home/shared/x && cat ~/mntB/home/shared/x       # 预期：v2（strong：写完另一边立刻读到）
flock ~/mntA/home/shared/lk -c 'sleep 10' & sleep 1
flock -n ~/mntB/home/shared/lk -c true || echo "B 拿不到锁（正确：锁是集群范围的）"
for i in $(seq 17); do flock ~/mntA/home/shared/lk -c true & done; time ls ~/mntA/home/shared >/dev/null   # 预期：ls 立刻返回（等锁的进程不占 FUSE 线程）
wait
python3 -c "import mmap,os; f=os.open(os.path.expanduser('~/mntA/home/shared/x'),os.O_RDWR); mmap.mmap(f,0)"   # 预期：OSError: [Errno 19] No such device
wmfs set ~/mntA/home trash=7d && echo important > ~/mntA/home/doc && rm ~/mntA/home/doc
ls ~/mntA/home/.wmfs-trash/                         # 预期：<时间>-<ino>-doc
mv ~/mntA/home/.wmfs-trash/*-doc ~/mntA/home/doc && cat ~/mntA/home/doc   # 预期：important
scripts/accept.sh M12
```

- **本阶段你需要理解的概念**：
  - **字节范围锁与 flock**：程序可以锁住文件的一段（fcntl）或整个文件（flock），别的程序要等它释放。在 wmfs 里锁存在元数据服务里，所以不同机器之间也互斥。拿到锁时客户端会丢掉旧缓存，放锁前会先把数据写出去——"锁就是同步点"。等锁是客户端自己隔一会儿问一次（不保证先来先得），这样等锁的程序再多也不会把挂载点卡住。
  - **strong 一致性**：在设了 `consistency=strong` 的目录里，客户端不缓存任何东西，每次读都先问最新长度，每次写都同步提交。代价是慢，好处是任何两台机器之间"写完立刻可见"。
  - **O_APPEND 偏移分配**：多个机器同时往一个文件末尾追加时，由元数据服务原子地分配"你写在哪个位置"，所以不会互相覆盖。
  - **回收站 / ttl**：`trash` 让 rm 变成"移进回收站"，到期才真删；`ttl` 让过期文件自动删除。它们都只是目录属性，由后台任务执行。

### M13 存储类 + 放置 + QoS + 分层（约 1,350 行）

- **目标**：定义与修改存储类；按标签、故障域、locality 放置数据；mClock 的上限、保底与低优先级在数据节点生效；分层按属性迁移；系统根据实际物理盘诚实显示隔离承诺。
- **包含**：存储类 CRUD（TOML）与 `wmfs class update`（只改 QoS 不迁移；改 replica/生命周期触发整树协调；改 label 拒绝）、`plugins/label-match` 完整版（dedicated、locality、rebalance 用的 exclude）、`plugins/mclock`（作业 id 轮转、字节令牌桶、后台队列、`priority = low`）、QoS 表经集群图下发与按节点均分、物理盘/namespace 解析与隔离承诺计算、`plugins/tiering`（从 M12 移来：它需要先能创建第二个存储类）、`plugins/rebalance`、`plugins/cache-local-nvme`；`wmfs class show` 显示 iops 换算。对应理念文档：第 6 章全部、4.1 qos（mclock）与 placement（label-match）、cache_store（local-nvme）、bg_task（tiering、rebalance）、5.2 `class`/`qos`/`locality`/`tier`。
- **开工前确认**：无新批次（批次 0 已覆盖 0007、0010、0011）。
- **验收标准**：目录的 `l=` 上限在单流写入时被观察到（指标与 dd 速度，误差 ±15%）；虚拟时钟单元测试里权重 1:3 的份额误差 < 5%、`priority = low` 的队列只在 normal 队列没有可派发请求时才被服务（磁盘不饱和时真实环境看不出这些，所以不用它们做演示）；改 `class` 触发迁移；`wmfs class update` 只改 qos 时不迁移、改 replica 时触发协调；`tier` 用短条件按预期迁移；`wmfs class show` 显示"shared：只承诺份额"，对声明了 `dedicated` 的类产生 `isolation_downgraded` 事件；物理盘解析单测（用构造的假 `/sys` 目录：同一块 NVMe 的两个分区、同一个 PV 上的两个 LV 都判为共享，loop 判为 unknown）；`locality=prefer-client` 时新文件的链头在客户端所在"主机"（开发集群里用 `wmfs dev mount --node n` 模拟）。
- **演示命令**：

```bash
wmfs dev up --nodes 3 && eval "$(wmfs dev env)"
cat > /tmp/gold.toml <<'EOF'
name = "gold"
[devices]
label = "*"
[redundancy]
replica = 2
[qos]
iops_max = 300
EOF
wmfs class create gold --file /tmp/gold.toml
wmfs class show gold                                # 预期：iops_max=300 ≈ 每秒 9 次 4 MiB 写 ≈ 36 MiB/s，按 3 个节点均分后每节点 100；isolation: shared（只承诺份额，不承诺尾延迟）
mkdir -p ~/wmfs-mnt && wmfs dev mount ~/wmfs-mnt --cache none
mkdir ~/wmfs-mnt/fast && wmfs set ~/wmfs-mnt/fast class=gold   # 放在根目录下：根上没有 replicas 属性，副本数就取 gold 类的 2
dd if=/dev/zero of=~/wmfs-mnt/fast/z bs=4M count=50 oflag=direct    # 预期：速度被限在约 10 MB/s 量级（一个文件只经过一个链头节点，每节点 100 成本单位/秒）
wmfs set ~/wmfs-mnt/fast qos=r=0,w=1,l=3000
dd if=/dev/zero of=~/wmfs-mnt/fast/z2 bs=4M count=50 oflag=direct   # 预期：明显变快（上限放大 10 倍）
curl --noproxy '*' -s 127.0.0.1:19180/metrics | grep wmfs_qos_served_total
wmfs status ~/wmfs-mnt/fast                         # 预期：class=gold，replicas 声明 2、实际 2
scripts/accept.sh M13
```

- **本阶段你需要理解的概念**：
  - **存储类**：一组"这类数据该怎么存"的打包设置：放在哪种盘上（标签）、几份副本、多少性能（QoS）、什么时候迁走（生命周期）。目录用 `class=` 绑定一个存储类——这就是我们的"虚拟盘"。
  - **mClock（保底 / 权重 / 上限）**：每个队列三个数：r 保底（至少给你这么多）、w 权重（有空余时按比例分）、l 上限（最多给你这么多）。上限在磁盘不忙时也能看到效果，所以演示用它。这里的"iops"其实是"成本单位"：4 KiB 的读写算 1，4 MiB 算 33。
  - **放置与故障域**：一个 chunk 的几个副本必须放在不同的节点上（故障域 = 节点），这样坏一台机器不会同时丢掉所有副本。
  - **诚实的隔离承诺**：只有一个存储类独占整块物理盘时，我们才承诺"份额 + 尾延迟"；和别的类共用一块盘时只承诺份额。我们看的是物理盘本身，不是分区——同一块盘切成两个分区也算共用。本机只有一块盘，所以永远显示 shared——这是如实告诉你，不是 bug。

### M14 管理面插件 + trace + 动态库 ABI（约 1,190 行）

- **目标**：dashboard、Prometheus 汇总、告警、审计日志、webhook 在独立进程里运行；所有策略属性都能在 dashboard 上看到声明与实际；一个请求跨节点的时间线可查；用 C 写的 .so 授权插件能被加载，ABI 不兼容的被拒绝，没装它的节点被拒绝。
- **包含**：`wmfs-mgr` crate（宿主：manifest 校验、唯一的 changelog 长轮询循环、fail point 与指标；dashboard、exporter、alert（含 `GET /alerts`）、audit-log、webhook）、`wmfs mgr`、`wmfs dev mgr`（`wmfs dev up --profile nas-safe` 自动调用）、`wmfs dev wait alert/audit-caught-up`、span 环形缓冲与 `wmfs trace`、`core/hooks/cabi.rs`（authz 动态库）、`core/include/wmfs_plugin.h`、`docs/plugin-abi.md`（ABI 规则与 AuthzOp 编号表）、`examples/dylib-deny-uid`（含 `abi_version = 2` 的坏插件）、完整 `nas-safe` profile。对应理念文档：4.1 event_sink（audit-log、webhook）与 mgmt_ext（prometheus-exporter、dashboard、alert）、4.2 进程外与动态库、第 3 章"可观测性基座"的 trace、第 9 章成功标准"所有策略旋钮……在 dashboard 上看到声明与实际"。
- **开工前确认**：批次 5（0012）。
- **验收标准**：dashboard 各面板有数据；**`/api/v1/status` 对 design.md 3.10 表里的每个属性都返回 `{key, declared, source, effect, actual, mismatched}`，验收脚本逐个键检查**，dashboard 的"声明 vs 实际"面板逐个属性显示；杀掉一个节点约 60 秒后告警触发、恢复后 resolved；审计日志包含预期事件，mgr 重启后无缺失（按 seq 可去重）；`wmfs trace` 显示链头 → 中间 → 链尾的时间线；deny-uid 插件生效；坏 ABI 插件导致 server **拒绝启动**并写明原因；集群插件清单加入 deny-uid 后，不带这个 .so 单独重启的节点被拒绝（退出并列出差异）；完整 `nas-safe` profile 通过。
- **演示命令**：

```bash
wmfs dev up --nodes 3 --profile nas-safe && eval "$(wmfs dev env)" && mkdir -p ~/wmfs-mnt && wmfs dev mount ~/wmfs-mnt
# 预期：最后一行打印 dashboard 网址（带 token）——nas-safe 会在集群起好后自动起一个 mgr；浏览器打开它
touch ~/wmfs-mnt/home/a && rm ~/wmfs-mnt/home/a && wmfs dev wait audit-caught-up
grep -hE '"op":"(Create|Unlink)"' ~/wmfs-dev/mgr/audit/audit-*.log | tail -2   # 预期：Create 与（进回收站的）Unlink 两条事件
wmfs status ~/wmfs-mnt/home --json | python3 -m json.tool | head -40          # 预期：每个属性一项 {key, declared, source, effect, actual, mismatched}
wmfs dev kill 3 && wmfs dev wait alert target_offline firing --timeout 120    # 预期：约 60 秒后告警触发
wmfs dev start 3 && wmfs dev wait alert target_offline resolved && wmfs dev wait chains-serving
dd if=/dev/zero of=~/wmfs-mnt/home/t bs=4M count=4 conv=fsync
wmfs trace --recent 5                               # 预期：最近 5 个请求的 trace_id、操作、耗时
wmfs trace <上一步列出的某个 WriteChunk 的 trace_id>   # 预期：链头 → 中间 → 尾 各阶段耗时（客户端自己的耗时在客户端日志里）
gcc -shared -fPIC -I core/include -o /tmp/deny_uid.so examples/dylib-deny-uid/deny_uid.c
gcc -shared -fPIC -I core/include -o /tmp/bad_abi.so examples/dylib-deny-uid/bad_abi.c
wmfs cluster plugins add authz:deny-uid             # 预期：提示"已写入；各节点会因插件集合不一致而退出，请用新配置重启全部节点"
wmfs dev restart-all --dylib /tmp/deny_uid.so && wmfs dev wait leader
wmfs fs --as-uid 4242 touch /home/zz                # 预期：Permission denied（动态库插件拒绝了 uid 4242）
wmfs dev restart 1; wmfs dev logs 1 | tail -3       # 不带 .so 重启节点 1；预期："插件集合与集群清单不一致：缺少 authz:deny-uid"，节点 1 退出
wmfs dev restart 1 --dylib /tmp/deny_uid.so --dylib /tmp/bad_abi.so; wmfs dev logs 1 | tail -3   # 预期："动态库 ABI 版本 2 不受支持（支持 [1]），拒绝启动"
wmfs dev restart 1 --dylib /tmp/deny_uid.so && wmfs dev wait chains-serving   # 恢复正常
scripts/accept.sh M14
```

  （`wmfs dev restart <n> --dylib F…` 用给出的列表改写该节点 node.toml 里的 `[plugins] dylib` 再重启；不给 `--dylib` 就是清空列表。）

- **本阶段你需要理解的概念**：
  - **进程外插件**：dashboard、告警这类东西跑在单独的 `wmfs mgr` 进程里，只能通过管理 API、`/metrics`、changelog 看系统。它崩了不影响文件读写；它的代码甚至在编译时就拿不到本体的内部接口。
  - **trace（追踪号）**：客户端给每个操作编一个号，这个号跟着请求经过元数据、链头、链上每个节点。出了慢请求，用这个号把各节点的记录拼成一条时间线，就知道慢在哪一步。
  - **动态库 C ABI 与版本规则**：第三方可以用 C 写一个 .so 授权插件。接口格式一旦发布就冻结；版本号对不上，wmfs 直接拒绝加载，绝不"猜着适配"。授权插件还必须每个节点都装：少装一台，那台就会拒绝启动。

### M15 profile 全套与发布门槛（约 300 行 + 测试）

- **目标**：三个承诺的插件组合（profile）全部通过全套测试，达到首发门槛。
- **包含**：`tests/profiles/{minimal, hpc_rdma, nas_safe}.rs` 定稿、`scripts/profiles.sh`、`scripts/accept.sh profile-* / chaos / release`、容器内完整 pjdfstest、每个 profile fsx 10 万次、IOR/mdtest 冒烟；`docs/semantics.md` 与 `docs/ops.md` 定稿；全部 ADR 状态更新。对应理念文档：第 9 章"质量与验收"、10.4"完成的定义"。
- **开工前确认**：无新批次（0015 已在批次 0 确认）。
- **发布门槛**：`minimal` 与 `nas-safe` 全部 PASS；`hpc-rdma` 在有 Soft-RoCE 时 PASS，没有时明确标注"未在本机验证"（发布说明里不能声称支持该 profile）；pjdfstest 的每个失败都能在 semantics.md 放松清单里找到对应条目；所有故障注入测试中客户端除重试外无错误；Jepsen-lite 线性一致；`cargo clippy -- -D warnings` 与全部单元测试通过；`scripts/loc.sh` 在预算内。
- **演示命令**：

```bash
scripts/build-tools-image.sh
scripts/accept.sh profile-minimal
scripts/accept.sh profile-nas-safe
scripts/accept.sh profile-hpc-rdma                  # 需要 Soft-RoCE，否则明确输出 SKIP
scripts/accept.sh chaos                             # kill -9、SIGSTOP、分区、时钟偏移 + 线性一致性检查
scripts/accept.sh release                           # 以上全部 + clippy + 单元测试 + 文档齐全检查
```

- **本阶段你需要理解的概念**：
  - **profile**：我们只承诺有限几种插件组合一定通过全套测试：`minimal`（单节点、无配额）、`hpc-rdma`（3 节点、RDMA、配额、QoS）、`nas-safe`（3 节点、3 副本、回收站、审计）。其他组合尽力而为（semantics.md 写明）。
  - **pjdfstest**：一套检查"文件系统行为是否符合 POSIX"的老牌测试，要用 root 跑，所以放在 docker 容器里执行。我们事先列出"因为文档里写明的放松而预期失败"的用例，清单外的失败就不能发布。
  - **诚实的"未验证"**：本机做不到的测试（真实 RDMA 性能、BeeGFS 对比、真实掉电）不会被说成"通过"，发布说明里如实写"未在本机验证"。

---

## 4. 简化储备（超预算时按顺序删减；都不违反理念文档的首发要求，除非特别注明）

1. readdirplus（只保留普通 readdir，−100；`ls -l` 大目录会变慢）。
2. `wmfs trace` 的跨节点合并（只保留单节点 API，−80）。
3. alert 只保留"事件型"规则，去掉指标表达式（−80）。
4. rebalance 简化为"新文件优先放进更空的链"，不迁移存量（−100）。
5. local-nvme 缓存改为整文件粒度（−80）。
6. webhook 去掉批量（每条一次 POST，−40）。
7. `wmfs debug` 只保留 `kv`、`chunk-path`、`corrupt-chunk`、`gen-key`（−100）。
8. `wmfs node drain`（−80）：改用 `wmfs node remove` + 链管理器自动补齐；代价是计划内下线时副本数暂时不足（`wmfs status` 可见）。
9. `wmfs debug meta-hash`（−40）：只保留 raft 模拟里的副本哈希比对。
10. `RENAME_EXCHANGE`（−60；改为返回 `EINVAL`，**会扩大 POSIX"不支持"清单，需要你同意**）。

合计约 760 行。（设计阶段已经直接采用的简化：F_SETLKW 改为客户端轮询、去掉服务端锁等待队列（−150）；动态库只做 authz（−150，需批次 5 同意）；去掉 mgr 自动拉起（−100）；去掉 `audit=all` 的 open 审计（−60）。）

---

## 5. 需要你执行或同意的系统改动（agent 不会自己运行；截至 2026-10-20 已全部定下，没有需要你做的了）

| 什么时候 | 做什么 | 为什么 | 不做会怎样 |
|---|---|---|---|
| ~~M2~~（已取消） | ~~Soft-RoCE~~ | 你决定本机不做 RDMA 实机测试（2026-10-20） | M2 与 hpc-rdma 里需要设备的检查显示 SKIP，RDMA 标为"未在本机验证" |
| M6（已同意，2026-10-20） | 联网构建 docker 镜像 `scripts/build-tools-image.sh`（它用 `docker build --network host --build-arg http_proxy=$http_proxy --build-arg https_proxy=$https_proxy`，让镜像里的 apt 与源码下载走本机代理） | pjdfstest、fio、fsx、IOR/mdtest 都要 root 或本机没有；容器里以 root 挂载客户端即可，不需要宿主机 sudo | 只能跑我们自己写的测试，发布门槛里的 pjdfstest 无法验证 |
| 永远不需要 | 修改 `/etc/fuse.conf` 的 `user_allow_other` | 多用户演示改用 `wmfs fs --as-uid` 与容器 | — |

