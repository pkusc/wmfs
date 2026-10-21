# {{PROJECT_NAME}}：设计理念与架构约束

> **用途**：本文是交给 AI agent（架构 / 编码助手）的最高优先级上下文。所有设计与实现决策必须与本文一致；收到与本文冲突的请求时，先指出冲突，再按人类的明确指示执行。
>
> **标记说明**
> - **[固定]**：不可更改。改动需要人类批准，并记录 ADR。
> - **[默认]**：当前默认值。agent 可在有充分理由时提出修改建议，但不得擅自偏离。
> - **[待定]**：尚未决定。agent 使用括号内的默认假设，并在产出物中显式标注"基于待定假设"。

---

## 0. 你的角色

你是本项目的架构与开发 agent。你的职责：

1. 按本文的原则做设计与实现，产出代码、测试、ADR（架构决策记录）与文档。
2. 遇到本文未覆盖的决策，按第 2 章原则的优先级推导，优先选择**可逆**的方案，并写一条 ADR 草稿说明理由。
3. 只有三类情况停下来询问人类：契约层（第 3 章）变更；不可逆的格式 / 协议变更；需要产品取舍的决策。其余自行决定并说明理由。
4. 永远不做的事：往本体里加功能；让插件持有本体不知道的状态；承诺做不到的语义或隔离；静默丢数据或静默降级。

---

## 1. 愿景

一个面向**小规模集群**（1–64 节点，典型 3–8 节点）的分布式文件系统：**本体极简、功能皆插件、极度可定制、开箱即用**。

目标用户是只有几台机器、却想要"大系统"能力的团队：超算队、实验室、小公司。他们想要软硬配额、权限控制、RDMA、分层、QoS、dashboard，但不想养一个 Ceph，也不想为 Lustre 配 Pacemaker。

一句话：**一个二进制、三台机器，得到一个可以逐目录定制的企业级文件系统。**

---

## 2. 核心原则（按优先级排序，冲突时靠前者胜）

1. **极简的是功能，不是契约。** 本体只包含"所有人都依赖、改了就要重写一切、正确性横切所有路径"的东西（第 3 章）。其余一律插件。判断标准不是"这个功能重不重要"，而是"没有它别的东西能不能正确地存在"。
2. **钩子少而窄、顺序固定。** 插件只能通过本体定义的有限钩子点介入（第 4.1 节）；同一钩子上的多个插件按本体分配的 altitude 区间排序，配置只能启停、不能改序；热路径钩子的 ABI 极窄。正面参考：Windows minifilter、Lustre LNet/OSD、SPDK bdev、Samba VFS 模块。反面教材：GlusterFS translator（层太多、顺序自由、状态分散，最终不可调试）。
3. **极度自定义 = 目录树上可继承的声明式策略 + 协调器。** 每一个可定制的行为都必须能表达为目录属性（`fs set <path> key=value`），子树继承、子目录覆盖、可查来源；本体的协调器负责把存量数据迁移到声明的状态。**不允许存在"只能在全局配置里改"的策略**——全局配置只放部署参数（端口、路径、设备）。
4. **小集群优先。** 单二进制、零外部依赖、单节点能跑、3 节点即元数据 HA。复杂度预算：一个人半天能搭起来，看 dashboard 就能运维。设计不得阻碍扩展到更大规模，但**不为大规模牺牲小规模体验**。
5. **RDMA-first 的数据路径。** 缓冲区模型（预注册内存池、零拷贝所有权、请求生命周期）在本体里按 RDMA 的要求设计；TCP 只是另一个传输插件。不允许"先用 TCP 做出来、以后再加 RDMA"——那意味着重写数据路径。
6. **可观测、可运维是本体能力。** 每个请求携带 trace 上下文，每个计数器可导出，每个策略违约可见。dashboard 本身是进程外插件，但它依赖的指标流、事件流与管理 API 是本体的稳定接口。
7. **诚实的语义。** 一致性等级、隔离强度、配额精度都要能被查询到并写进文档；做不到的不承诺（例如共享 HDD 上的延迟隔离）。宁可拒绝操作，不可静默丢数据、静默降级或静默忽略策略。

---

## 3. 本体（契约层）[固定]

以下内容必须在本体中实现，禁止做成插件。修改任何一条需要 ADR 与人类批准。

| 契约 | 本体必须提供 | 为什么不能是插件 |
|---|---|---|
| 命名空间语义与一致性模型 | POSIX 子集定义（第 8.2 节）；rename 原子性；可见性规则；缓存失效协议；目录属性 `consistency` 的两个等级：`cto`（close-to-open，默认）与 `strong`（租约 / 锁） | 所有插件依赖同一套"何时可见"的定义；两个插件理解不同就会静默丢数据 |
| 持久化格式与线上协议 | chunk 格式（头部：版本、校验、变换标记、所属 inode）；元数据 KV 布局；消息帧；能力协商；feature flags（参考 ZFS feature flags） | 改一次等于重写全系统；必须从第一天带版本号 |
| 复制、成员、选举、fencing | 元数据 Raft（3 副本）；数据链式复制；成员变更；租约与客户端驱逐；脑裂防护 | 正确性横切所有路径 |
| 事务与锁原语 | 元数据事务；字节范围锁；租约；跨节点原子操作 | 授权、配额、快照必须在同一事务内生效 |
| 缓冲区与内存模型 | 预注册内存池；零拷贝所有权显式转移；请求生命周期管理 | 这是"RDMA 作为插件"能成立的前提 |
| 钩子管线与插件 ABI | 钩子清单（4.1）、altitude 分配、manifest 格式、ABI 版本与兼容规则 | 插件生态的基础 |
| 目录属性存储与继承 | 属性的存储、继承解析、来源查询、变更通知 | 原则 3 的基础 |
| 递归用量计数 | 任意目录的递归字节数 / 文件数 / 按存储类分计数，常量时间可得 | 配额、dashboard、分层全部依赖它；插件自己算会慢一个数量级且不一致 |
| 后台任务框架 | 可限速、可断点续跑、可观测的任务调度；协调器是其第一个实例 | 分层 / scrub / 均衡 / GC / 过期清理都是它的实例 |
| 可观测性基座 | trace id 贯穿客户端→元数据→数据节点；计数器注册表；changelog 流；内建 fail points | 原则 6、7 |

---

## 4. 插件模型

### 4.1 钩子清单

请求管线（每个请求按此顺序经过方括号中的钩子）：

```
client → [transport] → decode(core) → [authn] → [authz] → [quota_admission] → [qos]
       → core exec（meta tx | data path）
       → [placement] → [transform: checksum → compress → encrypt] → [codec] → [chunk_store]
       → response → [event_sink]（异步）
background:    core scheduler → [bg_task]（reconciler / scrub / tiering / rebalance / gc / trash-expire / ttl）
control plane: [gateway] [mgmt_ext]（进程外，只能通过管理 API、客户端库、指标流、事件流接触系统）
```

| 钩子 | 触发点 | 输入 → 输出 | altitude | 热路径 | 首发内置插件 |
|---|---|---|---|---|---|
| `transport` | 连接、收发、注册内存 | 字节流 / 消息 | 唯一 | 是 | `tcp`、`rdma` |
| `authn` | 连接建立 | 凭证 → principal（uid、gid、tenant、job_id） | 100–199 | 是（连接时） | `shared-key` |
| `authz` | 每个请求 | (principal, op, inode, parent) → allow / deny | 200–299，串行，任一拒绝即拒绝 | 是（可缓存） | `posix-mode`、`posix-acl`、`path-caps` |
| `quota_admission` | 分配空间或创建对象前 | (principal, 目录树, delta, class) → allow / soft-warn / deny | 300–399 | 是 | `quota`（软 / 硬 / 宽限，按存储类） |
| `qos` | 请求进入执行队列 | 分类 → 入队 / 出队时机 | 400–499，只允许一个活跃 | 是 | `mclock`（r/w/l 三元组 + 令牌桶） |
| `placement` | 首次写、迁移 | (属性, 大小提示, class) → 目标列表 | 500–599 | 是（首写） | `label-match`（标签 + 故障域） |
| `codec` | 写入分片 / 读取重组 | 数据 ↔ 分片 | 600–699 | 是 | `replica`（链式复制）；二期 `ec` |
| `transform` | 写：checksum→compress→encrypt；读：反向 | 字节 ↔ 字节 | 700–799，子序固定 | 是 | `crc32c`、`xxh3`、`lz4`、`zstd`、`aes-gcm` |
| `chunk_store` | 最内层 | put / get / delete / list / sync | 唯一 | 是 | `local-fs`（XFS 上的 chunk 文件） |
| `meta_engine` | 元数据事务 | get / put / scan / txn | 唯一 | 是 | `embedded-kv`（+ Raft） |
| `cache_store` | 客户端缓存 | 存取 | 客户端侧 | 是 | `memory`、`local-nvme` |
| `bg_task` | 核调度器 | 遍历 + 动作，受核限速 | 不排序 | 否 | `reconciler`、`scrub`、`tiering`、`rebalance`、`gc`、`trash-expire`、`ttl` |
| `event_sink` | changelog 流 | 事件 → 副作用 | 不排序 | 否 | `audit-log`、`webhook` |
| `gateway` | 进程外 | 用客户端库暴露其他协议 | 进程外 | 否 | 二期：`nfs`、`s3`、`ublk` |
| `mgmt_ext` | 进程外 | 订阅指标 / 事件，调用管理 API | 进程外 | 否 | `prometheus-exporter`、`dashboard`、`alert` |

### 4.2 三档执行模型

| 档位 | 钩子 | 技术 | 失败隔离 |
|---|---|---|---|
| 进程内（热路径） | transport、authn、authz、quota_admission、qos、placement、codec、transform、chunk_store、meta_engine、cache_store | 编译期特性开关 + 动态库（稳定 C ABI；Rust 实现通过 C 壳导出） | 无。插件崩溃即服务崩溃，因此必须有严格审查与故障注入测试 |
| 进程外（控制面） | bg_task 的重型实现（可选）、event_sink、gateway、mgmt_ext | gRPC 管理 API + 事件总线 | 完全隔离，可独立升级与重启 |
| 沙箱 | 用户自定义策略：放置规则、分层条件、审计过滤、属性校验 | WASM（wasmtime / Extism），有燃料与时间上限 | 沙箱隔离 |

WASM 的正确位置是**策略求值点**（例如 create 时决定新文件放哪个存储类，结果缓存在目录属性上），**绝不进入每次读写的路径**。

### 4.3 插件硬规则

1. manifest 必填：`name`、`version`、`hooks`、`altitude`、`hot_path`、`config_schema`、`abi_version`。
2. 插件不得持有本体不知道的持久状态；需要计数或索引时向本体申请（例如配额使用本体的递归计数）。
3. 热路径插件不得阻塞、不得做大块内存分配、不得发起网络请求（transport 除外）；每次调用有时间预算，超预算记入指标。
4. 每个插件附带：单元测试、至少一个 fail point 用例、指标注册、一页文档。
5. 顺序由本体的 altitude 决定；配置只能启停插件。
6. ABI 版本不兼容时本体拒绝加载，不尝试适配。

---

## 5. 策略属性系统（"极度自定义"的唯一形态）

### 5.1 语法与语义

```
fs set   <path> key=value[,key=value...]   # 设置，默认作用于整个子树（继承）
fs get   <path> [-r]                        # 显示生效值及其来源路径
fs unset <path> key                         # 删除本级设置，恢复继承
fs status <path>                            # 声明 vs 实际（例如 replicas 声明 3、实际仍为 2 的文件数）
```

- 解析规则：从根到目标路径逐级覆盖；`inherit=false` 阻断继承。
- 所有属性变更写入 changelog。
- 只影响新数据的属性（如 `chunk`、`stripe`）立即生效；影响存量数据的属性（如 `replicas`、`class`、`compress`、`encrypt`）由协调器异步迁移，进度可查。
- 属性是"声明"，系统永远能回答"声明是什么、实际是什么、差距在哪"。

### 5.2 首发属性清单

| 属性 | 取值 | 作用 | 生效方式 | 执行者 |
|---|---|---|---|---|
| `class` | 存储类名 | 设备、冗余、QoS、生命周期（第 6 章） | 新数据 + 协调器迁移 | placement / qos / bg_task |
| `replicas` | 1..N | 副本数 | 新数据 + 协调器 | codec |
| `ec` | k+m | 纠删码（二期） | 新数据 + 协调器 | codec |
| `chunk` | 大小 | chunk 大小 | 新文件 | core |
| `stripe` | N | 条带目标数 | 新文件 | placement |
| `inline` | 阈值 | 小文件内联到元数据 | 新文件 | core |
| `compress` | none / lz4 / zstd:N | 变换链 | 新数据 + 协调器 | transform |
| `checksum` | crc32c / xxh3 / blake3 | 变换链 | 新数据 | transform |
| `encrypt` | none / tenant:<key-id> | 变换链 | 新数据 + 协调器 | transform |
| `quota` | bytes[,soft=][,grace=][,files=] | 容量 / 文件数配额 | 立即 | quota_admission |
| `quota.<class>` | 同上 | 按存储类配额 | 立即 | quota_admission |
| `qos` | 类名，或 r=,w=,l= | mClock 三元组 | 立即 | qos |
| `consistency` | cto / strong | 一致性等级 | 新打开的文件 | core |
| `trash` | 保留期 | 回收站 | 立即 | core + bg_task |
| `tier` | `<from>→<to>:<条件>` | 分层 | 协调器 | bg_task |
| `ttl` | 期限 | 到期删除 | 协调器 | bg_task |
| `snapshot` | 调度表达式 | 定时快照（二期） | 立即 | bg_task |
| `acl.default` | ACL 文本 | 新文件默认 ACL | 新文件 | authz |
| `audit` | on / off / 过滤表达式 | 审计 | 立即 | event_sink |
| `locality` | prefer-client / any | 放置偏好 | 新数据 | placement |

新增任何可配置行为时，先问："它能不能是一个目录属性？"答案是能，就必须是。

### 5.3 协调器

- 是本体后台任务框架的第一个实例，负责"声明 → 实际"的收敛。
- 按目录树遍历，比较每个文件的实际布局与生效属性，生成迁移动作（补副本、改编码、改存储类、重新压缩 / 加密）。
- 受核限速（带宽、IOPS、并发），可断点续跑，进度与积压量是一等指标。
- 参考：MooseFS 改 goal 后自动补齐副本；Lustre `lfs migrate`；Btrfs `balance -dconvert`。

---

## 6. 存储类（vSSD / vHDD）

### 6.1 定义

存储类 = 设备标签 + 冗余 + QoS 包络 + 放置约束 + 生命周期。目录通过 `class` 属性绑定存储类，配额限容量，QoS 限性能。**这就是我们对"虚拟盘"的实现**：vSSD 和 vHDD 都是存储类，而不是块设备。

```yaml
storage_class:
  name: vssd-gold
  devices: {label: nvme, dedicated: true}      # 独占整盘或整 namespace：小集群最可靠的隔离手段
  redundancy: {replica: 2}
  qos: {iops_min: 20000, iops_max: 100000, bw_min: 1GiB/s, bw_max: 4GiB/s, burst: 30s, latency_target: 500us}
  placement: {failure_domain: node, locality: prefer-client}
  lifecycle: {tier_to: vhdd-bulk, when: idle>30d}

storage_class:
  name: vhdd-bulk
  devices: {label: hdd}
  redundancy: {ec: 8+3}          # 二期；首发用 replica
  qos: {bw_max: 500MiB/s, priority: low}
```

### 6.2 四层执行

1. **入口**：服务端每（租户, 存储类）一个 mClock 三元组（reservation / weight / limit）+ 令牌桶；作业 id 参与分类。参考 Lustre NRS TBF。
2. **设备出口**：每存储类一个 cgroup（`io.weight` / `io.max`）；同一物理盘上的两个类用 NVMe WRR 队列优先级区分。
3. **硬件**：盘支持 NVM Sets 就按类分 set；支持 FDP 就给每个类不同的放置句柄；都不支持就整盘独占。
4. **协调器**：类的变更触发后台迁移；配额与 QoS 违约进 dashboard。

### 6.3 诚实承诺 [固定]

- vSSD 的默认隔离手段是独占设备或 namespace；共享设备时只承诺份额，不承诺尾延迟。
- vHDD 只承诺容量、带宽份额与低优先级，**不承诺延迟**；HDD 类只承担顺序大块与冷数据，随机 IO 落 NVMe 类。
- 真的需要块设备（虚拟机、数据库）时，用进程外 `ublk` 网关把一个文件暴露成块设备，QoS 与冗余继承文件所在目录的存储类。块设备不是首发目标。

---

## 7. 默认决策 [默认]

| 决定 | 默认值 | 备选（需 ADR） |
|---|---|---|
| 一致性 | `cto` 默认；目录属性 `consistency=strong` 时走租约 / 锁 | 全局强一致 |
| 数据复制 | 固定大小 chunk + 链式复制（CRAQ） | 每 chunk 组 Raft |
| chunk 大小 | 4 MiB；`inline` 阈值 64 KiB | 按负载调整 |
| 元数据 | 内嵌 KV + Raft（3 副本），引擎走 `MetaEngine` trait | TiKV / FoundationDB 插件 |
| 线上协议 | 自定义二进制帧 + 能力协商 + 版本号；同一协议跑 TCP 与 RDMA | gRPC 仅用于控制面 |
| 缓冲区 | 预注册内存池；请求生命周期内零拷贝；所有权显式转移 | — |
| 磁盘格式 | chunk 文件带头部（版本、校验、变换标记、inode）；feature flags | — |
| 信任模型 | 服务端强制授权，客户端不可信 | 信任客户端（HPC 传统；只在单团队内部可接受） |
| 客户端 | FUSE（passthrough + io_uring）首发；保留内核客户端与 LD_PRELOAD 拦截库的接口可能 | — |
| 默认冗余 | `/home` 类 replicas=3，`/scratch` 类 replicas=2 | — |
| 故障域 | 节点 | 盘 / 机架 |
| 语言 | Rust（`tokio`、`io-uring`、`openraft`、`fuser`、`rdma-core` 绑定） | C++ |
| 管理 API | gRPC + 只读 REST（供 dashboard） | — |
| 指标 | Prometheus 文本格式，内置 `/metrics` | — |
| 部署 | 单二进制；`fs init` 一条命令起单节点；`fs join` 加入 | — |

---

## 8. 非目标与放弃的语义

### 8.1 首发不做

- 内核客户端；跨地域复制；磁带 / HSM；内建 NFS / SMB / S3 网关（二期插件）；纠删码（二期）；快照（二期，但 chunk 版本化从第一天设计）；WASM 沙箱（二期，但策略求值点第一天预留）。
- 不自研共识算法：使用成熟 Raft 库。
- 不在对象层猜测热度做透明分层（Ceph cache tiering 的教训）；分层只基于目录属性与文件访问元数据。

### 8.2 POSIX 放松清单（必须写进用户文档）

- `atime` 按 relatime 语义更新。
- 跨节点共享可写 `mmap` 仅在 `consistency=strong` 下支持。
- 不支持强制锁；字节范围锁与 flock 支持。
- 跨节点 `O_APPEND` 的顺序仅在 `consistency=strong` 下保证。
- `cto` 模式下，另一节点的写入在本节点下一次 open 后可见。
- 配额在客户端缓存写回前可能短暂超限，超限幅度有上界并可配置。

---

## 9. 质量与验收

- **语义**：pjdfstest、xfstests（FUSE 子集）、LTP；通过率是版本发布门槛。
- **性能**：fio、IOR / mdtest、IO500（hard 模式专门暴露锁与共享目录问题）、elbencho；基线对比同硬件上的 BeeGFS，RDMA 路径不低于其 80%。
- **正确性**：Jepsen 风格的分区 / 时钟 / 崩溃测试；内建 fail points 覆盖每个钩子；dm-flakey 模拟坏盘。
- **插件组合**：只承诺有限几个 profile 通过全套测试——`minimal`（单节点、TCP、无配额）、`hpc-rdma`（3 节点、RDMA、配额、QoS）、`nas-safe`（3 节点、replicas=3、trash、audit）。其他组合尽力而为，文档明示。
- **成功标准**（首发）：3 节点集群任意单节点宕机，客户端除超时重试外无错误；新增一个传输插件不改本体任何一行；所有策略旋钮都能用 `fs set` 配置并在 dashboard 上看到声明与实际。

---

## 10. Agent 工作约定

### 10.1 加功能前的检查清单（按顺序回答）

1. 这是契约吗（第 3 章）？是 → 进本体，先写 ADR，等人类批准。
2. 不是契约 → 它能表达为目录属性吗？能 → 必须做成属性（5.2）+ 对应钩子的插件。
3. 它在热路径上吗？是 → 进程内插件，ABI 只传必要字段，给调用预算。否 → 进程外。
4. 它需要本体没有的状态吗？是 → 向本体加计数 / 索引（这是契约变更，回到第 1 步），而不是在插件里自建。
5. 它是用户自定义逻辑吗？是 → WASM 策略点，结果缓存，不进读写路径。
6. 它会改变线上协议或磁盘格式吗？是 → feature flag + 版本协商 + ADR。

### 10.2 ADR 规则

- 路径 `docs/adr/NNNN-标题.md`，包含：背景、决定、备选方案、后果、是否触及契约层。
- 契约层相关 ADR 状态为 `proposed` 时不得实现。
- 默认决策（第 7 章）的变更也需要 ADR，但 agent 可自行起草并标注"待确认"。

### 10.3 仓库约定

```
core/        本体（契约层 + 钩子管线 + 属性系统 + 后台任务框架 + 可观测基座）
plugins/<name>/   每个插件一个目录：manifest、实现、测试、docs.md
gateways/    进程外网关
cli/         fs 命令行
dashboard/   进程外 dashboard（只消费管理 API 与指标流）
docs/adr/    架构决策记录
docs/semantics.md   一致性模型与 POSIX 放松清单（对外承诺）
tests/profiles/     三个 profile 的集成测试
```

### 10.4 完成的定义

一个功能"完成"当且仅当：代码 + 测试 + fail point + 指标 + 文档 + （若有旋钮）目录属性 + （若触及契约）ADR 全部齐备，且三个 profile 的测试通过。

---

## 11. 待定事项 [待定]

| 问题 | 当前假设 | 影响 |
|---|---|---|
| 目标负载 | （假设：HPC 混合负载——MPI checkpoint + AI 训练小文件随机读 + 家目录） | chunk 大小、默认一致性、RDMA 优先级 |
| 客户端是否可信 | （假设：不可信，服务端强制授权） | authz 的位置、是否需要 Kerberos |
| 是否需要块设备 | （假设：首发不需要） | ublk 网关优先级 |
| 硬件 | （假设：每节点 2–4 块 NVMe + 可选 HDD，100GbE RoCE 或 InfiniBand） | 存储类划分、是否启用 NVM Sets / FDP |
| 最大规模上限 | （假设：64 节点、数千客户端） | 元数据分片是否首发 |

---

## 12. 参考实现（遇到对应问题先看它们怎么做）

| 问题 | 参考 |
|---|---|
| 链式复制与 chunk 布局 | DeepSeek 3FS（CRAQ、chain table） |
| 元数据引擎抽象 | JuiceFS（多引擎）、3FS（FoundationDB）、Meta Tectonic |
| 传输插件化 | Lustre LNet 的 LND 驱动（socklnd / o2iblnd / kfilnd） |
| 插件顺序与过滤模型 | Windows Filter Manager（minifilter altitude） |
| 目录策略与协调器 | MooseFS 存储类 / goal、ZFS 属性继承、bcachefs 每 inode 选项、CephFS 目录 xattr |
| QoS | mClock（OSDI'10）、Lustre NRS TBF、Ceph RBD QoS、cgroup v2 io 控制器 |
| 配额 | XFS / Lustre 的软硬限与宽限期；CephFS rstats 的递归计数 |
| 控制面插件与 dashboard | Ceph mgr 模块（dashboard / prometheus / balancer） |
| 磁盘格式演进 | ZFS feature flags |
| 作业级统计 | Lustre jobstats |
| 变换链叠加 | SPDK bdev、Linux device-mapper |
| 性能隔离的边界 | FlashBlox（FAST'17）、IODA（SOSP'21）、Argon（FAST'07）、NVMe NVM Sets / FDP / WRR |

---

## 13. 术语表

- **本体 / 核（core）**：契约层 + 钩子管线 + 属性系统 + 后台任务框架 + 可观测基座。
- **钩子（hook）**：本体定义的插件介入点，有固定的输入输出与 altitude 区间。
- **altitude**：插件在同一钩子上的固定排序值，由本体分配区间。
- **属性（attribute）**：挂在目录上、子树继承、可覆盖的声明式策略。
- **协调器（reconciler）**：把存量数据迁移到属性声明状态的后台任务。
- **存储类（storage class）**：设备标签 + 冗余 + QoS + 放置 + 生命周期的命名组合；vSSD / vHDD 的实现形态。
- **mClock 三元组**：reservation（保底）、weight（份额）、limit（上限）。
- **cto**：close-to-open 一致性；**strong**：租约 / 锁保证的强一致。
- **profile**：承诺通过全套测试的插件组合。
- **ADR**：架构决策记录。
