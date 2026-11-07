# label-match 放置插件

**钩子**：placement（altitude 510）　**热路径**：是（每次 create、截断到 0），预算 50 µs/次
**fail point**：`plugin.label-match.placement`（触发 = 这次放置失败，create 得到注入的错误码）　**指标**：`wmfs_label_match_ranked_total`

## 它做什么

新建文件（以及截断到 0 的文件）时，决定文件的数据放在哪几条**链**上（design.md 4.6、3.4.2.1）。一条链 = 几台机器上的几个副本；
链本身由本体生成（`gen_chains`：按存储类的标签挑 target，同一条链里的成员在不同节点上，节点不够时链短一些、并给出集群警告）。
本插件只做两件事：

1. **过滤**：只留下存储类与副本数都和文件的生效属性一致、至少有一个正常（Serving）成员、不在排除列表里的链；
2. **排序**：Serving 成员多的优先 → 使用率低的优先 → `hash(seed, 链号)`（seed = 新文件的 inode 号）。

本体再取前 k = min(stripe, 互不相同的链数) 条写进文件的 layout；一条都没有就拒绝创建（`ENOSPC`，不会静默放到别的设备上）。
同一个 seed 永远得到同一个结果（确定性），不同的文件被均匀地分散到各条链上。

## 配置

没有部署参数：存储类（`class`）、副本数（`replicas`）、条带数（`stripe`）、`locality` 都是目录属性。M7 起 `class`、`replicas`、`stripe` 可以
`wmfs set`（它们由这个插件消费：停用它之后设置这几个属性会被拒绝，"没有已启用的插件消费"）；可用的链不够 `stripe` 条、故障域不够
`replicas` 个时设置照样成功、给出警告（ADR-0016）；`locality` M13 起可以设置。

## locality、dedicated 与 rebalance（M13）

- **`locality = prefer-client`**（目录属性，或存储类的 `[placement] locality`）：新文件优先放在 head（链上第一个副本，写入口）在**客户端
  所在节点**上的链——客户端所在节点由会话报的 host_id 与节点的 host_id 对出来（开发集群里 `wmfs dev mount --node n` 冒充第 n 个节点所在的
  "主机"）。排序键变成：head 在客户端节点上的优先 → 有一个成员在客户端节点上的优先 → Serving 成员多的 → 使用率低的 → 散列。客户端不在
  任何节点上、或 `locality = any` 时与以前一样。读的时候客户端优先同一台主机上的副本（DataClient 按 codec 的 `read_order`，对所有文件
  都这样，不看 `locality`）。
- **dedicated 存储类**：它的链只建在它独占的 target 上（本体的链生成）；成员后来被别的 dedicated 类占走的链本体不交给本插件；一条可用的都没有
  就拒绝创建（`ENOSPC`），不会静默放到别的设备上。
- **exclude**：rebalance 迁移文件时把最满的链放进排除列表，新布局不会再用它。

## 诚实说明

- 使用率 = 链上最满的那个成员的 已用 / 容量，来自 leader 内存里的 target 心跳（每秒一次）；刚换 leader 时还没听到心跳的按 0 算。
- `prefer-client` 只是"优先"：插件只能在已有的链里挑。重启过的成员回到链里时排在末尾（design.md 3.4.2.7），首发不会把链头调回均匀：
  例如 3 个节点、3 副本的集群轮流重启一遍之后，所有链的 head 都在同一个节点上。这时别的节点上的客户端找不到以本机为 head 的链，就退而
  选有一个成员在本机上的链（读走本机副本；写入口仍在别的节点，写也就只能用到这个类在那个节点上的份额），之后按普通规则排
  （ADR-0044 第 8 条；自动均衡链头留作二期）。
