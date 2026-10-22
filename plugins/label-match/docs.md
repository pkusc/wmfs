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

没有部署参数：存储类（`class`）、副本数（`replicas`）、条带数（`stripe`）、`locality` 都是目录属性。

## 诚实说明

- 基础版（M4）：`locality = prefer-client`（优先放在客户端所在节点）与 dedicated 存储类的放置细节在 M13；"一个 dedicated 类独占
  它的 target"已经由本体的链生成保证。
- 使用率来自 target 心跳（M5 起）；在那之前所有链的使用率都按 0 算，排序只看 Serving 成员数与散列。
