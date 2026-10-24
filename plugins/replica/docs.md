# replica 副本插件

**钩子**：codec（altitude 610，编号 1）　**热路径**：是，预算 2 µs/次
**fail point**：`plugin.replica.codec`（触发 = 这次调用以注入的错误码失败）　**指标**：`wmfs_replica_read_orders_total`

## 它做什么

决定"数据怎么分到链上的各个成员、读的时候先问谁"（design.md 4.7）。首发只有多副本：链上**每个成员存一份完整的数据**
（`shards` 返回 n 个"整块"）；读的时候在正常服务（Serving）的副本里，同一台机器上的优先，其余按 chunk 的编号轮转，把读压力分散到
各个副本。数据怎么沿链复制、怎么确认，是本体的链式复制协议（契约，design.md 3.4.2），不归插件管。二期的纠删码（`ec`）会是另一个
codec 插件。

## 配置

没有部署参数：副本数是目录属性 `replicas`（M7 起可以 `wmfs set <目录> replicas=N`，1–8；这个插件与 label-match 都声明消费它）。

## 诚实说明

M5 是单节点（链长 1），只用到"本成员存整块"这一条。M9 起有多副本，但客户端还没有按 `read_order` 选副本（读按"同节点优先、其余按链序"选第一个 Serving 副本，连不上就换下一个）：读负载没有按 chunk 分散，留到 M13 与放置、locality 一起接入。
