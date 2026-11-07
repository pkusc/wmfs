# mclock 调度插件

**钩子**：qos（altitude 410；qos 只允许一个活跃，与 fifo 二选一）　**热路径**：是，预算 2 µs/次
**fail point**：`plugin.mclock.qos`（本体包在入队外层，触发 = 这个请求以注入的错误码失败）
**指标**：`wmfs_qos_served_total{tenant,qos_id,phase}`、`wmfs_qos_queue_depth{tenant,class}`、`wmfs_qos_wait_seconds{tenant,class}`、
`wmfs_qos_reservation_deficit{tenant,qos_id}`、`wmfs_job_io_bytes_total{job}`

## 它做什么

数据节点收到的读写请求先进 QoS 队列，再由 io 线程按插件给的顺序执行（design.md 4.5、6.2 第 1 层）。mclock 给每个队列三个数：

- **r 保底**：至少给你这么多（不论优先级）；
- **w 权重**：有空余时按比例分；
- **l 上限**：最多给你这么多（磁盘不忙时也看得到效果）。

单位是"成本单位/秒"：一次 IO 的成本 = 1 + 字节数 / 128 KiB（4 KiB = 1，1 MiB = 9，4 MiB = 33）。另有每队列一个**字节令牌桶**（`bw`，
桶深 = bw × burst，没写 burst 按 1 s）：新建的队列桶是满的，所以空闲很久之后来的一批请求照样能用上 burst。

- **队列** = (租户, qos 属性的来源目录, 存储类)；同一个队列里不同作业（Slurm 作业 id 等）的请求轮流来。
- **低优先级**（存储类 `priority = low`，label 为 hdd 的类默认如此）：按权重分的时候，普通队列都没有能派发的请求才轮到它；保底不受影响。
- **后台请求**（协调器迁移、scrub、重同步取数据）进一个固定的低优先级队列，字节速率受节点级后台限速（`wmfs cluster set bg_bytes_per_sec=…`）。

规格（三个数）由本体从集群图算好交给插件：目录的 `qos` 属性（集群图里的 QOS 表）或存储类的 `[qos]`，**按这个类现在有 Up target 的节点数
平均分到每个节点**（例如 iops_max = 300、3 个节点 → 每节点 100）。没有跨节点协调（dmClock）：负载不均时总保底可能达不到、总上限偏保守。

插件只决定"下一个做谁"，不睡觉也不阻塞（睡眠与唤醒归本体）。每个节点的保底欠额随心跳报给 leader，`wmfs status` 的 qos 一行显示它：
每 10 s 一个统计窗口，窗口里大半时间都有请求在排队、派发的又少于保底时，欠额 = 保底 − 实际派发的速率（刚有请求的队列、一次只有一个请求在途
的单流客户端不会误报）。欠额连续 10 s 不退，leader 写一条 changelog 事件 `PolicyViolation`（`kind=qos_reservation_unmet`）。

## 配置

没有部署参数。份额是目录属性 `qos=r=<n>,w=<n>,l=<n>[,bw=<字节/秒>][,burst=<时长>]`（或 `qos=<存储类名>`：借用那个类的包络，改了那个类的
`[qos]` 约 1 s 内跟着变，ADR-0044 第 2 条）与存储类的 `[qos]`（`wmfs class create|update`）。停用 mclock（minimal profile）时用 fifo，`qos`
属性没有插件消费、设置会被拒绝。

## 诚实说明

- 共享物理盘时只承诺份额，不承诺尾延迟（6.3）；`latency_target` 只测量与告警、不承诺：本体（不在这个插件里）按存储类统计客户端请求的
  p99 延迟，某个节点上连续 10 s 超过目标就写一条 `PolicyViolation`（`kind=latency_target_exceeded`，ADR-0044 第 5 条）。
- 每个队列最多排 512 个请求（整个节点最多 4096 个；本体记账，fifo 也一样）：上限 `l` 让一个租户的请求积压起来时，排满的只是它自己的
  队列（它的新请求拿到可重试的 `BUSY`），别的租户照常排队（ADR-0044 第 6 条）。
- 一个刚变活跃的队列从"同一优先级里最落后的那个队列"的进度起算，不会抢走一直在忙的队列的全部份额，也不会被它饿死。
- 元数据请求不经 QoS（ADR-0010）。
- 一个文件的写只经过它的链头节点：单流写入的上限是"每节点份额"，不是集群总份额。
