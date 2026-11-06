# scrub 巡检插件

**钩子**：bg_task（在**每个数据节点**上运行，一轮做完隔 24 h 再来）　**热路径**：否　**默认限速**：50 MiB/s
**fail point**：`plugin.scrub.bg_task`（本体包在每一步外层，触发 = 这一步失败）
**指标**：`wmfs_scrub_chunks_total{result="good"|"repaired"|"lost"|"later"}`、`wmfs_chunk_corrupt_total{target}`，以及框架的
`wmfs_task_*{task="scrub"}`

## 它做什么

磁盘上的数据会悄悄坏掉（位翻转、写了一半、掉电）。scrub 定期把本节点每个副本的每一块都读一遍、解码、核对校验和（design.md 4.12）：

- 坏了 → 从同一条链上另一个正常服务的副本取一份**好的**（对方此刻没有在写这个 chunk、取回来的每一块都解得开）装回来，changelog 里
  记一条 `ChecksumRepaired`；
- 链上只有这一个副本、或者别的副本也都坏了 → 如实报 `DataLossDetected`（changelog），之后读它返回 `EIO`——**不假装没事**；
- 本节点是链上的 Serving 成员、又不是最后一个时，再和链上最后一个正常副本对一遍清单：它有、我没有（或我的旧）的补上
  （"Serving 成员缺基础版本"的检查）。

查出不对时先再看一眼它的版本：查的这段时间里这个 chunk 正好提交了新版本（例如被整块覆盖写了），就不是它坏了——跳过，下一轮再查
（不然新写的数据也会被拒绝读，单副本时还会报一条假的 `DataLossDetected`）。

修好之后，卡在这个坏块上的重同步自然就做完了（ADR-0040 第 10 条）。读的时候发现的坏块（客户端换副本读）与写的时候发现的（要保留旧数据的
写、或 chunk 的头读不出）都由数据节点当场交给后台线程用同一个函数修（ADR-0042 第 7 条），不等 scrub 的下一轮；写稍后重试成功。修好或修不了
照样记 `ChecksumRepaired` / `DataLossDetected`。

每条链的 chunk 清单一轮只列一次（留在内存里，重启之后按进度重列一次），一步至少查一个，之后到了大约一秒的限速量或 1 s 就停。

## 配置

没有部署参数。`wmfs task run scrub` 让每个数据节点马上从头开始一轮（做到一半的也重来），`wmfs dev wait task-idle scrub` 等它们都做完；
`wmfs task set scrub bw=…` 改限速（`bw=0` = 回到默认的 50 MiB/s），`wmfs task pause|resume scrub` 暂停与恢复。进度在每个节点本地（`<data_dir>/run/task-scrub.cursor`），`wmfs task list` 逐个节点显示。

## 诚实说明

- 两次巡检之间坏掉的块，要到下一轮（默认 24 h 之后）或下一次读它时才被发现（读到的那一刻就开始修）；读到坏块的客户端会换副本读，
  所以多副本时应用感觉不到。
- 要保留旧数据的写（部分覆盖）碰上坏块时，链上有别的正常副本就先修好再写（客户端只多重试几次）；没有（单副本、或别的副本也坏了）才
  返回 `EIO`——整块覆盖写照常可用（chunk 的头也读不出、链上又还有别的副本时除外：不知道旧版本号，返回 `EIO`，ADR-0042 第 6 条）。
- 只做数据的节点（不在元数据 raft 里）看不到 `wmfs task pause|set|run` 之类的控制，一直按默认值运行；`wmfs dev wait task-idle scrub` 对它们
  只等这一轮做完。
