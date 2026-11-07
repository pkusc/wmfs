# reconciler 协调器插件

**钩子**：bg_task（在元数据 leader 上运行，每轮结束隔 1 s 再开始下一轮）　**热路径**：否
**fail point**：`plugin.reconciler.bg_task`（本体包在每一步外层，触发 = 这一步失败，1 s 后从同一个游标重试）
**指标**：`wmfs_reconciler_files_migrated_total`、`wmfs_reconciler_rstat_fixed_total`，以及框架的 `wmfs_task_*{task="reconciler"}`、
迁移原语的 `wmfs_migrations_total{result}`、`wmfs_migration_skipped_open_files`

## 它做什么

你用 `wmfs set` 写下的是"声明"（例如这个目录要压缩、要加密、要 2 副本、要放在某个存储类）；协调器不断比较每个已有文件的"实际"
（它现在的存储类与布局），不一致就把它迁到新布局（design.md 3.10 的迁移原语）：先登记一个新的 epoch，按新的布局把数据读出来、重新写一遍
（新的链、新的压缩与加密），落盘之后用一次原子的切换换过去，旧布局的数据交给 gc 删除。迁移过程中有人写这个文件——切换会失败，这次的
新数据作废，稍后重做；写的人随后会把自己的数据重写到新布局，**数据不会丢**。

- 管的四个属性：`class`、`replicas`、`compress`、`encrypt`（chunk、stripe、inline、checksum 只对新文件生效，不迁移）。
- "声明"= 文件所在目录的生效属性**再套用分层规则**（`tier`，或存储类的 `[lifecycle]`；M13）：与 tiering 任务用同一个函数，所以分层过去的
  文件不会被协调器迁回来。
- 触发：`wmfs set`/`unset` 这几个属性、目录 rename 时写一条队列项（RECONCILE_Q），协调器走一遍那个目录的子树；`wmfs class update` 改了
  副本数或分层规则时写一条根目录的队列项（M13），从根整树走一遍；另外每 24 h 扫一遍全部
  inode，漏网的也补上，并顺带检查每个目录的递归用量计数（不一致就修正，changelog 里记一条 `RStatDrift`；正常情况下永远不会发生）。
- 一直写打开着的文件先跳过（`wmfs status` 单独列出个数），关了之后再转换；被跳过超过 24 小时会产生一条 `PolicyViolation` 事件
  （`kind=migration_blocked_by_open`）。有文件没转换成的队列项过一会儿再重走：10 s 起，每次没成就翻倍，最多 10 min。
- 一步只做一点（≤ 100 个文件 / 大约一秒的限速量 / ≤ 1 s），做到哪记在游标里：元数据 leader 换了，新 leader 从游标接着做。

## 配置

没有部署参数。限速用 `wmfs task set reconciler bw=2M`（字节/秒，`bw=0` = 只受节点级上限；`items=` 限每秒的文件数），`wmfs task pause|resume reconciler` 暂停与恢复，
`wmfs task list` 看进度、速率与积压（积压 = 还没走完的队列项数），`wmfs status <目录>` 看每个属性还有多少文件与声明不同。

## 诚实说明

- 转换完成之前，往已有文件里新写的数据仍按旧设置存放（例如刚开加密时新写的仍是明文），见 docs/semantics.md 第 4 节。
- 一个很大的文件在一步里整个迁完（不在文件中间分步），这一步会超过一秒；限速按这一步搬的字节算，之后多睡一会儿补上。
- 全量扫描从本任期就绪起每 24 h 一次：元数据 leader 换得很勤时，全量扫描（与递归计数的自检）可能一直推后——队列项的处理不受影响。
- 一个文件的数据丢了（scrub 报了 `DataLossDetected`：一个好副本也没有）时它迁不了，读它的那一步失败：它所在的队列项一直留着、计入积压
  （`wmfs dev wait task-idle reconciler` 等不到），直到这个文件被覆盖写、截断到 0 或删掉。
