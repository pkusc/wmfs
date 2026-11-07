# tiering 后台任务

**钩子**：bg_task（只在元数据 leader 上跑，每轮结束隔 24 h；`wmfs task run tiering` 马上来一轮）
**fail point**：`plugin.tiering.bg_task`（本体包在每一步外层）　**指标**：`wmfs_tiering_files_moved_total`（另有框架的 `wmfs_task_*`）

## 它做什么

按"分层规则"把冷数据挪到便宜的存储类（design.md 4.12、第 5 章）。规则写成目录属性：

```
wmfs set /data/proj tier=gold->silver:idle>30d          # gold 类里 30 天没读写过的文件挪到 silver
wmfs set /data/logs tier=gold->silver:age>7d&size>100M  # 建了 7 天以上、而且大于 100M 的
```

条件：`idle>时长`（最后一次读写——atime、mtime 里较晚的那个——早于现在减时长）、`age>时长`（按创建时间）、`size>尺寸`，用 `&` 连接表示"都成立"。
时长可以写 `30d`，也可以写 `2s` 这样的短时长（演示与测试用）。存储类定义里的 `[lifecycle] tier_to = "…"`、`when = "…"` 等于在绑定这个类的
目录上隐式设了同样的规则。

一轮：先找出有规则的目录（"规则根"），逐个走它们的子树；"该迁"的文件（目录的类是规则的 from、条件此刻成立、它还不在 to 里）用与协调器
相同的迁移原语换到 to 类的新布局（新 epoch、按 to 类选链；内容不变）。

## 与协调器的关系

协调器（reconciler）与这里用同一个"声明"：目录的生效属性再套用分层规则。所以：

- 已经分层过去的文件不会被协调器迁回来；
- 已经在 to 类里的文件之后再被读写也留在那里——系统不按热度把数据挪来挪去（理念文档 8.1），想挪回去请改目录的属性。

`wmfs status <目录>` 的 tier 一行显示"条件已经成立、还没迁过去"的文件数。

## 诚实说明

- 一直写打开的文件会被跳过（与协调器相同，`wmfs status` 单独计数）；这一轮里迁不成的文件这一轮不再重试，下一轮（24 h 后，或
  `wmfs task run tiering`）再试。
- 每一步最多迁 100 个文件、约一秒的限速量或 1 s（`wmfs task set tiering bw=…` 限速、`wmfs task pause tiering` 暂停）；规则根的子树很大、
  条件还没成立时，一轮要走很多步，但每一步都很短，不会挡住别的后台任务。
- 嵌套的规则根会被走两遍（每一项幂等，只多花一点时间）。
