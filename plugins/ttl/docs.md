# ttl 过期删除插件

**钩子**：bg_task（在元数据 leader 上运行，每轮结束隔 60 s 再开始下一轮）　**热路径**：否
**fail point**：`plugin.ttl.bg_task`（本体包在每一步外层，触发 = 这一步失败，稍后从同一个游标重试）
**指标**：`wmfs_ttl_deleted_total`，以及框架的 `wmfs_task_*{task="ttl"}`

## 它做什么

目录属性 `ttl=<时长>`（例如 `wmfs set /scratch/tmp ttl=30d`）让这棵子树里"最后修改时间（mtime）早于现在减去时长"的文件被自动删除。
每轮先找出显式设了 `ttl` 的目录，逐个遍历它们的子树，把过期的文件删掉——与 `rm` 一样：那里的 `trash` 生效就进回收站（之后由
trash-expire 到期真删），还有人打开着就等他们关闭再回收数据。changelog 里是一条 `Unlink`，`extra` 带 `reason=ttl`。

- 子目录可以用 `ttl=off`（或别的时长）覆盖上级的设置；目录本身不会被删。
- 回收站里的文件不归 ttl 管（归 trash 的保留期）。

## 配置

没有部署参数。`wmfs task run ttl` 马上来一轮（不等 60 s），`wmfs task list` 看进度，`wmfs status <目录>` 的 ttl 一行显示"已经过期、
还没被删掉"的文件数。`ttl` 属性要有这个插件才能设置。`wmfs dev wait task-idle ttl` 要等开始等之后再做完两轮（leader 任务的规则：
第一轮可能在你的修改之前就开始了），也就是 60–120 s；脚本里只想等某个文件被删，`wmfs task run ttl` 之后按条件等它消失更快。

## 诚实说明

- 删除不是精确到秒：最多晚一轮（60 s）加上遍历的时间；很大的子树遍历一遍要更久。
- 判断用的是 mtime（leader 的时钟写的，放松 ⑫）；`touch` 一下就会推迟删除。
