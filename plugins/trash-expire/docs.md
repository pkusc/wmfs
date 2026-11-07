# trash-expire 回收站过期插件

**钩子**：bg_task（在元数据 leader 上运行，每轮结束隔 10 s 再开始下一轮）　**热路径**：否
**fail point**：`plugin.trash-expire.bg_task`（本体包在每一步外层，触发 = 这一步失败，稍后重试）
**指标**：`wmfs_trash_expired_total`，以及框架的 `wmfs_task_*{task="trash-expire"}`

## 它做什么

目录属性 `trash=<保留期>`（例如 `wmfs set /home trash=7d`）让 `rm` 变成"移进回收站"：被删的文件移到设置了 `trash` 的那个目录下的
`.wmfs-trash/<uid>/`，`<uid>` 是**文件的属主**（不是删它的人）；这个子目录是 0700、属于他，`.wmfs-trash` 本身是 0711——只有属主与 root
看得到、恢复得了自己的文件（同 Linux 桌面的 `.Trash-<uid>`）。名字是 `<unix秒>-<ino>-<原名>`，xattr `user.wmfs.origin` 记着原来的路径。
这一步由本体在 unlink 里直接做（不经插件）；这个插件负责**到期之后真正删掉**：每轮查一遍回收站的到期索引（与目录结构无关，各用户的子目录
里的都在），把到期的文件删掉（与普通的 rm 一样：还有人打开着就等他们关闭，之后由 gc 回收数据），changelog 里记一条 `TrashExpire`。空了的
用户子目录留着。

- 恢复：用普通的 `mv` 把文件移出回收站即可（例如 `mv /home/.wmfs-trash/$(id -u)/<名字> /home/doc`；到期时间随之清掉）。
- 回收站里再 `rm` = 真删。目录的 `rmdir` 不进回收站；删除目录树时文件平铺进各自属主的子目录，目录结构不保留（semantics.md 3.3
  第 16 条）。
- 回收站里的文件仍计入设置 `trash` 的那棵子树的配额。

## 配置

没有部署参数。保留期由目录属性 `trash` 决定（`off` 关闭，最长 365d）；`wmfs task run trash-expire` 马上来一轮，`wmfs task list` 的积压
只表示还有没有已经到期、还没删的（0 或 1，不数真正的个数）。`trash` 属性要有这个插件才能设置（属性注册表的"必须有插件消费"）。`wmfs dev wait task-idle trash-expire`
要等开始等之后再做完两轮（leader 任务的规则），也就是 10–20 s。

## 诚实说明

- 还有别的硬链接的文件，`rm` 掉其中一个名字时不进回收站（数据还在，用别的名字访问）。
- 对设了 `trash` 的目录**本身** `rm -r` 会失败一次：rm 先删掉了 `.wmfs-trash`，之后删的文件又把它建了回来，最后的 rmdir 报
  "Directory not empty"。先 `wmfs unset <目录> trash` 再删，或者 `rm -r` 两遍（第二遍是真删）。
- 回收站里**别的用户**的子目录（0700）你删不掉：里面还有他们的文件时，非 root 的人删不掉这棵树（`rm -r` 报 "Permission denied"），要等
  它们到期、或由 root 删。
- `.wmfs-trash` 下以某个 uid 为名的东西不是这个用户自己的目录（例如别人事先建的同名目录）时，不往里放：他的文件照常删除、不进回收站。
- 删除不是精确到秒：最多晚一轮（10 s）加上排队的时间。
