# audit-log 插件（wmfs mgr 内）

**钩子**：event_sink（进程外，`wmfs mgr` 里）　**游标**：`audit-log`
**fail point**：`plugin.audit-log.event_sink`（触发 = 这一批不写、游标不前进，稍后整批重投）　**指标**：`wmfs_audit_log_records_total`
**消费的目录属性**：`audit`（`audit=on[,ops=…][,uids=…]`：哪些修改带 `audited` 标记由 core 决定，3.8）

## 它做什么

把 changelog 里带 `audited: true` 的记录写成审计日志（design.md 4.13）：`<mgr 目录>/audit/audit-YYYYMMDD.log`（UTC 日期；一行一个 JSON，
就是 `GET /api/v1/changelog` 的事件，带 seq；一个文件超过 100 MiB 换 `audit-YYYYMMDD.1.log`、`.2.log`……）。

```bash
touch ~/wmfs-mnt/home/a && rm ~/wmfs-mnt/home/a && wmfs dev wait audit-caught-up
grep -hE '"op":"(Create|Unlink)"' ~/wmfs-dev/mgr/audit/audit-*.log | tail -2
```

## 不丢、可去重

- 每批写完 `fsync`（新文件连目录项一起）之后，宿主才提交游标 `audit-log`：至少一次投递。mgr 在写完、提交游标之前崩溃，重启后这一批会再写
  一次——**按 seq 去重**。
- 这个游标在 `audit` 第一次启用时已由 core 建好（宿主不替它建：没开过审计的集群里 audit-log 什么也不做，每 30 s 看一次游标出现了没有），
  所以 mgr 没运行期间的记录按游标保留（最多 7 天 / 1000 万条），mgr 重启后补写；这期间 `wmfs status` 的 audit 一行与 `/api/v1/health`
  显示"审计消费者落后 N 条"——mgr 停着时没有告警（告警是 mgr 自己发的，生产用 systemd 让它一直在运行，`docs/ops.md`）。mgr 在运行、
  审计却 10 分钟没有前进（例如写不出审计文件）时告警 `audit_stalled`。
- 每一行都是完整的 JSON：写失败（例如磁盘满）就把文件截回写之前的长度；上次写到一半进程崩了、文件末尾留着半行时，下次写之前先截掉它
  （那一批没有提交游标，会整批重投）。
- 落后超出保留范围、那一段已被修剪：mgr 读回显式的 410，写一行错误日志、告警 `changelog_gap`——不是静默丢失。
