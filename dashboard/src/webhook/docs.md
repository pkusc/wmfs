# webhook 插件（wmfs mgr 内）

**钩子**：event_sink（进程外，`wmfs mgr` 里）　**游标**：`webhook`（只有配置了 url 才启用，也才有这个游标）
**fail point**：`plugin.webhook.event_sink`（触发 = 这一批不发、游标不前进，稍后整批重投）　**指标**：`wmfs_webhook_posted_total`

## 它做什么

把 changelog 的全部记录按 JSON 数组 POST 给一个网址（design.md 4.13），给别的系统接事件用：

```toml
# mgr.toml（wmfs mgr --config mgr.toml）
[webhook]
url = "https://hooks.example.com/wmfs"   # http:// 或 https://（ureq 的 rustls）
proxy = ""                               # 走哪个 HTTP 代理；不写 = 不走代理（本机设置了 http_proxy 也不受影响）
```

- 每 100 条或 1 s 一批（design.md 4.13）：宿主给它攒批——不到 100 条时从第一次看到这一批起等 1 s 再发；每次 POST 最多 100 条
  （`[ChangeEvent, …]`，字段同 `GET /api/v1/changelog` 的事件，带 seq）。
- 回的不是 2xx、或连不上：这一批算失败，宿主对这个 sink 指数退避（1 s 起，上限 5 min）之后整批重发，退避期间别的 sink（audit-log、alert）
  照常投递。单次 POST 最多 3 s：mgr 只有一个 changelog 循环线程，接收方卡住（接受连接却不回）时，每次重试会让别的 sink 晚约 3 s。
- 全部发成功之后才提交游标：至少一次投递，接收方按 seq 去重。
