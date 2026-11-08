//! `wmfs mgr` 的装配（design.md 7.2、7.4）：读 mgr.toml、校验各插件的 manifest、注册页面、起 HTTP 与 tick 线程，最后在调用线程上跑
//! changelog 循环（不返回）。这是 wmfs-mgr 里唯一"知道有哪些插件"的地方（同 `cli/src/registry.rs` 之于进程内插件）。
//!
//! `<dir>` 下：`token`（dashboard token，0600；已有就沿用，mgr 重启后浏览器里的网址照样能用）、`mgr.pid`（`wmfs dev` 认进程用）、
//! `audit/`（审计日志）。token 来自 `/dev/urandom`（wmfs-mgr 不依赖 core 的 `rand`，0.2 第 7 条的例外，同 M0 的 fail point `pct()`）。
//! 同一个集群只运行一个 mgr（两个实例会重复投递、互相覆盖游标，`docs/ops.md`）。

use std::convert::Infallible;
use std::io::{Read, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;
use wmfs_api::hex::hex;
use wmfs_api::types::KeyFile;

use crate::alert::{self, rules};
use crate::api::ApiClient;
use crate::audit_log::AuditLog;
use crate::dashboard::Dashboard;
use crate::exporter::Exporter;
use crate::host::events::{self, Sink};
use crate::host::serve::{serve, ticks};
use crate::host::{EventSink, Hooked, MgmtExt, Router, check_manifest};
use crate::webhook::{self, Webhook};

/// `wmfs mgr` 的参数（7.2）。
pub struct MgrOpts {
    pub cluster: Vec<String>, // 全部节点的管理地址
    pub key: KeyFile,
    pub listen: String,
    pub dir: PathBuf,
    pub config: Option<PathBuf>, // mgr.toml
}

/// mgr.toml：只有插件的部署参数（`[webhook] url/proxy`、`[alert] rules`），不认识的键 = 错误。
#[derive(Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
struct MgrToml {
    webhook: WebhookToml,
    alert: AlertToml,
}

#[derive(Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
struct WebhookToml {
    url: String,
    proxy: String,
}

#[derive(Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
struct AlertToml {
    rules: String,
}

fn read(p: &Path) -> Result<String, String> {
    std::fs::read_to_string(p).map_err(|e| format!("读 {} 失败：{e}", p.display()))
}

/// `<dir>/token`：已有（32 位 hex）就沿用，否则新生成一个写进去（0600）。
fn token(dir: &Path) -> Result<String, String> {
    let p = dir.join("token");
    if let Ok(t) = std::fs::read_to_string(&p).map(|t| t.trim().to_string())
        && t.len() == 32
        && t.bytes().all(|b| b.is_ascii_hexdigit())
    {
        return Ok(t);
    }
    let mut b = [0u8; 16];
    std::fs::File::open("/dev/urandom")
        .and_then(|mut f| f.read_exact(&mut b))
        .map_err(|e| format!("读 /dev/urandom 失败：{e}"))?;
    let t = hex(&b);
    let failed = |e: std::io::Error| format!("写 {} 失败：{e}", p.display());
    let mut f =
        std::fs::OpenOptions::new().write(true).create(true).truncate(true).mode(0o600).open(&p).map_err(failed)?;
    f.write_all(format!("{t}\n").as_bytes()).map_err(failed)?;
    Ok(t)
}

/// 装配并运行（见文件头）；只有启动失败才返回（错误是给人看的中文）。
pub fn run(o: MgrOpts) -> Result<Infallible, String> {
    let cfg: MgrToml = match &o.config {
        Some(p) => toml::from_str(&read(p)?).map_err(|e| format!("{} 解析失败：{e}", p.display()))?,
        None => MgrToml::default(),
    };
    let rules = match cfg.alert.rules.as_str() {
        "" => rules::defaults(),
        f => rules::with_file(&read(Path::new(f))?)?,
    };
    let (alert_ext, alert_sink) = alert::plugin(rules, webhook::agent(&cfg.webhook.proxy)?);
    let exts: Vec<Box<dyn MgmtExt>> = vec![Box::new(Dashboard), Box::new(Exporter::default()), Box::new(alert_ext)];
    let audit_dir = o.dir.join("audit");
    let mut sinks: Vec<Box<dyn EventSink>> = vec![Box::new(AuditLog::new(&audit_dir)), Box::new(alert_sink)];
    if !cfg.webhook.url.is_empty() {
        sinks.push(Box::new(Webhook::new(&cfg.webhook.url, &cfg.webhook.proxy)?));
    }
    std::fs::create_dir_all(&audit_dir).map_err(|e| format!("建目录 {} 失败：{e}", audit_dir.display()))?;
    let api = ApiClient::new(o.cluster.clone(), o.key.clone());
    let mut router = Router::default();
    let mut ext_list = Vec::new();
    for mut e in exts {
        let name = check_manifest(e.manifest_toml(), "mgmt_ext")?;
        e.start(&api, &mut router).map_err(|why| format!("插件 {name} 启动失败：{why}"))?;
        ext_list.push((e, Hooked::new(&name, "mgmt_ext")));
    }
    let mut sink_list = Vec::new();
    for s in sinks {
        let name = check_manifest(s.manifest_toml(), "event_sink")?;
        // webhook 每 100 条或 1 s 一批（4.13）；audit-log 的游标由 core 在 audit 第一次启用时建，宿主不替它建（3.8）
        let hold = Duration::from_millis(if name == "webhook" { 1000 } else { 0 });
        let own_cursor = name != "audit-log";
        sink_list.push(Sink::new(s, Hooked::new(&name, "event_sink"), hold, own_cursor));
    }
    let token = token(&o.dir)?;
    let addr = serve(&o.listen, token.clone(), router)?;
    let pid = o.dir.join("mgr.pid");
    std::fs::write(&pid, format!("{}\n", std::process::id())).map_err(|e| format!("写 {} 失败：{e}", pid.display()))?;
    let d = o.dir.display();
    let line = format!(
        "wmfs mgr 已启动：dashboard http://{addr}/?token={token}（token 也写在 {d}/token；审计日志在 {d}/audit/）"
    );
    let _ = writeln!(std::io::stdout(), "{line}").and_then(|()| std::io::stdout().flush()); // 写不出去也不 panic
    ticks(api.clone(), ext_list)?;
    events::run(&api, sink_list)
}
