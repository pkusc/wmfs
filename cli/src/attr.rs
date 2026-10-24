//! 策略命令（design.md 7.2、3.8、3.1.5、3.9、3.10，roadmap M7）：`wmfs set/get/unset/usage/status`，都经带签名的管理 API
//! （`ApiClient`，409 自动跟随 leader）。`<path>` 可以是本机 wmfs 挂载点下的路径或文件系统内的绝对路径（`pathmap.rs`）。
//! `--json` 原样输出管理 API 的 JSON（`scripts/accept.sh` 只解析它）；失败时退出码 1，`--json` 下输出 `{"error", "message"}`。
//!
//! - `wmfs set <path> k=v[,k=v…]`：属性字符串原样发给服务端（服务端切分、解析、规范化，3.8），打印规范化后的结果与警告。
//! - `wmfs get <path> [key] [-r]`：每个属性一行"键=值  来源  生效方式"，之后是警告，最后一行 `semantics:`；给了 key 只打印那一行
//!   （`--json` 仍是完整的回复）。key 不在回复里（回复列着全部能查的键）→ 报错、退出码 1，不静默输出空行（原则 7）。
//! - `wmfs unset <path> <key>`；`wmfs usage <path>`（递归用量，常量时间）；`wmfs status [path]`（第一行起是集群警告，之后声明 vs 实际）。

use std::path::PathBuf;

use serde::Serialize;
use wmfs_core::api::types::{
    AttrGetReply, AttrSetReply, AttrSetReq, AttrUnsetReply, DEFAULT_SOURCE, KeyFile, StatusReply, UsageReply,
};
use wmfs_core::api::url::encode;
use wmfs_mgr::api::{ApiClient, ApiError};

use crate::{fail, load_key, out, pathmap};

/// 走管理 API 的命令共用的参数。
#[derive(clap::Args)]
pub struct ApiArgs {
    /// 集群的管理地址，逗号分隔（默认：路径在 wmfs 挂载点下时用挂载点的；否则 $WMFS_CLUSTER）
    #[arg(long)]
    pub cluster: Option<String>,
    /// admin 密钥文件（默认 $WMFS_KEY_FILE，否则 ~/.config/wmfs/admin.key）
    #[arg(long)]
    pub key_file: Option<PathBuf>,
    /// 输出 JSON（管理 API 的原样回复）
    #[arg(long)]
    pub json: bool,
}

#[derive(clap::Args)]
pub struct SetArgs {
    pub path: String,
    /// 属性，例如 chunk=1M,stripe=2 或 consistency=cto,attr_ttl=3s
    pub spec: String,
    #[command(flatten)]
    pub api: ApiArgs,
}

#[derive(clap::Args)]
pub struct GetArgs {
    pub path: String,
    /// 只看这一个属性（--json 时仍输出完整的回复；不认识的键照样报错）
    pub key: Option<String>,
    /// 另外列出子树里显式设置了属性的目录
    #[arg(short = 'r')]
    pub recursive: bool,
    #[command(flatten)]
    pub api: ApiArgs,
}

#[derive(clap::Args)]
pub struct UnsetArgs {
    pub path: String,
    pub key: String,
    #[command(flatten)]
    pub api: ApiArgs,
}

#[derive(clap::Args)]
pub struct PathArgs {
    pub path: String,
    #[command(flatten)]
    pub api: ApiArgs,
}

#[derive(clap::Args)]
pub struct StatusArgs {
    /// 不给 = 整个文件系统（/）
    pub path: Option<String>,
    #[command(flatten)]
    pub api: ApiArgs,
}

/// 管理 API 的客户端（用 `--cluster` 或换算出的地址、admin key）。
pub fn client(api: &ApiArgs, cluster: Vec<String>) -> Result<ApiClient, String> {
    let key: KeyFile = load_key(&api.key_file)?;
    Ok(ApiClient::new(cluster, key))
}

/// 一条命令：换算路径 → 调管理 API → 输出（JSON 或给人看的文字）。`path` 为 None：不换算，地址取 `--cluster`/`WMFS_CLUSTER`。
pub fn run<T: Serialize>(
    api: &ApiArgs,
    path: Option<&str>,
    call: impl FnOnce(&ApiClient, &str) -> Result<T, ApiError>,
    show: impl FnOnce(&T) -> String,
) -> i32 {
    let t = match path {
        Some(p) => pathmap::resolve(p, &api.cluster),
        None => crate::cluster_addrs(&api.cluster).map(|cluster| pathmap::Target { path: "/".into(), cluster }),
    };
    let r = t.and_then(|t| Ok((client(api, t.cluster)?, t.path)));
    let (c, p) = match r {
        Ok(x) => x,
        Err(e) => return failed(api.json, &ApiError { status: 0, code: "einval".into(), message: e }),
    };
    match call(&c, &p) {
        Ok(r) if api.json => out(serde_json::to_string(&r).unwrap_or_default()),
        Ok(r) => out(show(&r)),
        Err(e) => failed(api.json, &e),
    }
}

/// 失败的输出：`--json` 时 `{"error", "message"}`，否则一行中文；退出码 1。
pub fn failed(json: bool, e: &ApiError) -> i32 {
    if json {
        let _ = out(serde_json::json!({"error": e.code, "message": e.message}));
        return 1;
    }
    fail(format!("wmfs：{}（{}）", e.message, e.code))
}

fn warn_lines(w: &[String]) -> String {
    w.iter().map(|x| format!("\n警告：{x}")).collect()
}

pub fn set(a: SetArgs) -> i32 {
    let spec = a.spec;
    let call =
        move |c: &ApiClient, p: &str| c.post::<_, AttrSetReply>("/api/v1/attr", &AttrSetReq { path: p.into(), spec });
    let show = |r: &AttrSetReply| {
        let kv: Vec<String> = r.set.iter().map(|(k, v)| format!("{k}={v}")).collect();
        format!("已设置 {}：{}{}", r.path, kv.join(" "), warn_lines(&r.warnings))
    };
    run(&a.api, Some(&a.path), call, show)
}

pub fn get(a: GetArgs) -> i32 {
    let rec = if a.recursive { 1 } else { 0 };
    let key = a.key.as_deref();
    let call = |c: &ApiClient, p: &str| {
        let r: AttrGetReply = c.get(&format!("/api/v1/attr?path={}&recursive={rec}", encode(p)))?;
        match key {
            Some(k) if !r.attrs.iter().any(|e| e.key == k) => Err(no_such_key(k, &r)),
            _ => Ok(r),
        }
    };
    let show = |r: &AttrGetReply| {
        let mut s = String::new();
        for e in r.attrs.iter().filter(|e| key.is_none_or(|k| k == e.key)) {
            let from = if e.source == DEFAULT_SOURCE { e.source.clone() } else { format!("来自 {}", e.source) };
            s += &format!("{:<28} {from}（生效：{}）\n", format!("{}={}", e.key, e.value), e.effect);
        }
        if key.is_some() {
            return s.trim_end().to_string();
        }
        if !r.subtree.is_empty() {
            s += "子树里显式设置了属性的目录：\n";
            for d in &r.subtree {
                let kv: Vec<String> = d.attrs.iter().map(|(k, v)| format!("{k}={v}")).collect();
                s += &format!("  {}：{}\n", d.path, kv.join(" "));
            }
            if r.truncated {
                s += "  ……（结果被截断）\n";
            }
        }
        for w in &r.warnings {
            s += &format!("警告：{w}\n");
        }
        s + &format!("semantics: {}", r.semantics)
    };
    run(&a.api, Some(&a.path), call, show)
}

/// `wmfs get <path> <key>` 的 key 不在回复里：回复列着全部能查的键，所以它不是能查的键。
fn no_such_key(k: &str, r: &AttrGetReply) -> ApiError {
    let have: Vec<&str> = r.attrs.iter().map(|e| e.key.as_str()).collect();
    ApiError {
        status: 0, code: "einval".into(), message: format!("没有属性 {k}（能查的有 {}）", have.join("、"))
    }
}

pub fn unset(a: UnsetArgs) -> i32 {
    let key = a.key.as_str();
    let call = |c: &ApiClient, p: &str| {
        c.delete::<AttrUnsetReply>(&format!("/api/v1/attr?path={}&key={}", encode(p), encode(key)))
    };
    let show = |r: &AttrUnsetReply| format!("已删除 {} 上的 {}（原来是 {}）", r.path, r.key, r.old);
    run(&a.api, Some(&a.path), call, show)
}

/// 给人看的字节数（1536 → "1.5 KiB"）。
fn human(n: u64) -> String {
    let units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    let (mut v, mut i) = (n as f64, 0);
    while v >= 1024.0 && i + 1 < units.len() {
        v /= 1024.0;
        i += 1;
    }
    if i == 0 { format!("{n} B") } else { format!("{v:.1} {}", units[i]) }
}

pub fn usage(a: PathArgs) -> i32 {
    let call = |c: &ApiClient, p: &str| c.get::<UsageReply>(&format!("/api/v1/usage?path={}", encode(p)));
    let show = |r: &UsageReply| {
        let mut s = format!(
            "{}：{} 字节（{}），{} 个文件，{} 个子目录（子树合计，不含目录自己）",
            r.path,
            r.bytes,
            human(r.bytes),
            r.files,
            r.dirs
        );
        for k in &r.per_class {
            s += &format!("\n  存储类 {}：{} 字节，{} 个文件", k.class, k.bytes, k.files);
        }
        s
    };
    run(&a.api, Some(&a.path), call, show)
}

pub fn status(a: StatusArgs) -> i32 {
    let path = a.path.unwrap_or_else(|| "/".into());
    let call = |c: &ApiClient, p: &str| c.get::<StatusReply>(&format!("/api/v1/status?path={}", encode(p)));
    let show = |r: &StatusReply| {
        let mut s = if r.warnings.is_empty() { "没有集群警告\n".to_string() } else { String::new() };
        for w in &r.warnings {
            s += &format!("警告：{w}\n");
        }
        s += &format!("{} 的声明 vs 实际（实际 = 这里的新文件此刻得到的值）：", r.path);
        for x in &r.attrs {
            let ok = if x.mismatched { "✗ 不一致" } else { "✓" };
            let from =
                if x.source == DEFAULT_SOURCE { "默认值".to_string() } else { format!("来源 {}", x.source) };
            let short =
                x.actual.short_files.map_or(String::new(), |n| format!("；已有文件里实际副本少于声明的 {n} 个"));
            s += &format!("\n  {:<12} 声明 {}（{from}），实际 {}{short}  {ok}", x.key, x.declared, x.actual.new_files);
        }
        s
    };
    run(&a.api, Some(&path), call, show)
}
