//! `wmfs class create|update|list|show|delete`（design.md 7.2、6.1、6.3，roadmap M13）：经带签名的管理 API（admin key，409 自动跟随 leader）；
//! `--json` 输出管理 API 的 JSON（`ClassReply` / `ClassesReply` / `ClassInfo`）。
//!
//! - `create <名字> --file class.toml`：TOML 原样发给 `POST /api/v1/classes?name=<名字>`（服务端解析、校验：文件里写了 name 就必须相同）。
//! - `update <名字> --file class.toml`：`PUT /api/v1/classes/<名字>`（完整的新定义）。只改 [qos] → 只下发新份额、不迁移；改 replica 或 [lifecycle]
//!   → 整树协调（回复的 `reconcile`）；改 label、dedicated、failure_domain → 拒绝。
//! - `show <名字>`：定义、QoS 换算成带宽的结果（"iops_max = 300 ≈ 每秒 9 次 4 MiB 写 ≈ 36 MiB/s"——成本单位：一次 IO = 1 + 字节/128 KiB，
//!   4 MiB = 33）、按节点数均分后的每节点份额、由实际部署算出的隔离承诺。`list` 每个类一行。

use std::path::PathBuf;

use wmfs_core::api::types::{ClassInfo, ClassReply, ClassesReply};
use wmfs_core::api::url::encode;
use wmfs_core::base::units::{fmt_duration, fmt_size};
use wmfs_core::meta::cluster::classes::cost;
use wmfs_mgr::api::{ApiClient, ApiError};

use crate::attr::{ApiArgs, failed, run};

#[derive(clap::Subcommand)]
pub enum ClassCmd {
    /// 新建一个存储类（TOML：[devices] label/dedicated、[redundancy] replica、[qos]、[placement]、[lifecycle]）
    Create {
        name: String,
        #[arg(long)]
        file: PathBuf,
        #[command(flatten)]
        api: ApiArgs,
    },
    /// 修改存储类（只改 [qos] 不迁移；改 replica 或 [lifecycle] 触发整树协调；不能改 label、dedicated、failure_domain）
    Update {
        name: String,
        #[arg(long)]
        file: PathBuf,
        #[command(flatten)]
        api: ApiArgs,
    },
    /// 列出全部存储类
    List {
        #[command(flatten)]
        api: ApiArgs,
    },
    /// 一个存储类：定义、QoS 的带宽换算与每节点份额、隔离承诺
    Show {
        name: String,
        #[command(flatten)]
        api: ApiArgs,
    },
    /// 删除一个没用过的存储类
    Delete {
        name: String,
        #[command(flatten)]
        api: ApiArgs,
    },
}

/// iops（成本单位/秒）换算成 4 MiB 写的次数与带宽："300 ≈ 每秒 9 次 4 MiB 写 ≈ 36 MiB/s"。
fn per_4m(v: u32) -> String {
    let n = v as f64 / cost(4 << 20) as f64; // 4 MiB 的 IO 成本 = 1 + 4 MiB / 128 KiB = 33
    format!("{v} ≈ 每秒 {n:.0} 次 4 MiB 写 ≈ {:.0} MiB/s", n * 4.0)
}

/// `wmfs class show` 的文字（见文件头）。
fn show(k: &ClassInfo) -> String {
    let mut s = format!(
        "存储类 {}（id {}）：label={} dedicated={} replica={} priority={} locality={}",
        k.name, k.id, k.label, k.dedicated, k.replica, k.priority, k.locality
    );
    let (q, p, n) = (&k.qos, &k.per_node, k.nodes);
    s += "\nQoS（集群级，单位是成本单位/秒：一次 IO = 1 + 字节/128 KiB，4 KiB = 1、4 MiB = 33；按这个类现在的节点数均分）：";
    let mut any = false;
    for (name, v, each) in [("iops_min", q.iops_min, p.iops_min), ("iops_max", q.iops_max, p.iops_max)] {
        if v > 0 {
            s += &format!("\n  {name}={}，按 {n} 个节点均分后每节点 {each}", per_4m(v));
            any = true;
        }
    }
    for (name, v, each) in [("bw_min", q.bw_min, p.bw_min), ("bw_max", q.bw_max, p.bw_max)] {
        if v > 0 {
            s += &format!("\n  {name}={}/s，每节点 {}/s", fmt_size(v), fmt_size(each));
            any = true;
        }
    }
    if q.burst_ms > 0 {
        s += &format!("\n  burst={}", fmt_duration(std::time::Duration::from_millis(q.burst_ms as u64)));
    }
    if !any {
        s += "\n  不限";
    }
    s += &format!("\n  weight={}", q.weight);
    let down = if k.downgraded { "；声明了 dedicated，实际做不到：isolation_downgraded" } else { "" };
    s += &format!("\nisolation: {}（{}{down}）", k.isolation, k.promise);
    if let (Some(to), Some(when)) = (&k.tier_to, &k.tier_when) {
        s += &format!("\n分层：{when} 的文件迁到 {to}");
    }
    s
}

fn upload(api: &ApiArgs, file: &PathBuf, call: impl FnOnce(&ApiClient, &[u8]) -> Result<ClassReply, ApiError>) -> i32 {
    let body = match std::fs::read(file) {
        Ok(b) => b,
        Err(e) => {
            let message = format!("读 {} 失败：{e}", file.display());
            return failed(api.json, &ApiError::new(0, "einval", message));
        }
    };
    let shown = |r: &ClassReply| {
        let note = if r.reconcile {
            "\n改了 replica 或 [lifecycle]：已有文件由协调器从根整树协调（进度见 wmfs task list）"
        } else {
            ""
        };
        format!("{}{note}", show(&r.class))
    };
    run(api, None, move |c, _| call(c, &body), shown)
}

pub fn run_class(cmd: ClassCmd) -> i32 {
    match cmd {
        ClassCmd::Create { name, file, api } => {
            let path = format!("/api/v1/classes?name={}", encode(&name));
            upload(&api, &file, move |c, b| c.send_raw("POST", &path, b))
        }
        ClassCmd::Update { name, file, api } => {
            let path = format!("/api/v1/classes/{}", encode(&name));
            upload(&api, &file, move |c, b| c.send_raw("PUT", &path, b))
        }
        ClassCmd::List { api } => {
            let call = |c: &ApiClient, _: &str| c.get::<ClassesReply>("/api/v1/classes");
            let lines = |r: &ClassesReply| {
                let row = |k: &ClassInfo| {
                    format!(
                        "{:<16} id {:<3} replica {} label {} isolation {}",
                        k.name, k.id, k.replica, k.label, k.isolation
                    )
                };
                r.classes.iter().map(row).collect::<Vec<_>>().join("\n")
            };
            run(&api, None, call, lines)
        }
        ClassCmd::Show { name, api } => {
            let call = move |c: &ApiClient, _: &str| {
                let r = c.get::<ClassesReply>("/api/v1/classes")?;
                let none = || ApiError::new(404, "enoent", format!("没有存储类 {name}"));
                r.classes.into_iter().find(|k| k.name == name).ok_or_else(none)
            };
            run(&api, None, call, show)
        }
        ClassCmd::Delete { name, api } => {
            let call =
                move |c: &ApiClient, _: &str| c.delete::<ClassReply>(&format!("/api/v1/classes/{}", encode(&name)));
            run(&api, None, call, |r: &ClassReply| format!("已删除存储类 {}（id {}）", r.class.name, r.class.id))
        }
    }
}
