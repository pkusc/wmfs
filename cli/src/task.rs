//! `wmfs task list|show|pause|resume|run|set` 与 `wmfs datakey create`（design.md 7.2、7.1、3.10，roadmap M11）：经带签名的管理 API
//! （admin key，409 自动跟随 leader）。`--json` 输出管理 API 的 JSON。
//!
//! - `wmfs task list`：leader 的 `GET /api/v1/tasks`（leader 任务的进度、速率、积压；节点任务的集群级控制），再逐个问集群里每个节点的
//!   `GET /api/v1/node`，把节点任务（scrub、gc-local）在各节点上的进度一并列出（`--json`：`TasksReply`，节点上的那几行 `node`
//!   是节点号；连不上的节点在文字输出里显示成"连不上"，`--json` 里没有它，ADR-0042 第 12 条）。`wmfs task show <任务>` 只列这一个。
//!   协调器的积压是队列里的项数，显示成"积压 N 个目录"、不显示这一轮的百分比（它处理的是文件：比例没有意义）；
//!   没有积压的任务也不显示百分比。只做数据的节点（不在元数据 raft 里）收不到 pause/set/run，一直按默认值跑（`GET /api/v1/node` 的
//!   `controlled` 为 false）。
//! - `wmfs task pause|resume|run <任务>`：暂停、恢复、马上开始新的一轮（节点任务：每个节点都从头来一轮）。
//! - `wmfs task set <任务> bw=2M[,items=1000]`：限速（字节/秒：0 = 用任务自己的默认值，scrub 是 50 MiB/s、其余只受节点级上限；对象/秒：
//!   0 = 不限）。限速是运维参数，存在任务记录里，不是数据策略。
//! - `wmfs datakey create <名字> [--tenant N]`：建一把数据加密密钥，打印它的数字 id（`encrypt=tenant:<名字>` 用名字）。

use wmfs_core::api::types::{
    DataKeyCreateReply, DataKeyCreateReq, KeyFile, NodeInfoReply, TaskInfo, TaskSetReq, TasksReply, backlog_in_dirs,
};
use wmfs_core::base::units::fmt_size;
use wmfs_mgr::api::{ApiClient, ApiError};

use crate::attr::{ApiArgs, run};

#[derive(clap::Subcommand)]
pub enum TaskCmd {
    /// 列出全部后台任务的进度、速率与积压（节点任务逐个节点列出）
    List {
        #[command(flatten)]
        api: ApiArgs,
    },
    /// 只看一个任务
    Show {
        kind: String,
        #[command(flatten)]
        api: ApiArgs,
    },
    /// 暂停一个任务
    Pause {
        kind: String,
        #[command(flatten)]
        api: ApiArgs,
    },
    /// 恢复一个暂停了的任务
    Resume {
        kind: String,
        #[command(flatten)]
        api: ApiArgs,
    },
    /// 马上开始新的一轮（节点任务：每个节点都来一轮；只做数据的节点收不到，一直按默认值跑）
    Run {
        kind: String,
        #[command(flatten)]
        api: ApiArgs,
    },
    /// 限速，例如 wmfs task set reconciler bw=2M（字节/秒；0 = 任务的默认值）或 items=1000（对象/秒；0 = 不限）
    Set {
        kind: String,
        spec: String,
        #[command(flatten)]
        api: ApiArgs,
    },
}

#[derive(clap::Subcommand)]
pub enum DataKeyCmd {
    /// 建一把数据加密密钥（打印数字 id；之后 wmfs set <目录> encrypt=tenant:<名字>）
    Create {
        name: String,
        #[arg(long, default_value_t = 0)]
        tenant: u32,
        #[command(flatten)]
        api: ApiArgs,
    },
}

/// 一个节点的 `GET /api/v1/node`：(管理地址, 回复；连不上 → 原因)。
pub type NodeTasks = (String, Result<NodeInfoReply, String>);

/// leader 上的任务列表，与集群里每个节点的 `GET /api/v1/node`（连不上的节点也在里面：`wmfs dev wait task-idle` 不把它当空闲，
/// `wmfs task list` 把它显示成连不上）。
pub fn all_tasks(c: &ApiClient, addrs: &[String], key: &KeyFile) -> Result<(TasksReply, Vec<NodeTasks>), ApiError> {
    let r = c.get::<TasksReply>("/api/v1/tasks")?;
    let mut nodes = vec![];
    if r.tasks.iter().any(|t| !t.on_leader()) {
        for a in addrs {
            let n = ApiClient::new(vec![a.clone()], key.clone()).get::<NodeInfoReply>("/api/v1/node");
            nodes.push((a.clone(), n.map_err(|e| e.to_string())));
        }
    }
    Ok((r, nodes))
}

/// `wmfs task list` 的结果：管理 API 的回复（`--json` 原样输出它）与连不上的节点（只在文字输出里列出）。
#[derive(serde::Serialize)]
struct Listed {
    #[serde(flatten)]
    reply: TasksReply,
    #[serde(skip)]
    unreachable: Vec<String>,
}

/// `wmfs task list`（`only` 给了就只列这个任务，没有它 → 404）：各节点上的行接在 leader 的列表后面（见文件头）。
fn list(api: &ApiArgs, only: Option<String>) -> i32 {
    let addrs = crate::cluster_addrs(&api.cluster).unwrap_or_default();
    let key = crate::load_key(&api.key_file);
    let call = move |c: &ApiClient, _: &str| {
        let key = key.map_err(|message| ApiError::new(0, "einval", message))?;
        let (mut r, nodes) = all_tasks(c, &addrs, &key)?;
        let mut unreachable = vec![];
        for (addr, n) in nodes {
            match n {
                Ok(n) => r.tasks.extend(n.tasks),
                Err(e) => unreachable.push(format!("{addr} 连不上（{e}）")),
            }
        }
        if let Some(kind) = &only {
            r.tasks.retain(|t| t.kind == *kind);
            if r.tasks.is_empty() {
                return Err(ApiError::new(404, "enoent", format!("没有后台任务 {kind}")));
            }
        }
        Ok(Listed { reply: r, unreachable })
    };
    run(api, None, call, |l: &Listed| {
        let tail: String = l.unreachable.iter().map(|u| format!("\n节点 {u}：它的节点任务看不到")).collect();
        show(&l.reply) + &tail
    })
}

fn line(t: &TaskInfo) -> String {
    let at = if t.on_leader() {
        "leader".to_string()
    } else if t.node == 0 {
        "(各节点)".to_string()
    } else {
        format!("node{}", t.node)
    };
    let limit = if t.bw == 0 { "只受节点级上限".to_string() } else { format!("{}/s", fmt_size(t.bw)) };
    let mut s = format!("{:<11} {:<9} {:<8}", t.kind, at, t.state);
    if t.on_leader() || t.node != 0 {
        let rate = t.rate_bps as f64 / (1 << 20) as f64;
        let dirs = backlog_in_dirs(&t.kind); // 协调器的积压是目录数：没有这一轮的百分比（见文件头）
        s +=
            &format!(" 做完 {} 轮  已处理 {}  积压 {}{}", t.pass, t.done, t.backlog, if dirs { " 个目录" } else { "" });
        if t.backlog > 0 && !dirs {
            s += &format!("  这一轮 {:.0}%", t.progress * 100.0);
        }
        s += &format!("  速率 {rate:.1} MiB/s");
    }
    s += &format!("  限速 {limit}");
    if !t.last_error.is_empty() {
        s += &format!("  最近一次出错：{}", t.last_error);
    }
    s
}

fn show(r: &TasksReply) -> String {
    let lines: Vec<String> = r.tasks.iter().map(line).collect();
    if lines.is_empty() { "没有后台任务".into() } else { lines.join("\n") }
}

/// `bw=2M,items=1000` → 请求。
fn parse_set(spec: &str) -> Result<TaskSetReq, String> {
    let mut r = TaskSetReq::default();
    for part in spec.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        match part.split_once('=') {
            Some(("bw", v)) => r.bw = Some(v.to_string()),
            Some(("items", v)) => r.items = Some(v.parse().map_err(|_| format!("items 应是整数：{v}"))?),
            _ => return Err(format!("不认识的 {part:?}（写成 bw=2M 或 items=1000）")),
        }
    }
    if r.bw.is_none() && r.items.is_none() {
        return Err("要给 bw=… 或 items=…".into());
    }
    Ok(r)
}

fn send_action(api: &ApiArgs, kind: String, action: &str, body: Option<TaskSetReq>) -> i32 {
    let path = format!("/api/v1/tasks/{kind}/{action}");
    let body = body.unwrap_or_default();
    let call = move |c: &ApiClient, _: &str| c.post::<_, TaskInfo>(&path, &body);
    let action = action.to_string();
    run(api, None, call, move |t: &TaskInfo| format!("{}：{action} 完成\n{}", t.kind, line(t)))
}

pub fn run_task(cmd: TaskCmd) -> i32 {
    match cmd {
        TaskCmd::List { api } => list(&api, None),
        TaskCmd::Show { kind, api } => list(&api, Some(kind)),
        TaskCmd::Pause { kind, api } => send_action(&api, kind, "pause", None),
        TaskCmd::Resume { kind, api } => send_action(&api, kind, "resume", None),
        TaskCmd::Run { kind, api } => send_action(&api, kind, "run", None),
        TaskCmd::Set { kind, spec, api } => match parse_set(&spec) {
            Ok(b) => send_action(&api, kind, "set", Some(b)),
            Err(m) => crate::attr::failed(api.json, &ApiError::new(0, "einval", m)),
        },
    }
}

pub fn run_datakey(cmd: DataKeyCmd) -> i32 {
    let DataKeyCmd::Create { name, tenant, api } = cmd;
    let req = DataKeyCreateReq { name, tenant };
    let call = move |c: &ApiClient, _: &str| c.post::<_, DataKeyCreateReply>("/api/v1/datakeys", &req);
    run(&api, None, call, |r: &DataKeyCreateReply| format!("已建数据密钥 {}：id {}", r.name, r.id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_specs_are_parsed() {
        assert_eq!(parse_set("bw=2M").unwrap(), TaskSetReq { bw: Some("2M".into()), items: None });
        assert_eq!(parse_set("bw=0,items=10").unwrap(), TaskSetReq { bw: Some("0".into()), items: Some(10) });
        assert!(parse_set("").is_err() && parse_set("speed=1").is_err() && parse_set("items=x").is_err());
    }
}
