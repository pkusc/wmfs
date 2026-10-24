//! `wmfs chain list|show|force-serving`（design.md 7.2、3.4.2）：经带签名的管理 API（`GET /api/v1/chains`、
//! `POST /api/v1/chains/<id>/force-serving?target=`，409 自动跟随 leader）。`--json` 原样输出管理 API 的 JSON。

use wmfs_core::api::types::{ChainInfo, ChainsReply, ForceServingReq};
use wmfs_mgr::api::{ApiClient, ApiError};

use crate::attr::{ApiArgs, failed, run};

#[derive(clap::Subcommand)]
pub enum ChainCmd {
    /// 列出全部链：成员（链序，Serving / Syncing）、期望长度、incarnation，以及集群警告
    List {
        #[command(flatten)]
        api: ApiArgs,
    },
    /// 一条链的详情
    Show {
        id: u32,
        #[command(flatten)]
        api: ApiArgs,
    },
    /// 让 target 成为这条链唯一的 Serving 成员（可能丢掉别的成员独有的写：必须加 --i-accept-data-loss）
    ForceServing {
        id: u32,
        target: u32,
        #[arg(long)]
        i_accept_data_loss: bool,
        #[command(flatten)]
        api: ApiArgs,
    },
}

/// 一条链一行：`chain 3  default  3 副本（期望 3）  ver 5  incarnation 1  [t1@node1 Serving, t2@node2 Syncing]`。
fn line(c: &ChainInfo) -> String {
    let m: Vec<String> =
        c.members.iter().map(|m| format!("t{}@node{} {}", m.target, m.node, title(&m.state))).collect();
    let mut s = format!(
        "chain {:<3} {:<8} {} 副本（期望 {}）  ver {}  incarnation {}  [{}]",
        c.id,
        c.class,
        c.replicas,
        c.want,
        c.ver,
        c.incarnation,
        m.join(", ")
    );
    if let Some(t) = c.last_serving {
        s += &format!("  等待最后的副本 t{t}");
    }
    if !c.removed.is_empty() {
        s += &format!("  最近移除 {:?}", c.removed);
    }
    s
}

/// 成员状态的显示：链记录里的 Serving / Syncing；leader 最近没听到它的心跳、或它刚重启还没被链管理器处理（管理 API 的
/// `unheard`、`restarting`）。
pub fn title(state: &str) -> &str {
    match state {
        "serving" => "Serving",
        "syncing" => "Syncing",
        "restarting" => "刚重启",
        "unheard" => "失联",
        other => other,
    }
}

pub fn run_chain(cmd: ChainCmd) -> i32 {
    match cmd {
        ChainCmd::List { api } => {
            let call = |c: &ApiClient, _: &str| c.get::<ChainsReply>("/api/v1/chains");
            let show = |r: &ChainsReply| {
                let mut s: Vec<String> = r.chains.iter().map(line).collect();
                if s.is_empty() {
                    s.push("还没有链（链在第一次建文件时按 (存储类, 副本数) 生成）".into());
                }
                s.extend(r.warnings.iter().map(|w| format!("警告：{w}")));
                s.join("\n")
            };
            run(&api, None, call, show)
        }
        ChainCmd::Show { id, api } => {
            let call = move |c: &ApiClient, _: &str| {
                let r = c.get::<ChainsReply>("/api/v1/chains")?;
                let no = || ApiError { status: 404, code: "enoent".into(), message: format!("没有链 {id}") };
                r.chains.into_iter().find(|x| x.id == id).ok_or_else(no)
            };
            run(&api, None, call, line)
        }
        ChainCmd::ForceServing { id, target, i_accept_data_loss, api } => {
            if !i_accept_data_loss {
                let message = "force-serving 可能丢掉别的成员独有的写：确认之后加 --i-accept-data-loss".to_string();
                return failed(api.json, &ApiError { status: 0, code: "einval".into(), message });
            }
            let path = format!("/api/v1/chains/{id}/force-serving?target={target}");
            let call = move |c: &ApiClient, _: &str| {
                c.post::<_, ChainInfo>(&path, &ForceServingReq { i_accept_data_loss: true })
            };
            run(&api, None, call, |c: &ChainInfo| {
                format!("{}\n（incarnation 已加一；其余成员会从它重新同步）", line(c))
            })
        }
    }
}
