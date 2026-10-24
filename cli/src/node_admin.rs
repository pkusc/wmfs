//! `wmfs node list|drain|remove|promote`（design.md 7.2、3.4.1.9；`wmfs init|join|server` 在 `node.rs`）：经带签名的管理 API
//! （`GET /api/v1/cluster`、`POST /api/v1/nodes/<id>/<操作>`，409 自动跟随 leader；`--cluster` 可以给多个地址，连不上就换下一个）。
//! `--json` 原样输出管理 API 的 JSON（`scripts/accept.sh` 只解析它）。

use wmfs_core::api::types::{ClusterReply, NodeInfo, NodeOpReply};
use wmfs_mgr::api::ApiClient;

use crate::attr::{ApiArgs, run};

#[derive(clap::Subcommand)]
pub enum NodeCmd {
    /// 列出节点：地址、状态、元数据角色（voter/learner）、谁是 leader、是否追上
    List {
        #[command(flatten)]
        api: ApiArgs,
    },
    /// 让节点不再当 leader、不再被提升（M9 起还会把链从它上面迁走）
    Drain {
        id: u32,
        #[command(flatten)]
        api: ApiArgs,
    },
    /// 把节点移出元数据 raft（只能由管理员做；不能移除最后一个 voter）
    Remove {
        id: u32,
        #[command(flatten)]
        api: ApiArgs,
    },
    /// 指定节点为元数据 voter（先以 learner 加入，追上之后提升）
    Promote {
        id: u32,
        #[command(flatten)]
        api: ApiArgs,
    },
}

pub fn run_node(cmd: NodeCmd) -> i32 {
    let (op, id, api) = match cmd {
        NodeCmd::List { api } => {
            let call = |c: &ApiClient, _: &str| c.get::<ClusterReply>("/api/v1/cluster");
            return run(&api, None, call, show_list);
        }
        NodeCmd::Drain { id, api } => ("drain", id, api),
        NodeCmd::Remove { id, api } => ("remove", id, api),
        NodeCmd::Promote { id, api } => ("promote", id, api),
    };
    let path = format!("/api/v1/nodes/{id}/{op}");
    let call = move |c: &ApiClient, _: &str| c.post::<_, NodeOpReply>(&path, &serde_json::json!({}));
    run(&api, None, call, |r: &NodeOpReply| format!("{}\n{}", line(&r.node), r.message))
}

fn line(n: &NodeInfo) -> String {
    let rpc: Vec<&str> = n.addrs.iter().map(|(_, a)| a.as_str()).collect();
    let role = if n.leader { format!("{}（leader）", n.member) } else { n.member.clone() };
    let caught = if n.member == "none" {
        String::new()
    } else if n.caught_up {
        format!("matched {}（追平）", n.matched)
    } else {
        format!("matched {}（追赶中）", n.matched)
    };
    format!("node{:<3} {:<22} {:<16} {:<9} {caught}", n.node_id, rpc.join(","), role, n.state)
}

fn show_list(r: &ClusterReply) -> String {
    let mut s = format!(
        "元数据：leader node{}，term {}，commit {}，voter {:?}，learner {:?}\n",
        r.leader, r.term, r.commit, r.voters, r.learners
    );
    if !r.voters_outgoing.is_empty() {
        s += &format!("（joint 配置：旧 voter {:?}）\n", r.voters_outgoing);
    }
    for n in &r.nodes {
        s += &line(n);
        s.push('\n');
    }
    for w in &r.warnings {
        s += &format!("警告：{w}\n");
    }
    s.trim_end().to_string()
}
