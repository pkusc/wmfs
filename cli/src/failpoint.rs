//! `wmfs failpoint set|clear|list <node> …`（design.md 3.11、7.2）：经**带签名**的管理 API 配置某个节点的 fail point
//! （节点的 node.toml 里要有 `failpoints_api = true`，`wmfs dev` 默认打开）。
//!
//! `<node>` 是节点号（例如 1）：CLI 依次问 `--cluster` / `WMFS_CLUSTER` 里每个管理地址的 `/api/v1/health`，找到这个号的节点；
//! 也可以直接写管理地址（含冒号，例如 127.0.0.1:19180）。fail point 名字只能由字母、数字和 `.` `_` `-` 组成
//! （与服务端同一条规则，先在本地检查），所以放进 `?name=` 不用转义。

use std::path::PathBuf;

use wmfs_core::api::failpoint::check_name;
use wmfs_core::api::types::{Failpoint, FailpointList, Health};
use wmfs_mgr::api::ApiClient;

use crate::{cluster_addrs, fail, load_key, out};

#[derive(clap::Subcommand)]
pub enum FailpointCmd {
    /// 设置一个 fail point，例如 wmfs failpoint set 1 meta.reply.drop 'times(1,return(EIO))'
    Set {
        node: String,
        name: String,
        action: String,
        #[command(flatten)]
        c: ApiArgs,
    },
    /// 清除一个 fail point
    Clear {
        node: String,
        name: String,
        #[command(flatten)]
        c: ApiArgs,
    },
    /// 列出节点上当前的全部 fail point
    List {
        node: String,
        /// 输出 JSON（管理 API 的原样回复）
        #[arg(long)]
        json: bool,
        #[command(flatten)]
        c: ApiArgs,
    },
}

#[derive(clap::Args)]
pub struct ApiArgs {
    /// 集群的管理地址，逗号分隔（默认 $WMFS_CLUSTER）
    #[arg(long)]
    cluster: Option<String>,
    /// 管理员密钥（默认 $WMFS_KEY_FILE，否则 ~/.config/wmfs/admin.key）
    #[arg(long)]
    key_file: Option<PathBuf>,
}

pub fn run(cmd: FailpointCmd) -> i32 {
    let (node, c) = match &cmd {
        FailpointCmd::Set { node, c, .. }
        | FailpointCmd::Clear { node, c, .. }
        | FailpointCmd::List { node, c, .. } => (node, c),
    };
    if let FailpointCmd::Set { name, .. } | FailpointCmd::Clear { name, .. } = &cmd
        && let Err(e) = check_name(name)
    {
        return fail(e);
    }
    let api = match node_api(node, c) {
        Ok(a) => a,
        Err(e) => return fail(e),
    };
    let r: Result<FailpointList, _> = match &cmd {
        FailpointCmd::Set { name, action, .. } => {
            api.post("/api/v1/failpoints", &Failpoint { name: name.clone(), action: action.clone() })
        }
        FailpointCmd::Clear { name, .. } => api.delete(&format!("/api/v1/failpoints?name={name}")),
        FailpointCmd::List { .. } => api.get("/api/v1/failpoints"),
    };
    let l = match r {
        Ok(l) => l,
        Err(e) => return fail(format!("node {node}：{e}")),
    };
    let text = if matches!(cmd, FailpointCmd::List { json: true, .. }) {
        serde_json::to_string(&l).unwrap_or_default()
    } else if l.failpoints.is_empty() {
        format!("node {node}：没有 fail point（空列表）")
    } else {
        let lines: Vec<String> = l.failpoints.iter().map(|f| format!("  {} = {}", f.name, f.action)).collect();
        format!("node {node} 的 fail point：\n{}", lines.join("\n"))
    };
    out(text)
}

/// 找到节点 `node` 的管理地址，返回只连它的 ApiClient。
fn node_api(node: &str, c: &ApiArgs) -> Result<ApiClient, String> {
    let key = load_key(&c.key_file)?;
    if node.contains(':') {
        return Ok(ApiClient::new(vec![node.to_string()], key));
    }
    let id: u32 =
        node.parse().map_err(|_| format!("<node> 应是节点号（例如 1）或管理地址（例如 127.0.0.1:19180）：{node}"))?;
    let addrs = cluster_addrs(&c.cluster)?;
    for a in &addrs {
        let api = ApiClient::new(vec![a.clone()], key.clone());
        if api.get::<Health>("/api/v1/health").is_ok_and(|h| h.node.node_id == id) {
            return Ok(api);
        }
    }
    Err(format!("在 {addrs:?} 里找不到节点 {id}（节点没在运行？）"))
}
