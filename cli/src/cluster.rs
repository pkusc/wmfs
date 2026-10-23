//! `wmfs cluster set <名>=<值>…`（design.md 7.2、3.2.8）：`{名字: 值}` 发给 `POST /api/v1/cluster/settings`（admin），打印改完之后的
//! 全部集群运行参数。M8 的 `wmfs cluster plugins show|add|remove` 也放在这里。

use std::collections::BTreeMap;

use wmfs_core::api::types::ClusterSettingsReply;
use wmfs_mgr::api::{ApiClient, ApiError};

use crate::attr::{ApiArgs, failed, run};

#[derive(clap::Subcommand)]
pub enum ClusterCmd {
    /// 改集群运行参数（admin），例如 wmfs cluster set epoch_gc_grace=0s session_lease=45s
    Set {
        #[arg(required = true)]
        items: Vec<String>,
        #[command(flatten)]
        api: ApiArgs,
    },
}

pub fn run_cluster(cmd: ClusterCmd) -> i32 {
    let ClusterCmd::Set { items, api } = cmd;
    let mut m = BTreeMap::new();
    for it in &items {
        let Some((k, v)) = it.split_once('=') else {
            let message = format!("{it:?} 应写成 名字=值（例如 epoch_gc_grace=0s）");
            return failed(api.json, &ApiError { status: 0, code: "einval".into(), message });
        };
        m.insert(k.trim().to_string(), v.trim().to_string());
    }
    let call = move |c: &ApiClient, _: &str| c.post::<_, ClusterSettingsReply>("/api/v1/cluster/settings", &m);
    let show = |r: &ClusterSettingsReply| {
        let kv: Vec<String> = r.settings.iter().map(|(k, v)| format!("{k} = {v}")).collect();
        format!("集群运行参数（已更新）：\n  {}", kv.join("\n  "))
    };
    run(&api, None, call, show)
}
