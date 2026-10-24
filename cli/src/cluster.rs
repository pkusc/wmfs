//! `wmfs cluster set <名>=<值>…`（design.md 7.2、3.2.8）：`{名字: 值}` 发给 `POST /api/v1/cluster/settings`（admin），打印改完之后的
//! 全部集群运行参数。M8：`wmfs cluster plugins show|add|remove <钩子:名字>`（3.7.2）——集群插件清单；改了之后各节点因清单不一致退出，
//! 用新配置重启全部节点。

use std::collections::BTreeMap;

use wmfs_core::api::types::{ClusterSettingsReply, PluginsReply, PluginsReq};
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
    /// 集群插件清单：每个节点都必须启用的插件（钩子:名字）
    Plugins {
        #[command(subcommand)]
        cmd: PluginsCmd,
    },
}

#[derive(clap::Subcommand)]
pub enum PluginsCmd {
    /// 列出集群插件清单
    Show {
        #[command(flatten)]
        api: ApiArgs,
    },
    /// 加一项（admin）；之后各节点因不一致退出，用新配置重启全部节点
    Add {
        item: String,
        #[command(flatten)]
        api: ApiArgs,
    },
    /// 删一项（admin）；之后各节点因不一致退出，用新配置重启全部节点
    Remove {
        item: String,
        #[command(flatten)]
        api: ApiArgs,
    },
}

fn plugins(cmd: PluginsCmd) -> i32 {
    let show_list = |r: &PluginsReply| format!("集群插件清单（每个节点都必须启用）：\n  {}", r.plugins.join("\n  "));
    let (req, api) = match cmd {
        PluginsCmd::Show { api } => {
            let call = |c: &ApiClient, _: &str| c.get::<PluginsReply>("/api/v1/cluster/plugins");
            return run(&api, None, call, show_list);
        }
        PluginsCmd::Add { item, api } => (PluginsReq { add: vec![item], remove: vec![] }, api),
        PluginsCmd::Remove { item, api } => (PluginsReq { add: vec![], remove: vec![item] }, api),
    };
    let call = move |c: &ApiClient, _: &str| c.post::<_, PluginsReply>("/api/v1/cluster/plugins", &req);
    let show_changed =
        |r: &PluginsReply| format!("{}\n各节点会因插件清单不一致而退出：用新配置重启全部节点", show_list(r));
    run(&api, None, call, show_changed)
}

pub fn run_cluster(cmd: ClusterCmd) -> i32 {
    let (items, api) = match cmd {
        ClusterCmd::Set { items, api } => (items, api),
        ClusterCmd::Plugins { cmd } => return plugins(cmd),
    };
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
