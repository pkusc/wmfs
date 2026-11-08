//! `wmfs feature list|enable`（design.md 7.2、3.2.10，ADR-0045 第 11 条）：经带签名的管理 API（`GET /api/v1/features`、
//! `POST /api/v1/features/<name>/enable`，409 自动跟随 leader）。`--json` 原样输出管理 API 的 JSON。首发没有能启用的 incompat 特性：
//! 3.2.10 的预留名（都是二期）报"二期功能"（feature_unsupported），不认识的名字报 einval。

use wmfs_core::api::types::FeaturesReply;
use wmfs_core::api::url::encode;
use wmfs_mgr::api::ApiClient;

use crate::attr::{ApiArgs, run};

#[derive(clap::Subcommand)]
pub enum FeatureCmd {
    /// 集群已启用的 incompat 特性，与各节点声明支持的特性
    List {
        #[command(flatten)]
        api: ApiArgs,
    },
    /// 启用一个 incompat 特性（admin，不可逆；首发没有能启用的：预留名都是二期功能）
    Enable {
        name: String,
        #[command(flatten)]
        api: ApiArgs,
    },
}

/// 给人看的样子：已启用的一行，每个节点一行（没有 = "（无）"）。
fn show(r: &FeaturesReply) -> String {
    let list = |v: &[String]| if v.is_empty() { "（无）".to_string() } else { v.join("、") };
    let mut s = vec![format!("集群已启用的 incompat 特性：{}", list(&r.enabled)), "各节点声明支持的特性：".into()];
    s.extend(r.nodes.iter().map(|n| format!("  node{}：{}", n.node_id, list(&n.features))));
    s.join("\n")
}

pub fn run_feature(cmd: FeatureCmd) -> i32 {
    match cmd {
        FeatureCmd::List { api } => {
            run(&api, None, |c: &ApiClient, _: &str| c.get::<FeaturesReply>("/api/v1/features"), show)
        }
        FeatureCmd::Enable { name, api } => {
            let path = format!("/api/v1/features/{}/enable", encode(&name));
            let call = move |c: &ApiClient, _: &str| c.post::<_, FeaturesReply>(&path, &serde_json::json!({}));
            run(&api, None, call, |r: &FeaturesReply| format!("已启用 {name}（不可逆）\n{}", show(r)))
        }
    }
}
