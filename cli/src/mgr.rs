//! `wmfs mgr --cluster <管理地址[,…]> --listen 127.0.0.1:9500 [--dir D] [--config mgr.toml] [--key-file F]`（design.md 7.2、7.4）：在前台
//! 运行进程外插件宿主（`wmfs-mgr` crate 的 dashboard、prometheus-exporter、alert、audit-log、webhook）。手动启动：server 不拉起它（生产用
//! systemd，`docs/ops.md`；开发用 `wmfs dev mgr`）。`--cluster` 默认 `WMFS_CLUSTER`，`--dir` 默认 `WMFS_MGR_DIR`，key 默认 `WMFS_KEY_FILE`。
//! 同一集群只运行一个 mgr。

use std::path::PathBuf;

use crate::fail;

#[derive(clap::Args)]
pub struct MgrArgs {
    /// 集群的管理地址，逗号分隔（默认 $WMFS_CLUSTER）
    #[arg(long)]
    pub cluster: Option<String>,
    /// dashboard、/metrics、/alerts 的监听地址
    #[arg(long, default_value = "127.0.0.1:9500")]
    pub listen: String,
    /// mgr 的目录：审计日志在 <dir>/audit/，dashboard 的 token 在 <dir>/token（默认 $WMFS_MGR_DIR）
    #[arg(long)]
    pub dir: Option<PathBuf>,
    /// mgr.toml（[webhook] url/proxy、[alert] rules）
    #[arg(long)]
    pub config: Option<PathBuf>,
    /// admin 密钥文件（默认 $WMFS_KEY_FILE，否则 ~/.config/wmfs/admin.key）
    #[arg(long)]
    pub key_file: Option<PathBuf>,
}

/// 只有启动失败才返回（mgr 启动之后一直在前台运行）。
pub fn run_mgr(a: MgrArgs) -> i32 {
    let r = crate::cluster_addrs(&a.cluster).and_then(|cluster| {
        let key = crate::load_key(&a.key_file)?;
        let dir = a.dir.or_else(|| std::env::var_os("WMFS_MGR_DIR").map(PathBuf::from));
        let dir =
            dir.ok_or("不知道 mgr 的目录：用 --dir 指定，或先 eval \"$(wmfs dev env)\"（它设置 WMFS_MGR_DIR）")?;
        wmfs_mgr::run(wmfs_mgr::MgrOpts { cluster, key, listen: a.listen, dir, config: a.config })
    });
    let Err(e) = r;
    fail(format!("wmfs mgr 启动失败：{e}"))
}
