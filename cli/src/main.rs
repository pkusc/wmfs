//! wmfs：唯一的二进制（design.md 1.1、7.2，ADR-0006）。
//!
//! M0 只有 `wmfs --version` 与 `wmfs doctor`；其余子命令（init、server、ping、dev、fs、mount、set/get……）随里程碑加入。
//! 插件注册表 `registry.rs`（M1 起）是全仓库唯一"知道有哪些插件"的文件。

mod doctor;

use clap::{Parser, Subcommand};

/// `wmfs --version` 的输出（clap 会在前面加 "wmfs "）：软件版本 + 线上协议主版本 + 静态插件 ABI + 动态库 C ABI。
/// 单元测试保证它与 core 里的常量一致。
const VERSION: &str = "0.1.0 (proto 1, core-abi 1, c-abi 1)";

#[derive(Parser)]
#[command(name = "wmfs", version = VERSION, about = "wmfs：给小集群用的分布式文件系统（一个二进制、三台机器）")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// 环境体检：这台机器能测什么（逐项 ✓ 可以 / ! 能用但有限制 / ✗ 缺失）
    Doctor {
        /// 输出 JSON（scripts/accept.sh 只解析这个）
        #[arg(long)]
        json: bool,
    },
}

fn main() {
    let cli = Cli::parse();
    let code = match cli.cmd {
        Cmd::Doctor { json } => doctor::run(json),
    };
    std::process::exit(code);
}

/// 把一条命令的全部输出一次写到标准输出（各子命令共用）。不用 println!：它写失败时会 panic。
/// 读的一方提前退出（`wmfs doctor | head -1`、`| true`）不算错误；其他写失败（例如 `> /dev/full`）
/// 打印原因并返回 `Err(1)`（调用方把它当退出码）。
fn write_stdout(text: &str) -> Result<(), i32> {
    use std::io::Write;
    let mut out = std::io::stdout().lock();
    match out.write_all(text.as_bytes()).and_then(|()| out.flush()) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => Ok(()),
        Err(e) => {
            eprintln!("写标准输出失败：{e}");
            Err(1)
        }
    }
}

#[cfg(test)]
mod tests {
    use wmfs_core::hooks::manifest::{C_ABI_VERSIONS, CORE_ABI_VERSION};
    use wmfs_core::proto::frame::PROTO_MAJOR;

    #[test]
    fn version_string_matches_constants() {
        let c_abi: Vec<String> = C_ABI_VERSIONS.iter().map(|v| v.to_string()).collect();
        let want = format!(
            "{} (proto {}, core-abi {}, c-abi {})",
            env!("CARGO_PKG_VERSION"),
            PROTO_MAJOR,
            CORE_ABI_VERSION,
            c_abi.join(",")
        );
        assert_eq!(super::VERSION, want, "改了协议/ABI 版本号时同步改 VERSION");
    }

    #[test]
    fn cli_definition_is_valid() {
        use clap::CommandFactory;
        super::Cli::command().debug_assert();
    }
}
