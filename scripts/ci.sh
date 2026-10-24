#!/usr/bin/env bash
# CI（design.md 9.7 末尾）：格式 → 编码约定 → clippy（警告即错误）→ 单元测试与金样 → raft 模拟 → 行数对账 → profile 测试。
# 还没有的步骤先打印 SKIP，到了对应里程碑再接上（raft 模拟从 M3 起跑单节点部分；profile 从 M6 起跑 minimal）。
# 用法：scripts/ci.sh          全部步骤；任何一步失败立即停止并返回非 0。
#       scripts/ci.sh rules    只做编码约定的机械检查（scripts/accept.sh M0 也调用它）。
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
export no_proxy=127.0.0.1,localhost NO_PROXY=127.0.0.1,localhost
# 会改变测试行为的变量（见 scripts/accept.sh 开头的说明）
unset WMFS_GOLDEN_UPDATE WMFS_FAILPOINTS

step() { echo "== $*"; }

# design.md 0.2 里能机械检查的硬规则：每个 .rs 文件 ≤ 400 行；unsafe 只在白名单文件里；不写 macro_rules!；
# 不引入信号处理 crate（ctrlc、signal-hook）与 rand crate；不写过程宏。`// SAFETY:` 注释由 clippy 的
# undocumented_unsafe_blocks 检查（根 Cargo.toml 的 [workspace.lints]）。
# 另有 2.1 的两条：core/src 里除注释外没有 rdma（M2 的验收也用这一条 grep）；插件只用 wmfs_core 的公开 API
# （不直接 use wmfs_api，也不引用别的插件的模块）。
rules() {
    local bad=0 files f
    files="$(find api core cli dashboard plugins tests examples -name '*.rs' -not -path '*/target/*' | sort)"
    for f in $files; do
        if [ "$(wc -l <"$f")" -gt 400 ]; then
            echo "超过 400 行（0.2 第 1 条）：$f（$(wc -l <"$f") 行）"
            bad=1
        fi
        # 去掉 // 之后的注释再找；白名单见 0.2 第 4 条
        if sed 's|//.*||' "$f" | grep -qw unsafe; then
            case "$f" in
                core/src/buf/pool.rs | core/src/base/sys.rs | core/src/hooks/cabi.rs | plugins/rdma/*) ;;
                *)
                    echo "unsafe 只允许出现在 pool.rs、sys.rs、cabi.rs、plugins/rdma/（0.2 第 4 条）：$f"
                    bad=1
                    ;;
            esac
        fi
        if sed 's|//.*||' "$f" | grep -q 'macro_rules!'; then
            echo "不写 macro_rules!（0.2 第 2 条）：$f"
            bad=1
        fi
    done
    for f in $(find . -name Cargo.toml -not -path './target/*' | sort); do
        if grep -nE '^[[:space:]]*(ctrlc|signal-hook|signal_hook|rand)[[:space:]]*(=|\.)' "$f"; then
            echo "不引入信号处理 crate 与 rand crate（0.2 第 4、7 条）：$f"
            bad=1
        fi
        if grep -nE '^[[:space:]]*proc-macro[[:space:]]*=[[:space:]]*true' "$f"; then
            echo "不写过程宏（0.2 第 2 条）：$f"
            bad=1
        fi
    done
    # core/src 除注释外 grep 不到 rdma（2.1）。实现注记（M2）：比 roadmap 的演示命令（grep -v '//' 去掉"含 // 的整行"）更严——
    # 先去掉每行 // 之后的注释再找，所以 "rdma://…" 这样的字符串与带行尾注释的代码也查得出来（与上面 unsafe 的检查同一写法）
    local hits
    hits="$(for f in $(find core/src -name '*.rs' | sort); do sed 's|//.*||' "$f" | { grep -in rdma || true; } | sed "s|^|$f:|"; done)"
    if [ -n "$hits" ]; then
        printf '%s\n' "$hits"
        echo "core/src 里除注释外不得出现 rdma（design.md 2.1：core 里没有任何传输名字的特殊分支）"
        bad=1
    fi
    # 插件只能 use wmfs_core::... 的公开 API，不得 use 其他插件模块（2.1）
    local d name other
    for d in plugins/*/; do
        name="$(basename "$d")"
        if grep -rnE '\bwmfs_api\b' --include='*.rs' "$d"; then
            echo "插件 $name 不得直接用 wmfs_api（用 wmfs_core::api，design.md 2.1）"
            bad=1
        fi
        for other in plugins/*/; do
            other="$(basename "$other" | tr '-' '_')"
            [ "$other" = "$(echo "$name" | tr '-' '_')" ] && continue
            if grep -rnE "(crate|super|wmfs_plugins)::$other\b" --include='*.rs' "$d"; then
                echo "插件 $name 不得引用别的插件模块 $other（design.md 2.1）"
                bad=1
            fi
        done
    done
    if grep -nE '^[[:space:]]*wmfs-api[[:space:]]*(=|\.)' plugins/Cargo.toml; then
        echo "plugins/Cargo.toml 不得直接依赖 wmfs-api（插件经 wmfs_core::api 使用，design.md 2.1）"
        bad=1
    fi
    if [ "$bad" -eq 0 ]; then
        echo "编码约定检查通过：$(echo "$files" | wc -l) 个 .rs 文件"
    fi
    return "$bad"
}

if [ "${1:-}" = "rules" ]; then
    rules
    exit $?
fi

step "1/7 格式：cargo fmt --check"
cargo fmt --all -- --check

step "2/7 编码约定：scripts/ci.sh rules"
rules

step "3/7 clippy：cargo clippy --all-targets -- -D warnings"
cargo clippy --workspace --all-targets -- -D warnings
# 第三方依赖里"将来的 Rust 会拒绝"的写法（.cargo/config.toml 关掉了每次构建的那行提示，design.md 2.3 实现注记（M2））：在这里看得见
cargo report future-incompatibilities 2>&1 | head -20 || true

step "4/7 单元测试、fail point 用例与金样：cargo test"
cargo test --release --workspace

step "5/7 确定性 raft 模拟（1000 个种子，内存后端）"
# M3：单节点部分（design.md 9.2）；普通 cargo test 只跑 200 个种子，这里按"每次提交 1000 个"跑。
# M8：多节点部分（3 节点 × 400 步，掉电、丢消息、分区、重排、换 leader、成员变更、快照；普通 cargo test 跑 100 个种子，这里 1000 个，约 75 s）
WMFS_SIM_SEEDS=1000 cargo test --release -p wmfs-testkit --test raft_sim
WMFS_SIM_SEEDS=1000 cargo test --release -p wmfs-testkit --test raft_sim_cluster -- --nocapture

step "6/7 行数对账：scripts/loc.sh"
scripts/loc.sh

step "7/7 profile 测试（design.md 9.7 的分阶段规则：M6–M8 跑 minimal；nas-safe 从 M9 起）"
scripts/profiles.sh minimal

echo "CI PASS"
