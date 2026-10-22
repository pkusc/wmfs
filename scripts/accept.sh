#!/usr/bin/env bash
# wmfs 里程碑验收（roadmap §0 第 2 条）。
#
# 用法：
#   scripts/accept.sh M0     跑 M0 的全部检查
#   scripts/accept.sh all    依次重跑全部已完成的里程碑（回归：scripts/accept.d/ 里有哪些就跑哪些）
#
# 输出：每条检查一行 "✓ 说明" / "✗ 说明" / "SKIP 说明（原因：…）"；SKIP 不算通过，也不算失败。
#       最后一行是 "Mx PASS" 或 "Mx FAIL（第 n 条失败，请看 <日志路径>）"；all 模式最后再加一行 "all PASS/FAIL"。
# 规则：
#   - 只解析命令的 --json 输出（用 jq），不解析给人看的文字；
#   - 需要等待的地方一律用 `wmfs dev wait <条件>`（M3 起才有），不用固定的 sleep；
#   - 遇到第一条失败就停下：保留 ~/wmfs-dev/ 与全部日志，并打印日志路径，方便排查；全部通过时删掉本次的日志。
#
# 给以后的里程碑加检查：新建 scripts/accept.d/mN.sh，在里面定义函数 accept_mN，用下面三个函数写检查
# （参考 scripts/accept.d/m0.sh）：
#   check      "中文说明" 命令 参数…                 命令退出码为 0 即通过
#   check_json "中文说明" 'jq 表达式' 命令 参数…     命令的 stdout 是 JSON，`jq -e 表达式` 为真即通过
#   skip       "中文说明" "原因"                      本机做不到的检查（不算通过，也不算失败）
# 各里程碑共用的检查命令（交给 check 运行）：
#   build_no_warnings                                cargo build --release --workspace 成功且零警告
#   one_test <cargo test 参数…> <完整测试名>          只跑这一个测试（-- --exact），并确认它真的跑了
#   tests_at_least <N> <cargo test 参数…>            通过，且至少跑了 N 个测试
#   dev_ports_free <里程碑>                          开发集群的端口 19100、19180 空闲（演示用的开发集群要先 wmfs dev down）
#   expect_error <错误码> 命令…                      命令失败（退出码非 0），且 --json 输出里 .error 是这个错误码
# 以及给检查函数用的：
#   dev_metric <指标名>                              node1（127.0.0.1:19180）/metrics 里一个不带标签的指标的值（没有就是空）
# 每条检查的完整输出写在日志目录里的 NN.log（NN = 检查序号）。
set -u
# 这两个变量会改变测试的行为：WMFS_GOLDEN_UPDATE=1 让金样测试"重写金样并直接通过"（假通过），
# WMFS_FAILPOINTS 会让单元测试进程里的正常代码失败（假失败）。验收一律在干净的环境里跑。
unset WMFS_GOLDEN_UPDATE WMFS_FAILPOINTS

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PATH="$ROOT/target/release:$PATH"
# 本机设置了 http_proxy：访问本机的 HTTP 必须绕过代理
export no_proxy=127.0.0.1,localhost NO_PROXY=127.0.0.1,localhost

command -v jq >/dev/null 2>&1 || { echo "需要 jq 来解析 JSON（Ubuntu：apt 包 jq）"; exit 2; }

N=0      # 当前里程碑里的检查序号
CUR=""   # 当前里程碑，例如 M0
LOGDIR="" # 本次运行的日志目录

# check "说明" 命令…：运行命令，输出写进日志；失败就打印 FAIL 行并结束本里程碑（保留现场）。
check() {
    local desc="$1"
    shift
    N=$((N + 1))
    local log
    log="$LOGDIR/$(printf '%02d' "$N").log"
    printf '$ %s\n' "$*" >"$log"
    if "$@" >>"$log" 2>&1; then
        echo "✓ $desc"
        return 0
    fi
    echo "✗ $desc"
    echo "$CUR FAIL（第 $N 条失败，请看 $log）"
    exit 1
}

# check_json "说明" 'jq 表达式' 命令…
check_json() {
    local desc="$1" expr="$2"
    shift 2
    check "$desc" json_expect "$expr" "$@"
}

# 运行命令，把它输出的 JSON 记进日志，再用 jq -e 判断表达式是否为真。
json_expect() {
    local expr="$1"
    shift
    local out rc
    out="$("$@")"
    rc=$?
    printf '%s\n' "$out"
    if [ "$rc" -ne 0 ]; then
        echo "命令退出码 $rc：$*"
        return 1
    fi
    echo "--- jq -e '$expr'"
    printf '%s' "$out" | jq -e "$expr"
}

# skip "说明" "原因"
skip() {
    N=$((N + 1))
    echo "SKIP $1（原因：$2）"
}

# cargo build；cargo 与 rustc 的警告都打印在 stderr 上，有一行以 warning 开头就算失败。
build_no_warnings() {
    local err rc
    err="$(mktemp)"
    cargo build --release --workspace 2>"$err"
    rc=$?
    cat "$err"
    if grep -q '^warning' "$err"; then
        echo "有编译警告（见上面）"
        rc=1
    fi
    rm -f "$err"
    return "$rc"
}

# one_test <cargo test 参数…> <完整测试名>：只跑这一个测试，并确认它真的跑了（名字写错时 cargo 也退出 0）。
one_test() {
    local out
    out="$(cargo test "$@" -- --exact 2>&1)"
    printf '%s\n' "$out"
    printf '%s' "$out" | grep -q 'test result: ok. 1 passed'
}

# tests_at_least <N> <cargo test 的参数…>：通过，且至少跑了 N 个测试（按名字过滤一个都没匹配上时 cargo 也退出 0）。
tests_at_least() {
    local want="$1" out n
    shift
    out="$(cargo test "$@" 2>&1)" || {
        printf '%s\n' "$out"
        return 1
    }
    printf '%s\n' "$out"
    n=$(printf '%s' "$out" | sed -n 's/^test result: ok\. \([0-9]*\) passed.*/\1/p' | awk '{s += $1} END {print s + 0}')
    echo "跑了 $n 个测试（至少要 $want 个）"
    [ "$n" -ge "$want" ]
}

# dev_ports_free <里程碑>：开发集群的 rpc 与管理端口上已经有人在听（多半是演示用的开发集群还在运行）→ 失败并提示 wmfs dev down。
dev_ports_free() {
    local p
    for p in 19100 19180; do
        if (exec 3<>"/dev/tcp/127.0.0.1/$p") 2>/dev/null; then
            echo "127.0.0.1:$p 已被占用：演示用的开发集群还在运行？请先 wmfs dev down，再跑 scripts/accept.sh $1"
            return 1
        fi
    done
    echo "19100、19180 都空闲"
}

# expect_error <错误码> 命令…：命令必须失败（退出码非 0），且 --json 输出里 .error 是期望的错误码。
expect_error() {
    local want="$1"
    shift
    local out rc
    out="$("$@")"
    rc=$?
    printf '%s\n' "$out"
    if [ "$rc" -eq 0 ]; then
        echo "命令应该失败，却成功了"
        return 1
    fi
    printf '%s' "$out" | jq -e --arg w "$want" '.error == $w'
}

# dev_metric <指标名>：node1 的 /metrics 里一个不带标签的指标的值（没有这个指标就输出空，调用方当失败处理）。
dev_metric() {
    curl --noproxy '*' -s "http://127.0.0.1:19180/metrics" | awk -v n="$1" '$1 == n { print $2 }'
}

# 跑一个里程碑（在子 shell 里，失败时 check 用 exit 结束它）。
run_one() {
    local m="$1"
    local n="${m#M}"
    local f="$ROOT/scripts/accept.d/m$n.sh"
    if [ ! -f "$f" ]; then
        echo "没有 $m 的验收脚本：$f"
        return 2
    fi
    # shellcheck source=/dev/null
    source "$f"
    # 目录名带随机后缀：同一秒启动的两次验收（几个 agent 同时跑）不会共用、互删日志目录
    mkdir -p "$HOME/wmfs-dev/accept" || return 2
    LOGDIR="$(mktemp -d "$HOME/wmfs-dev/accept/$m-$(date +%Y%m%d-%H%M%S)-XXXX")" || return 2
    echo "== $m 验收（每条检查的日志在 $LOGDIR/）=="
    (
        N=0
        CUR="$m"
        cd "$ROOT" || exit 1
        "accept_m$n"
    )
    local rc=$?
    if [ "$rc" -eq 0 ]; then
        echo "$m PASS"
        rm -rf "$LOGDIR"
        rmdir "$HOME/wmfs-dev/accept" "$HOME/wmfs-dev" 2>/dev/null || true
    fi
    return "$rc"
}

case "${1:-}" in
    all)
        milestones=$(find "$ROOT/scripts/accept.d" -name 'm*.sh' | sed -E 's|.*/m([0-9]+)\.sh$|\1|' | sort -n)
        for n in $milestones; do
            if ! run_one "M$n"; then
                echo "all FAIL（M$n 没有通过）"
                exit 1
            fi
        done
        echo "all PASS"
        ;;
    M[0-9]*)
        run_one "$1"
        exit $?
        ;;
    *)
        echo "用法：scripts/accept.sh M0 | M1 | … | all"
        exit 2
        ;;
esac
