#!/usr/bin/env bash
# wmfs 里程碑验收（roadmap §0 第 2 条）。
#
# 用法：
#   scripts/accept.sh M0     跑 M0 的全部检查
#   scripts/accept.sh all    依次重跑全部已完成的里程碑（回归：scripts/accept.d/ 里有哪些就跑哪些）
#   scripts/accept.sh profile-minimal | profile-nas-safe | profile-hpc-rdma | chaos | release
#                            M15 的发布门槛（函数在 scripts/accept.d/m15.sh，那里写着各自跑什么、大约多久）：三个 profile 的全套、
#                            chaos（kill -9、SIGSTOP、分区、网络延迟、时钟偏移 + Jepsen-lite）、release = 以上全部 + clippy + 全部单元测试 +
#                            文档齐全检查 + 行数。最后一行是 "<名字> PASS" / "<名字> FAIL（…）"；全部检查都是 SKIP 时（本机没有 RDMA 设备的
#                            profile-hpc-rdma）是 "<名字> SKIP（未在本机验证…）"，退出码 0；有 SKIP 的 PASS 在同一行写明 SKIP 了几条。
#
# 输出：每条检查一行 "✓ 说明" / "✗ 说明" / "SKIP 说明（原因：…）"；SKIP 不算通过，也不算失败。
#       最后一行是 "Mx PASS"（有 SKIP 时同一行写明 SKIP 了几条）或 "Mx FAIL（第 n 条失败，请看 <日志路径>）"；all 模式最后再加
#       一行 "all PASS/FAIL"。里程碑与发布门槛的入口共用 run_entry（日志目录、子 shell、最后一行）。
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
#   dev_ports_free <里程碑> [节点数]                 开发集群的端口 19n00、19n80（n = 1..节点数，默认 1）空闲（演示用的开发集群要先 wmfs dev down）
#   ignored_test <cargo test 参数…> <完整测试名>     跑一个 #[ignore] 的用例（对着本次验收的开发集群），并确认它真的跑了
#   expect_error <错误码> 命令…                      命令失败（退出码非 0），且 --json 输出里 .error 是这个错误码
# 以及给检查函数用的：
#   dev_metric <指标名>                              node1（127.0.0.1:19180）/metrics 里一个不带标签的指标的值（没有就是空）
#   poll <次数> <间隔秒> 命令…                       反复运行命令直到它成功（最多这么多次）：按条件等待，不用固定的 sleep
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
    keep_mount_logs
    echo "$CUR FAIL（第 $N 条失败，请看 $log）"
    exit 1
}

# 失败时把开发挂载的 mount.log 拷进 $LOGDIR/mount-logs：它们在 wmfs dev umount / dev down 时随挂载的记录目录一起删（cli/src/dev/mount.rs），
# 而退出时的清理（各里程碑的 trap）会 dev down。
keep_mount_logs() {
    [ -n "${WMFS_DEV_DIR:-}" ] && [ -d "$WMFS_DEV_DIR/mounts" ] || return 0
    mkdir -p "$LOGDIR/mount-logs" && (cd "$WMFS_DEV_DIR/mounts" && find . -name '*.log' -exec cp --parents {} "$LOGDIR/mount-logs/" \;)
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
    echo "$1" >>"$LOGDIR/skipped" # 最后一行写明 SKIP 了几条（run_entry）
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

# dev_ports_free <里程碑> [节点数]：开发集群节点 n（1..节点数，默认 1）的 rpc 端口 19n00 与管理端口 19n80 上已经有人在听（多半是
# 演示用的开发集群还在运行）→ 失败并提示 wmfs dev down。
dev_ports_free() {
    local n p ports=""
    for n in $(seq 1 "${2:-1}"); do ports="$ports 19${n}00 19${n}80"; done
    for p in $ports; do
        if (exec 3<>"/dev/tcp/127.0.0.1/$p") 2>/dev/null; then
            echo "127.0.0.1:$p 已被占用：演示用的开发集群还在运行？请先 wmfs dev down，再跑 scripts/accept.sh $1"
            return 1
        fi
    done
    echo "端口$ports 都空闲"
}

# ignored_test <cargo test 参数…> <完整测试名>：跑一个 #[ignore] 的用例（对着本次验收的开发集群），并确认它真的跑了。
ignored_test() {
    local out
    out="$(cargo test "$@" -- --exact --ignored --nocapture 2>&1)"
    printf '%s\n' "$out"
    printf '%s' "$out" | grep -q 'test result: ok. 1 passed'
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

# poll <次数> <间隔秒> 命令…：反复运行命令，成功就返回 0；试满这么多次还不成功就返回 1（命令自己的输出照常留在日志里）。
poll() {
    local n="$1" gap="$2" i
    shift 2
    for i in $(seq 1 "$n"); do
        "$@" && return 0
        [ "$i" -lt "$n" ] && sleep "$gap"
    done
    return 1
}

# run_entry <名字> <脚本> <函数>：加载脚本、在子 shell 里跑函数（失败时 check 用 exit 结束它），每条检查的日志在本次的日志目录里。
# 最后一行：PASS（有 SKIP 时写明 SKIP 了几条：skip() 与 m15.sh 记进 $LOGDIR/skipped 的）；函数以 3 退出 = 全部检查都是 SKIP
# （本机没有 RDMA 设备的 profile-hpc-rdma）→ "<名字> SKIP（…）"，退出码 0；失败时 check 已经打印了 FAIL 行。通过时删掉日志目录。
run_entry() {
    local name="$1" f="$2" fn="$3" rc skipped=0
    if [ ! -f "$f" ]; then
        echo "没有 $name 的验收脚本：$f"
        return 2
    fi
    # shellcheck source=/dev/null
    source "$f"
    # 目录名带随机后缀：同一秒启动的两次验收（几个 agent 同时跑）不会共用、互删日志目录
    mkdir -p "$HOME/wmfs-dev/accept" || return 2
    LOGDIR="$(mktemp -d "$HOME/wmfs-dev/accept/$name-$(date +%Y%m%d-%H%M%S)-XXXX")" || return 2
    echo "== $name（每条检查的日志在 $LOGDIR/）=="
    (
        N=0
        CUR="$name"
        cd "$ROOT" || exit 1
        "$fn"
    )
    rc=$?
    [ -f "$LOGDIR/skipped" ] && skipped="$(wc -l <"$LOGDIR/skipped")"
    if [ "$rc" -eq 3 ]; then
        echo "$name SKIP（未在本机验证：不算通过，也不算失败）"
        rc=0
    elif [ "$rc" -eq 0 ] && [ "$skipped" -gt 0 ]; then
        echo "$name PASS（另有 $skipped 条 SKIP：未在本机验证，不算通过也不算失败）"
    elif [ "$rc" -eq 0 ]; then
        echo "$name PASS"
    fi
    if [ "$rc" -eq 0 ]; then
        rm -rf "$LOGDIR"
        rmdir "$HOME/wmfs-dev/accept" "$HOME/wmfs-dev" 2>/dev/null || true
    fi
    return "$rc"
}

# 跑一个里程碑 Mn（scripts/accept.d/mn.sh 的 accept_mn）。
run_one() {
    local n="${1#M}"
    run_entry "$1" "$ROOT/scripts/accept.d/m$n.sh" "accept_m$n"
}

# 发布门槛的入口（M15）：profile-minimal | profile-nas-safe | profile-hpc-rdma | chaos | release（函数 accept_<名字> 在 m15.sh）。
run_named() {
    run_entry "$1" "$ROOT/scripts/accept.d/m15.sh" "accept_${1//-/_}"
}

case "${1:-}" in
    profile-minimal | profile-nas-safe | profile-hpc-rdma | chaos | release)
        run_named "$1"
        exit $?
        ;;
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
        echo "用法：scripts/accept.sh M0 | M1 | … | M15 | all | profile-minimal | profile-nas-safe | profile-hpc-rdma | chaos | release"
        exit 2
        ;;
esac
