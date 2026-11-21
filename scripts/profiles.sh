#!/usr/bin/env bash
# profile 测试（design.md 9.7）：只承诺三个 profile 通过全套测试。
# 用法：scripts/profiles.sh minimal|nas-safe|hpc-rdma [--full]
#   不带 --full：各里程碑的阶段版（ADR-0015：M6 起跑 minimal、M9 起跑 nas-safe；scripts/accept.d/m6.sh … m14.sh 调用）；没有镜像
#   wmfs-tools 时容器里的外部测试显示 SKIP；
#   --full：发布门槛的全套（M15，scripts/accept.sh profile-* 与 release 调用）——容器里另跑完整的 pjdfstest、fsx 10 万次、fsstress、
#   IOR/mdtest 冒烟（ADR-0015 第 1 条）。它们是发布门槛：没有镜像就算失败（✗，先 scripts/build-tools-image.sh），不是 SKIP。
#
#   minimal（1 节点、TCP、default 类 1 副本、不写 replicas 属性、停用 quota 与 mclock，wmfs dev up --profile minimal）：
#     ① cargo 的 profile_minimal（tests/profiles/minimal.rs：自己起集群、挂载、目录与文件、链接、xattr、截断、inline 与 chunk 文件、
#        属性 set/get 与用量、quota/qos 被拒绝、kill -9 / 重启后 fsync 过的数据完整）；
#     ② 再起一个同样的部署，在 wmfs-tools 容器里跑：fio verify；pjdfstest（阶段版：不涉及 chown/权限的子集；--full：完整套件）；
#        fsx（阶段版 1 万次；--full：10 万次，另加 -l 65536 的 inline 一轮 1 万次）；--full 另有 fsstress、IOR、mdtest。
#   nas-safe（3 节点、replicas=3、/home trash=7d、audit=on，集群起好之后起一个 mgr）：
#     ① tests/profiles/nas_safe.rs（写入中依次 kill -9 每个节点、fsync 过的文件全部完好、回收站恢复、②–⑤ 经两个挂载点、scrub 修复、
#        mgr 重启后审计日志完整）；
#     ② 再起一个 nas-safe 部署跑 ⑦ Jepsen-lite（tests/it/jepsen_register.rs）：单个 chunk 的寄存器在随机的捣乱（kill -9、SIGSTOP、
#        提升里随机挑，约 8 s、一两次）下线性一致、客户端除重试外没有错误；--full 另跑经两个挂载点的 strong 文件寄存器（阶段版不跑：
#        scripts/accept.d/m12.sh 自己跑它）。那里另有一个 drain 用例，drain 撤销不了，只由 scripts/accept.d/m10.sh 最后跑。之后等链全部
#        Serving、跑不变式检查器（tests/it/replica_check.rs）；
#     ③ --full：在同一个部署上，容器里跑 fio verify、fsx 10 万次、fsstress、IOR、mdtest。
#   hpc-rdma：要 RDMA 设备（wmfs doctor 的 rdma 一项有 ACTIVE 的 RoCE v2 IPv4 GID；调用方已经查过的话经环境变量 WMFS_RDMA_IP 给出）；
#     本机没有，显示 SKIP——未在本机验证，发布说明不得声称支持它（semantics.md 第 10 节）。有设备时跑 tests/profiles/hpc_rdma.rs；
#     --full 另起一个 hpc-rdma 部署，容器里跑 fsx 10 万次与 fsstress（容器里的客户端走 tcp：把 RDMA 设备与 rdma-core 带进容器首发不做，
#     design.md 9.7 的 M15 注记）。
# 每个 cargo 用例都确认它真的跑了（"1 passed"）。输出：每项一行 ✓ / ✗ / SKIP，最后一行 "<profile> PASS" / "<profile> FAIL" /
# "<profile> SKIP（…）"。用开发集群的端口 19n00/19n80（要空闲；nas-safe 另用 mgr 的 19090），开发目录是临时的：用完、失败、超时或
# 按 Ctrl-C 之后都 wmfs dev down --purge（先卸载挂载、停 mgr，再停节点），不留挂载与进程。有一项失败时，先把各部署的日志（节点、mgr、
# 挂载的 *.log；挂载的日志在卸载之前就拷）拷进环境变量 WMFS_PROFILE_LOGS 指的目录（scripts/accept.sh 给的；没给就不拷），再删开发目录
# （roadmap §0 第 2 条）。
set -u
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT" || exit 1
export PATH="$ROOT/target/release:$PATH"
export no_proxy=127.0.0.1,localhost NO_PROXY=127.0.0.1,localhost
unset WMFS_GOLDEN_UPDATE WMFS_FAILPOINTS

PROFILE="${1:-}"
FULL=0
[ "${2:-}" = --full ] && FULL=1
fail=0
DIRS="" # 起过的开发目录：退出时（包括失败、Ctrl-C）一律 wmfs dev down --purge
LOG="$(mktemp "${TMPDIR:-/tmp}/wmfs-profile-$PROFILE-XXXX.log")"
cleanup() {
    local d
    for d in $DIRS; do WMFS_DEV_DIR="$d" wmfs dev down --purge >/dev/null 2>&1; done
    rm -f "$LOG"
}
trap cleanup EXIT
ok() { echo "✓ $1"; }
bad() {
    echo "✗ $1"
    fail=1
}

# newdev <名字>：一个新的临时开发目录，放进 DEV 并导出为 WMFS_DEV_DIR（记进 DIRS：退出时兜底清理）
newdev() {
    DEV="$(mktemp -d "${TMPDIR:-/tmp}/wmfs-profile-$1-XXXX")"
    DIRS="$DIRS $DEV"
    export WMFS_DEV_DIR="$DEV"
}

# keep_logs <开发目录>：已经有一项失败时，把它的日志（节点、mgr、挂载的 *.log，不拷 target 里的数据）拷进 $WMFS_PROFILE_LOGS。
# 挂载的 mount.log 在 wmfs dev umount 时随记录目录一起删：卸载之前先调用它（jepsen_fuse）。
keep_logs() {
    if [ "$fail" = 1 ] && [ -n "${WMFS_PROFILE_LOGS:-}" ] && [ -d "$1" ]; then
        local to
        to="$WMFS_PROFILE_LOGS/$(basename "$1")"
        mkdir -p "$to" && (cd "$1" && find . -name '*.log' -not -path '*/targets/*' -exec cp --parents {} "$to/" \;)
    fi
}

# done_dev <开发目录>：这个部署用完了：先 keep_logs，再 purge。
done_dev() {
    keep_logs "$1"
    WMFS_DEV_DIR="$1" wmfs dev down --purge >/dev/null 2>&1
}

# bounded <秒> <命令…>：带总期限地跑一个命令（到期时 timeout 连同它起的子进程一起结束）。timeout 把命令放进它自己的进程组，终端的
# Ctrl-C 到不了那里：这里把收到的 SIGINT 转给 timeout（它再转给整个进程组），等它们都结束，然后以 130 结束本脚本（EXIT 时照常清理）。
# 同 scripts/accept.d/m15.sh 的 m15_bounded。
bounded() {
    local secs="$1" pid rc stop=0
    shift
    timeout "$secs" "$@" &
    pid=$!
    trap 'stop=1; kill -INT "$pid" 2>/dev/null' INT
    wait "$pid"
    rc=$?
    while kill -0 "$pid" 2>/dev/null; do # wait 被 Ctrl-C 打断了：等 timeout 收拾完
        wait "$pid"
        rc=$?
    done
    trap - INT
    if [ "$stop" = 1 ]; then
        echo "✗ 被 Ctrl-C 打断"
        exit 130
    fi
    return "$rc"
}

# one <期限秒> <cargo test 的参数…>：带期限地跑（应当恰好）一个用例，输出写进 $LOG；还要确认它真的跑了——名字写错、#[ignore] 被误删时
# cargo 也退出 0，只是 0 个用例（同 scripts/accept.sh 的 one_test）
one() {
    local secs="$1"
    shift
    bounded "$secs" cargo test --release "$@" >"$LOG" 2>&1 && grep -q 'test result: ok. 1 passed' "$LOG"
}

# check_one "说明" "失败时的说明" <期限秒> <cargo test 的参数…>
check_one() {
    local desc="$1" fdesc="$2"
    shift 2
    if one "$@"; then
        ok "$desc"
    else
        tail -30 "$LOG"
        bad "$fdesc"
    fi
}

# suite "说明" <container-tests.sh 的参数…>：在容器里跑一组外部测试（对着 WMFS_DEV_DIR 的开发集群）
suite() {
    local desc="$1"
    shift
    if scripts/container-tests.sh "$@" >"$LOG" 2>&1; then
        ok "$desc"
    else
        tail -30 "$LOG"
        bad "$desc（scripts/container-tests.sh $*）"
    fi
}

# 容器里的全套（--full）：fsx 10 万次、fsstress、IOR、mdtest
full_suites() {
    suite "容器内 fsx 10 万次无不一致" fsx 100000
    suite "容器内 fsstress（4 个进程 × 1 万次操作）冒烟：跑完目录树能遍历、能删干净" fsstress 10000
    suite "容器内 IOR 冒烟：easy（每个 rank 一个文件）+ hard（共享文件、47008 字节交错），mpirun -np 8，写完读回校验" ior
    suite "容器内 mdtest 冒烟：easy（各自目录）+ hard（共享目录、每文件 3901 字节），mpirun -np 8" mdtest
}

have_image() { docker image inspect wmfs-tools >/dev/null 2>&1; }
NO_IMAGE="没有镜像 wmfs-tools，先运行 scripts/build-tools-image.sh（要联网）"

# no_image "说明"：没有镜像——阶段版显示 SKIP；--full（发布门槛）算失败
no_image() {
    if [ "$FULL" = 1 ]; then
        bad "$1（$NO_IMAGE；发布门槛不能缺这些测试）"
    else
        echo "SKIP $1（原因：$NO_IMAGE）"
    fi
}

# ⑦ 的 FUSE 版本（9.5）：strong 目录里的一个文件经两个挂载点当寄存器（--full 跑：发布门槛；阶段版由 scripts/accept.d/m12.sh 自己跑）
jepsen_fuse() {
    local ma="$DEV/ma" mb="$DEV/mb" t=a_strong_file_through_two_mounts_stays_linearizable_while_nodes_die_and_freeze
    eval "$(wmfs dev env)"
    mkdir -p "$ma" "$mb"
    if wmfs dev mount "$ma" >/dev/null && wmfs dev mount "$mb" >/dev/null && mkdir "$ma/home/shared" &&
        wmfs set /home/shared consistency=strong --json >/dev/null; then
        WMFS_JEPSEN_FILES="$ma/home/shared/reg,$mb/home/shared/reg" check_one \
            "⑦ Jepsen-lite（FUSE）：strong 目录里的一个文件经两个挂载点当寄存器，在随机 kill -9 / SIGSTOP 下线性一致、读写没有错误" \
            "Jepsen-lite（FUSE，cargo test -p wmfs-testkit --test jepsen_register $t -- --exact --ignored）" \
            600 -p wmfs-testkit --test jepsen_register "$t" -- --exact --ignored --nocapture
    else
        bad "⑦ Jepsen-lite（FUSE）的准备：两个 wmfs dev mount、strong 目录 /home/shared"
    fi
    keep_logs "$DEV" # 失败时两个挂载的 mount.log 也留下（卸载会删掉它们）
    wmfs dev umount "$ma" >/dev/null 2>&1
    wmfs dev umount "$mb" >/dev/null 2>&1
}

case "$PROFILE" in
minimal)
    check_one "冒烟：目录与文件、改名、链接、xattr、截断、inline 与 chunk 文件、目录属性 set/get 与递归用量、quota/qos 被拒绝（没有插件消费）、kill -9 / 重启后 fsync 过的数据完整" \
        "冒烟（cargo test -p wmfs --test profile_minimal -- --ignored）" 900 -p wmfs --test profile_minimal -- --ignored
    if have_image; then
        newdev minimal
        if wmfs dev up --nodes 1 --profile minimal >/dev/null && wmfs dev wait leader --timeout 30 >/dev/null; then
            suite "容器内 fio 写 + verify=crc32c" fio
            if [ "$FULL" = 1 ]; then
                suite "容器内完整的 pjdfstest（全部目录、每个测试文件的用例数与计划相符；失败都在预期清单里，每条对应 semantics.md 的一条放松）" pjdfstest-all
                suite "容器内 fsx -l 65536（文件全程 inline、内容常比长度短）1 万次无不一致" fsx 10000 -l 65536
                full_suites
            else
                suite "容器内 pjdfstest 不涉及 chown/权限的子集" pjdfstest
                suite "容器内 fsx 1 万次" fsx 10000
            fi
        else
            bad "起 minimal 部署（wmfs dev up --profile minimal）"
        fi
        done_dev "$DEV"
    else
        no_image "容器内的外部测试"
    fi
    ;;
nas-safe)
    # 两部分都有总期限（timeout 连同测试起的节点、挂载进程与 mgr 一起结束），超时或失败之后照样 wmfs dev down --purge 兜底：不留进程、挂载与目录。
    newdev nas-safe
    WMFS_PROFILE_DIR="$DEV" check_one "写入中依次 kill -9 每个节点：客户端 0 个错误；fsync 过的文件全部完好；节点回来之后先 Syncing、后 Serving，副本差距归零；回收站恢复；②–⑤（rename 原子、strong 追加、F_SETLKW 锁互斥各 1000 次、配额上界）；scrub 修好注入的损坏；mgr 重启后审计日志完整" \
        "nas-safe 的必过测试（cargo test -p wmfs --test profile_nas_safe -- --ignored）" 1800 -p wmfs --test profile_nas_safe -- --ignored --nocapture
    done_dev "$DEV"
    newdev dev
    JEPSEN=a_register_on_one_chunk_stays_linearizable_while_nodes_die_and_freeze
    if wmfs dev up --nodes 3 --profile nas-safe >/dev/null; then
        check_one "⑦ Jepsen-lite：寄存器（单个 chunk）在随机的捣乱（kill -9、SIGSTOP、提升里随机挑，约 8 s）下线性一致，客户端除重试外没有错误" \
            "Jepsen-lite（cargo test -p wmfs-testkit --test jepsen_register $JEPSEN -- --exact --ignored）" \
            600 -p wmfs-testkit --test jepsen_register "$JEPSEN" -- --exact --ignored --nocapture
        [ "$FULL" = 1 ] && jepsen_fuse
    else
        bad "起 nas-safe 部署（wmfs dev up --nodes 3 --profile nas-safe）"
    fi
    # 捣乱过后（节点回来、重同步期间寄存器一直在写）：链全部 Serving 之后所有副本一致
    if wmfs dev wait chains-serving --timeout 120 >"$LOG" 2>&1 &&
        cargo test --release -p wmfs-testkit --test replica_check -- --ignored >>"$LOG" 2>&1 &&
        grep -q 'test result: ok. 1 passed' "$LOG"; then
        ok "不变式检查器：捣乱之后所有 Serving 副本的已提交版本与内容一致"
    else
        tail -30 "$LOG"
        bad "不变式检查器（cargo test -p wmfs-testkit --test replica_check -- --ignored）"
    fi
    if [ "$FULL" = 1 ]; then
        if have_image; then
            suite "容器内 fio 写 + verify=crc32c（3 副本）" fio
            full_suites
        else
            no_image "容器内的外部测试"
        fi
    fi
    done_dev "$DEV"
    ;;
hpc-rdma)
    ip="${WMFS_RDMA_IP-$(wmfs doctor --json 2>/dev/null |
        jq -r '.checks[] | select(.id == "rdma" and .status == "ok") | .data.gids[]? | select(.active) | .ipv4' | head -1)}"
    if [ -z "$ip" ]; then
        echo "SKIP hpc-rdma（原因：本机没有可用的 RDMA 设备，人类决定本机不做 RDMA 实机测试——未在本机验证，发布说明不得声称支持这个 profile）"
        echo "hpc-rdma SKIP（未在本机验证）"
        exit 0
    fi
    newdev hpc-rdma
    WMFS_PROFILE_DIR="$DEV" check_one "hpc-rdma（RDMA $ip）：IOR hard 形状的负载中 kill leader 与一个数据节点 0 错误、两个租户的 QoS（l= 上限的指标）、配额并发、strong 追加、锁互斥、数据路径走 RDMA（服务端的 RDMA READ 与 WRITE）" \
        "hpc-rdma（cargo test -p wmfs --test profile_hpc_rdma -- --ignored）" 1800 -p wmfs --test profile_hpc_rdma -- --ignored --nocapture
    done_dev "$DEV"
    if [ "$FULL" = 1 ]; then
        if have_image; then
            newdev dev
            if wmfs dev up --nodes 3 --profile hpc-rdma --rdma "$ip" >/dev/null; then
                suite "容器内 fsx 10 万次无不一致（hpc-rdma 部署；容器里的客户端走 tcp）" fsx 100000
                suite "容器内 fsstress（4 个进程 × 1 万次操作）冒烟（hpc-rdma 部署；容器里的客户端走 tcp）" fsstress 10000
            else
                bad "起 hpc-rdma 部署（wmfs dev up --nodes 3 --profile hpc-rdma --rdma $ip）"
            fi
            done_dev "$DEV"
        else
            no_image "容器内的 fsx 与 fsstress"
        fi
    fi
    ;;
*)
    echo "用法：scripts/profiles.sh minimal | nas-safe | hpc-rdma [--full]"
    exit 2
    ;;
esac
if [ "$fail" -eq 0 ]; then
    echo "$PROFILE PASS"
else
    echo "$PROFILE FAIL"
fi
exit "$fail"
