#!/usr/bin/env bash
# profile 测试（design.md 9.7）：只承诺三个 profile 通过全套测试；功能是一个个里程碑长出来的，所以按阶段收紧（M6 起跑 minimal）。
# 用法：scripts/profiles.sh minimal
#   minimal（1 节点、TCP、default 类 1 副本、不写 replicas 属性、停用 quota 与 mclock，wmfs dev up --profile minimal）：
#     ① cargo 的 profile_minimal 冒烟（tests/profiles/minimal.rs：自己起集群、挂载、inline 与 chunk 文件、kill -9 / 重启后 fsync 过的
#        数据完整……）；
#     ② 再起一个同样的部署，在 wmfs-tools 容器里跑 fio verify、pjdfstest 子集、fsx（没有镜像 → SKIP，不算通过也不算失败）。
#     M7 起冒烟里有目录属性的 set/get 与递归用量。
#   nas-safe（M9 起的阶段版：3 节点、replicas=3）：① tests/profiles/nas_safe.rs（写入中依次 kill -9 每个节点、fsync 过的文件全部完好）；
#     ② 再起一个 nas-safe 部署跑 Jepsen-lite（tests/it/jepsen_register.rs：寄存器在随机 kill / SIGSTOP 下线性一致），之后等链全部 Serving、
#     跑不变式检查器（tests/it/replica_check.rs）。
#   hpc-rdma：本机没有 RDMA 设备，显示 SKIP（不算通过，也不算失败）。
# 输出：每项一行 ✓ / ✗ / SKIP，最后一行 "minimal PASS" 或 "minimal FAIL"。用开发集群的端口 19100/19180（要空闲），开发目录是临时的，
# 结束时 wmfs dev down --purge（先卸载挂载、再停节点），不留挂载与进程。
set -u
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT" || exit 1
export PATH="$ROOT/target/release:$PATH"
export no_proxy=127.0.0.1,localhost NO_PROXY=127.0.0.1,localhost
unset WMFS_GOLDEN_UPDATE WMFS_FAILPOINTS

PROFILE="${1:-}"
fail=0
LOG="$(mktemp "${TMPDIR:-/tmp}/wmfs-profile-$PROFILE-XXXX.log")"
trap 'rm -f "$LOG"' EXIT
ok() { echo "✓ $1"; }
bad() {
    echo "✗ $1"
    fail=1
}

case "$PROFILE" in
minimal)
    if cargo test --release -p wmfs --test profile_minimal -- --ignored >"$LOG" 2>&1; then
        ok "冒烟：目录与文件、改名、链接、截断、inline 与 chunk 文件、目录属性 set/get 与递归用量、kill -9 / 重启后 fsync 过的数据完整"
    else
        tail -30 "$LOG"
        bad "冒烟（cargo test -p wmfs --test profile_minimal -- --ignored）"
    fi
    if docker image inspect wmfs-tools >/dev/null 2>&1; then
        dev="$(mktemp -d "${TMPDIR:-/tmp}/wmfs-profile-dev-XXXX")"
        export WMFS_DEV_DIR="$dev"
        if wmfs dev up --nodes 1 --profile minimal >/dev/null && wmfs dev wait leader --timeout 30 >/dev/null; then
            scripts/container-tests.sh fio >/dev/null && ok "容器内 fio 写 + verify=crc32c" || bad "容器内 fio verify"
            scripts/container-tests.sh pjdfstest >/dev/null && ok "容器内 pjdfstest 不涉及 chown/权限的子集" || bad "容器内 pjdfstest 子集"
            scripts/container-tests.sh fsx 10000 >/dev/null && ok "容器内 fsx 1 万次" || bad "容器内 fsx"
        else
            bad "起 minimal 部署（wmfs dev up --profile minimal）"
        fi
        wmfs dev down --purge >/dev/null 2>&1
    else
        echo "SKIP 容器内 fio verify、pjdfstest 子集、fsx（原因：没有镜像 wmfs-tools，先运行 scripts/build-tools-image.sh）"
    fi
    ;;
nas-safe)
    # M9–M11 的阶段版（design.md 9.7）：3 节点、TCP、/home replicas=3（trash、audit 在 M12 起）。两部分都有总期限（timeout 连同测试起的
    # 节点与挂载进程一起结束），超时或失败之后照样 wmfs dev down --purge 兜底：不留进程、挂载与目录。
    pdir="$(mktemp -d "${TMPDIR:-/tmp}/wmfs-profile-nas-safe-XXXX")"
    if WMFS_PROFILE_DIR="$pdir" timeout 1200 cargo test --release -p wmfs --test profile_nas_safe -- --ignored --nocapture \
        >"$LOG" 2>&1; then
        ok "写入中依次 kill -9 每个节点：客户端 0 个错误；fsync 过的文件全部完好；节点回来之后先 Syncing、后 Serving，副本差距归零"
    else
        tail -30 "$LOG"
        bad "单点故障无错误 + 已确认写不丢（cargo test -p wmfs --test profile_nas_safe -- --ignored）"
    fi
    WMFS_DEV_DIR="$pdir" wmfs dev down --purge >/dev/null 2>&1
    dev="$(mktemp -d "${TMPDIR:-/tmp}/wmfs-profile-dev-XXXX")"
    export WMFS_DEV_DIR="$dev"
    if wmfs dev up --nodes 3 --profile nas-safe >/dev/null &&
        timeout 600 cargo test --release -p wmfs-testkit --test jepsen_register -- --ignored --nocapture >"$LOG" 2>&1; then
        ok "Jepsen-lite：寄存器（单个 chunk）在随机 kill -9 / SIGSTOP 下线性一致"
    else
        tail -30 "$LOG"
        bad "Jepsen-lite（cargo test -p wmfs-testkit --test jepsen_register -- --ignored）"
    fi
    # 捣乱过后（节点回来、重同步期间寄存器一直在写）：链全部 Serving 之后所有副本一致
    if wmfs dev wait chains-serving --timeout 120 >"$LOG" 2>&1 &&
        cargo test --release -p wmfs-testkit --test replica_check -- --ignored >>"$LOG" 2>&1; then
        ok "不变式检查器：捣乱之后所有 Serving 副本的已提交版本与内容一致"
    else
        tail -30 "$LOG"
        bad "不变式检查器（cargo test -p wmfs-testkit --test replica_check -- --ignored）"
    fi
    wmfs dev down --purge >/dev/null 2>&1
    ;;
hpc-rdma)
    echo "SKIP hpc-rdma（原因：本机没有可用的 RDMA 设备，人类决定本机不做 RDMA 实机测试）"
    exit 0
    ;;
*)
    echo "用法：scripts/profiles.sh minimal | nas-safe | hpc-rdma"
    exit 2
    ;;
esac
if [ "$fail" -eq 0 ]; then
    echo "$PROFILE PASS"
else
    echo "$PROFILE FAIL"
fi
exit "$fail"
