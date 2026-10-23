#!/usr/bin/env bash
# profile 测试（design.md 9.7）：只承诺三个 profile 通过全套测试；功能是一个个里程碑长出来的，所以按阶段收紧（M6 起跑 minimal）。
# 用法：scripts/profiles.sh minimal
#   minimal（1 节点、TCP、default 类 1 副本、不写 replicas 属性、停用 quota 与 mclock，wmfs dev up --profile minimal）：
#     ① cargo 的 profile_minimal 冒烟（tests/profiles/minimal.rs：自己起集群、挂载、inline 与 chunk 文件、kill -9 / 重启后 fsync 过的
#        数据完整……）；
#     ② 再起一个同样的部署，在 wmfs-tools 容器里跑 fio verify、pjdfstest 子集、fsx（没有镜像 → SKIP，不算通过也不算失败）。
#     属性 set/get 在 M7 加入。
#   nas-safe（M9 起）、hpc-rdma（有 Soft-RoCE 时）还没有。
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
ok() { echo "✓ $1"; }
bad() {
    echo "✗ $1"
    fail=1
}

case "$PROFILE" in
minimal)
    if cargo test --release -p wmfs --test profile_minimal -- --ignored >/tmp/wmfs-profile-$$.log 2>&1; then
        ok "冒烟：目录与文件、改名、链接、截断、inline 与 chunk 文件、kill -9 / 重启后 fsync 过的数据完整"
    else
        tail -30 /tmp/wmfs-profile-$$.log
        bad "冒烟（cargo test -p wmfs --test profile_minimal -- --ignored）"
    fi
    rm -f /tmp/wmfs-profile-$$.log
    echo "SKIP 属性 set/get（原因：目录属性在 M7）"
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
nas-safe | hpc-rdma)
    echo "$PROFILE 从 M9 起才有（design.md 9.7 的分阶段执行）"
    exit 2
    ;;
*)
    echo "用法：scripts/profiles.sh minimal"
    exit 2
    ;;
esac
if [ "$fail" -eq 0 ]; then
    echo "$PROFILE PASS"
else
    echo "$PROFILE FAIL"
fi
exit "$fail"
