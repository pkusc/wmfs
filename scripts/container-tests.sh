#!/usr/bin/env bash
# 在 wmfs-tools 容器里跑外部测试（design.md 9.6）：客户端在容器里以 root 挂载（--device /dev/fuse --cap-add SYS_ADMIN
# --security-opt apparmor=unconfined --network host；宿主机的 target/release/wmfs 与 client key 只读挂进去），服务端是宿主机上
# 正在运行的开发集群（WMFS_DEV_DIR，默认 ~/wmfs-dev；node1 = 127.0.0.1:19100）。
# 用法：scripts/container-tests.sh pjdfstest   不涉及 chown/权限的子集；失败与 tests/pjdfstest-expected.txt 逐条比对，出现清单外的
#                                              失败即不通过（清单里每一条都对应 semantics.md 的一条放松）
#       scripts/container-tests.sh fsx [N [参数…]]  xfstests 的 fsx，N 次操作（默认 10000），其余参数原样交给 fsx（例如 `-l 65536`）
#       scripts/container-tests.sh fio         fio 写 + verify=crc32c
# 退出码：0 通过；1 不通过；3 没有镜像 wmfs-tools（先 scripts/build-tools-image.sh；调用方显示 SKIP）。
# 只起一次性的 wmfs-suite-* 容器（--rm），不碰机器上别的容器与镜像。
set -u
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SUITE="${1:-}"
shift || true
DEV="${WMFS_DEV_DIR:-$HOME/wmfs-dev}"
docker image inspect wmfs-tools >/dev/null 2>&1 || { echo "没有镜像 wmfs-tools（先 scripts/build-tools-image.sh）"; exit 3; }
KEY="$DEV/n1/keys/client.key"
[ -r "$KEY" ] || { echo "读不到 $KEY：先 wmfs dev up"; exit 1; }
OUT="$(mktemp)"
docker run --rm --name "wmfs-suite-$SUITE-$$" --device /dev/fuse --cap-add SYS_ADMIN --security-opt apparmor=unconfined \
    --network host -v "$ROOT/target/release/wmfs:/usr/local/bin/wmfs:ro" -v "$KEY:/root/.config/wmfs/client.key:ro" \
    -v "$ROOT/tests/docker:/opt/wmfs-tests:ro" wmfs-tools /opt/wmfs-tests/run-suite.sh "$SUITE" "$@" | tee "$OUT"
rc=${PIPESTATUS[0]}
if [ "$SUITE" = pjdfstest ]; then
    expected="$(grep -vE '^[[:space:]]*(#|$)' "$ROOT/tests/pjdfstest-expected.txt" | awk '{print $1}' | sort -u)"
    got="$(awk '$1 == "FAIL" {print $2}' "$OUT" | sort -u)"
    extra="$(comm -13 <(printf '%s\n' "$expected") <(printf '%s\n' "$got") | grep -v '^$')"
    gone="$(comm -23 <(printf '%s\n' "$expected") <(printf '%s\n' "$got") | grep -v '^$')"
    grep -q '^SUMMARY ' "$OUT" || { echo "pjdfstest 没有跑完"; rc=1; }
    if [ -n "$extra" ]; then
        echo "清单外的失败（不通过）："; printf '%s\n' "$extra"; rc=1
    fi
    [ -n "$gone" ] && { echo "清单里的这些现在通过了（可以从 tests/pjdfstest-expected.txt 删掉）："; printf '%s\n' "$gone"; }
fi
rm -f "$OUT"
exit "$rc"
