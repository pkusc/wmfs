#!/usr/bin/env bash
# 在 wmfs-tools 容器里跑外部测试（design.md 9.6）：客户端在容器里以 root 挂载（--device /dev/fuse --cap-add SYS_ADMIN
# --security-opt apparmor=unconfined --network host；宿主机的 target/release/wmfs 与 client key 只读挂进去），服务端是宿主机上
# 正在运行的开发集群（WMFS_DEV_DIR，默认 ~/wmfs-dev；node1 = 127.0.0.1:19100）。
# 用法：scripts/container-tests.sh pjdfstest   不涉及 chown/权限的子集；失败与 tests/pjdfstest-expected.txt 逐条比对，出现清单外的
#                                              失败即不通过（清单里每一条都对应 semantics.md 的一条放松）
#       scripts/container-tests.sh pjdfstest-perm  M10：权限相关的子集（切换用户的用例与 chmod、chown；挂载带 --allow-other），
#                                              同样与 tests/pjdfstest-perm-expected.txt 逐条比对
#       scripts/container-tests.sh pjdfstest-all  M15：完整的 pjdfstest（全部目录、全部测试文件；挂载带 --allow-other），与两份清单
#                                              的并集逐条比对（完整套件 = 上面两个子集 + 只测 FreeBSD、在 Linux 上自己跳过的三个目录）
#       三种 pjdfstest 都要求：跑到了用例（SUMMARY 的 files、tests 不是 0；每个测试文件的计划 1..N 由 run-suite.sh 核对），清单的
#       每一行都写明对应 semantics.md 的哪一条（`<目录>/<文件>:<编号>  # semantics.md …`）
#       scripts/container-tests.sh fsx [N [参数…]]  xfstests 的 fsx，N 次操作（默认 10000），其余参数原样交给 fsx（例如 `-l 65536`）
#       scripts/container-tests.sh fsstress [N]  M15：fsstress（4 个进程、各 N 次操作，默认 10000）冒烟
#       scripts/container-tests.sh ior | mdtest  M15：IOR / mdtest 冒烟（easy + hard，mpirun -np 8）
#       scripts/container-tests.sh fio         fio 写 + verify=crc32c
#       scripts/container-tests.sh pathmap     M7：容器里 root 挂载（类型 fuse）时 CLI 的挂载点路径换算
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
lists=()
case "$SUITE" in
pjdfstest | pjdfstest-perm) lists=("$ROOT/tests/$SUITE-expected.txt") ;;
pjdfstest-all) lists=("$ROOT/tests/pjdfstest-expected.txt" "$ROOT/tests/pjdfstest-perm-expected.txt") ;;
esac
if [ "${#lists[@]}" -gt 0 ]; then
    # 清单的每一行都要写明对应 semantics.md 的哪一条放松（`<目录>/<文件>:<编号>  # semantics.md …`），不然不算数
    bare="$(cat "${lists[@]}" | grep -vE '^[[:space:]]*(#|$)' | grep -vE '#.*semantics\.md')"
    if [ -n "$bare" ]; then
        echo "预期失败清单里这些行没写对应 semantics.md 的哪一条（格式：<目录>/<文件>:<编号>  # semantics.md …）："
        printf '%s\n' "$bare"
        rc=1
    fi
    expected="$(cat "${lists[@]}" | grep -vE '^[[:space:]]*(#|$)' | awk '{print $1}' | sort -u)"
    got="$(awk '$1 == "FAIL" {print $2}' "$OUT" | sort -u)"
    extra="$(comm -13 <(printf '%s\n' "$expected") <(printf '%s\n' "$got") | grep -v '^$')"
    gone="$(comm -23 <(printf '%s\n' "$expected") <(printf '%s\n' "$got") | grep -v '^$')"
    summary="$(grep '^SUMMARY ' "$OUT" | tail -1)"
    if [ -z "$summary" ]; then
        echo "pjdfstest 没有跑完"
        rc=1
    elif printf '%s\n' "$summary" | grep -qE 'files=0 |tests=0 '; then
        echo "pjdfstest 一个用例也没跑：$summary"
        rc=1
    fi
    if [ -n "$extra" ]; then
        echo "清单外的失败（不通过）："; printf '%s\n' "$extra"; rc=1
    fi
    [ -n "$gone" ] && { echo "清单里的这些现在通过了（可以从 ${lists[*]#"$ROOT/"} 里删掉）："; printf '%s\n' "$gone"; }
fi
rm -f "$OUT"
exit "$rc"
