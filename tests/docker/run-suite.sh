#!/bin/bash
# 在 wmfs-tools 容器里跑一组外部测试（design.md 9.6），由宿主机上的 scripts/container-tests.sh 拉起：
# 先以 root 把宿主机的开发集群挂到 /mnt/wmfs（wmfs 二进制与 client key 是只读挂进来的），跑完卸载。
#   run-suite.sh pjdfstest      不涉及 chown/权限的子集：12 个目录里没有切换到别的用户（-u/-g）的测试文件；每个失败打印一行
#                               "FAIL <目录>/<文件>:<编号> <说明>"，最后 "SUMMARY files=… tests=… failed=…"
#   run-suite.sh fsx <次数> [fsx 的其余参数…]   xfstests 的 fsx（随机读写、截断、mmap，与内存模型比对）；例如 `-l 65536`：文件一直
#                               不超过 64 KiB（全程 inline，内容常比长度短）
#   run-suite.sh fio            fio 顺序写 + verify=crc32c（冒烟）
set -u
SUITE="${1:-}"
MNT=/mnt/wmfs
mkdir -p "$MNT"
wmfs mount 127.0.0.1:19100 "$MNT" --log /tmp/mount.log >/tmp/mount.out 2>&1 &
# 以 root 直接挂载（不经 fusermount3）时类型显示为 fuse（没有 .wmfs 子类型），来源仍是 wmfs:<集群号>
mounted() { grep -qE " $MNT .* - fuse(\.wmfs)? wmfs:" /proc/self/mountinfo; }
for _ in $(seq 1 300); do
    mounted && break
    sleep 0.1
done
if ! mounted; then
    echo "MOUNT-FAILED"; cat /tmp/mount.out /tmp/mount.log 2>/dev/null; exit 2
fi
WORK="$MNT/suite-$SUITE-$$"
mkdir -p "$WORK"
cd "$WORK" || exit 2
rc=0
case "$SUITE" in
pjdfstest)
    files=0; tests=0; failed=0
    for d in ftruncate link mkdir mkfifo mknod open rename rmdir symlink truncate unlink utimensat; do
        for t in /opt/pjdfstest/tests/$d/*.t; do
            grep -qE -- '-[ug] ([0-9]|\$)' "$t" && continue # 切换到别的用户（-u 65534、-u $UID_NOBODY）= 权限测试，不在子集里
            files=$((files + 1))
            out="$(bash "$t" 2>/dev/null)"
            n=$(printf '%s\n' "$out" | grep -cE '^(not )?ok ')
            tests=$((tests + n))
            while IFS= read -r line; do
                num="$(printf '%s' "$line" | sed -E 's/^not ok ([0-9]+).*/\1/')"
                echo "FAIL $d/$(basename "$t"):$num ${line#not ok $num}"
                failed=$((failed + 1))
            done < <(printf '%s\n' "$out" | grep -E '^not ok [0-9]+' | grep -v '# TODO')
        done
    done
    echo "SUMMARY files=$files tests=$tests failed=$failed"
    ;;
fsx)
    n="${2:-10000}"
    shift 2 2>/dev/null || shift $#
    /opt/xfstests/ltp/fsx -q -N "$n" -S 42 "$@" fsx.data; rc=$?
    echo "FSX-RC $rc"
    ;;
fio)
    fio --name=verify --filename=fio.data --size=256M --bs=1M --rw=write --verify=crc32c --do_verify=1 \
        --ioengine=psync --output-format=terse >/tmp/fio.out 2>&1; rc=$?
    echo "FIO-RC $rc"
    ;;
*)
    echo "不认识的测试组：$SUITE"; rc=2
    ;;
esac
cd /
rm -rf "$WORK"
fusermount3 -u "$MNT" 2>/dev/null || umount "$MNT" 2>/dev/null || umount -l "$MNT"
wait
exit "$rc"
