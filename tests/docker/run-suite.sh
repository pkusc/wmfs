#!/bin/bash
# 在 wmfs-tools 容器里跑一组外部测试（design.md 9.6），由宿主机上的 scripts/container-tests.sh 拉起：
# 先以 root 把宿主机的开发集群挂到 /mnt/wmfs（wmfs 二进制与 client key 是只读挂进来的），跑完卸载。
#   run-suite.sh pjdfstest      不涉及 chown/权限的子集：12 个目录里没有切换到别的用户（-u/-g）的测试文件；每个失败打印一行
#                               "FAIL <目录>/<文件>:<编号> <说明>"，最后 "SUMMARY files=… tests=… failed=…"。每个测试文件打印的计划
#                               "1..N" 也核对：没打印计划、或者少跑了的用例（测试文件中途退出），缺的编号各记一条 FAIL
#   run-suite.sh pjdfstest-perm M10：权限相关的子集——上面 12 个目录里切换用户的测试文件，加上 chmod 与 chown 两个目录的全部；
#                               挂载带 --allow-other（只在容器里：别的用户要能进这个挂载）。输出格式同上
#   run-suite.sh fsx <次数> [fsx 的其余参数…]   xfstests 的 fsx（随机读写、截断、mmap，与内存模型比对）；例如 `-l 65536`：文件一直
#                               不超过 64 KiB（全程 inline，内容常比长度短）
#   run-suite.sh pjdfstest-all  M15：完整的 pjdfstest——tests/ 下的全部目录、全部测试文件（= 上面两个子集，加上 chflags、granular、
#                               posix_fallocate 三个还没跑过的目录：它们只测 FreeBSD，在 Linux 上自己 quick_exit），挂载带 --allow-other
#   run-suite.sh fio            fio 顺序写 + verify=crc32c（冒烟）
#   run-suite.sh fsstress [次数]  M15：xfstests 的 fsstress（4 个进程各做这么多次随机的元数据与数据操作，默认 10000），跑完目录树照样
#                               能遍历、能删干净（冒烟：fsstress 自己不比对内容，内容一致性由 fsx 管）
#   run-suite.sh ior            M15：IOR 冒烟（mpirun --allow-run-as-root -np 8）：easy = 每个 rank 一个文件、1 MiB 传输；hard = 8 个
#                               rank 共用一个文件、47008 字节交错（IO500 的 ior-hard）；都写完读回校验
#   run-suite.sh mdtest         M15：mdtest 冒烟（-np 8）：easy = 每个 rank 自己的目录、空文件；hard = 共用一个目录、每个文件写读 3901
#                               字节（IO500 的 mdtest-hard）
#   run-suite.sh pathmap        M7：以 root 直接挂载时 mountinfo 里的类型是 fuse（不是 fuse.wmfs），`wmfs get <挂载点下的路径>` 照样按
#                               来源 wmfs: 认出挂载点、读 user.wmfs.mgmt 找到集群（ADR-0037 第 10 条）；读用 client key 就行
set -u
SUITE="${1:-}"
MNT=/mnt/wmfs
mkdir -p "$MNT"
EXTRA=()
case "$SUITE" in pjdfstest-perm | pjdfstest-all) EXTRA=(--allow-other) ;; esac # 切换到别的用户的测试：别的 uid 要能进这个挂载
wmfs mount 127.0.0.1:19100 "$MNT" --log /tmp/mount.log "${EXTRA[@]}" >/tmp/mount.out 2>&1 &
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
pjdfstest | pjdfstest-perm | pjdfstest-all)
    files=0; tests=0; failed=0
    BASE="ftruncate link mkdir mkfifo mknod open rename rmdir symlink truncate unlink utimensat"
    case "$SUITE" in
    pjdfstest) dirs="$BASE" ;;
    pjdfstest-perm) dirs="$BASE chmod chown" ;;
    pjdfstest-all) dirs="$(cd /opt/pjdfstest/tests && ls -d -- */ | tr -d / | tr '\n' ' ')" ;; # 全部目录
    esac
    for d in $dirs; do
        for t in /opt/pjdfstest/tests/$d/*.t; do
            perm=0
            grep -qE -- '-[ug] ([0-9]|\$)' "$t" && perm=1 # 切换到别的用户（-u 65534、-u $UID_NOBODY）= 权限测试
            if [ "$SUITE" = pjdfstest ]; then
                [ "$perm" = 1 ] && continue # 不涉及权限的子集：跳过切换用户的
            elif [ "$SUITE" = pjdfstest-perm ] && [ "$perm" = 0 ] && [ "$d" != chmod ] && [ "$d" != chown ]; then
                continue # 权限子集：只要切换用户的（chmod、chown 两个目录全要）
            fi
            files=$((files + 1))
            out="$(bash "$t" 2>/dev/null)"
            n=$(printf '%s\n' "$out" | grep -cE '^(not )?ok ')
            tests=$((tests + n))
            while IFS= read -r line; do
                num="$(printf '%s' "$line" | sed -E 's/^not ok ([0-9]+).*/\1/')"
                echo "FAIL $d/$(basename "$t"):$num ${line#not ok $num}"
                failed=$((failed + 1))
            done < <(printf '%s\n' "$out" | grep -E '^not ok [0-9]+' | grep -v '# TODO')
            # 计划 1..N：没打印计划、或者少跑了的用例（挂载进程崩了、测试文件中途出错退出）各记一条 FAIL，不能当作没失败
            plan="$(printf '%s\n' "$out" | sed -nE 's/^1\.\.([0-9]+).*/\1/p' | head -1)"
            if [ -z "$plan" ]; then
                echo "FAIL $d/$(basename "$t"):plan 没有打印计划（1..N）"
                failed=$((failed + 1))
                continue
            fi
            for num in $(printf '%s\n' "$out" | awk -v plan="$plan" '/^(not )?ok [0-9]+/ { seen[($1 == "not") ? $3 : $2] = 1 }
                    END { for (i = 1; i <= plan; i++) if (!(i in seen)) print i }'); do
                echo "FAIL $d/$(basename "$t"):$num 没有跑到（计划 1..$plan，测试文件中途退出？）"
                failed=$((failed + 1))
            done
        done
    done
    if [ "$SUITE" = pjdfstest-all ]; then # 完整套件：tests/ 下的每个测试文件都跑了
        all=$(find /opt/pjdfstest/tests -name '*.t' | wc -l)
        [ "$files" -eq "$all" ] || { echo "FAIL all:files 只跑了 $files 个测试文件（一共 $all 个）"; failed=$((failed + 1)); }
    fi
    echo "SUMMARY files=$files tests=$tests failed=$failed"
    ;;
fsx)
    n="${2:-10000}"
    shift 2 2>/dev/null || shift $#
    /opt/xfstests/ltp/fsx -q -N "$n" -S 42 "$@" fsx.data; rc=$?
    echo "FSX-RC $rc"
    ;;
pathmap)
    # 要测的正是"类型是 fuse（不是 fuse.wmfs）"这种挂载：没测到它就不算通过
    type_ok=1
    grep -E " $MNT .* - fuse wmfs:" /proc/self/mountinfo || { echo "（这个挂载的类型不是 fuse：没测到要测的情况）"; type_ok=0; }
    out="$(wmfs get "$WORK" --json --key-file /root/.config/wmfs/client.key)"; rc=$?
    echo "$out"
    printf '%s' "$out" | grep -q "\"dir\":\"/suite-pathmap-" || rc=1
    [ "$type_ok" -eq 1 ] || rc=1
    echo "PATHMAP-RC $rc"
    ;;
fsstress)
    /opt/xfstests/ltp/fsstress -d "$WORK/fss" -n "${2:-10000}" -p 4 -s 42 >/tmp/fsstress.out 2>&1; rc=$?
    tail -3 /tmp/fsstress.out
    find "$WORK/fss" >/dev/null || rc=1 # 跑完之后目录树照样能遍历、能删干净
    rm -rf "$WORK/fss" || rc=1
    echo "FSSTRESS-RC $rc"
    ;;
ior | mdtest)
    export OMPI_MCA_btl_vader_single_copy_mechanism=none # 容器里没有 SYS_PTRACE：关掉 openmpi 的跨进程单拷贝（CMA）
    mpi=(mpirun --allow-run-as-root -np 8)
    if [ "$SUITE" = ior ]; then
        # -e 写完 fsync、-C 读别的 rank 写的、-W 写完立刻读回校验、-R 读的时候校验、--warningAsErrors 校验出错（IOR 只报 WARNING）就失败退出
        "${mpi[@]}" ior -a POSIX -F -e -C -w -r -W -R --warningAsErrors -t 1m -b 16m -o "$WORK/ior-easy" &&
            "${mpi[@]}" ior -a POSIX -e -C -w -r -W -R --warningAsErrors -t 47008 -b 47008 -s 200 -o "$WORK/ior-hard"; rc=$?
    else
        "${mpi[@]}" mdtest -n 1000 -u -F -d "$WORK/mdt-easy" &&
            "${mpi[@]}" mdtest -n 500 -F -w 3901 -e 3901 -d "$WORK/mdt-hard"; rc=$?
    fi
    echo "MPI-RC $rc"
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
