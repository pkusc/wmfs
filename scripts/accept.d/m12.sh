# M12 锁 + strong + O_APPEND + 回收站 / ttl：逐条对应 roadmap §3 M12 的"验收标准"与"演示命令"，以及主管交代的事项（持锁的客户端被驱逐之后
# 锁才转给别人、锁即同步点、只读 fd 的 close 释放 POSIX 锁、strong 目录里的 inline 小文件写打开时先转出、审计标记与被拒绝的修改），另加验收时
# 发现并修掉的一件：leader 的元数据库随写入一直涨（快照 pin 一直不放，design.md 3.4.1.7 的 M12 注记）。
# 由 scripts/accept.sh 加载（用法见那里的说明）；这里只定义函数，不直接运行。
#
# 验收标准：两个挂载点在 strong 目录并发追加 2 × 1000 行 → 正好 2000 行且格式完整；strong 目录里两个挂载点并发追加同一个小文件也正确；
#           strong 下写后另一挂载点立即读到；flock/fcntl 跨挂载点互斥（计数器测试结果恰好 2000）；17 个进程同时 F_SETLKW 等同一把锁时，
#           挂载点上的 ls 仍立即返回；F_GETLK 能看到 l_len = 0（到文件尾）的冲突锁；strong 文件上 mmap(MAP_SHARED) 得到 ENODEV；rm 后能从
#           回收站 mv 回来；ttl 用短时长按预期删除；在锁回调里调用内核通知不出问题（cargo 的 fuse_locks 与这里的计数器）。
#
# 开发集群与挂载点只用本次验收自己的目录（$LOGDIR/dev、$LOGDIR/mntA、$LOGDIR/mntB；端口照常是 19n00/19n80）。无论成败，退出前都先恢复被冻住的
# 挂载进程、停掉自己起的持锁 / 等锁进程，再 wmfs dev down（先卸载挂载点、再停节点）并删掉生成的文件（m12_cleanup）。验收脚本自己从不拿着挂载点上
# 的 fd（被冻住的挂载上，继承了 fd 的子进程退出时会卡在 FLUSH 上）：拿着锁、开着文件的都是单独的 python3 进程。只解析 --json 输出、指标与退出码，
# 例外几处正是要检查的东西：wc/grep（追加的内容）、cat（读到的内容）、python3 打印的系统调用结果、节点日志里驱逐的先后。等待一律按条件（poll、wmfs
# dev wait）。

# shellcheck source=/dev/null
source "$ROOT/scripts/accept.d/m6.sh"

accept_m12() {
    check "开发集群的端口 19100–19380 都空闲（演示用的开发集群要先 wmfs dev down）" dev_ports_free M12 3
    check "没有残留的 wmfs FUSE 挂载（有的话先 wmfs dev down --purge）" m6_no_mounts
    check "cargo build --release 成功且零警告" build_no_warnings
    check "单元：锁表的纯函数（随机序列与逐字节的暴力模型对比、到文件尾的切分不溢出）、trash/ttl/audit/strong 的解析与规范化、lock-wait 判定将死的线程、trash-expire 与 ttl 插件、快照 pin 用完就放掉" \
        m12_unit
    check "金样：status 的 trash（files、bytes）与 audit（lag）的实际、changelog 操作码表（TrashExpire = 107）" m12_golden
    check "命名空间（内存 KV）：rm 进回收站（.wmfs-trash/<属主 uid>/：名字、origin、到期索引、计数仍在来源子树）、mv 恢复、回收站里再删 = 真删、rename 覆盖的目标进回收站、多链接不进、用旧版本覆盖恢复之后回收站的 size 仍对；别的用户看不到、读不到别人回收站里的文件、root 删 alice 的文件进 alice 的子目录（ADR-0043 第 1 条）；到期与 ttl 的删除；audit 标记与 ops/uids 过滤、rename 也看目标目录、被拒绝的修改也记；strong 目录不 inline" \
        tests_at_least 7 --release -p wmfs-testkit --test meta_trash
    check "完整节点：锁的冲突与读读共享、GetLock 回到文件尾、locked_inos、会话关闭释放、驱逐第一阶段之后锁还在、第二阶段才释放；AppendReserve 并发分到首尾相接的区间、配额精确；被拒绝的修改经执行器也审计" \
        tests_at_least 4 --release -p wmfs-testkit --test meta_locks
    check "客户端文件层：strong 并发追加不丢不乱、B 开着时 A 写完 B 立即读到、写打开 inline 文件前先转出；F_SETLKW 交给 lock-wait 线程、锁即同步点、将死的等待者 EINTR 且不拿锁、close 释放" \
        tests_at_least 3 --release -p wmfs-testkit --test client_strong
    check "FUSE（cargo）：F_GETLK 的 spike（l_len = 0）、只读 fd 的 close 释放 POSIX 锁、flock 跨挂载点、OFD 锁随最后一个 close 释放；fd 一直开着的计数器（拿到锁时在锁回调与 lock-wait 线程里 inval_inode，不卡住、读到对方的提交）与没有竞争的 F_SETLK 也丢旧缓存；17 个等锁的进程不卡 ls、被 kill -9 的等锁进程 1.5 s 内退出；写出一直失败时锁照样放得掉、错误报给 close；strong 上 MAP_SHARED → ENODEV、F_SETFL 加上的 O_APPEND 也互不覆盖" \
        tests_at_least 5 --release -p wmfs --test fuse_locks
    check "回归：驱逐与旧句柄（M10）、FUSE 的 cto 与 POSIX 语义（M6）、迁移原语与后台任务的 fail point（M11；M12 起 strong 目录里残留的 inline 文件也由全量扫描转出，trash-expire 与 ttl 的一步失败之后照样删掉）" \
        m12_regress

    export WMFS_DEV_DIR="$LOGDIR/dev"
    MA="$LOGDIR/mntA"
    MB="$LOGDIR/mntB"
    mkdir -p "$MA" "$MB"
    PIDS=""
    FROZEN=""
    trap m12_cleanup EXIT
    check "演示：wmfs dev up --nodes 3" wmfs dev up --nodes 3
    eval "$(wmfs dev env)"
    check "演示：wmfs dev mount 两个挂载点" m12_mount_two
    check_json "演示：mkdir …/home/shared && wmfs set …/home/shared consistency=strong" '.set.consistency == "strong"' m12_strong_dir
    check "演示：两个挂载点在 strong 目录并发追加 2 × 1000 行 → wc -l 与格式完整的行数都是 2000（A、B 各自 1..1000、有序）" m12_append_log
    check "strong 目录里两个挂载点并发追加同一个小文件（目录改成 strong 之前它是 inline 的：写打开时先转出）→ 1 + 2 × 200 行、开头不变" \
        m12_append_small
    check "演示：echo v2 > A/x && cat B/x → v2；另外 B 一直开着 x、A 再写，B 的下一次读立即看到新内容（strong：读前 GetAttr）" m12_strong_visible
    check "演示：A 持有 flock 时 B 的 flock -n 失败（锁是集群范围的）" m12_flock_exclusive
    check "演示：17 个进程同时 F_SETLKW 等同一把锁时（按条件等它们都卡在 FUSE 请求里）ls 立即返回（< 1 s，等锁的进程不占 FUSE 线程），之后它们都拿到锁" m12_seventeen
    check "flock、fcntl 跨挂载点互斥：两个挂载上的进程各加 1000 次同一个计数器（fd 一直开着）→ 恰好 2000（fcntl、flock 各一遍）" m12_counter
    check "F_GETLK 能看到 l_len = 0（到文件尾）的冲突锁：另一个挂载持有整个文件的写锁，F_GETLK 回 F_WRLCK、l_start 0、l_len 0" m12_getlk
    check "演示：strong 文件上 mmap(MAP_SHARED) → OSError: [Errno 19] No such device" m12_mmap
    check_json "演示：wmfs set …/home trash=7d" '.set.trash == "7d"' wmfs set "$MA/home" trash=7d --json
    check "演示：echo important > doc && rm doc → ls .wmfs-trash/\$(id -u)/ 是 <时间>-<ino>-doc（.wmfs-trash 是 0711、子目录 0700）；mv 回来 cat → important；status 的 trash 一行与 changelog 的 Unlink（trash=）" \
        m12_trash
    check "ttl 用短时长按预期删除：ttl=5s 的目录里过期的旧文件被删（/home 有 trash：进回收站；changelog 的 Unlink 带 reason=ttl），新文件留着" m12_ttl
    check "回收站到期真删：trash=1s 的目录里删掉的文件到期后由 trash-expire 真删（changelog TrashExpire）；wmfs dev wait task-idle trash-expire 可用" \
        m12_trash_expire
    check "audit：audit=on 之后修改带 audited（第一次启用时建了 audit-log 游标）；被拒绝的修改也记一条；health 与 status 显示审计消费者落后的条数" m12_audit
    check "Jepsen-lite 的 FUSE 版本（design.md 9.5 的检查器 ⑦，M12 起）：strong 目录里一个文件当寄存器，3 个线程经两个挂载点读写，同时随机 kill -9 / SIGSTOP 节点 → 线性一致（失败时打印种子）" \
        m12_jepsen
    check "持锁的客户端被冻住（kill -STOP）→ 被驱逐之后锁才转给别人：等锁的一边约 30–45 s 才拿到；日志里'数据节点确认'在'驱逐第二阶段：释放锁（1 个 inode）'之前" \
        m12_evict_lock
    check "元数据库不随写入一直涨：每个节点的 meta.redb 都 ≤ 256 MiB（M12 修了快照 pin 一直不放：以前这套演示跑到一半 leader 的就有 1.4 GiB）" \
        m12_meta_size
    check "wmfs dev down（先卸载挂载点，再停全部节点）" wmfs dev down
    check "结束：本次验收自己的挂载都已卸载（$LOGDIR 下没有 fuse.wmfs 挂载）" m6_no_own_mounts
    check "profile minimal（scripts/profiles.sh minimal；ADR-0015：M6 起每个里程碑跑）" scripts/profiles.sh minimal
    check "profile nas-safe 阶段版（scripts/profiles.sh nas-safe；ADR-0015：M12 起 /home 加上 trash=7d、audit=on）" scripts/profiles.sh nas-safe
}

# 每个节点的 meta.redb 实际占的空间（du）都不超过 256 MiB（design.md 3.4.1.7 的 M12 注记：快照 pin 是一个 redb 读事务，以前一直不放，
# leader 的库就回收不了页）。
m12_meta_size() {
    local n k bad=0
    for n in 1 2 3; do
        k="$(du -k "$WMFS_DEV_DIR/n$n/meta/meta.redb" | cut -f1)"
        echo "node$n：meta.redb 占 ${k} KiB"
        [ "$k" -le 262144 ] || bad=1
    done
    return "$bad"
}

# 不论成败：先恢复被冻住的挂载进程（不然卸载与子进程退出会卡住），停掉自己起的进程，再停开发集群（它先卸载挂载点），删掉生成的文件。
m12_cleanup() {
    [ -z "${FROZEN:-}" ] || kill -CONT "$FROZEN" 2>/dev/null
    touch "$LOGDIR/go" "$LOGDIR/release" 2>/dev/null
    local p
    for p in ${PIDS:-}; do kill "$p" 2>/dev/null; done
    wmfs dev down >/dev/null 2>&1
    [ -n "$LOGDIR" ] && [ "${WMFS_DEV_DIR:-}" = "$LOGDIR/dev" ] && rm -rf "$WMFS_DEV_DIR"/n*/targets
}

m12_unit() {
    tests_at_least 2 --release -p wmfs-core --lib meta::locks || return 1
    one_test --release -p wmfs-core --lib meta::raft::snapshot::tests::pins_are_released_after_serving_or_when_nobody_fetches_them ||
        return 1
    one_test --release -p wmfs-core --lib meta::attr_registry::tests::strong_trash_ttl_and_audit_are_parsed || return 1
    one_test --release -p wmfs-core --lib client::locks::tests::a_live_thread_is_not_dying_and_a_missing_one_is || return 1
    tests_at_least 4 --release -p wmfs-plugins -- trash_expire ttl
}

m12_golden() {
    one_test --release -p wmfs-api types::mgmt::tests::usage_status_semantics_json_golden || return 1
    one_test --release -p wmfs-api ops::tests::values_match_adr_0029 || return 1
    tests_at_least 1 --release -p wmfs-core --test golden_constants
}

m12_regress() {
    tests_at_least 2 --release -p wmfs-testkit --test evict_lease || return 1
    tests_at_least 1 --release -p wmfs-testkit --test evict_inflight || return 1
    tests_at_least 2 --release -p wmfs --test fuse_cto || return 1
    tests_at_least 8 --release -p wmfs-testkit --test failpoint_migrate
}

m12_mount_two() { wmfs dev mount "$MA" && wmfs dev mount "$MB"; }

m12_strong_dir() { mkdir "$MA/home/shared" && wmfs set "$MA/home/shared" consistency=strong --json; }

# m12_each <文件> <标签> <n>：文件里标签开头的行恰好是 "标签 1" … "标签 n"、按顺序
m12_each() { [ "$(grep "^$2 " "$1" | cut -d' ' -f2 | tr '\n' ' ')" = "$(seq "$3" | tr '\n' ' ')" ]; }

m12_append_log() {
    local f="$MA/home/shared/log" a b n good
    (for i in $(seq 1000); do echo "A $i" >>"$MA/home/shared/log"; done) &
    a=$!
    (for i in $(seq 1000); do echo "B $i" >>"$MB/home/shared/log"; done) &
    b=$!
    wait "$a" && wait "$b" || return 1
    n="$(wc -l <"$f")"
    good="$(grep -cE '^(A|B) [0-9]+$' "$f")"
    echo "wc -l：$n；格式完整的：$good"
    [ "$n" -eq 2000 ] && [ "$good" -eq 2000 ] && m12_each "$f" A 1000 && m12_each "$f" B 1000
}

m12_append_small() {
    local f="$MA/home/sm/s" a b
    mkdir "$MA/home/sm" && echo hdr >"$f" || return 1
    json_expect '.inline == true' wmfs fs stat /home/sm/s --json || return 1
    wmfs set "$MA/home/sm" consistency=strong --json || return 1
    (for i in $(seq 200); do echo "A $i" >>"$MA/home/sm/s"; done) &
    a=$!
    (for i in $(seq 200); do echo "B $i" >>"$MB/home/sm/s"; done) &
    b=$!
    wait "$a" && wait "$b" || return 1
    json_expect '.inline == false' wmfs fs stat /home/sm/s --json || return 1
    echo "$(wc -l <"$f") 行，第一行：$(head -1 "$f")"
    [ "$(wc -l <"$f")" -eq 401 ] && [ "$(head -1 "$f")" = hdr ] && m12_each "$f" A 200 && m12_each "$f" B 200
}

m12_strong_visible() {
    echo v2 >"$MA/home/shared/x" || return 1
    [ "$(cat "$MB/home/shared/x")" = v2 ] || return 1
    python3 - "$MA/home/shared/x" "$MB/home/shared/x" <<'EOF'
import os, sys
r = os.open(sys.argv[2], os.O_RDONLY)  # B 先打开着
w = os.open(sys.argv[1], os.O_WRONLY)
os.pwrite(w, b"v3-longer\n", 0)        # A 写（strong：同步写链、同步提交长度）
got = os.pread(r, 100, 0)
print("B 读到", got)
sys.exit(0 if got == b"v3-longer\n" else 1)
EOF
}

# B 上的 flock -n 失败 = A 持有着锁（给 poll 用）
m12_held() { ! flock -n "$MB/home/shared/lk" -c true; }

# 演示里是 flock … -c 'sleep 10'；这里拿着锁直到 $LOGDIR/release 出现（m12_seventeen 量完 ls 再放），不靠固定的时长
m12_flock_exclusive() {
    touch "$MA/home/shared/lk" || return 1
    flock "$MA/home/shared/lk" -c "while [ ! -e '$LOGDIR/release' ]; do sleep 0.1; done" &
    PIDS="$PIDS $!"
    poll 100 0.1 m12_held || return 1
    echo "B 拿不到锁（正确：锁是集群范围的）"
}

# 这些进程都正卡在 FUSE 请求里等挂载进程回复（/proc/<pid>/wchan = request_wait_answer：F_SETLKW 已经交给 lock-wait 线程）
m12_all_waiting() {
    local p
    for p in "$@"; do
        [ "$(cat "/proc/$p/wchan" 2>/dev/null)" = request_wait_answer ] || return 1
    done
}

m12_seventeen() {
    local i t0 t1 waiters="" p
    for i in $(seq 17); do
        flock "$MA/home/shared/lk" -c true &
        waiters="$waiters $!"
    done
    PIDS="$PIDS $waiters"
    # shellcheck disable=SC2086
    poll 300 0.1 m12_all_waiting $waiters || { echo "17 个进程没有都卡在 F_SETLKW 里等锁"; return 1; }
    t0="$(date +%s%3N)"
    ls "$MA/home/shared" >/dev/null || return 1
    t1="$(date +%s%3N)"
    echo "17 个进程在等锁时 ls 用了 $((t1 - t0)) ms"
    touch "$LOGDIR/release" # 放锁：17 个等锁的进程依次拿到
    for p in $waiters; do wait "$p" || return 1; done
    [ $((t1 - t0)) -lt 1000 ]
}

# 计数器程序与 cargo 的 fuse_locks 共用（cli/tests/lock_counter.py）
m12_counter() {
    local kind a b got py="$ROOT/cli/tests/lock_counter.py"
    for kind in fcntl flock; do
        echo 0 >"$MA/home/cnt-$kind" || return 1
        timeout 300 python3 "$py" "$MA/home/cnt-$kind" "$kind" 1000 &
        a=$!
        timeout 300 python3 "$py" "$MB/home/cnt-$kind" "$kind" 1000 &
        b=$!
        wait "$a" && wait "$b" || return 1
        got="$(cat "$MA/home/cnt-$kind")"
        echo "$kind：计数器 = $got"
        [ "$got" = 2000 ] || return 1
    done
}

m12_getlk() {
    touch "$MA/home/glk" || return 1
    python3 - "$MA/home/glk" "$LOGDIR/ready" "$LOGDIR/go" <<'EOF' &
import fcntl, os, sys, time
fd = os.open(sys.argv[1], os.O_RDWR)
fcntl.lockf(fd, fcntl.LOCK_EX)        # l_start 0、l_len 0：整个文件到文件尾
open(sys.argv[2], "w").close()
while not os.path.exists(sys.argv[3]):
    time.sleep(0.05)
EOF
    PIDS="$PIDS $!"
    poll 200 0.1 test -e "$LOGDIR/ready" || return 1
    python3 - "$MB/home/glk" <<'EOF'
import fcntl, os, struct, sys
fd = os.open(sys.argv[1], os.O_RDWR)
fmt = "hhqqi"                          # struct flock（64 位 Linux）
t, _, start, length, pid = struct.unpack(fmt, fcntl.fcntl(fd, fcntl.F_GETLK, struct.pack(fmt, fcntl.F_RDLCK, 0, 0, 0, 0)))
print("F_GETLK：l_type", t, "l_start", start, "l_len", length, "l_pid", pid)
sys.exit(0 if (t, start, length) == (fcntl.F_WRLCK, 0, 0) else 1)
EOF
    local rc=$?
    touch "$LOGDIR/go"
    return "$rc"
}

m12_mmap() {
    local out
    out="$(python3 -c "import mmap,os; f=os.open('$MA/home/shared/x',os.O_RDWR); mmap.mmap(f,0)" 2>&1)"
    printf '%s\n' "$out"
    printf '%s' "$out" | grep -q 'OSError: \[Errno 19\] No such device'
}

# 回收站按文件属主分子目录（ADR-0043 第 1 条）：.wmfs-trash 是 0711（列不出有哪些用户），自己的子目录 0700
m12_trash() {
    local name t="$MA/home/.wmfs-trash" mine
    mine="$t/$(id -u)"
    echo important >"$MA/home/doc" && rm "$MA/home/doc" || return 1
    ls -la "$t" "$mine"
    [ "$(stat -c %a "$t")" = 711 ] && [ "$(stat -c %a:%u "$mine")" = "700:$(id -u)" ] || { echo "回收站两层的权限不对"; return 1; }
    name="$(ls "$mine" | grep -E -- '-doc$')"
    [[ "$name" =~ ^[0-9]+-[0-9]+-doc$ ]] || { echo "回收站里的名字不对：$name"; return 1; }
    json_expect '[.attrs[] | select(.key == "trash")][0] | .actual.files == 1 and .actual.bytes == 10' \
        wmfs status "$MA/home" --json || return 1
    json_expect "any(.events[]; .op == \"Unlink\" and (.extra | any(.[0] == \"trash\" and .[1] == \"$name\")))" \
        wmfs changelog tail -n 200 --json || return 1
    mv "$mine/"*-doc "$MA/home/doc" || return 1
    echo "cat：$(cat "$MA/home/doc")"
    [ "$(cat "$MA/home/doc")" = important ] && [ -z "$(ls "$mine")" ]
}

m12_expired() {
    wmfs status "$MA/home/tt" --json | jq -e '[.attrs[] | select(.key == "ttl")][0].actual.mismatched_files >= 1' >/dev/null
}

m12_ttl() {
    mkdir "$MA/home/tt" && echo old >"$MA/home/tt/old" || return 1
    json_expect '.set.ttl == "5s"' wmfs set "$MA/home/tt" ttl=5s --json || return 1
    poll 300 0.1 m12_expired || return 1 # 按条件等它过期（status 的 ttl 一行：已经过期、还没删的 ≥ 1）
    echo new >"$MA/home/tt/new" || return 1
    # ttl 每轮隔 60 s：wmfs task run 让它马上做一轮，按条件等旧文件消失（task-idle 要再等满一轮，见 m12_trash_expire 的注释）
    wmfs task run ttl --json && poll 100 0.1 test ! -e "$MA/home/tt/old" || return 1
    ls -la "$MA/home/tt" "$MA/home/.wmfs-trash/$(id -u)"
    [ ! -e "$MA/home/tt/old" ] && [ -e "$MA/home/tt/new" ] || return 1
    ls "$MA/home/.wmfs-trash/$(id -u)/" | grep -qE -- '-old$' || { echo "过期的文件没进回收站"; return 1; }
    json_expect 'any(.events[]; .op == "Unlink" and .name == "old" and (.extra | any(.[0] == "reason" and .[1] == "ttl")))' \
        wmfs changelog tail -n 200 --json
}

# 回收站到期真删：trash=1s 的目录里删掉的文件，trash-expire（每轮隔 10 s）到期之后真删（changelog TrashExpire）。也确认
# wmfs dev wait task-idle 对新任务可用：leader 任务要在开始等之后做完两轮（M11 的规则），这里是 10–20 s。
m12_trash_expire() {
    mkdir "$MA/home/te" && json_expect '.set.trash == "1s"' wmfs set "$MA/home/te" trash=1s --json || return 1
    echo gone >"$MA/home/te/f" && rm "$MA/home/te/f" || return 1
    ls "$MA/home/te/.wmfs-trash/$(id -u)/" | grep -qE -- '-f$' || { echo "删掉的文件不在回收站里"; return 1; }
    wmfs dev wait task-idle trash-expire --timeout 60 || return 1
    [ -z "$(ls "$MA/home/te/.wmfs-trash/$(id -u)/")" ] || { echo "到期了还在回收站里"; return 1; }
    json_expect 'any(.events[]; .op == "TrashExpire")' wmfs changelog tail -n 50 --json
}

m12_audit() {
    json_expect '.set.audit == "on"' wmfs set "$MA/home" audit=on --json || return 1
    touch "$MA/home/audited" || return 1
    expect_error ENOENT wmfs fs rm /home/no-such-file --json || return 1 # 被拒绝的修改（经执行器）
    json_expect 'any(.events[]; .op == "Create" and .name == "audited" and .audited) and
                 any(.events[]; .op == "Unlink" and .name == "no-such-file" and .result == 2 and .audited)' \
        wmfs changelog tail -n 50 --json || return 1
    json_expect '.audit_lag >= 2' curl --noproxy '*' -s "http://127.0.0.1:19$(wmfs dev leader)80/api/v1/health" || return 1
    json_expect '[.attrs[] | select(.key == "audit")][0] | .declared == "on" and .actual.lag >= 2 and .mismatched' \
        wmfs status "$MA/home" --json
}

m12_jepsen() {
    export WMFS_JEPSEN_FILES="$MA/home/shared/reg,$MB/home/shared/reg"
    ignored_test --release -p wmfs-testkit --test jepsen_register \
        a_strong_file_through_two_mounts_stays_linearizable_while_nodes_die_and_freeze || return 1
    wmfs dev wait leader --timeout 60 && wmfs dev wait chains-serving --timeout 180
}

m12_evict_lock() {
    local sid t0 t1 log c p
    touch "$MA/home/elk" || return 1
    rm -f "$LOGDIR/ready" "$LOGDIR/go"
    python3 - "$MB/home/elk" "$LOGDIR/ready" "$LOGDIR/go" <<'EOF' &
import fcntl, os, sys, time
f = open(sys.argv[1]); fcntl.flock(f, fcntl.LOCK_EX); open(sys.argv[2], "w").close()
while not os.path.exists(sys.argv[3]):
    time.sleep(0.1)
EOF
    PIDS="$PIDS $!"
    poll 200 0.1 test -e "$LOGDIR/ready" || return 1
    sid="$(wmfs session list --json | jq -r '.sessions[] | select((.mount | endswith("/mntB")) and .state == "active") | .id')"
    [ -n "$sid" ] || return 1
    FROZEN="$(wmfs dev client-pid "$MB")" || return 1
    t0="$(date +%s%3N)"
    kill -STOP "$FROZEN" || return 1
    echo "冻住挂载 B（会话 $sid、进程 $FROZEN），A 上 flock 同一个文件"
    t1="$(timeout 120 python3 -c 'import fcntl,sys,time; f=open(sys.argv[1]); fcntl.flock(f, fcntl.LOCK_EX); print(int(time.time()*1000))' "$MA/home/elk")" ||
        return 1
    echo "A 在冻住之后 $((t1 - t0)) ms 拿到锁"
    kill -CONT "$FROZEN" && FROZEN="" && touch "$LOGDIR/go" || return 1
    log="$(grep -l "会话 $sid 驱逐第二阶段" "$WMFS_DEV_DIR"/n*/logs/*.log | head -1)"
    [ -n "$log" ] || { echo "没有哪个节点的日志里有第二阶段"; return 1; }
    c="$(grep -n "会话 $sid：数据节点确认" "$log" | head -1 | cut -d: -f1)"
    p="$(grep -n "会话 $sid 驱逐第二阶段：释放锁（1 个 inode）" "$log" | head -1 | cut -d: -f1)"
    grep -E "会话 $sid( |：)" "$log"
    echo "$log：数据节点确认在第 ${c:-?} 行，释放锁在第 ${p:-?} 行"
    [ -n "$c" ] && [ -n "$p" ] && [ "$c" -lt "$p" ] && [ $((t1 - t0)) -ge 28000 ] && [ $((t1 - t0)) -le 50000 ]
}
