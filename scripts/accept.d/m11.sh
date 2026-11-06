# M11 后台任务 + 协调器 + 全部变换：逐条对应 roadmap §3 M11 的"验收标准"与"演示命令"，以及主管交代的事项（运行中连续提交失败 →
# 只读并移出写路径、同步源上的坏 chunk、Serving 成员缺 L 有的版本、gc 的完整版、ReportCorrupt / DataLossDetected、节点任务的本地游标）。
# 由 scripts/accept.sh 加载（用法见那里的说明）；这里只定义函数，不直接运行。
#
# 验收标准：设置 compress=zstd:3 后存量全部转换、内容不变、物理占用明显下降；转换途中杀 leader → 新 leader 从断点继续；
#           encrypt=tenant:k1 后 target 目录里搜不到明文；replicas 3 → 2 转换；人为损坏一个副本 → scrub 发现并从健康副本修复（全部副本都坏
#           了如实报 DataLossDetected）；GC 在文件仍被打开时不删除旧 epoch；一直写打开的文件被跳过并单独计数；O_DIRECT 写的同时触发迁移，内容不丢。
#
# 开发集群与挂载点只用本次验收自己的目录（$LOGDIR/dev、$LOGDIR/mnt，端口照常是 19n00/19n80）。无论成败，退出前都停掉自己起的写者进程、关掉
# fd 4、wmfs dev down（先卸载挂载点、再停节点），并删掉各节点 target 里的 chunk 与 $LOGDIR 里的源文件（m11_cleanup）。只解析 --json 输出、指标
# 与退出码，例外几处正是外部工具：md5sum 与 cmp、du、grep（搜明文）、dd（O_DIRECT 读报错）。等待一律按条件（wmfs dev wait、`poll`）。

# shellcheck source=/dev/null
source "$ROOT/scripts/accept.d/m6.sh"

MiB=1048576

accept_m11() {
    check "开发集群的端口 19100–19380 都空闲（演示用的开发集群要先 wmfs dev down）" dev_ports_free M11 3
    check "没有残留的 wmfs FUSE 挂载（有的话先 wmfs dev down --purge）" m6_no_mounts
    check "cargo build --release 成功且零警告" build_no_warnings
    check "单元：变换插件 xxh3、blake3、lz4、zstd、aes-gcm（往返、压不动就原样存、AAD 与 nonce、拿不到密钥 NOT_READY）与 reconciler、scrub、gc（含本地孤儿扫描与旧链目录的取舍）的 manifest、任务属性与游标" \
        tests_at_least 20 --release -p wmfs-plugins -- xxh3 blake3 lz4 zstd aes_gcm reconciler scrub gc
    check "单元：后台任务框架（限速、本地游标文件、status 的逐文件统计、\"只有四个键会迁移\"、加密目录里 inline 的比较）、compress/encrypt 属性、数据节点问的 epoch 死活" m11_core_unit
    check "金样：任务与数据密钥的管理 API JSON、status 的逐文件统计与配额的实际、changelog 操作码 RStatDrift = 108、本地游标文件 LocalTaskV1" m11_golden
    check "协调器（单节点）：compress=lz4 / zstd:5、encrypt=tenant:k1 再改回 none 把已有文件（含 inline 小文件）换到新布局，内容不变、status 的实际跟上（inline 一行也一致）、落盘变小、target 上没有明文；一直写打开的文件被跳过、计数、告警，关掉之后迁移；暂停、限速、404；暂停中换 leader、恢复之后转换完；节点任务重启后隔满间隔；遍历器；数据密钥的包裹" \
        tests_at_least 6 --release -p wmfs-testkit --test bg_reconcile
    check "fail point：bg.migrate.before_switch（迁移途中被改写 → 作废重做，新内容不丢）、chunk.sync.eio（新布局的数据没持久 → 不切换）、reconciler.full_every_ms（RStatDrift；声明一致了也放弃旧 leader 留下的迁移意图）、plugin.reconciler/scrub.bg_task（从同一个游标重试）、拷得久的迁移途中重签 cap（clock.wall.skew_ms）" \
        tests_at_least 6 --release -p wmfs-testkit --test failpoint_migrate
    check "scrub（组件测试）：坏副本从别的 Serving 成员修好；全坏（块坏、头坏）→ Lost；对方有在途的版本 → 稍后、修复途中本地变了（新版本、pending、头坏的又读得出）→ 不装；写路径先修复再写；头读不出又没有修复来源：只有自己 → 照常写、有 Syncing 后继 → EIO；ADR-0040 第 10 条——同步源的坏 chunk 修好之后重同步完成、Serving 成员缺 L 有的版本被补上、连续提交失败 → 只读；scrub 任务报 DataLossDetected" \
        tests_at_least 9 --release -p wmfs-testkit --test scrub_repair
    check "gc 完整版（ADR-0042 第 5 条）：迁移换下来的旧布局在文件打开着时不删、关了才删，截断换下来的过了宽限期就删（文件开着也删）；本地孤儿扫描不碰 leader 的 gc 还管着的、删回不去的旧链目录（不认识的链、最近写过的留着）；去重记录、changelog 修剪（更早的 410，之后的一条不少）" \
        tests_at_least 6 --release -p wmfs-testkit --test gc_full
    check "多节点：换 leader 后协调器从持久化的游标续走；task run scrub 从 L 补上 Serving 成员缺的 chunk；读到坏块马上修好（不等 scrub）；只做数据的节点不受控制、task-idle 照样成立" \
        tests_at_least 4 --release -p wmfs-testkit --test bg_cluster
    check "回归：后台任务框架的 fail point（plugin.gc.bg_task、bg.step.crash_after_effect）、gc 的宽限期与孤儿、读句柄碰上 layout 变化、变换插件的 fail point" m11_regress

    export WMFS_DEV_DIR="$LOGDIR/dev"
    M="$LOGDIR/mnt"
    mkdir -p "$M"
    WRITER=""
    KILLED=""
    trap m11_cleanup EXIT
    check "wmfs dev up --nodes 3（3 个元数据 voter）" wmfs dev up --nodes 3
    eval "$(wmfs dev env)"
    check "wmfs dev mount 一个挂载点" wmfs dev mount "$M"
    check_json "演示：wmfs cluster set epoch_gc_grace=0s" '.settings.epoch_gc_grace == "0s"' wmfs cluster set epoch_gc_grace=0s --json
    check "演示：mkdir /home/data，写 20 个 8 MiB 的文本文件；记下内容的 md5 与 3 个节点合计的物理占用" m11_write_data
    check_json "演示：wmfs task set reconciler bw=2M" '.kind == "reconciler" and .bw == 2097152' wmfs task set reconciler bw=2M --json
    check_json "演示：wmfs set … compress=zstd:3 → 成功，并提醒已有文件要等协调器转换完才按新设置" \
        '.set.compress == "zstd:3" and any(.warnings[]; contains("已有文件由协调器在后台转换"))' \
        wmfs set "$M/home/data" compress=zstd:3 --json
    check_json "演示：wmfs status：compress 声明 zstd:3，20 个文件里与声明不同的还有约 20 个；带着协调器的进度" \
        '([.attrs[] | select(.key == "compress")][0] | .declared == "zstd:3" and .actual.files == 20 and .actual.mismatched_files >= 15 and .mismatched) and .reconciler.kind == "reconciler"' \
        wmfs status "$M/home/data" --json
    check "演示：wmfs task list：reconciler 在跑、有积压，速率约 2 MiB/s（不超过 3 MiB/s）" m11_rate
    check "演示：杀掉元数据 leader → wmfs dev wait leader；新 leader 从断点继续推进（与声明不同的文件接着变少；累计处理数从持久化的进度接着涨、不倒退，还是同一轮，不从头再来）" m11_kill_leader
    check "演示：被杀的节点回来并同步完（wmfs dev start、wmfs dev wait chains-serving）" m11_restart_killed
    check "演示：bw=200M 之后 wmfs dev wait task-idle reconciler --timeout 300；status：compress 不一致 0；逐个文件（wmfs fs stat）都是 zstd:3" \
        m11_idle_converted compress '.layout.compress == 2 and .layout.compress_level == 3'
    check "演示：wmfs task run gc && wmfs dev wait task-idle gc；md5 不变，物理占用明显变小（3 个节点合计降到原来的 1/3 以下）" m11_gc_shrinks
    check "O_DIRECT 写的同时触发迁移：compress=lz4，迁移把新布局的数据写好、切换之前被 fail point bg.migrate.before_switch 拖住 3 s，这段时间里 O_DIRECT 覆盖写 → 切换的 CAS 失败、作废重做；之后内容是最后一次写的，不一致 0" \
        m11_odirect
    check "一直写打开的文件被跳过并单独计数：写者进程一直开着一个文件 → status 的 open_files = 1；协调器跑过之后别的已转换、它还是旧布局（积压留着）；关掉之后它也转换完、内容都在" \
        m11_open_skip
    check "GC 在文件仍被打开时不删除旧 epoch（迁移换下来的旧布局，ADR-0042 第 5 条）：只读打开着、协调器把它转换到 lz4 → gc 又跑了几轮旧 chunk 还在；关掉之后被删，内容不变" m11_gc_open
    check_json "演示：wmfs datakey create k1 → 打印数据密钥的 id" '.id >= 1 and .name == "k1"' wmfs datakey create k1 --json
    check "演示：encrypt=tenant:k1 && wmfs dev wait task-idle reconciler；逐个文件 encrypt_key ≠ 0；gc 之后 target 目录里搜不到明文（grep -rl 没有输出）；md5 不变" m11_encrypt
    check "演示：wmfs debug corrupt-chunk /home/data/f1 0 --node 2 --block 0；wmfs task run scrub && wmfs dev wait task-idle scrub → changelog 有 f1 的 ChecksumRepaired；md5 不变；再来一轮：节点 2 不再修复、查过的好副本变多" \
        m11_scrub
    check "全部副本都坏了：三个节点上都损坏一个新文件的 chunk 0 → scrub 如实报 DataLossDetected；O_DIRECT 读它报错（不返回坏数据）；删掉它" m11_data_loss
    check "replicas 3 → 2：协调器转换完之后文件的 layout 是 2 副本、它的链有 2 个成员；md5 不变" m11_replicas
    check "wmfs task pause / resume / show（节点任务逐个节点列出）、不认识的任务（--json：enoent）；暂停中杀掉元数据 leader，恢复之后照常转换完" m11_task_control
    check "运行中连续提交失败（节点 3 上 fail point chunk.commit.before_rename=return(EIO)）→ 它的 target 只读、被移出每条链，写照常、内容正确；清掉 fail point、重启节点 3 之后回到链里" \
        m11_read_only
    check "wmfs dev down（先卸载挂载点，再停全部节点）" wmfs dev down
    check "结束：本次验收自己的挂载都已卸载（$LOGDIR 下没有 fuse.wmfs 挂载）" m6_no_own_mounts
    check "profile minimal（scripts/profiles.sh minimal；ADR-0015：M6 起每个里程碑跑）" scripts/profiles.sh minimal
    check "profile nas-safe 阶段版（scripts/profiles.sh nas-safe；ADR-0015：M9–M11 跑）" scripts/profiles.sh nas-safe
}

# 不论成败：关掉 fd 4、停掉自己起的写者进程、停开发集群（它先卸载挂载点）、删掉 target 里的 chunk 与源文件。
m11_cleanup() {
    exec 4<&- 2>/dev/null
    [ -z "${WRITER:-}" ] || { touch "$LOGDIR/w.go"; kill "$WRITER" 2>/dev/null; } # 写者进程（本次验收起的）自己收尾退出
    wmfs dev down >/dev/null 2>&1
    [ -n "$LOGDIR" ] && [ "${WMFS_DEV_DIR:-}" = "$LOGDIR/dev" ] && rm -rf "$WMFS_DEV_DIR"/n*/targets
    rm -f "$LOGDIR"/*.src "$LOGDIR"/*.expect
}

m11_core_unit() {
    tests_at_least 5 --release -p wmfs-core --lib bg:: || return 1
    one_test --release -p wmfs-core --lib meta::attr_registry::tests::compress_and_encrypt_are_parsed || return 1
    one_test --release -p wmfs-core --lib meta::data_reports::tests::epochs_that_are_current_or_being_migrated_are_alive
}

m11_golden() {
    one_test --release -p wmfs-api types::tasks::tests::tasks_and_datakeys_json_golden || return 1
    one_test --release -p wmfs-api types::mgmt::tests::usage_status_semantics_json_golden || return 1
    one_test --release -p wmfs-api ops::tests::values_match_adr_0029 || return 1
    tests_at_least 1 --release -p wmfs-core --test golden_constants || return 1
    tests_at_least 1 --release -p wmfs-core --test golden_records
}

m11_regress() {
    one_test --release -p wmfs-testkit --test failpoint_bg a_failed_or_crashed_step_is_retried_and_replaying_it_is_harmless || return 1
    tests_at_least 3 --release -p wmfs-testkit --test data_gc || return 1
    tests_at_least 1 --release -p wmfs-testkit --test client_reopen || return 1
    tests_at_least 1 --release -p wmfs-testkit --test failpoint_chunk
}

# 一个属性在 wmfs status 里的那一行（m11_attr <目录> <键>）；/home/data 里一个键还有几个文件与声明不同
m11_attr() { wmfs status "$1" --json | jq -c --arg k "$2" '[.attrs[] | select(.key == $k)][0]'; }
m11_mismatched() { m11_attr "$M/home/data" "$1" | jq '.actual.mismatched_files'; }
# 任务列表里一个任务的集群级那一行（leader 任务的进度就在这一行）
m11_task() { wmfs task list --json | jq -c --arg k "$1" '[.tasks[] | select(.kind == $k and .node == 0)][0]'; }
# m11_task_is <任务> <jq 条件>、m11_attr_is <目录> <键> <jq 条件>：那一行满足条件（给 poll 用）
m11_task_is() { jq -e "$2" <<<"$(m11_task "$1")" >/dev/null; }
m11_attr_is() { jq -e "$3" <<<"$(m11_attr "$1" "$2")" >/dev/null; }
# m11_metric <节点> <带标签的指标>：这个节点 /metrics 里它的值（没有就是 0）；m11_retries：3 个节点上迁移原语作废重做的次数之和
m11_metric() { curl --noproxy '*' -s "http://127.0.0.1:19${1}80/metrics" | awk -v n="$2" '$1 == n { v = $2 } END { print v + 0 }'; }
m11_retries() { local n s=0; for n in 1 2 3; do s=$((s + $(m11_metric "$n" 'wmfs_migrations_total{result="retry"}'))); done; echo "$s"; }
m11_du() { du -sc --block-size=1 "$WMFS_DEV_DIR"/n*/targets | tail -1 | cut -f1; }
m11_md5() { cat "$M"/home/data/f* | md5sum | cut -d' ' -f1; }

m11_write_data() {
    local i
    mkdir "$M/home/data" || return 1
    for i in $(seq 1 20); do yes "WMFS-PLAINTEXT-MARKER $i" | head -c 8M >"$M/home/data/f$i" || return 1; done
    MD5="$(m11_md5)" || return 1
    DU0="$(m11_du)"
    echo "内容的 md5：$MD5；3 个节点的 target 合计物理占用 $DU0 字节"
    [ "$DU0" -ge $((400 * MiB)) ]
}

m11_rate() {
    # 最多 60 s：协调器搬完第一个文件、开始下一步时才有速率
    poll 300 0.2 m11_task_is reconciler '.state == "running" and .rate_bps > 0 and .backlog >= 1' || return 1
    wmfs task list
    local t
    t="$(m11_task reconciler)"
    echo "速率 $(jq '.rate_bps' <<<"$t") 字节/秒（限速 $((2 * MiB))）"
    [ "$(jq '.rate_bps' <<<"$t")" -le $((3 * MiB)) ]
}

# m11_kill_leader 用：新 leader 接着推进了吗（与声明不同的接着变少、累计处理数接着涨）；累计处理数倒退过就记在 LOW 里
m11_moved_on() {
    AFTER="$(m11_mismatched compress 2>/dev/null)" && T="$(m11_task reconciler 2>/dev/null)" || return 1
    D1="$(jq '.done // empty' <<<"$T")" && [ -n "$D1" ] && [ -n "$AFTER" ] || return 1
    [ "$D1" -lt "$D0" ] && LOW="$D1" # 累计处理数倒退了 = 从头再来
    [ "$AFTER" -lt "$BEFORE" ] && [ "$D1" -gt "$D0" ]
}

m11_kill_leader() {
    local p0 L u0
    # 等进度刚持久化完的那一刻再杀（调度器每 5 s 最多持久化一次，3.10）：这时 leader 内存里的进度就是库里的，新 leader 必须从它接着数
    u0="$(m11_task reconciler | jq '.updated_ms')"
    poll 600 0.05 m11_task_is reconciler ".updated_ms != $u0" || return 1
    T="$(m11_task reconciler)" && BEFORE="$(m11_mismatched compress)" && D0="$(jq '.done' <<<"$T")" && LOW="" || return 1
    p0="$(jq '.pass' <<<"$T")"
    L="$(wmfs dev leader)" || return 1
    echo "杀掉元数据 leader node$L（这时：与声明不同的文件 $BEFORE 个，协调器累计处理 $D0 个）"
    [ "$BEFORE" -ge 1 ] || { echo "协调器已经做完了：看不到断点续跑"; return 1; }
    wmfs dev kill "$L" && KILLED="$L" && wmfs dev wait leader --timeout 60 || return 1
    # 最多 120 s：被杀节点上的链成员要等一个 target 租约才被移出，之后迁移接着写
    poll 600 0.2 m11_moved_on || { echo "新 leader 没有接着推进：$T"; return 1; }
    echo "新 leader node$(wmfs dev leader) 上：与声明不同的文件 $AFTER 个，协调器累计处理 $D1 个（之前第 $p0 轮）：$T"
    [ -z "$LOW" ] || { echo "累计处理数倒退到了 $LOW"; return 1; }
    [ "$(jq '.pass' <<<"$T")" = "$p0" ]
}

m11_restart_killed() { wmfs dev start "$KILLED" && KILLED="" && wmfs dev wait chains-serving --timeout 180; }

# m11_idle_converted <键> [逐文件的 jq 条件]：协调器做完、status 里这个键不一致 0（给了条件就再逐个文件看 wmfs fs stat 的 layout）
m11_idle_converted() {
    wmfs task set reconciler bw=200M --json || return 1
    wmfs dev wait task-idle reconciler --timeout 300 || return 1
    json_expect "[.attrs[] | select(.key == \"$1\")][0] | .actual.mismatched_files == 0 and (.mismatched | not)" \
        wmfs status "$M/home/data" --json || return 1
    [ -z "${2:-}" ] || m11_each_file "$2"
}

# /home/data 下每个 f* 的 `wmfs fs stat --json` 都满足 jq 条件（逐文件，不只看 status 的汇总）
m11_each_file() {
    local i
    for i in $(seq 1 20); do wmfs fs stat "/home/data/f$i" --json | jq -e "$1" >/dev/null || { echo "f$i：不满足 $1"; return 1; }; done
}

m11_gc_shrinks() {
    local du md5
    wmfs task run gc --json || return 1
    wmfs dev wait task-idle gc --timeout 120 || return 1
    md5="$(m11_md5)" || return 1
    du="$(m11_du)"
    echo "md5：$md5（转换前 $MD5）；物理占用：$du 字节（转换前 $DU0 字节）"
    [ "$md5" = "$MD5" ] && [ $((du * 3)) -lt "$DU0" ]
}

m11_hit() { [ "$(m11_metric "$1" 'wmfs_failpoint_hits_total{name="bg.migrate.before_switch"}')" -gt "$2" ]; }
m11_retried() { [ "$(m11_retries)" -gt "$1" ]; }

m11_odirect() {
    local d="$M/home/od" L r0 h0
    mkdir "$d" || return 1
    head -c 16M /dev/urandom >"$LOGDIR/od1.src" && head -c 16M /dev/urandom >"$LOGDIR/od2.src" || return 1
    dd if="$LOGDIR/od1.src" of="$d/f" bs=1M oflag=direct status=none || return 1
    L="$(wmfs dev leader)" && r0="$(m11_retries)" || return 1
    h0="$(m11_metric "$L" 'wmfs_failpoint_hits_total{name="bg.migrate.before_switch"}')"
    # 迁移把新布局的数据写好、切换之前拖住 3 s（fail point），这段时间里 O_DIRECT 覆盖写两次：切换的 CAS 失败、作废重做，内容是最后写的
    wmfs failpoint set "$L" bg.migrate.before_switch 'times(1,sleep(3000))' || return 1
    wmfs set "$d" compress=lz4 --json || return 1
    poll 300 0.1 m11_hit "$L" "$h0" || { echo "迁移没有走到切换之前"; return 1; }
    dd if="$LOGDIR/od2.src" of="$d/f" bs=1M oflag=direct conv=notrunc status=none || return 1
    dd if="$LOGDIR/od1.src" of="$d/f" bs=1M oflag=direct conv=notrunc status=none || return 1
    poll 300 0.2 m11_retried "$r0" || { echo "迁移没有因为这两次写作废"; return 1; }
    echo "迁移在切换之前碰上了 O_DIRECT 覆盖写：作废重做 $(($(m11_retries) - r0)) 次"
    wmfs dev wait task-idle reconciler --timeout 300 || return 1
    cmp "$LOGDIR/od1.src" "$d/f" || return 1
    json_expect '[.attrs[] | select(.key == "compress")][0] | .declared == "lz4" and .actual.mismatched_files == 0' \
        wmfs status "$d" --json
}

# 写者进程：写打开 $1、写一段并 fsync（已提交）、不关；等到 $LOGDIR/w.go 出现再写一段、关掉，把结果写进 $LOGDIR/w.result。用 python 是
# 因为它等待时不起子进程（子进程会继承这个写 fd）。
m11_start_writer() {
    rm -f "$LOGDIR/w.ready" "$LOGDIR/w.go" "$LOGDIR/w.result"
    python3 -c '
import os, sys, time
path, d = sys.argv[1], sys.argv[2]
fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
os.write(fd, b"WMFS-OPEN-WRITER\n" * 65536)
os.fsync(fd)
open(d + "/w.ready", "w").close()
while not os.path.exists(d + "/w.go"):
    time.sleep(0.1)
try:
    os.write(fd, b"MORE\n" * 65536)
    os.close(fd)
    r = "ok"
except OSError as e:
    r = os.strerror(e.errno)
open(d + "/w.result", "w").write(r)
' "$1" "$LOGDIR" &
    WRITER=$!
    poll 100 0.1 test -e "$LOGDIR/w.ready" || { echo "写者进程 10 s 还没写好"; return 1; }
}

m11_open_skip() {
    local d="$M/home/open"
    mkdir "$d" || return 1
    yes "WMFS-OPEN-CLOSED" | head -c 4M >"$d/closed" || return 1
    m11_start_writer "$d/w" || return 1
    # 先暂停协调器：leader 经会话心跳（≤ 3 s 一次）才知道谁写打开着什么，在那之前开始的迁移照常做（写者随后的提交拿到 STALE_LAYOUT、
    # 重写到新布局，数据不丢——3.10 第 6 步），就看不到"跳过"了；调度器每秒读一次控制字段（任务列表里 leader 的那一行是它此刻的样子）
    json_expect '.state == "paused"' wmfs task pause reconciler --json || return 1
    poll 100 0.1 m11_task_is reconciler '.state == "paused"' || { echo "10 s 了调度器还没停"; return 1; }
    wmfs set "$d" compress=lz4 --json || return 1
    # 最多 60 s：status 数到写打开着的那个
    poll 300 0.2 m11_attr_is "$d" compress '.actual | .files == 2 and .open_files == 1 and .mismatched_files == 2' || return 1
    m11_attr "$d" compress
    json_expect '.state != "paused"' wmfs task resume reconciler --json || return 1
    # 最多 60 s：协调器转换完关着的那个、跳过写打开着的
    poll 300 0.2 m11_attr_is "$d" compress '.actual | .files == 2 and .open_files == 1 and .mismatched_files == 1' || return 1
    m11_attr "$d" compress
    json_expect '.backlog >= 1' m11_task reconciler || return 1
    touch "$LOGDIR/w.go"
    wait "$WRITER"
    WRITER=""
    echo "写者进程：$(cat "$LOGDIR/w.result" 2>/dev/null)"
    [ "$(cat "$LOGDIR/w.result" 2>/dev/null)" = ok ] || return 1
    wmfs dev wait task-idle reconciler --timeout 120 || return 1
    json_expect '[.attrs[] | select(.key == "compress")][0].actual | .open_files == 0 and .mismatched_files == 0' \
        wmfs status "$d" --json || return 1
    python3 -c 'import sys; sys.stdout.buffer.write(b"WMFS-OPEN-WRITER\n" * 65536 + b"MORE\n" * 65536)' >"$LOGDIR/w.expect"
    cmp "$LOGDIR/w.expect" "$d/w"
}

m11_gc_open() {
    local d="$M/home/keep" old t0 p0
    mkdir "$d" && head -c 3M /dev/urandom >"$LOGDIR/keep1.src" && cp "$LOGDIR/keep1.src" "$d/f" || return 1
    old="$(wmfs debug chunk-path /home/keep/f 0)" && echo "旧布局的 chunk：$old" && [ -e "$old" ] || return 1
    exec 4<"$d/f" || return 1 # 只读打开着（一直开到下面关掉；迁移只跳过写打开的）
    # 协调器把它换到新布局：旧布局记为垃圾（GC 记录 kind 3，epoch_gc_grace 是 0）。截断换下来的过了宽限期就删，不等关闭（组件测试 gc_full）
    wmfs set "$d" compress=lz4 --json >/dev/null || { exec 4<&-; return 1; }
    poll 300 0.2 m11_attr_is "$d" compress '.actual.mismatched_files == 0' || { exec 4<&-; return 1; }
    t0=$SECONDS
    p0="$(m11_task gc | jq '.pass')"
    while :; do # 看满 10 s、gc 又跑了至少 3 轮：旧 chunk 一直在
        [ -e "$old" ] || { echo "文件还打开着，旧布局的 chunk 被删了"; exec 4<&-; return 1; }
        [ $((SECONDS - t0)) -ge 10 ] && [ "$(m11_task gc | jq '.pass')" -ge $((p0 + 3)) ] && break
        [ $((SECONDS - t0)) -lt 60 ] || { echo "60 s 里 gc 没跑够 3 轮"; exec 4<&-; return 1; }
        sleep 0.5
    done
    echo "转换之后 $((SECONDS - t0)) s、gc 又跑了 $(($(m11_task gc | jq '.pass') - p0)) 轮：旧 chunk 还在"
    exec 4<&-
    # 关掉之后：心跳报上关闭（≤ 3 s），再过 gc 的一轮；最多 60 s
    poll 600 0.1 test ! -e "$old" || { echo "关掉 60 s 之后旧 chunk 还在"; return 1; }
    echo "关掉之后旧 chunk 被删了"
    cmp "$LOGDIR/keep1.src" "$d/f"
}

m11_encrypt() {
    local hits md5
    json_expect '.set.encrypt == "tenant:k1"' wmfs set "$M/home/data" encrypt=tenant:k1 --json || return 1
    wmfs dev wait task-idle reconciler --timeout 300 || return 1
    json_expect '[.attrs[] | select(.key == "encrypt")][0] | .declared == "tenant:k1" and .actual.mismatched_files == 0' \
        wmfs status "$M/home/data" --json || return 1
    m11_each_file '.layout.encrypt_key != 0' || return 1
    wmfs task run gc --json >/dev/null || return 1
    wmfs dev wait task-idle gc --timeout 120 || return 1
    hits="$(grep -rl "WMFS-PLAINTEXT-MARKER" "$WMFS_DEV_DIR"/n*/targets/ | head -1)"
    [ -z "$hits" ] || { echo "target 目录里还有明文：$hits"; return 1; }
    echo "grep -rl WMFS-PLAINTEXT-MARKER（3 个节点的 target）：没有输出"
    md5="$(m11_md5)"
    echo "md5：$md5（原来 $MD5）"
    [ "$md5" = "$MD5" ]
}

m11_scrub() {
    local ino rep good
    ino="$(wmfs fs stat /home/data/f1 --json | jq '.ino')"
    wmfs debug corrupt-chunk /home/data/f1 0 --node 2 --block 0 || return 1
    wmfs task run scrub --json || return 1
    wmfs dev wait task-idle scrub --timeout 300 || return 1
    json_expect "any(.events[]; .op == \"ChecksumRepaired\" and .ino == $ino)" wmfs changelog tail -n 50 --json || return 1
    [ "$(m11_md5)" = "$MD5" ] || return 1
    # 再来一轮：节点 2 上修好的副本自己也校验得过（不再修复，查过的好副本变多）
    rep="$(m11_metric 2 'wmfs_scrub_chunks_total{result="repaired"}') $(m11_metric 2 'wmfs_scrub_chunks_total{result="good"}')"
    wmfs task run scrub --json >/dev/null && wmfs dev wait task-idle scrub --timeout 300 || return 1
    good="$(m11_metric 2 'wmfs_scrub_chunks_total{result="repaired"}') $(m11_metric 2 'wmfs_scrub_chunks_total{result="good"}')"
    echo "节点 2 的（修复数 好副本数）：$rep → $good"
    [ "${good% *}" = "${rep% *}" ] && [ "${good#* }" -gt "${rep#* }" ]
}

m11_data_loss() {
    local f="$M/home/lost" ino n
    head -c 4M /dev/urandom >"$LOGDIR/lost.src" && cp "$LOGDIR/lost.src" "$f" && sync "$f" || return 1
    ino="$(wmfs fs stat /home/lost --json | jq '.ino')"
    for n in 1 2 3; do
        wmfs debug corrupt-chunk /home/lost 0 --node "$n" --block 0 || return 1
    done
    wmfs task run scrub --json >/dev/null && wmfs dev wait task-idle scrub --timeout 300 || return 1
    json_expect "any(.events[]; .op == \"DataLossDetected\" and .ino == $ino)" wmfs changelog tail -n 50 --json || return 1
    if dd if="$f" of=/dev/null bs=1M count=1 iflag=direct status=none; then
        echo "三个副本都坏了，O_DIRECT 读却成功了"
        return 1
    fi
    echo "O_DIRECT 读报错（不返回坏数据）"
    rm -f "$f" # 删掉它：丢了数据的文件会一直挡着协调器的那一项
}

m11_replicas() {
    json_expect '.set.replicas == "2"' wmfs set "$M/home/data" replicas=2 --json || return 1
    wmfs dev wait task-idle reconciler --timeout 300 || return 1
    json_expect '[.attrs[] | select(.key == "replicas")][0] | .declared == "2" and .actual.mismatched_files == 0' \
        wmfs status "$M/home/data" --json || return 1
    json_expect '.layout.replicas == 2 and (.chains | length) >= 1 and all(.chains[]; (.members | length) == 2)' \
        wmfs fs stat /home/data/f1 --json || return 1
    [ "$(m11_md5)" = "$MD5" ]
}

m11_task_control() {
    local L
    json_expect '.kind == "reconciler" and .state == "paused"' wmfs task pause reconciler --json || return 1
    poll 100 0.1 m11_task_is reconciler '.state == "paused"' || return 1
    json_expect 'any(.tasks[]; .kind == "reconciler" and .state == "paused")' wmfs task list --json || return 1
    # 暂停中设一个新的压缩、杀掉元数据 leader：新 leader 读回的 Paused 只是控制字段，恢复之后照常转换完（不会做一步就又停了）
    wmfs set "$M/home/data" compress=zstd:1 --json >/dev/null || return 1
    L="$(wmfs dev leader)" || return 1
    wmfs dev kill "$L" && wmfs dev wait leader --timeout 60 || return 1
    KILLED="$L"
    json_expect '.kind == "reconciler" and .state != "paused"' wmfs task resume reconciler --json || return 1
    m11_restart_killed || return 1
    m11_idle_converted compress || return 1
    [ "$(m11_md5)" = "$MD5" ] || return 1
    json_expect '[.tasks[] | select(.kind == "scrub")] | length == 4 and all(.[]; .runs_on == "every-node") and ([.[] | .node] | sort == [0, 1, 2, 3])' \
        wmfs task show scrub --json || return 1
    expect_error enoent wmfs task pause nope --json
}

m11_read_only() {
    wmfs failpoint set 3 chunk.commit.before_rename 'return(EIO)' || return 1
    head -c 4M /dev/urandom >"$LOGDIR/ro.src" || return 1
    cp "$LOGDIR/ro.src" "$M/home/ro" || { echo "写失败（应当重试到节点 3 被移出链）"; return 1; }
    # 最多 120 s：心跳报 read_only，链管理器把它移出（2 副本的链先补上别的成员、同步完）
    poll 600 0.2 json_expect 'all(.chains[]; all(.members[]; .node != 3))' wmfs chain list --json >/dev/null ||
        { wmfs chain list --json; echo "节点 3 的 target 还在链里"; return 1; }
    cmp "$LOGDIR/ro.src" "$M/home/ro" || return 1
    echo again >"$M/home/ro2" && [ "$(cat "$M/home/ro2")" = again ] || return 1
    wmfs failpoint clear 3 chunk.commit.before_rename || return 1
    wmfs dev stop 3 && wmfs dev start 3 || return 1 # 只读是运行中的状态：重启之后清掉，它的 target 回到链里
    # 集群图里的 read_only 要等 leader 处理了节点 3 重启之后的第一次心跳才清掉；在那之前期望长度仍按 2 个故障域算，chains-serving 马上
    # 就成立。所以先等节点 3 的 target 回到链里、成为 Serving（最多 180 s），再等全部链到齐——这时每条链的期望长度都回到声明的副本数
    poll 900 0.2 json_expect 'any(.chains[]; any(.members[]; .node == 3 and .state == "serving"))' wmfs chain list --json >/dev/null ||
        { wmfs chain list --json; echo "节点 3 的 target 没有回到链里"; return 1; }
    wmfs dev wait chains-serving --timeout 180 || return 1
    json_expect 'all(.chains[]; .want == .replicas)' wmfs chain list --json || return 1
    cmp "$LOGDIR/ro.src" "$M/home/ro"
}
