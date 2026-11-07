# M13 存储类 + 放置 + QoS + 分层：逐条对应 roadmap §3 M13 的"验收标准"与"演示命令"。
# 由 scripts/accept.sh 加载（用法见那里的说明）；这里只定义函数，不直接运行。
#
# 验收标准：目录的 l= 上限在单流写入时被观察到（指标与 dd 速度，误差 ±15%）；虚拟时钟单元测试里权重 1:3 的份额误差 < 5%、priority = low 的
#           队列只在 normal 队列没有可派发请求时才被服务；改 class 触发迁移；wmfs class update 只改 qos 时不迁移、改 replica 时触发协调；
#           tier 用短条件按预期迁移；wmfs class show 显示"shared：只承诺份额"，对声明了 dedicated 的类产生 isolation_downgraded 事件；
#           物理盘解析单测（构造的假 /sys：同一块 NVMe 的两个分区、同一个 PV 上的两个 LV 都判为共享，loop 判为 unknown）；
#           locality=prefer-client 时新文件的链头在客户端所在"主机"（wmfs dev mount --node n）。
#
# 开发集群与挂载点只用本次验收自己的目录（$LOGDIR/dev、$LOGDIR/mnt、$LOGDIR/m2；端口照常是 19n00/19n80）；无论成败，退出前都 wmfs dev down
# （先卸载挂载点、再停节点）并删掉生成的大文件（m13_cleanup）。只解析 --json 输出、指标与退出码；例外两处正是要检查的东西：dd 的速度用
# 前后的时刻自己算（不解析 dd 的文字），`wmfs class show` 的文字里的 iops 换算（roadmap 的"显示 iops 换算"）。等待一律按条件（poll、wmfs dev wait）。
#
# QoS 上限的判据（m13_limit）：目录 qos=r=0,w=1,l=270、default 类 3 个节点 → 每节点上限 90 成本单位/秒；dd bs=1M oflag=direct 的每次写是一个
# 1 MiB 的 FUSE WRITE = 链头上一个成本 9 的请求（1 + 1 MiB / 128 KiB）→ 每秒 10 个、10 MiB/s。dd 的速度与"三个节点 /metrics 里这个 qos_id 的
# 派发数 × 9 / 用时"都要在 90 的 ±15% 里。上限只会让它变慢：机器忙的时候 1 MiB 的写也远不到 100 ms，留的余量（15%）只吸收计时与第一个请求
# 不用等的误差，不放宽到"没限住也能过"。量之前按条件等两件事（m13_probe）：QoS 表经集群图到了链头（各节点按心跳拉新图，刚 set 完的
# 一两百毫秒里还按旧图、不限速），以及这个文件的写路径不卡（新文件的第一批写、换图那一下的 FENCED 重试都在探测里过去）；探测与测量
# 写同一个文件（conv=notrunc：layout 不变，还是同一个链头）。

# shellcheck source=/dev/null
source "$ROOT/scripts/accept.d/m6.sh"

accept_m13() {
    check "开发集群的端口 19100–19380 都空闲（演示用的开发集群要先 wmfs dev down）" dev_ports_free M13 3
    check "没有残留的 wmfs FUSE 挂载（有的话先 wmfs dev down --purge）" m6_no_mounts
    check "cargo build --release 成功且零警告" build_no_warnings
    check "单元（虚拟时钟）：mclock 权重 1:3 的份额误差 < 5%" \
        one_test --release -p wmfs-plugins --lib mclock::tests::weights_1_to_3_share_within_5_percent
    check "单元（虚拟时钟）：priority = low 的队列只在 normal 队列没有可派发请求时才被服务（权重大得多也一样）" \
        one_test --release -p wmfs-plugins --lib mclock::tests::low_priority_is_served_only_when_normal_has_nothing_dispatchable
    check "单元：mclock 的上限、保底、字节令牌桶（桶满起步）、作业 id 轮转（只记前 100 个）、后台队列、刚活跃的队列只从同一优先级起算、保底欠额只在窗口大半时间有积压时才报、空闲队列回收" \
        tests_at_least 11 --release -p wmfs-plugins --lib mclock
    check "单元：物理盘解析（构造的假 /sys：同一块 NVMe 的两个分区、同一个 PV 上的两个 LV 都判为共享，loop 判为 unknown）与隔离承诺" \
        one_test --release -p wmfs-core --lib meta::cluster::classes::tests::isolation_follows_physical_disks_not_partitions_or_lvs
    check "单元：存储类的 TOML、缺省值与校验、update 的规则（只改 qos 不迁移、改 replica/lifecycle 协调、改 label 拒绝）、dedicated 独占 target、别的类的旧链也算共用、不许饿死别的类、每节点份额不取整" \
        tests_at_least 7 --release -p wmfs-core --lib meta::cluster::classes
    check "单元：心跳报的保底欠额（5 s 内的之和）与持续 10 s 才写一次 PolicyViolation（金样：extra 的键）、同步源按实际搬的字节计费" m13_units
    check "单元：qos、tier、locality 的切分、校验与 DIRATTR 里存的形式（金样），tier 条件的求值" \
        one_test --release -p wmfs-core --lib meta::attr_registry::tests::qos_tier_and_locality_are_parsed
    check "单元：label-match 的 prefer-client、tiering、rebalance（差距最大的那一组）、cache-local-nvme（LRU、挂载时只清空自己的文件）" \
        tests_at_least 9 --release -p wmfs-plugins --lib -- label_match tiering rebalance cache_local_nvme
    check "金样：存储类端点的 JSON；常量表" m13_golden
    check "完整节点（进程内）：存储类增删改查与删除的规则、改 class 迁移、update 只改 qos 不迁移 / 改 replica 整树协调、tier 短条件迁移且条件不再成立也不被迁回、dedicated 共用物理盘 → isolation_downgraded、label * 的 dedicated 被拒、qos 表随集群图下发与 GET /api/v1/qos、qos=<类> 是活的借用（改了类的 [qos] 跟着变、被借用的类删不掉）、3 节点：prefer-client 的链头、每节点份额、读副本按 chunk 分散、rebalance 迁走最满链上的文件" \
        tests_at_least 5 --release -p wmfs-testkit --test classes
    check "fail point：plugin.mclock.qos、客户端读的 plugin.replica.codec（read_order 出错退回链序）、plugin.tiering.bg_task、plugin.rebalance.bg_task" \
        tests_at_least 1 --release -p wmfs-testkit --test failpoint_m13
    check "fail point：plugin.cache-local-nvme.cache_store（缓存出错只绕过缓存，读照样正确）" \
        one_test --release -p wmfs --test failpoint_fuse a_failing_local_nvme_cache_only_bypasses_the_cache
    check "回归：属性（M7）、管理 API、协调器与迁移原语（M11）" m13_regress
    check "M13 收尾（ADR-0044）：p99 的计算与 10 s 窗口、p99 持续超标一次一写与 extra 的金样、每个 QoS 队列最多 512 个排队请求、label hdd 在 nvme 盘上的标注、prefer-client 的退路、新会话不会刚开就被驱逐" \
        m13_adr44

    export WMFS_DEV_DIR="$LOGDIR/dev"
    M="$LOGDIR/mnt"
    M2="$LOGDIR/m2"
    mkdir -p "$M" "$M2"
    trap m13_cleanup EXIT
    check "演示：wmfs dev up --nodes 3" wmfs dev up --nodes 3
    eval "$(wmfs dev env)"
    printf 'name = "gold"\n[devices]\nlabel = "*"\n[redundancy]\nreplica = 2\n[qos]\niops_max = 300\n' >"$LOGDIR/gold.toml"
    check_json "演示：wmfs class create gold --file gold.toml" '.class.name == "gold" and .class.replica == 2' \
        wmfs class create gold --file "$LOGDIR/gold.toml" --json
    check_json "演示：wmfs class show gold → iops_max 300、3 个节点均分后每节点 100、isolation shared（只承诺份额，不承诺尾延迟）" \
        '.qos.iops_max == 300 and .nodes == 3 and .per_node.iops_max == 100 and .isolation == "shared" and .promise == "只承诺份额，不承诺尾延迟"' \
        wmfs class show gold --json
    check "wmfs class show 的文字显示 iops 换算：iops_max=300 ≈ 每秒 9 次 4 MiB 写 ≈ 36 MiB/s，按 3 个节点均分后每节点 100" m13_show_text
    check "声明了 dedicated 的类按实际部署算是 shared → 回复里 downgraded、changelog 里 PolicyViolation（kind=isolation_downgraded）；没用过的类删得掉；label * 的 dedicated 类（会让 default 一个 target 也没有）建不了" \
        m13_dedicated
    check "演示：wmfs dev mount <挂载点> --cache none" wmfs dev mount "$M" --cache none
    check_json "演示：mkdir fast && wmfs set fast class=gold" '.set.class == "gold"' m13_fast_dir
    check "演示：dd bs=4M count=50 oflag=direct 写进 gold 目录 → 速度被限在每节点 100 成本单位/秒（约 11 MiB/s，±15%）" m13_demo_slow
    check "演示：wmfs set fast qos=r=0,w=1,l=3000 之后 dd 明显变快（上限放大 10 倍：至少快 1.5 倍）" m13_demo_fast
    check "演示：/metrics 里有 wmfs_qos_served_total（三个节点合起来，fast 目录的队列有派发）" m13_metric_present
    check_json "演示：wmfs status fast → class=gold，replicas 声明 2、实际 2" \
        '(.attrs[] | select(.key == "class") | .declared == "gold" and (.mismatched | not)) and (.attrs[] | select(.key == "replicas") | .declared == "2" and .actual.new_files == "2")' \
        wmfs status "$M/fast" --json
    check "目录的 l= 上限在单流写入时被观察到：qos=r=0,w=1,l=270 → 每节点 90 成本单位/秒，dd bs=1M 的速度与指标算出的派发速率都在 ±15% 里" \
        m13_limit
    check "改 class 触发迁移：default 类里的文件，目录改成 class=gold 之后由协调器迁过去（status 的 class 一行一致、layout 是 2 副本）" m13_class_moves
    check "wmfs class update：只改 qos → reconcile=false、文件的 epoch 不变；改 replica → reconcile=true、协调器把文件迁成 2 副本" m13_update
    check "tier 用短条件按预期迁移：tier=gold->cold:size>100K&age>1s → wmfs task run tiering 之后大文件到了 cold（1 副本），小文件留在 gold；之后截短到条件不再成立，协调器也不把它迁回去" m13_tier
    check "locality=prefer-client：wmfs dev mount --node 2 的客户端新建的文件，链头都在节点 2 上；--cache local-nvme 读回一致、缓存目录里有块" m13_locality
    check "wmfs dev down（先卸载挂载点，再停全部节点）" wmfs dev down
    check "结束：本次验收自己的挂载都已卸载（$LOGDIR 下没有 fuse.wmfs 挂载）" m6_no_own_mounts
    check "profile minimal（scripts/profiles.sh minimal；ADR-0015：M6 起每个里程碑跑；qos 用 fifo）" scripts/profiles.sh minimal
    check "profile nas-safe 阶段版（scripts/profiles.sh nas-safe；ADR-0015：M12–M13 /home 带 trash=7d、audit=on）" scripts/profiles.sh nas-safe
}

m13_cleanup() {
    wmfs dev down >/dev/null 2>&1
    [ -n "$LOGDIR" ] && [ "${WMFS_DEV_DIR:-}" = "$LOGDIR/dev" ] && rm -rf "$WMFS_DEV_DIR"/n*/targets "$LOGDIR/nv"
}

m13_golden() {
    one_test --release -p wmfs-api types::classes::tests::classes_json_golden || return 1
    one_test --release -p wmfs-api types::mgmt::tests::usage_status_semantics_json_golden || return 1
    tests_at_least 1 --release -p wmfs-core --test golden_constants
}

m13_units() {
    tests_at_least 2 --release -p wmfs-core --lib meta::cluster::targets || return 1
    one_test --release -p wmfs-core --lib meta::cluster::cluster_mgr::tests::the_qos_violation_event_is_golden || return 1
    one_test --release -p wmfs-core --lib data::service::tests::sync_calls_are_charged_by_what_they_move
}

m13_adr44() {
    tests_at_least 2 --release -p wmfs-core --lib data::latency || return 1
    one_test --release -p wmfs-core --lib data::slots::tests::a_full_queue_gets_busy_and_the_others_do_not || return 1
    one_test --release -p wmfs-core --lib \
        meta::cluster::targets::tests::a_class_p99_over_its_target_is_reported_once_per_episode || return 1
    one_test --release -p wmfs-core --lib meta::cluster::cluster_mgr::tests::the_latency_violation_event_is_golden || return 1
    one_test --release -p wmfs-core --lib meta::cluster::classes::tests::an_hdd_label_on_an_nvme_disk_is_marked || return 1
    one_test --release -p wmfs-plugins --lib \
        label_match::tests::prefer_client_falls_back_to_chains_with_a_member_on_the_client_node || return 1
    one_test --release -p wmfs-testkit --test evict_lease a_new_session_without_a_heartbeat_record_gets_a_full_lease
}

m13_regress() {
    tests_at_least 5 --release -p wmfs-testkit --test meta_attrs || return 1
    tests_at_least 2 --release -p wmfs-testkit --test mgmt_api || return 1
    tests_at_least 4 --release -p wmfs-testkit --test bg_reconcile
}

m13_show_text() {
    local out
    out="$(wmfs class show gold)" || return 1
    printf '%s\n' "$out"
    printf '%s' "$out" | grep -qF 'iops_max=300 ≈ 每秒 9 次 4 MiB 写 ≈ 36 MiB/s，按 3 个节点均分后每节点 100' &&
        printf '%s' "$out" | grep -qF 'isolation: shared（只承诺份额，不承诺尾延迟）'
}

m13_dedicated() {
    printf '[devices]\nlabel = "ded-test"\ndedicated = true\n[redundancy]\nreplica = 1\n' >"$LOGDIR/ded.toml"
    json_expect '.class.isolation == "shared" and .class.downgraded' wmfs class create ded --file "$LOGDIR/ded.toml" --json ||
        return 1
    json_expect '[.events[] | select(.op == "PolicyViolation" and (.extra | any(.[0] == "kind" and .[1] == "isolation_downgraded")) and (.extra | any(.[0] == "class" and .[1] == "ded")))] | length == 1' \
        wmfs changelog tail -n 20 --json || return 1
    json_expect '.class.name == "ded"' wmfs class delete ded --json || return 1
    printf '[devices]\nlabel = "*"\ndedicated = true\n[redundancy]\nreplica = 1\n' >"$LOGDIR/all.toml"
    expect_error einval wmfs class create all --file "$LOGDIR/all.toml" --json
}

m13_fast_dir() { mkdir "$M/fast" && wmfs set "$M/fast" class=gold --json; }

# m13_dd <文件> <块> <块数> [dd 的其它参数]：O_DIRECT 写，回 MiB/s（自己按前后时刻算，不解析 dd 的文字）
m13_dd() {
    local t0 t1
    t0="$(date +%s%3N)"
    dd if=/dev/zero of="$1" bs="$2" count="$3" oflag=direct status=none "${@:4}" || return 1
    t1="$(date +%s%3N)"
    awk -v b="$(stat -c %s "$1")" -v ms="$((t1 - t0))" 'BEGIN { printf "%.2f", b / 1048576 / (ms / 1000) }'
}

# m13_within <值> <期望> <比例>：|值 − 期望| ≤ 期望 × 比例
m13_within() { awk -v x="$1" -v w="$2" -v r="$3" 'BEGIN { exit !(x >= w * (1 - r) && x <= w * (1 + r)) }'; }

m13_demo_slow() {
    SLOW="$(m13_dd "$M/fast/z" 4M 50)" || return 1
    echo "gold 目录（每节点 100 成本单位/秒，1 MiB 的写成本 9 → 约 11.1 MiB/s）：$SLOW MiB/s"
    m13_within "$SLOW" 11.11 0.15
}

m13_demo_fast() {
    local fast
    wmfs set "$M/fast" qos=r=0,w=1,l=3000 --json | jq -e '.set.qos == "r=0,w=1,l=3000"' || return 1
    fast="$(m13_dd "$M/fast/z2" 4M 50)" || return 1
    echo "上限放大 10 倍之后：$fast MiB/s（之前 $SLOW MiB/s）"
    awk -v a="$fast" -v b="$SLOW" 'BEGIN { exit !(a >= 1.5 * b) }'
}

# roadmap 演示的那一行只看节点 1（每个节点都有后台队列的这一族，所以总有输出）；fast 的文件的链头在哪个节点由放置决定，所以按三个节点
# 合起来看 fast 目录（qos 的来源目录 = 它的 inode）的队列有没有派发
m13_metric_present() {
    local ino
    curl --noproxy '*' -s 127.0.0.1:19180/metrics | grep wmfs_qos_served_total | head -5
    curl --noproxy '*' -s 127.0.0.1:19180/metrics | grep -q wmfs_qos_served_total || return 1
    ino="$(wmfs fs stat /fast --json | jq -r .ino)" || return 1
    echo "fast（qos_id $ino）三个节点合起来派发了 $(m13_served "$ino") 个请求"
    [ "$(m13_served "$ino")" -gt 0 ]
}

# m13_served <qos_id>：三个节点 /metrics 里这个 qos_id 的派发数之和（权重阶段与保底阶段）
m13_served() {
    local n s=0 v
    for n in 1 2 3; do
        v="$(curl --noproxy '*' -s "http://127.0.0.1:19${n}80/metrics" |
            awk -v q="qos_id=\"$1\"" 'index($0, "wmfs_qos_served_total{") == 1 && index($0, q) { s += $2 } END { print s + 0 }')"
        s=$((s + v))
    done
    echo "$s"
}

# m13_probe <文件>：往文件开头写 3 个 1 MiB（O_DIRECT、不截断）。链头按目录的 qos 限速时后两个各要等一个间隔（100 ms），整个在
# 180–400 ms 之间才算"上限已经生效、写路径也不卡"（更快 = 链头还没拉到新的集群图；更慢 = 这会儿在卡，再等等）。
m13_probe() {
    local t0 t1
    t0="$(date +%s%3N)"
    dd if=/dev/zero of="$1" bs=1M count=3 oflag=direct conv=notrunc status=none || return 1
    t1="$(date +%s%3N)"
    echo "探测：3 MiB 用了 $((t1 - t0)) ms"
    [ "$((t1 - t0))" -ge 180 ] && [ "$((t1 - t0))" -le 400 ]
}

m13_limit() {
    local ino a b t0 t1 speed rate
    mkdir "$M/lim" && wmfs set "$M/lim" qos=r=0,w=1,l=270 --json >/dev/null || return 1
    ino="$(wmfs fs stat /lim --json | jq -r .ino)" || return 1
    poll 100 0.3 m13_probe "$M/lim/z" || return 1
    a="$(m13_served "$ino")"
    t0="$(date +%s%3N)"
    speed="$(m13_dd "$M/lim/z" 1M 60 conv=notrunc)" || return 1
    t1="$(date +%s%3N)"
    b="$(m13_served "$ino")"
    rate="$(awk -v n="$((b - a))" -v ms="$((t1 - t0))" 'BEGIN { printf "%.1f", n * 9 / (ms / 1000) }')"
    echo "dd：$speed MiB/s（期望 10）；指标：派发了 $((b - a)) 个 1 MiB 的写 → $rate 成本单位/秒（期望每节点上限 90）"
    [ "$((b - a))" -ge 60 ] && m13_within "$speed" 10 0.15 && m13_within "$rate" 90 0.15
}

m13_class_moves() {
    mkdir "$M/mv" && head -c 300000 /dev/urandom >"$M/mv/f" || return 1
    json_expect '.layout.replicas == 3' wmfs fs stat /mv/f --json || return 1
    wmfs set "$M/mv" class=gold --json >/dev/null || return 1
    wmfs dev wait task-idle reconciler --timeout 120 || return 1
    json_expect '.attrs[] | select(.key == "class") | .actual.mismatched_files == 0 and .declared == "gold"' wmfs status "$M/mv" --json &&
        json_expect '.layout.replicas == 2' wmfs fs stat /mv/f --json
}

m13_epoch() { wmfs fs stat /up/f --json | jq -r .layout.epoch; }
m13_replicas_now() { [ "$(wmfs fs stat /up/f --json | jq -r .layout.replicas)" = "$1" ]; }

m13_update() {
    local e0
    printf '[devices]\nlabel = "*"\n[redundancy]\nreplica = 1\n' >"$LOGDIR/up.toml"
    wmfs class create up --file "$LOGDIR/up.toml" --json >/dev/null || return 1
    mkdir "$M/up" && wmfs set "$M/up" class=up --json >/dev/null && head -c 300000 /dev/urandom >"$M/up/f" || return 1
    e0="$(m13_epoch)"
    printf '[devices]\nlabel = "*"\n[redundancy]\nreplica = 1\n[qos]\niops_max = 5000\n' >"$LOGDIR/up.toml"
    json_expect '.reconcile == false and .class.qos.iops_max == 5000' wmfs class update up --file "$LOGDIR/up.toml" --json || return 1
    wmfs dev wait task-idle reconciler --timeout 120 || return 1
    echo "只改 qos：epoch $e0 → $(m13_epoch)"
    [ "$(m13_epoch)" = "$e0" ] && m13_replicas_now 1 || return 1
    printf '[devices]\nlabel = "*"\n[redundancy]\nreplica = 2\n[qos]\niops_max = 5000\n' >"$LOGDIR/up.toml"
    json_expect '.reconcile == true' wmfs class update up --file "$LOGDIR/up.toml" --json || return 1
    poll 120 1 m13_replicas_now 2 || return 1
    echo "改 replica：epoch $e0 → $(m13_epoch)，layout 2 副本"
    expect_error einval wmfs class update up --file <(printf '[devices]\nlabel = "ssd"\n[redundancy]\nreplica = 2\n') --json
}

# 分层：跑一轮 tiering，看大文件的 layout 是不是已经换成 cold 的 1 副本（age>1s：刚建的文件要过 1 s 才成立，所以按条件反复跑）
m13_tier_moved() {
    wmfs task run tiering --json >/dev/null || return 1
    [ "$(wmfs fs stat /tr/big --json | jq -r .layout.replicas)" = 1 ]
}

m13_tier() {
    printf '[devices]\nlabel = "*"\n[redundancy]\nreplica = 1\n' >"$LOGDIR/cold.toml"
    wmfs class create cold --file "$LOGDIR/cold.toml" --json >/dev/null || return 1
    mkdir "$M/tr" && wmfs set "$M/tr" class=gold --json >/dev/null || return 1
    head -c 300000 /dev/urandom >"$M/tr/big" && head -c 70000 /dev/urandom >"$M/tr/small" || return 1
    json_expect '.set.tier == "gold->cold:size>100K&age>1s"' wmfs set "$M/tr" 'tier=gold->cold:size>100K&age>1s' --json || return 1
    poll 60 1 m13_tier_moved || return 1
    json_expect '.layout.replicas == 2' wmfs fs stat /tr/small --json || return 1
    # 分层过去之后条件不再成立（截短到 100K 以下）：已经在 cold 里的文件留在 cold，协调器把 tr 整个走一遍也不把它迁回 gold
    truncate -s 50000 "$M/tr/big" && wmfs set "$M/tr" compress=none --json >/dev/null || return 1
    wmfs dev wait task-idle reconciler --timeout 120 || return 1
    json_expect '.layout.replicas == 1' wmfs fs stat /tr/big --json || return 1
    json_expect '(.attrs[] | select(.key == "tier") | .actual.mismatched_files == 0) and (.attrs[] | select(.key == "class") | .actual.mismatched_files == 0)' \
        wmfs status "$M/tr" --json
}

# 文件 /loc/<名字> 的第一条链的链头在哪个节点
m13_head_node() {
    local ch
    ch="$(wmfs fs stat "/loc/$1" --json | jq -r '.layout.chains[0]')" || return 1
    wmfs chain show "$ch" --json | jq -r '.members[0].node'
}

m13_locality() {
    local i n sum1 sum2
    wmfs dev mount "$M2" --node 2 --cache "local-nvme:$LOGDIR/nv:64M" || return 1
    mkdir "$M2/loc" && wmfs set "$M2/loc" locality=prefer-client --json >/dev/null || return 1
    for i in 1 2 3; do
        head -c 200000 /dev/urandom >"$M2/loc/f$i" || return 1
        n="$(m13_head_node "f$i")"
        echo "f$i 的链头在节点 $n"
        [ "$n" = 2 ] || return 1
    done
    sum1="$(md5sum <"$M2/loc/f1")" && sum2="$(md5sum <"$M2/loc/f1")" && [ "$sum1" = "$(md5sum <"$M/loc/f1")" ] || return 1
    [ "$sum1" = "$sum2" ] && [ -n "$(ls "$LOGDIR/nv")" ] && echo "local-nvme 缓存里有 $(ls "$LOGDIR/nv" | wc -l) 个块"
}
