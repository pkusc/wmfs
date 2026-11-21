# M15 profile 全套与发布门槛：逐条对应 roadmap §3 M15 的"发布门槛"与"演示命令"。
# 由 scripts/accept.sh 加载（用法见那里的说明）；这里只定义函数，不直接运行。
#
# 发布门槛（scripts/accept.sh release 逐条检查）：minimal 与 nas-safe 全部 PASS；hpc-rdma 在有 RDMA 设备时 PASS，没有时明确标注"未在本机
# 验证"（SKIP，不算通过也不算失败；发布说明不得声称支持它）；pjdfstest 的每个失败都能在 semantics.md 放松清单里找到对应条目（完整套件与
# 预期失败清单逐条比对，清单外的失败即不通过）；所有故障注入测试中客户端除重试外无错误；Jepsen-lite 线性一致；`cargo clippy -- -D warnings`
# 与全部单元测试通过；scripts/loc.sh 在预算内（非测试代码合计 ≤ 38,000 行，人类 2026-11-05 定的上限，ADR-0041 第 2 条）。
# 容器里的外部测试（完整 pjdfstest、fsx 10 万次、fsstress、IOR/mdtest）是发布门槛：没有镜像 wmfs-tools 时 profile-* 与 release 失败
# （先 scripts/build-tools-image.sh），不是 SKIP——镜像能联网构建，不属于"本机没有的硬件或权限"。
#
# 各入口跑什么、大约多久（本机）：
#   scripts/accept.sh M15（`all` 里也是它）：发布门槛里较快的部分——编码约定、M15 的单元与 fail point 用例、文档齐全、行数、hpc-rdma 的
#     SKIP、chaos（kill -9 / SIGSTOP / 分区 / 网络延迟 / 时钟偏移 + Jepsen-lite + 零错误），约 5 分钟（构建已是最新时）；
#   scripts/accept.sh profile-minimal：scripts/profiles.sh minimal --full（冒烟 + 容器里 fio、完整 pjdfstest、fsx 10 万次与 inline 的一轮、
#     fsstress、IOR、mdtest），约 10 分钟；
#   scripts/accept.sh profile-nas-safe：scripts/profiles.sh nas-safe --full（必过测试 + 两种 Jepsen-lite + 不变式检查器 + 容器里 fio、
#     fsx 10 万次、fsstress、IOR、mdtest），约 16 分钟；
#   scripts/accept.sh profile-hpc-rdma：本机没有 RDMA 设备，SKIP（未在本机验证）；
#   scripts/accept.sh chaos：约 4 分钟；
#   scripts/accept.sh release：以上全部 + clippy + 全部单元测试（cargo test --workspace）+ 文档齐全 + 行数，约 40 分钟（本机实测 38 分钟）。
#
# 开发集群与挂载点只用本次验收自己的目录（$LOGDIR/dev、$LOGDIR/mnt；端口照常是 19n00/19n80，mgr 是 19090）；profile 的部署由
# scripts/profiles.sh 在自己的临时目录里起、跑完就 wmfs dev down --purge（失败时先把日志拷进 $LOGDIR/profile-<名字>-logs/）。无论成败，
# 退出前都 wmfs dev down（先卸载挂载点、停 mgr，再停节点）并删掉 chaos 写进 target 的数据（m15_cleanup）。只解析 --json 输出、退出码与
# scripts/profiles.sh 每行开头的 ✓/✗/SKIP；等待一律按条件（wmfs dev wait）。带期限的长用例（timeout）按 Ctrl-C 也停得下来（m15_bounded）。

# shellcheck source=/dev/null
source "$ROOT/scripts/accept.d/m14.sh" # m14_ports_free；它又加载了 m6.sh（m6_no_mounts、m6_no_own_mounts）

M15_CHAOS=clients_see_no_errors_and_a_register_stays_linearizable_under_kill_stop_partition_and_clock_skew

accept_m15() {
    m15_start
    m15_fast_gates
    m15_hpc_rdma || true # 本机没有设备：一条 SKIP
    m15_chaos
}

accept_profile_minimal() {
    m15_start
    m15_image
    m15_profile minimal
}

accept_profile_nas_safe() {
    m15_start
    m15_image
    m15_profile nas-safe
}

# 先构建（没有 target/release/wmfs 时 wmfs doctor 找不到，会被误判成没有设备），再问一次有没有 RDMA 设备：没有就只有一条 SKIP
# （exit 3：scripts/accept.sh 打印"profile-hpc-rdma SKIP（未在本机验证）"）；有就照常做端口、残留挂载、镜像的预检再跑。
accept_profile_hpc_rdma() {
    check "cargo build --release 成功且零警告" build_no_warnings
    M15_RDMA_IP="$(m15_rdma_ip)"
    [ -n "$M15_RDMA_IP" ] || { m15_hpc_rdma; exit 3; }
    check "开发集群的端口 19100–19380 与 mgr 的 19090 都空闲（演示用的开发集群要先 wmfs dev down）" m14_ports_free
    check "没有残留的 wmfs FUSE 挂载（有的话先 wmfs dev down --purge）" m6_no_mounts
    m15_image
    m15_hpc_rdma
}

accept_chaos() {
    m15_start
    m15_chaos
}

accept_release() {
    m15_start
    m15_image
    check "cargo fmt --check" cargo fmt --check
    check "cargo clippy --workspace --all-targets -- -D warnings（零警告）" cargo clippy --workspace --all-targets -- -D warnings
    check "全部单元测试与组件测试：cargo test --workspace --release（#[ignore] 的对着开发集群的用例在下面各项里跑）" \
        cargo test --workspace --release
    m15_fast_gates
    m15_profile minimal
    m15_profile nas-safe
    m15_hpc_rdma || true # 没有设备：一条 SKIP（最后一行写明）
    m15_chaos
}

# 每个入口开头：端口空闲、没有残留挂载、零警告构建。
m15_start() {
    check "开发集群的端口 19100–19380 与 mgr 的 19090 都空闲（演示用的开发集群要先 wmfs dev down）" m14_ports_free
    check "没有残留的 wmfs FUSE 挂载（有的话先 wmfs dev down --purge）" m6_no_mounts
    check "cargo build --release 成功且零警告" build_no_warnings
}

# 容器里的外部测试是发布门槛：没有镜像就失败（见文件头）。
m15_image() {
    check "镜像 wmfs-tools 在（没有就先 scripts/build-tools-image.sh，要联网：完整 pjdfstest、fsx、fsstress、IOR/mdtest 都在容器里跑）" \
        docker image inspect wmfs-tools
}

# 发布门槛里较快的部分（M15 与 release 都跑）。
m15_fast_gates() {
    check "编码约定：每个文件 ≤ 400 行、unsafe 只在白名单文件里、不写宏、不引入信号处理 crate 与 rand（scripts/ci.sh rules）" scripts/ci.sh rules
    check "fail point net.drop.to/from.<节点>、net.delay（design.md 3.11 的首批，M15 补上）：发往 / 来自某个节点的请求与回复丢在路上（到期 ETIMEDOUT、OneWay 也丢、只丢那个节点的、丢了一帧的流结束、pct 丢一部分）、每一帧入队之后晚 N ms 才发（不累加：同时发 10 个只晚一次），以及丢掉 head 回复后的数据写重试去重（ADR-0046）；清掉之后照常" \
        one_test --release -p wmfs-testkit --test failpoint_net net_drop_and_delay_simulate_partitions_and_slow_links
    check "单元：hpc-rdma 的部署模板（default 的初始 KV + 文件数据走 rdma：init 在 NODE 之前写 node.toml、rdma 地址公布进 NodeRecV1；dev up 不给 --rdma 就报错）、数据族另走的传输（core：node.toml 与公布的地址）、fail point 的 active()（热路径上先问它再拼名字）、数据写去重表与跨换链头重试去重（ADR-0046）" \
        m15_units
    check "Jepsen-lite 的检查器本身（先新后旧、写返回之后读到旧值都判为不线性一致；结果未知的写可生效可不生效）" \
        one_test --release -p wmfs-testkit --test jepsen_register the_checker_accepts_linearizable_and_rejects_stale_reads
    check "文档齐全：semantics.md、ops.md、plugin-abi.md、每个插件与 mgr 插件的 docs.md 都在；没有留给人类的待办字样（需批准、需你决定之类）；ADR 都是 accepted" \
        m15_docs
    check_json "scripts/loc.sh 在预算内：非测试代码合计 ≤ 38,000 行（ADR-0041 第 2 条；上限只写在 scripts/loc.sh 里）" '.within_limit' \
        scripts/loc.sh --json
}

m15_units() {
    one_test --release -p wmfs --bin wmfs node::tests::hpc_rdma_is_default_plus_rdma_published_at_init || return 1
    one_test --release -p wmfs-core --lib init::tests::a_data_transport_goes_into_node_toml_and_the_published_addrs || return 1
    one_test --release -p wmfs-api --lib failpoint::tests::active_is_true_while_any_point_is_configured || return 1
    one_test --release -p wmfs --bin wmfs dev::tests::up_hpc_rdma_needs_rdma || return 1
    one_test --release -p wmfs-core --lib data::dedup::tests::dedup_remembers_committed_writes_and_evicts_oldest || return 1
    one_test --release -p wmfs-core --lib data::dedup::tests::dedup_evicts_entries_older_than_ttl || return 1
    one_test --release -p wmfs-testkit --test craq_edges retried_writes_and_truncates_deduplicate_across_head_failover
}

# 文档齐全（发布门槛）：对外文档与每个插件的 docs.md 都在、非空；没有留给人类的待办字样；ADR 都已批准。
m15_docs() {
    local f d bad=0 hits n
    for f in docs/semantics.md docs/ops.md docs/plugin-abi.md docs/design.md docs/roadmap.md; do
        [ -s "$f" ] || { echo "缺 $f"; bad=1; }
    done
    for d in plugins/*/ dashboard/src/*/; do
        [ -f "$d/manifest.toml" ] || continue # 只有插件目录（有 manifest.toml）要 docs.md
        [ -s "$d/docs.md" ] || { echo "缺 ${d}docs.md"; bad=1; }
    done
    # 设计文档里"需批准/需确认"的标记（【ADR-0012，需批准】、【ADR-0014】需批准、（需确认）……）都已改成批准记录；第 11 节表格的"批准"列
    # 是类别（单元格里只有"需批准"三个字，前后不带括号），不算
    hits="$(grep -nE '待确认|待人类确认|需你决定|需你批准|需人类决定|决定】|】需(批准|确认)|需(批准|确认)[】）]' \
        docs/semantics.md docs/ops.md docs/plugin-abi.md docs/design.md docs/roadmap.md plugins/*/docs.md dashboard/src/*/docs.md)"
    [ -z "$hits" ] || { printf '还有待办字样：\n%s\n' "$hits"; bad=1; }
    hits="$(grep -nE 'TODO|TBD|FIXME' docs/semantics.md docs/ops.md docs/plugin-abi.md plugins/*/docs.md dashboard/src/*/docs.md)"
    [ -z "$hits" ] || { printf '对外文档里有 TODO：\n%s\n' "$hits"; bad=1; }
    n=0
    for f in docs/adr/[0-9]*.md; do
        grep -qE '^- 状态：accepted' "$f" || { echo "$f 的状态不是 accepted"; n=$((n + 1)); }
    done
    if [ "$n" = 0 ]; then
        echo "ADR：$(ls docs/adr/[0-9]*.md | wc -l) 个，都是 accepted"
    else
        echo "ADR：$n 个不是 accepted"
        bad=1
    fi
    return "$bad"
}

# m15_profile <profile>：跑 scripts/profiles.sh <profile> --full，把它输出的每一行 ✓/✗/SKIP 原样计为一条检查（SKIP 的也记进
# $LOGDIR/skipped：最后一行如实写明 SKIP 了几条）；日志在 $LOGDIR/profile-<profile>.log，失败时各部署的日志拷在
# $LOGDIR/profile-<profile>-logs/，提示指向第一条 ✗。
m15_profile() {
    local log="$LOGDIR/profile-$1.log" rc line first=""
    echo "（scripts/profiles.sh $1 --full 运行中，日志 $log）"
    WMFS_PROFILE_LOGS="$LOGDIR/profile-$1-logs" scripts/profiles.sh "$1" --full >"$log" 2>&1
    rc=$?
    while IFS= read -r line; do
        case "$line" in
        "✓ "*) N=$((N + 1)) && echo "$line" ;;
        "SKIP "*) N=$((N + 1)) && echo "$line" && echo "$line" >>"$LOGDIR/skipped" ;;
        "✗ "*) N=$((N + 1)) && echo "$line" && [ -z "$first" ] && first=$N ;;
        esac
    done <"$log"
    if [ "$rc" -ne 0 ]; then
        echo "$CUR FAIL（第 ${first:-$N} 条失败，请看 $log）"
        exit 1
    fi
}

# 本机 RDMA 设备的 IPv4（wmfs doctor 的 rdma 一项里 ACTIVE 的 RoCE v2 GID；没有就是空）。scripts/profiles.sh 没拿到 WMFS_RDMA_IP 时用同一条 jq。
m15_rdma_ip() {
    wmfs doctor --json 2>/dev/null |
        jq -r '.checks[] | select(.id == "rdma" and .status == "ok") | .data.gids[]? | select(.active) | .ipv4' | head -1
}

# hpc-rdma：有 RDMA 设备时跑 scripts/profiles.sh hpc-rdma --full（设备的 IPv4 经 WMFS_RDMA_IP 交给它，不再问一遍）；没有就 SKIP（未在
# 本机验证；不算通过也不算失败），返回 3。M15_RDMA_IP 是入口已经问过的结果（没问过就在这里问）。
m15_hpc_rdma() {
    local ip="${M15_RDMA_IP-$(m15_rdma_ip)}"
    if [ -z "$ip" ]; then
        skip "profile hpc-rdma（3 节点、RDMA、配额、QoS）" \
            "本机没有可用的 RDMA 设备，人类决定本机不做 RDMA 实机测试——未在本机验证，发布说明不得声称支持这个 profile"
        return 3
    fi
    WMFS_RDMA_IP="$ip" m15_profile hpc-rdma
}

# chaos：3 节点 nas-safe 部署 + 一个挂载点；tests/it/chaos.rs 在 kill -9、SIGSTOP、分区、网络延迟、时钟偏移下跑寄存器与 FUSE 写者。
m15_chaos() {
    export WMFS_DEV_DIR="$LOGDIR/dev"
    M="$LOGDIR/mnt"
    mkdir -p "$M"
    trap m15_cleanup EXIT
    check "chaos 的部署：wmfs dev up --nodes 3 --profile nas-safe（集群起好之后自动起 mgr）" wmfs dev up --nodes 3 --profile nas-safe
    eval "$(wmfs dev env)"
    check "chaos 的挂载：wmfs dev mount" wmfs dev mount "$M"
    check "chaos（tests/it/chaos.rs）：kill -9、SIGSTOP（第二遍比 target 租约久）、分区（fail point net.drop.to.*，两边都核对在触发；第二遍一直分开到它的 target 被移出链，恢复后重同步）、网络延迟（net.delay，核对在触发）、时钟（单调时钟快或慢 10%、墙钟偏 ±30 s，核对在触发）轮流各两次——寄存器（Jepsen-lite）线性一致且每个操作都成功、FUSE 写者 0 个错误、fsync 过的文件从数据节点读回全部一致（不经页缓存；失败时打印种子）" \
        m15_chaos_test
    check "不变式检查器：捣乱之后链全部 Serving，各 Serving 副本的已提交版本与内容一致" m15_replicas
    check "wmfs dev down（先卸载挂载点、停 mgr，再停节点）" wmfs dev down
    check "结束：本次验收自己的挂载都已卸载、mgr 已停" m14_clean_end
}

m15_chaos_test() {
    WMFS_CHAOS_DIR="$M/home/chaos" m15_bounded 1200 cargo test --release -p wmfs-testkit --test chaos "$M15_CHAOS" -- \
        --exact --ignored --nocapture >"$LOGDIR/chaos.out" 2>&1
    cat "$LOGDIR/chaos.out"
    grep -q 'test result: ok. 1 passed' "$LOGDIR/chaos.out"
}

m15_replicas() {
    wmfs dev wait chains-serving --timeout 180 &&
        ignored_test --release -p wmfs-testkit --test replica_check every_serving_replica_has_the_same_committed_versions_and_bytes
}

# m15_bounded <秒> <命令…>：带总期限地跑一个命令（到期时 timeout 连同子进程一起结束）。timeout 把命令放进它自己的进程组，终端的
# Ctrl-C 到不了那里：这里把收到的 SIGINT 转给 timeout（它再转给整个进程组），等它们都结束再返回（被打断时返回 130，这条检查失败，
# 退出时照常 m15_cleanup）。同 scripts/profiles.sh 的 bounded。
m15_bounded() {
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
        echo "被 Ctrl-C 打断"
        return 130
    fi
    return "$rc"
}

m15_cleanup() {
    wmfs dev down >/dev/null 2>&1
    if [ -n "$LOGDIR" ] && [ "${WMFS_DEV_DIR:-}" = "$LOGDIR/dev" ]; then
        rm -rf "$WMFS_DEV_DIR"/n*/targets # chaos 写进去的数据（约 2 GiB），失败时也删掉，日志留着
    fi
}
