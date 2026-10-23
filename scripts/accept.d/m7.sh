# M7 目录属性 + 递归用量 + 管理 API：逐条对应 roadmap §3 M7 的"验收标准"与"演示命令"。
# 由 scripts/accept.sh 加载（用法见那里的说明）；这里只定义函数，不直接运行。
#
# 验收标准：继承、inherit=false、来源显示正确；未知属性名被拒绝；没有插件消费的属性被拒绝；单节点上 stripe=2、replicas=3 设置成功
#           但给出警告；wmfs usage 的字节数等于挂载点下所有普通文件大小之和，且立即返回；随机操作后 rstat-check 通过；changelog 包含
#           AttrSet、Mkdir、Create；wmfs status --json 里每个已实现的属性都有 declared/actual。另有 M7 第一步的三处小修（ADR-0037 第
#           12–14 条：ReopenHandle、readdir 的 ..、dev mount 失败时保留 mount.log）、以前留给 M7 的几件（changelog 长轮询的唤醒、
#           /home replicas=3 的警告、chunk=64K 的读写测试、容器里 fuse 类型挂载的路径换算、attr_ttl 生效）、新模块的单元测试与 fail point，
#           以及演示命令。
#
# 开发集群与挂载点只用本次验收自己的目录（$LOGDIR/dev、$LOGDIR/mnt*，端口照常是 19100/19180）：通过时随日志一起删除，失败时保留；
# 无论成败，退出前都 wmfs dev down（它先卸载 wmfs dev mount 挂上的挂载点、再停节点），不留挂载、不留进程（m7_cleanup）。
# 只解析 --json 输出与命令的退出码，例外两处（检查的正是给人看的输出）：dev mount 报错里指着的日志路径（ADR-0037 第 14 条）、
# wmfs get 的最后一行 semantics:（roadmap 的演示）。等待一律用 wmfs dev mount / wmfs dev wait leader；轮询只有 changelog -f 那一项：
# 等它挂上长轮询（指标 wmfs_changelog_longpolls）、等它打印出新事件，都有上限。
# "wmfs usage 等于普通文件大小之和"按 inode 去重之后比较（硬链接只计一次，3.9），并给出耗时上限 2 s。

# 复用 M6 的两个挂载检查（m6_no_mounts、m6_no_own_mounts）：只跑 M7 时 accept.sh 不会加载 m6.sh
# shellcheck source=/dev/null
source "$ROOT/scripts/accept.d/m6.sh"

accept_m7() {
    check "开发集群的端口 19100、19180 空闲（演示用的开发集群要先 wmfs dev down）" dev_ports_free M7
    check "没有残留的 wmfs FUSE 挂载（有的话先 wmfs dev down --purge，或 fusermount3 -uz <挂载点>）" m6_no_mounts
    check "cargo build --release 成功且零警告" build_no_warnings
    check "第一步 ①（ADR-0037 第 4、12 条）：ReopenHandle——属主经句柄重新取一个 0444 文件的新 layout 照常提交；其他用户 chmod o-w 之后 EACCES" \
        one_test --release -p wmfs-testkit --test meta_handle_authz reopen_handle_gives_the_owner_a_fresh_layout_of_a_read_only_file
    check "第一步 ①（FUSE）：属主写一个 0444 文件时另一个挂载把它截断到 0，写者照常重写进新 epoch、close 成功（以前是 ESTALE）" \
        one_test --release -p wmfs --test fuse_handle owner_rewrites_a_read_only_file_after_another_mount_truncated_it
    check "金样：MetaOp::ReopenHandle 追加在 MetaOp 末尾（只增不改）" cargo test --release -p wmfs-core --test golden_proto
    check "第一步 ②（ADR-0037 第 13 条）：readdir 的 .. 是父目录（根是它自己；自己 rename 之后跟着变；另一个挂载 lookup / readdirplus 之后也对）" \
        one_test --release -p wmfs --test fuse_attrs dotdot_in_readdir_is_the_parent_directory
    check "属性注册表：服务端切分的规则、各键的校验与规范化、显示形式、还不能设置的键（strong、quota……）明确拒绝、manifest 对账" \
        tests_at_least 5 --release -p wmfs-core --lib meta::attr_registry
    check "继承解析与缓存：最近的祖先值、inherit=false、来源、按 attr_epoch 失效、批次作废时 clear 丢掉没提交的结果" \
        tests_at_least 3 --release -p wmfs-core --lib meta::attrs
    check "递归计数的增量运算、集群运行参数的名字与取值范围、管理 API 的 JSON 金样与查询参数编码" m7_unit
    check "目录属性（内存 KV）：继承、inherit=false、来源、只能挂在目录上、存储类存在、没有插件消费的键被拒绝、单 target 上的警告、changelog、attr_epoch、新文件按属性建 layout、attr_ttl、声明 vs 实际、用量" \
        tests_at_least 5 --release -p wmfs-testkit --test meta_attrs
    check "RSTAT：3.9 增量表逐条（硬链接、跨目录 rename、覆盖目标、对调、孤儿、rmdir）+ 150 个种子的随机操作每步离线自检" \
        tests_at_least 2 --release -p wmfs-testkit --test meta_rstat
    check "管理 API（完整节点）：属性 set/get/unset 的状态码与错误、用量、status、semantics、cluster settings；changelog 分页、长轮询立即唤醒、最多 2 个长轮询、游标" \
        tests_at_least 2 --release -p wmfs-testkit --test mgmt_api
    check "fail point meta.exec.before_propose：属性设置的批次作废时什么也没应用、之后照常；RETRY 由管理 API 自己重试" \
        tests_at_least 1 --release -p wmfs-testkit --test failpoint_attr
    check "FUSE：chunk=64K 的文件写、读、跨 chunk 覆盖写、截断到 chunk 中间、扩长与模型一致；attr_ttl=5s / 0s 真的成为内核的属性缓存时长" \
        tests_at_least 3 --release -p wmfs --test fuse_attrs
    check "CLI 的挂载点路径换算（按来源 wmfs: 认，容器里的类型是 fuse）与 changelog tail -f 不忙等（服务端没挂住时隔 1 s 再问）" m7_cli_unit

    export WMFS_DEV_DIR="$LOGDIR/dev"
    M="$LOGDIR/mnt"
    mkdir -p "$M" "$LOGDIR/mnt-bad"
    TAIL_PID=""
    trap m7_cleanup EXIT
    check "wmfs dev up --nodes 1" wmfs dev up --nodes 1
    eval "$(wmfs dev env)"
    check "wmfs dev wait leader" wmfs dev wait leader --timeout 30
    check "第一步 ③（ADR-0037 第 14 条）：wmfs dev mount 失败时报错里指着的 mount.log 还在、里面有原因" m7_mount_log
    check "wmfs dev mount 一个挂载点" wmfs dev mount "$M"
    check "mkdir -p proj/a/b；wmfs set proj chunk=1M,stripe=2 成功，并警告「只有 1 条可用的链，新文件实际只有 1 个条带」" m7_set_proj
    check "wmfs set proj/a chunk=8M；wmfs get proj/a/b：chunk=8M 来自 /proj/a、stripe=2 来自 /proj、其余 (默认值)、有 semantics 一行" m7_get_inherit
    check "wmfs unset proj/a chunk 之后 get proj/a/b：chunk=1M 来自 /proj" m7_unset
    check "inherit=false：proj/a 自己的值仍生效、上面的 Inherit 键被挡住（proj/a/b 的 chunk、stripe 回到默认值）；get proj/a 显示 inherit=false 来自 /proj/a" \
        m7_inherit_false
    check "未知属性名被拒绝：wmfs set proj chunck=4M → einval「未知属性 chunck」；get/unset 的键写错也报错（unset quota 与 set 一样 eopnotsupp）" m7_unknown
    check "没有插件消费的值被拒绝：checksum=xxh3（xxh3 插件没有启用）；consistency=strong 在 M12 之前 → eopnotsupp；属性只能挂在目录上" \
        m7_rejects
    check "单节点上 replicas=3 设置成功但警告「当前只有 1 个故障域，新文件实际为 1 副本」" m7_replicas_warn
    check "/home（init 写的 replicas=3）：wmfs get 的 semantics 行是 replicas:声明3/实际1，前面有警告；wmfs status 的警告里有 /home" m7_home
    check "cp -r /usr/include 到 proj/a/；wmfs usage proj 立即返回（< 2 s），字节数 = find 的普通文件大小之和" m7_usage
    check "wmfs usage 挂载根 = 整个挂载点下普通文件大小之和（按 inode 去重：硬链接只计一次）" m7_usage_root
    check "changelog 包含 AttrSet、Mkdir、Create（wmfs changelog tail -n 100000 --json：边读边打印，每页一行）" m7_changelog
    check "changelog 长轮询：wmfs changelog tail -f 挂上长轮询之后，新的 mkdir 提交后 2 s 内打印出它（立即唤醒，不是等 5 s 到期）" m7_follow
    check "wmfs status --json：8 个已实现的属性都有 declared 与 actual；replicas 声明 3、实际 1、不一致" m7_status
    check "wmfs cluster set epoch_gc_grace=0s：集群运行参数更新，changelog 里有 ClusterSet；写错的参数 --json 下回 einval" m7_cluster_set
    if docker image inspect wmfs-tools >/dev/null 2>&1; then
        check "容器里以 root 挂载（mountinfo 类型 fuse）：wmfs get <挂载点下的路径> 照样换算、找到集群" \
            scripts/container-tests.sh pathmap
    else
        skip "容器里 fuse 类型挂载的路径换算" "没有镜像 wmfs-tools：先运行 scripts/build-tools-image.sh（要联网）"
    fi
    check "带种子的随机操作（create/写/truncate/fallocate/unlink/跨目录 rename 与覆盖/link/rmdir/chmod/symlink，种子见日志）之后 usage 与 find 一致" \
        m7_random
    check "wmfs dev down 之后离线 wmfs debug rstat-check：RSTAT 与按全部 inode 重算的结果一致" m7_rstat_check
    check "minimal profile 冒烟（scripts/profiles.sh minimal；M7 起含属性 set/get 与用量）" scripts/profiles.sh minimal
    check "结束：本次验收自己的挂载都已卸载（$LOGDIR 下没有 fuse.wmfs 挂载）" m6_no_own_mounts
}

# 不论成败：结束 changelog -f、停开发集群（它先卸载 dev mount 的挂载点）。
m7_cleanup() {
    [ -n "${TAIL_PID:-}" ] && kill "$TAIL_PID" 2>/dev/null
    wmfs dev down >/dev/null 2>&1
}

m7_unit() {
    tests_at_least 2 --release -p wmfs-core --lib -- meta::rstat meta::cluster::settings || return 1
    tests_at_least 3 --release -p wmfs-api types::mgmt || return 1
    tests_at_least 1 --release -p wmfs-api url::
}

m7_cli_unit() {
    one_test --release -p wmfs --bin wmfs pathmap::tests::mounts_are_found_by_source_and_paths_are_mapped || return 1
    one_test --release -p wmfs --bin wmfs changelog::tests::follow_does_not_busy_loop_when_the_server_does_not_hold_the_poll
}

m7_mount_log() {
    local out log
    out="$(wmfs dev mount "$LOGDIR/mnt-bad" --key-file "$LOGDIR/no-such.key" 2>&1)" && { echo "应该失败：$out"; return 1; }
    printf '%s\n' "$out"
    log="$(printf '%s' "$out" | sed -n 's/.*请看日志 \(.*mount\.log\).*/\1/p')"
    [ -n "$log" ] && [ -f "$log" ] && grep -q "no-such.key" "$log" && echo "日志还在：$log"
}

m7_set_proj() {
    mkdir -p "$M/proj/a/b" || return 1
    json_expect '.set.chunk == "1M" and .set.stripe == "2" and any(.warnings[]; test("只有 1 条可用的链，新文件实际只有 1 个条带"))' \
        wmfs set "$M/proj" chunk=1M,stripe=2 --json
}

# jq：attrs 里键 k 的 (value, source)
A='def a(k): (.attrs[] | select(.key == k));'

m7_get_inherit() {
    wmfs set "$M/proj/a" chunk=8M --json || return 1
    json_expect "$A"' a("chunk").value == "8M" and a("chunk").source == "/proj/a" and a("stripe").value == "2"
        and a("stripe").source == "/proj" and ([.attrs[] | select(.key != "chunk" and .key != "stripe") | .source]
        | all(. == "(默认值)")) and (.semantics | startswith("consistency=cto attr_ttl=1s"))' \
        wmfs get "$M/proj/a/b" --json || return 1
    wmfs get "$M/proj/a/b" | tail -1 | grep -q '^semantics: ' # 给人看的输出：最后一行是 semantics:
}

m7_unset() {
    wmfs unset "$M/proj/a" chunk --json || return 1
    json_expect "$A"' a("chunk").value == "1M" and a("chunk").source == "/proj"' wmfs get "$M/proj/a/b" --json
}

m7_inherit_false() {
    wmfs set "$M/proj/a" inherit=false,inline=0 --json || return 1
    json_expect "$A"' a("chunk").value == "4M" and a("chunk").source == "(默认值)" and a("stripe").value == "1"
        and a("inline").value == "0" and a("inline").source == "/proj/a"' wmfs get "$M/proj/a/b" --json || return 1
    json_expect "$A"' a("inherit").value == "false" and a("inherit").source == "/proj/a"' wmfs get "$M/proj/a" --json || return 1
    wmfs unset "$M/proj/a" inherit --json >/dev/null && wmfs unset "$M/proj/a" inline --json >/dev/null # 还原：后面 cp 进 proj/a
}

m7_unknown() {
    expect_error einval wmfs set "$M/proj" chunck=4M --json || return 1
    wmfs set "$M/proj" chunck=4M --json | jq -e '.message == "未知属性 chunck"' || return 1
    expect_error einval wmfs get "$M/proj" chunck --json || return 1 # 不静默输出空行
    wmfs get "$M/proj" chunk >/dev/null || return 1                    # 写对了照常
    expect_error einval wmfs unset "$M/proj" chunck --json || return 1
    expect_error eopnotsupp wmfs unset "$M/proj" quota --json # 与 set 的说法相同（在 M10 实现）
}

m7_rejects() {
    expect_error einval wmfs set "$M/proj" checksum=xxh3 --json || return 1
    wmfs set "$M/proj" checksum=xxh3 --json | jq -e '.message | test("xxh3 的插件没有启用")' || return 1
    expect_error eopnotsupp wmfs set "$M/proj" consistency=strong --json || return 1
    expect_error eopnotsupp wmfs set "$M/proj" quota=10G --json || return 1
    echo x >"$M/proj/file" && expect_error einval wmfs set "$M/proj/file" chunk=1M --json
}

m7_replicas_warn() {
    mkdir -p "$M/r3" || return 1
    json_expect '.set.replicas == "3" and any(.warnings[]; test("当前只有 1 个故障域，新文件实际为 1 副本"))' \
        wmfs set "$M/r3" replicas=3 --json
}

m7_home() {
    json_expect '(.semantics | test("replicas:声明3/实际1")) and any(.warnings[]; test("1 个故障域"))' wmfs get /home --json || return 1
    json_expect 'any(.warnings[]; test("目录 /home 声明 3 副本"))' wmfs status --json
}

m7_usage() {
    cp -r /usr/include "$M/proj/a/" || return 1
    local t0 t1 got want
    t0=$(date +%s%N)
    got="$(wmfs usage "$M/proj" --json)" || return 1
    t1=$(date +%s%N)
    printf '%s\n' "$got"
    want="$(find "$M/proj" -type f -printf '%i %s\n' | sort -u | awk '{s+=$2} END {print s+0}')"
    echo "wmfs usage 用了 $(((t1 - t0) / 1000000)) ms；find 的普通文件大小之和 $want"
    [ $(((t1 - t0) / 1000000)) -lt 2000 ] && printf '%s' "$got" | jq -e --argjson w "$want" '.bytes == $w and .files > 1000'
}

m7_usage_root() {
    local want
    want="$(find "$M" -type f -printf '%i %s\n' | sort -u | awk '{s+=$2} END {print s+0}')"
    echo "find：$want"
    json_expect ".bytes == $want" wmfs usage "$M" --json
}

m7_changelog() {
    local raw out
    raw="$(wmfs changelog tail -n 100000 --json)" || return 1
    echo "--json 输出了 $(printf '%s\n' "$raw" | wc -l) 行（每页一行；cp -r /usr/include 之后远多于 1 页）"
    [ "$(printf '%s\n' "$raw" | wc -l)" -ge 2 ] || return 1
    out="$(printf '%s\n' "$raw" | jq -cs '{events: [.[].events[]]}')" || return 1 # 各页并成一个
    printf '%s' "$out" | jq -c '[.events[].op] | group_by(.) | map({(.[0]): length}) | add'
    printf '%s' "$out" | jq -e '[.events[].op] as $o | (["AttrSet", "AttrUnset", "Mkdir", "Create"] | all(. as $w | $o | index($w)))'
}

m7_follow() {
    local log="$LOGDIR/follow.out" i n=""
    wmfs changelog tail -n 0 -f >"$log" 2>&1 &
    TAIL_PID=$!
    # 等它问到最新的 seq、挂上长轮询（指标 wmfs_changelog_longpolls ≥ 1）再 mkdir：不靠固定的 sleep
    for i in $(seq 1 50); do
        n="$(dev_metric wmfs_changelog_longpolls)"
        [ "${n:-0}" -ge 1 ] 2>/dev/null && break
        sleep 0.1
    done
    echo "挂着的长轮询：${n:-（还没有这个指标）}"
    mkdir "$M/followed" || return 1
    # 立即唤醒：2 s 内就要打印出来（不唤醒的话要等 wait_ms = 5 s 到期）
    for i in $(seq 1 20); do
        grep -q 'Mkdir .*name=followed' "$log" && break
        sleep 0.1
    done
    kill "$TAIL_PID" 2>/dev/null
    wait "$TAIL_PID" 2>/dev/null
    TAIL_PID=""
    cat "$log"
    grep -q 'Mkdir .*name=followed' "$log"
}

m7_status() {
    json_expect '([.attrs[].key] | sort) == (["checksum", "chunk", "class", "consistency", "inherit", "inline", "replicas", "stripe"])
        and all(.attrs[]; (.declared | length) > 0 and (.actual.new_files | length) > 0)
        and (.attrs[] | select(.key == "replicas") | .declared == "3" and .actual.new_files == "1" and .mismatched)' \
        wmfs status --json
}

m7_cluster_set() {
    json_expect '.settings.epoch_gc_grace == "0s" and .settings.session_lease == "30s"' wmfs cluster set epoch_gc_grace=0s --json || return 1
    wmfs changelog tail -n 5 --json | jq -se 'any(.[].events[]; .op == "ClusterSet")' || return 1
    expect_error einval wmfs cluster set epoch_gc_grace --json || return 1 # 漏了 =：也是 {"error","message"}
    expect_error einval wmfs cluster set session_lease=5s --json # 盖不住 3 次会话心跳（每 3 s 一次）
}

# 带种子的随机操作（种子打印在日志里；失败时用同一个种子重放：python3 <脚本> <目录> <种子>）。
m7_random() {
    local seed=$((RANDOM * 32768 + RANDOM)) want
    echo "随机操作的种子：$seed"
    mkdir -p "$M/rnd" || return 1
    python3 - "$M/rnd" "$seed" <<'EOF' || return 1
import os, random, sys
root, seed = sys.argv[1], int(sys.argv[2])
r = random.Random(seed)
names = ["a", "b", "c", "d", "e"]
done = 0
for step in range(1000):
    dirs = [root] + [os.path.join(d, x) for d, ds, _ in os.walk(root) for x in ds]
    p = os.path.join(r.choice(dirs), r.choice(names))
    q = os.path.join(r.choice(dirs), r.choice(names))
    op = r.randrange(11)
    try:
        if op == 0:
            os.mkdir(p)
        elif op in (1, 2):
            with open(p, "ab") as f:
                f.write(os.urandom(r.choice([0, 10, 5000, 70000, 300000])))
        elif op == 3:
            os.truncate(p, r.choice([0, 1, 65536, 100000, 5000000]))
        elif op == 4:
            os.unlink(p)
        elif op == 5:
            os.rename(p, q)  # 跨目录、可能覆盖目标
        elif op == 6:
            os.link(p, q)
        elif op == 7:
            os.rmdir(p)
        elif op == 8:
            os.chmod(p, 0o700 | r.randrange(0o100))  # setattr；属主一直 rwx（目录也照样能进）
        elif op == 9:
            os.symlink("x", p)
        else:
            with open(p, "ab") as f:  # fallocate 模式 0：长度变长（递归计数跟着变）
                os.posix_fallocate(f.fileno(), r.randrange(100000), 1 + r.randrange(300000))
        done += 1
    except OSError:
        pass
print("成功的操作：", done)
sys.exit(0 if done > 200 else 1)
EOF
    want="$(find "$M" -type f -printf '%i %s\n' | sort -u | awk '{s+=$2} END {print s+0}')"
    json_expect ".bytes == $want" wmfs usage "$M" --json
}

m7_rstat_check() {
    wmfs dev down || return 1
    json_expect '.ok == true and .dirs > 100 and .problems == []' wmfs debug rstat-check "$WMFS_DEV_DIR/n1" --json
}
