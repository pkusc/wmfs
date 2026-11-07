# M10 授权 + 配额 + 驱逐：逐条对应 roadmap §3 M10 的"验收标准"与"演示命令"，以及主管交代的第一步（ADR-0040 第 11–14 条）、留给
# M10 的事项与链成员变更的读窗口（ADR-0041 第 13–17 条）。由 scripts/accept.sh 加载（用法见那里的说明）；这里只定义函数，不直接运行。
#
# 验收标准：单个写者恰好在 100 MiB 附近得到 EDQUOT；8 个并发写者时总用量 ≤ 100 MiB + 8 × quantum；软限超出 → SoftWarn 事件，宽限期后
#           按软限拒绝；files= 精确；ACL 授权正确；path-caps 密钥越界被拒；kill -STOP 客户端约 30–40 秒后会话被驱逐，数据节点拒绝它的
#           cap，锁在数据节点确认之后才释放（日志可见顺序），恢复后旧句柄 EIO、新打开正常；pjdfstest 权限子集通过。
#
# 开发集群与挂载点只用本次验收自己的目录（$LOGDIR/dev、$LOGDIR/mnt、$LOGDIR/mnt2，端口照常是 19n00/19n80）。无论成败，退出前都先让被冻住
# 的挂载进程继续（只对本次验收自己挂的、命令行核对过的进程）、停掉自己起的读者与写者进程、wmfs dev down（先卸载挂载点、再停节点），不留挂载、
# 不留进程，并删掉写配额用的大文件（m10_cleanup）。只解析 --json 输出与退出码，例外几处检查的正是外部工具或日志：dd 的报错
# （"Disk quota exceeded"）、getfacl 的输出、cat 的报错（"Input/output error"）、节点日志里驱逐的几行（要看先后顺序）。等待一律按条件
# （wmfs dev wait、有上限的轮询），不用固定的 sleep。
# 实现注记（M12）：验收脚本自己不再拿着挂载点上的 fd（以前 `exec 3<`）：M12 起只读打开不回 FOPEN_NOFLUSH（POSIX 锁要经 FLUSH 在 close 时
# 释放，design.md 8.7 的 M12 注记），挂载进程被冻住时，继承了这个 fd 的子进程（之后的每个命令）退出时都会卡在 FLUSH 上。所以只读打开着的
# 旧句柄放在一个单独的读者进程里（m10_start_reader），它等着、不起子进程。

# shellcheck source=/dev/null
source "$ROOT/scripts/accept.d/m6.sh"

MiB=1048576

accept_m10() {
    check "开发集群的端口 19100–19380 都空闲（演示用的开发集群要先 wmfs dev down）" dev_ports_free M10 3
    check "没有残留的 wmfs FUSE 挂载（有的话先 wmfs dev down --purge）" m6_no_mounts
    check "cargo build --release 成功且零警告" build_no_warnings
    check "第一步 ①（ADR-0040 第 11 条）：刚从 Syncing 被提升为 last serving 的成员一个 target 租约内对读回 BUSY（fail point 拖住旧 L 换图），客户端换副本读到旧值；租约过后恢复" \
        one_test --release -p wmfs-testkit --test failpoint_promote a_tail_promoted_to_last_serving_answers_busy_for_one_lease
    check "第一步 ②③（ADR-0040 第 12、13 条）：新 Syncing 成员等满一个 target 租约才第一轮同步；target 租约调小的过渡期里链管理器只移除、不追加不提升" m10_first_step
    check "ADR-0041 第 13、16 条（链管理器）：活着的成员两步移出——最后一个 Serving 原地降、其余移到末尾，它报过新图（或满一个租约）才移出；同一轮里转发失败与 drain 只降级；调小租约的过渡期里不降级活着的、离线的照旧一步移出；已是 Syncing 的迁出成员不挡住排在它后面的成员" \
        tests_at_least 10 --release -p wmfs-core --lib meta::cluster::chain_plan::tests::two_step
    check "ADR-0041 第 13–15 条（组件测试）：活着的中间成员、链尾两步移出（降级的图只交给一部分节点时写确认不了、读不新于已确认的写）；新 L 对被占着的 chunk 回 BUSY（丢了转发的 β）；换图途中按旧图读已被规则 2 提交的 chunk → BUSY；非 L 成员提交期间读 → CHUNK_DIRTY；窗口结束之前旧 L 已 FENCED" \
        tests_at_least 5 --release -p wmfs-testkit --test craq_members
    check "ADR-0041 第 13 条（fail point chain.ack.drop）：L 后面还有 Syncing 成员时原地降——它换上新图之前写确认不了（不让后面成员的重复确认越过它）" \
        one_test --release -p wmfs-testkit --test failpoint_promote a_last_serving_member_demoted_in_place_holds_the_ack_until_it_switches
    check "单元：posix-acl（Linux 的判定次序、mask、root 交给 posix-mode、属主经句柄放行）、path-caps（子树、只读根 EROFS、Admin 弃权）、quota（4.4 的判定、SoftWarn、max_growth）" \
        tests_at_least 20 --release -p wmfs-plugins
    check "单元：quota 与 acl.default 的属性解析（拆分、规范化、显示）、ACL 文本格式" m10_attr_unit
    check "配额（内存 KV）：files 精确、quantum 额度与精确检查、CommitWrite 护栏、软限与宽限（总量与按类的各自计时、设配额时重判、重放同一份配置不推后宽限期、unset 删掉起点）、Stacked/quota.<类>/跨 scope rename（覆盖、对调）/孤儿不计、硬链接不跨配额范围（EXDEV）、acl.default 继承、path-caps 的子树、路径遍历与 link 的源文件" \
        tests_at_least 12 --release -p wmfs-testkit --test meta_quota
    check "扩展 ACL 由 posix-acl 判（ACL 放行 uid 2000、其他人照旧拒绝）" \
        one_test --release -p wmfs-testkit --test meta_ns_attr extended_acls_are_judged_by_posix_acl
    check "fail point plugin.posix-acl.authz、plugin.path-caps.authz、plugin.quota.quota_admission（与 M4 的两个一起）" \
        one_test --release -p wmfs-testkit --test failpoint_ns_plugins authz_and_placement_fail_points
    check "两阶段驱逐（fail point client.heartbeat.stop）：约 10 s 后第一阶段、数据节点确认之后第二阶段，第二阶段时数据节点已拒绝它的 cap；恢复后旧句柄 EIO、没提交的写丢失、新会话正常" \
        one_test --release -p wmfs-testkit --test failpoint_evict a_silent_client_is_evicted_in_two_phases
    check "在途的写回（M10 审查）：被驱逐时正在写出去的两批数据（提交之前睡着的、卡在自停闸门上的）不会在新会话里写出、提交，新句柄读自己的写也看不到它们" \
        one_test --release -p wmfs-testkit --test evict_inflight writes_in_flight_when_the_session_is_evicted_are_never_written_out
    check "ADR-0038 第 5 条（会话）：调小 session_lease 之后立刻失联的客户端，在旧租约期满之前不被驱逐" \
        one_test --release -p wmfs-testkit --test evict_lease shrinking_the_session_lease_does_not_evict_before_the_old_lease_ends
    check "ADR-0041 第 9 条（核对时复现的回归）：换了会话之后经旧句柄的写回 SESSION_EVICTED、不在写回缓冲里留下旧会话的空 chunk；新句柄随后写同一个 chunk 照常写出、close 成功、数据都在" \
        one_test --release -p wmfs-testkit --test evict_lease an_old_handle_write_after_the_switch_leaves_nothing_behind
    check "管理 API：key 的建立（路径限制、uid 范围）、列表（限制跟着 rename）、撤销（会话一并驱逐、开不了新会话、401、最后一把 admin key 不能撤销）；会话列表与手动驱逐；path-caps 限制按路径的查询（403）" \
        one_test --release -p wmfs-testkit --test mgmt_keys keys_with_path_caps_revocation_and_sessions
    check "uid 范围与 root_squash（client key 只能声明范围里的 uid；root 映射成 nobody）" \
        one_test --release -p wmfs-testkit --test meta_ns_engine client_keys_are_held_to_their_uid_range_and_root_squash
    check "金样：配额用量、key 与会话的管理 API JSON（只增字段）" m10_golden

    export WMFS_DEV_DIR="$LOGDIR/dev"
    M="$LOGDIR/mnt"
    M2="$LOGDIR/mnt2"
    mkdir -p "$M" "$M2"
    FROZEN=""
    READER=""
    trap m10_cleanup EXIT
    check "wmfs dev up --nodes 3（3 个元数据 voter）" wmfs dev up --nodes 3
    eval "$(wmfs dev env)"
    check "wmfs dev mount 两个挂载点" m10_mount_two
    check_json "演示：mkdir /home/q && wmfs set … quota=100M,soft=80M,grace=1m,files=1000,quantum=1M" \
        '.set.quota == "100M,soft=80M,grace=1m,files=1000,quantum=1M"' m10_set_q
    check "跨配额范围的硬链接（ADR-0041 第 1 条）：ln 进 /home/q → \"Invalid cross-device link\"；有两个链接的文件 mv 进去 → mv 自动改成复制再删除（之后是两个文件、各一个链接）" \
        m10_xdev
    check "演示：dd 300 MiB → 写到 100 MiB 时 \"Disk quota exceeded\"；文件正好 100 MiB" m10_single_writer
    check_json "wmfs usage：100 MiB ≤ 用量 ≤ 100 MiB + 1 个 quantum；软限已超，宽限剩余约 1 分钟" \
        ".quotas[0] | .used_bytes >= 100 * $MiB and .used_bytes <= 101 * $MiB and .soft_since_ms > 0 and .grace_left_ms > 30000 and .grace_left_ms <= 60000" \
        wmfs usage "$M/home/q" --json
    check "8 个并发写者（真的同时在写：都开始之后才有人结束）：每个都得到 EDQUOT，总用量 ≤ 100 MiB + 8 × quantum" m10_eight_writers
    check "软限 + 短宽限（soft=10M,grace=5s）：越过软限 → changelog 有 PolicyViolation（kind=quota_soft）；宽限期内照常按硬限写；等到宽限期过了（≥ 5 s）→ 按软限拒绝" \
        m10_soft_limit
    check "files= 精确（files=5：第 6 个文件、子目录都 EDQUOT；删掉一个之后又能建）" m10_files_exact
    check "演示：mkdir /home/private、chmod 700、echo secret > s；wmfs fs --as-uid 2000 读它 → EACCES" m10_private
    check "演示：setfacl -m u:2000:rx 目录、u:2000:r 文件（getfacl 看得到）→ wmfs fs --as-uid 2000 cat 读到 secret（ACL 放行）" m10_acl
    check "演示：wmfs key create q-only --role client --path /home/q；用它 ls /home/private → EACCES，/home/q 照常；它对管理 API 按路径的查询（usage/get/status）在子树外也被拒绝（403）" \
        m10_path_caps
    check "演示：一个读者进程只读打开第二个挂载上的 s（另有一个写者进程覆盖写一段、不关，留在写回缓冲里）；kill -STOP 它的挂载进程 → wmfs dev wait session-evicted 在约 30–40 s 后成立；wmfs session list 里它是 EVICTED" \
        m10_evict
    check "驱逐的日志：leader 上\"第一阶段\" → \"数据节点确认\" → \"第二阶段：释放锁\"依次出现；3 个数据节点都记下了\"此后拒绝它的 cap\"；changelog 有 SessionEvicted" \
        m10_evict_logs
    check "演示：kill -CONT 之后经旧的只读 fd 读 → Input/output error（旧句柄不再可用）；cat 新打开 → secret（自动建立了新会话）" m10_resume
    check "被驱逐时写回缓冲里没提交的写丢失（M10 审查）：旧的写 fd 再写 → Input/output error；挂载日志记下丢掉的写之后，两个挂载上新打开读到的都是驱逐之前提交的内容（旧数据没有在新会话里被写出）" \
        m10_lost_write
    check "Jepsen-lite：寄存器在随机 kill -9 / SIGSTOP / 提升（ADR-0040 第 11 条：别的节点换图慢一拍时重启一个节点）下线性一致" \
        ignored_test --release -p wmfs-testkit --test jepsen_register a_register_on_one_chunk_stays_linearizable_while_nodes_die_and_freeze
    check "wmfs dev wait chains-serving（Jepsen 之后链都回到 Serving）" wmfs dev wait chains-serving --timeout 120
    m10_pjdfstest_perm
    check "Jepsen-lite（ADR-0041 第 13、14 条）：drain 持有寄存器所在链 last serving 的节点、同时用 chain.map.delay 拖住它换图 → 先原地降为 Syncing、它换上新图之后才移出；一个只读的线程用自己的客户端一直先问那个节点，检查器仍判定线性一致（drain 撤销不了：放在最后）" \
        ignored_test --release -p wmfs-testkit --test jepsen_register a_register_stays_linearizable_while_the_node_with_its_last_serving_is_drained
    check "wmfs dev wait chains-serving（drain 之后每条链剩两个成员，都是 Serving）" wmfs dev wait chains-serving --timeout 120
    check "wmfs dev down（先卸载挂载点，再停全部节点）" wmfs dev down
    check "结束：本次验收自己的挂载都已卸载（$LOGDIR 下没有 fuse.wmfs 挂载）" m6_no_own_mounts
}

# 不论成败：让被冻住的挂载进程继续（只对本次验收自己挂的）、停掉读者进程、停开发集群（它先卸载挂载点）、删掉写配额用的大文件。
m10_cleanup() {
    if [ -n "${FROZEN:-}" ] && tr '\0' ' ' <"/proc/$FROZEN/cmdline" 2>/dev/null | grep -q "wmfs mount .* $M2"; then
        kill -CONT "$FROZEN" 2>/dev/null
    fi
    if [ -n "${READER:-}" ]; then
        touch "$LOGDIR/r.go" # 读者进程自己收尾退出（它是本次验收起的）
        kill "$READER" 2>/dev/null
    fi
    if [ -n "${WRITER:-}" ]; then
        touch "$LOGDIR/w.go" # 写者进程自己收尾退出（它是本次验收起的）
        kill "$WRITER" 2>/dev/null
    fi
    wmfs dev down >/dev/null 2>&1
    if [ -n "$LOGDIR" ] && [ "${WMFS_DEV_DIR:-}" = "$LOGDIR/dev" ]; then
        rm -rf "$WMFS_DEV_DIR"/n*/targets
    fi
}

m10_first_step() {
    one_test --release -p wmfs-core --lib meta::cluster::chain_plan::tests::while_the_target_lease_shrinks_the_manager_only_removes || return 1
    tests_at_least 7 --release -p wmfs-testkit --test craq_edges
}

m10_attr_unit() {
    one_test --release -p wmfs-core --lib meta::attr_registry::tests::quota_and_acl_default_are_split_parsed_and_shown || return 1
    one_test --release -p wmfs-core --lib meta::ns::acl::tests::acl_default_text_is_parsed_and_normalized
}

m10_golden() {
    tests_at_least 1 --release -p wmfs-api types::access::tests::keys_and_sessions_json_golden || return 1
    tests_at_least 1 --release -p wmfs-api types::mgmt::tests::usage_status_semantics_json_golden
}

m10_mount_two() {
    wmfs dev mount "$M" || return 1
    wmfs dev mount "$M2"
}

m10_set_q() {
    mkdir "$M/home/q" || return 1
    wmfs set "$M/home/q" quota=100M,soft=80M,grace=1m,files=1000,quantum=1M --json
}

# 硬链接不跨配额范围（ADR-0041 第 1 条）：/home 没有配额、/home/q 有，两边是不同的配额范围。
m10_xdev() {
    local a="$M/home/hl" b="$M/home/hl2" c="$M/home/q/hl" err="$LOGDIR/ln.err"
    echo linked >"$a" && ln "$a" "$b" || return 1 # 同一个配额范围（都没有配额）里照常
    if ln "$a" "$c" 2>"$err"; then
        echo "跨配额范围的 ln 应当失败"
        return 1
    fi
    cat "$err"
    grep -q "Invalid cross-device link" "$err" || return 1
    mv "$a" "$c" || return 1 # rename 回 EXDEV → mv 复制再删除
    echo "mv 之后：$(stat -c '%n inode %i 链接数 %h' "$c" "$b" | tr '\n' ' ')"
    [ "$(cat "$c")" = linked ] && [ ! -e "$a" ] || return 1
    [ "$(stat -c %i "$c")" != "$(stat -c %i "$b")" ] && [ "$(stat -c %h "$c")" = 1 ] && [ "$(stat -c %h "$b")" = 1 ] || return 1
    rm "$c" "$b"
}

m10_single_writer() {
    local err="$LOGDIR/dd-q.err" size
    if dd if=/dev/zero of="$M/home/q/f" bs=1M count=300 2>"$err"; then
        echo "dd 写完了 300 MiB，应在 100 MiB 时报 EDQUOT"
        return 1
    fi
    cat "$err"
    grep -q "Disk quota exceeded" "$err" || return 1
    size="$(stat -c %s "$M/home/q/f")"
    echo "文件长度 $size"
    [ "$size" -eq $((100 * MiB)) ]
}

m10_eight_writers() {
    local d="$M/home/q8" i rc=0 used
    mkdir "$d" || return 1
    wmfs set "$d" quota=100M,quantum=1M --json || return 1
    rm -f "$LOGDIR/go"
    for i in 1 2 3 4 5 6 7 8; do
        (
            while [ ! -e "$LOGDIR/go" ]; do sleep 0.01; done # 起跑线：8 个一起开始
            echo "${EPOCHREALTIME//[!0-9]/}" >"$LOGDIR/w$i.start"
            dd if=/dev/zero of="$d/f$i" bs=1M count=300 2>"$LOGDIR/w$i.err"
            echo $? >"$LOGDIR/w$i.rc"
            echo "${EPOCHREALTIME//[!0-9]/}" >"$LOGDIR/w$i.end"
        ) &
    done
    touch "$LOGDIR/go"
    wait
    for i in 1 2 3 4 5 6 7 8; do
        echo "写者 $i：退出码 $(cat "$LOGDIR/w$i.rc")，$(tail -1 "$LOGDIR/w$i.err")"
        [ "$(cat "$LOGDIR/w$i.rc")" != 0 ] && grep -q "Disk quota exceeded" "$LOGDIR/w$i.err" || rc=1
    done
    local last_start first_end
    last_start="$(cat "$LOGDIR"/w*.start | sort -n | tail -1)"
    first_end="$(cat "$LOGDIR"/w*.end | sort -n | head -1)"
    echo "最后一个开始于 $last_start µs，第一个结束于 $first_end µs"
    [ "$last_start" -lt "$first_end" ] || { echo "不是真的并发"; rc=1; }
    used="$(wmfs usage "$d" --json | jq '.quotas[0].used_bytes')"
    echo "总用量 $used 字节（上限 100 MiB + 8 MiB = $((108 * MiB))）"
    [ "$used" -ge $((100 * MiB)) ] && [ "$used" -le $((108 * MiB)) ] || rc=1
    rm -f "$d"/f*
    return "$rc"
}

m10_soft_limit() {
    local d="$M/home/s" ino since t0 i left
    mkdir "$d" || return 1
    wmfs set "$d" quota=100M,soft=10M,grace=5s,quantum=1M --json || return 1
    ino="$(wmfs fs stat /home/s --json | jq '.ino')"
    dd if=/dev/zero of="$d/f" bs=1M count=20 || return 1
    json_expect "any(.events[]; .op == \"PolicyViolation\" and .ino == $ino and (.extra | any(.[0] == \"kind\" and .[1] == \"quota_soft\")))" \
        wmfs changelog tail -n 200 --json || return 1
    since="$(wmfs usage "$d" --json | jq '.quotas[0].soft_since_ms')"
    dd if=/dev/zero of="$d/f" bs=1M count=1 oflag=append conv=notrunc || { echo "宽限期内应按硬限照常写"; return 1; }
    for i in $(seq 1 300); do # 等到宽限期过去（最多 30 s）
        left="$(wmfs usage "$d" --json | jq '.quotas[0].grace_left_ms')"
        [ "$left" = 0 ] && break
        sleep 0.1
    done
    [ "$left" = 0 ] || { echo "30 s 还没过宽限期：$left"; return 1; }
    if dd if=/dev/zero of="$d/f" bs=1M count=1 oflag=append conv=notrunc 2>"$LOGDIR/dd-s.err"; then
        echo "宽限期过了还能写：应按软限拒绝"
        return 1
    fi
    cat "$LOGDIR/dd-s.err"
    grep -q "Disk quota exceeded" "$LOGDIR/dd-s.err" || return 1
    t0="$(date +%s%3N)"
    echo "越过软限于 $since，按软限拒绝于 $t0（相隔 $((t0 - since)) ms，宽限 5000 ms）"
    [ $((t0 - since)) -ge 5000 ]
}

m10_files_exact() {
    local d="$M/home/n" i
    mkdir "$d" || return 1
    wmfs set "$d" quota=1G,files=5 --json || return 1
    mkdir "$d/sub" || return 1
    for i in 1 2 3 4; do touch "$d/f$i" || return 1; done
    if touch "$d/f5" 2>"$LOGDIR/files.err" || mkdir "$d/sub2" 2>>"$LOGDIR/files.err"; then
        echo "第 6 个（文件或子目录）应 EDQUOT"
        return 1
    fi
    cat "$LOGDIR/files.err"
    grep -q "Disk quota exceeded" "$LOGDIR/files.err" || return 1
    json_expect '.quotas[0].used_files == 5' wmfs usage "$d" --json || return 1
    rm "$d/f1" && touch "$d/f5"
}

m10_private() {
    mkdir "$M/home/private" && chmod 700 "$M/home/private" && echo secret >"$M/home/private/s" || return 1
    expect_error EACCES wmfs fs --as-uid 2000 get /home/private/s "$LOGDIR/s.copy" --json
}

m10_acl() {
    local got
    setfacl -m u:2000:rx "$M/home/private" && setfacl -m u:2000:r "$M/home/private/s" || return 1
    getfacl -n --omit-header "$M/home/private/s"
    getfacl -n --omit-header "$M/home/private/s" | grep -qx 'user:2000:r--' || return 1
    got="$(wmfs fs --as-uid 2000 cat /home/private/s)" || return 1
    echo "uid 2000 读到：$got"
    [ "$got" = secret ] || return 1
    expect_error EACCES wmfs fs --as-uid 3000 get /home/private/s "$LOGDIR/s.copy" --json # 别的用户照旧拒绝
}

m10_path_caps() {
    local k="$LOGDIR/q.key"
    json_expect '.id >= 3' wmfs key create q-only --role client --path /home/q --out "$k" --json || return 1
    expect_error EACCES wmfs fs --key-file "$k" ls /home/private --json || return 1
    json_expect 'any(.entries[]; .name == "f")' wmfs fs --key-file "$k" ls /home/q --json || return 1
    json_expect '.quotas[0].dir == "/home/q"' wmfs usage "$M/home/q" --key-file "$k" --json || return 1
    expect_error eacces wmfs usage "$M/home/private" --key-file "$k" --json || return 1
    expect_error eacces wmfs get "$M/home/private" --key-file "$k" --json || return 1
    expect_error eacces wmfs status "$M/home/private" --key-file "$k" --json || return 1
    json_expect 'any(.keys[]; .name == "q-only" and .paths[0].path == "/home/q" and (.revoked | not))' wmfs key list --json
}

m10_evict() {
    local t0 t1
    m10_start_reader "$M2/home/private/s" || return 1 # 让第二个客户端打开一个文件（读者进程开着它，直到 m10_resume）
    echo committed >"$M2/home/w" || return 1 # 写好、关掉：已提交
    m10_start_writer "$M2/home/w" || return 1
    SID="$(wmfs session list --json | jq -r '.sessions[] | select((.mount | endswith("/mnt2")) and .state == "active") | .id')"
    echo "第二个挂载的会话：$SID"
    [ -n "$SID" ] || return 1
    FROZEN="$(wmfs dev client-pid "$M2")" || return 1
    t0="$(date +%s%3N)"
    kill -STOP "$FROZEN" || return 1
    wmfs dev wait session-evicted "$M2" --timeout 90 || return 1
    t1="$(date +%s%3N)"
    echo "冻住之后 $((t1 - t0)) ms 被驱逐完（两个阶段）"
    json_expect "any(.sessions[]; .id == \"$SID\" and .state == \"evicted\")" wmfs session list --json || return 1
    [ $((t1 - t0)) -ge 28000 ] && [ $((t1 - t0)) -le 45000 ]
}

m10_evict_logs() {
    local log p1 c p2 n
    log="$(grep -l "会话 $SID 驱逐第一阶段" "$WMFS_DEV_DIR"/n*/logs/*.log | head -1)" # 做驱逐的 leader 的日志
    [ -n "$log" ] || { echo "没有哪个节点的日志里有第一阶段"; return 1; }
    p1="$(grep -n "会话 $SID 驱逐第一阶段" "$log" | head -1 | cut -d: -f1)"
    c="$(grep -n "会话 $SID：数据节点确认" "$log" | head -1 | cut -d: -f1)"
    p2="$(grep -n "会话 $SID 驱逐第二阶段：释放锁" "$log" | head -1 | cut -d: -f1)"
    grep -E "会话 $SID( |：)" "$log"
    echo "$log：第一阶段在第 ${p1:-?} 行，数据节点确认在第 ${c:-?} 行，释放锁在第 ${p2:-?} 行"
    [ -n "$p1" ] && [ -n "$c" ] && [ -n "$p2" ] && [ "$p1" -lt "$c" ] && [ "$c" -lt "$p2" ] || return 1
    for n in 1 2 3; do
        grep -h "会话 $SID 已被驱逐，此后拒绝它的 cap" "$WMFS_DEV_DIR/n$n"/logs/*.log || { echo "node$n 没有记下"; return 1; }
    done
    json_expect "any(.events[]; .op == \"SessionEvicted\" and (.extra | any(.[0] == \"session\" and .[1] == \"$SID\")))" \
        wmfs changelog tail -n 200 --json
}

# 读者进程：只读打开 $1（旧句柄），等到 $LOGDIR/r.go 出现再经它读一次，把结果写进 $LOGDIR/r.result（读到的内容或错误）。它等待时不起
# 子进程（见文件头的 M12 注记）。
m10_start_reader() {
    python3 -c '
import os, sys, time
fd = os.open(sys.argv[1], os.O_RDONLY)
open(sys.argv[2] + "/r.ready", "w").close()
while not os.path.exists(sys.argv[2] + "/r.go"):
    time.sleep(0.1)
try:
    r = os.read(fd, 100).decode()
except OSError as e:
    r = os.strerror(e.errno)
open(sys.argv[2] + "/r.result", "w").write(r)
' "$1" "$LOGDIR" &
    READER=$!
    poll 100 0.1 test -e "$LOGDIR/r.ready"
}

m10_resume() {
    local got
    kill -CONT "$FROZEN" || return 1 # 冻住时记下的 pid（冻住期间不再对这个挂载跑任何命令）
    FROZEN=""
    touch "$LOGDIR/r.go"
    poll 100 0.1 test -e "$LOGDIR/r.result" || return 1
    echo "旧句柄读到：$(cat "$LOGDIR/r.result")"
    [ "$(cat "$LOGDIR/r.result")" = "Input/output error" ] || return 1
    wait "$READER"
    READER=""
    got="$(cat "$M2/home/private/s")" || return 1
    echo "新打开读到：$got"
    [ "$got" = secret ]
}

# 写者进程：写打开 $1、覆盖开头 5 字节、不关（留在写回缓冲里，5 s 之后才写出——调用方马上冻住挂载进程）；等到 $LOGDIR/w.go 出现再写一次、
# 再关掉 fd，把 close 与这次 write 的结果分别写进 $LOGDIR/w.close、$LOGDIR/w.result（先写 w.close）。用 python 是因为它等待时不起子进程：
# 子进程会继承这个写 fd，退出时的 FLUSH 会把这段数据提交掉，或在挂载进程被冻住时卡住（验收脚本自己也不能拿着写 fd，否则之后每个命令
# 退出时都卡在 FLUSH 上）。
m10_start_writer() {
    local i
    python3 -c '
import os, sys, time
path, d = sys.argv[1], sys.argv[2]
fd = os.open(path, os.O_RDWR)
os.write(fd, b"STALE")
open(d + "/w.ready", "w").close()
while not os.path.exists(d + "/w.go"):
    time.sleep(0.1)
def result(f):
    try:
        f()
        return "ok"
    except OSError as e:
        return os.strerror(e.errno)
r = result(lambda: os.write(fd, b"x"))
open(d + "/w.close", "w").write(result(lambda: os.close(fd)))
open(d + "/w.result", "w").write(r)
' "$1" "$LOGDIR" &
    WRITER=$!
    for i in $(seq 1 100); do
        [ -e "$LOGDIR/w.ready" ] && return 0
        sleep 0.1
    done
    echo "写者进程 10 s 还没写好"
    return 1
}

m10_lost_write() {
    local i m log="$WMFS_DEV_DIR/mounts/mnt2/mount.log" got end
    touch "$LOGDIR/w.go"
    for i in $(seq 1 300); do # 写者进程再写一次、关掉（最多等 30 s）
        [ -e "$LOGDIR/w.result" ] && break
        sleep 0.1
    done
    echo "旧的写 fd 再写：$(cat "$LOGDIR/w.result" 2>/dev/null)；关掉它：$(cat "$LOGDIR/w.close" 2>/dev/null)"
    [ "$(cat "$LOGDIR/w.result" 2>/dev/null)" = "Input/output error" ] || return 1
    [ "$(cat "$LOGDIR/w.close" 2>/dev/null)" = "Input/output error" ] || { echo "旧的写 fd 的 close 应报 EIO"; return 1; }
    wait "$WRITER"
    WRITER=""
    for i in $(seq 1 100); do # 换会话时丢掉没提交的写（最多等 10 s）
        grep -q "会话被驱逐：丢掉" "$log" && break
        sleep 0.1
    done
    grep "会话被驱逐：丢掉" "$log" || { echo "$log 里没有记下丢掉的写"; return 1; }
    end=$((SECONDS + 8)) # 看满 8 s：跨过几轮 flusher（脏数据 5 s 之后写出、每 1 s 一轮），旧数据一直没有被写出去
    while :; do
        for m in "$M" "$M2"; do
            got="$(cat "$m/home/w")" || return 1
            [ "$got" = committed ] || { echo "$m/home/w：$got（旧会话的写被写出去了）"; return 1; }
        done
        [ "$SECONDS" -ge "$end" ] && break
        sleep 0.5
    done
    echo "8 s 里两个挂载上读到的一直是 committed"
}

# 容器里 pjdfstest 的权限子集（切换用户的用例与 chmod、chown；挂载带 --allow-other）：没有 wmfs-tools 镜像时 SKIP。
m10_pjdfstest_perm() {
    if ! docker image inspect wmfs-tools >/dev/null 2>&1; then
        skip "pjdfstest 权限子集（容器内）" "没有镜像 wmfs-tools（先 scripts/build-tools-image.sh）"
        return 0
    fi
    check "pjdfstest 权限子集（容器内 root 挂载 --allow-other；切换用户的用例与 chmod、chown）：没有清单外的失败" \
        scripts/container-tests.sh pjdfstest-perm
}
