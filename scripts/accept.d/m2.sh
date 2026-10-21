# M2 RDMA 传输：逐条对应 roadmap §3 M2 的"验收标准"与"演示命令"。
# 由 scripts/accept.sh 加载（用法见那里的说明）；这里只定义函数，不直接运行。
#
# 验收标准：rdma ping 带 4 MiB bulk 200 次 CRC 全对，且指标显示走了 RDMA READ/WRITE；断开重连测试通过；
#           git diff --stat m2-base -- core/ 为空；core/src 里除注释外搜不到 rdma；本机没有可用 RDMA 设备，所以需要设备的检查
#           打印 SKIP（原因：本机按决定不做 RDMA 实机测试），其余检查（不需设备的单元测试、core/ 零改动、core/src 无 rdma）必须 ✓。
#
# 有没有可用设备看 wmfs doctor --json 的 rdma 一项（.data.gids 里有 ACTIVE 端口上的 RoCE v2 IPv4 地址）；本机 4 个 RoCE 端口
# 都是 DOWN，需要设备的检查显示 SKIP。开发集群只用本次验收自己的目录（$LOGDIR/dev）；无论成败，退出前都停掉本次起的节点。

M2_HTTP=http://127.0.0.1:19180
# 不需要设备的 rdma 单元测试至少这么多个：cargo test 按名字过滤，一个都没匹配上也退出 0，所以先数一数
M2_MIN_UNIT_TESTS=40
# 需要设备的组件测试（全部 #[ignore = "需要 RDMA 设备"]，本机不跑）：正好这几个
M2_DEVICE_TESTS="rdma_broken_connections_are_reestablished_and_retried rdma_full_pool_answers_busy_without_disconnecting
rdma_ping_1000_and_4m_bulk_200_with_crc rdma_role_table_and_unknown_messages_do_not_disconnect
rdma_start_call_cb_timeout_and_dropped_responder rdma_wrong_keys_and_role_claims_are_rejected"
M2_NO_DEVICE="本机按决定不做 RDMA 实机测试"

accept_m2() {
    check "cargo build --release 成功且零警告（rdma 在默认 feature 里；第三方依赖的「将来不兼容」提示由 .cargo/config.toml 关掉）" \
        m2_build
    check "不需要设备的 rdma 单元测试至少 $M2_MIN_UNIT_TESTS 个（cargo test … rdma -- --list）" m2_unit_tests_present
    check "cargo test --release -p wmfs-plugins --features rdma rdma：描述符表、信用控制、RdmaSetup 金样、握手前禁止单边操作、收帧状态机、120 KiB 上限、接收槽校验、C-10 的 QP 属性、选 GID、推迟注册、拆除顺序、假网卡上的整套 RPC" \
        cargo test --release -p wmfs-plugins --features rdma rdma
    check "doctor 的 RDMA 一项：识别 rxe 设备、列出 RoCE v2 的 IPv4 GID 候选（假 sysfs 上的单元测试）" \
        cargo test --release -p wmfs doctor::rdma
    check "传输一致性（同一套用例跑 tcp 与 mem）：ping 1000、4 MiB bulk ×200、认证与角色、超时、池满回 BUSY" \
        cargo test --release -p wmfs-testkit --test net_rpc
    check "断开重连（tcp 与 mem）：fail point 让一次发送失败 = 连接作废，重连重试后成功" \
        cargo test --release -p wmfs-testkit --test reconnect
    check "断开重连（rdma，假网卡上的 core 全套）：fail point plugin.rdma.transport 让一次发送失败 = 连接作废，重连建出新 QP 后成功" \
        m2_one_test rdma::tests::stack::rdma_stack_failpoint_breaks_the_connection_and_retry_reconnects
    check "需要设备的 rdma 组件测试都标了 #[ignore]（正好这 6 个）" m2_device_tests_ignored
    check "core/ 自 m2-base 以来零改动（git diff，另查未跟踪的新文件：git diff 看不到它们）" m2_core_untouched
    check "core/src 里除注释外搜不到 rdma（roadmap 的命令）" m2_core_no_rdma
    check "编码约定（scripts/ci.sh rules；其中 core/src 的 rdma 检查更严：先去掉每行 // 之后的注释再找）" scripts/ci.sh rules
    check_json "wmfs doctor --json 的 RDMA 一项：没有可用设备时 ! 且写明「本机不做 RDMA 实机测试」，有设备时 ✓" \
        '[.checks[] | select(.id == "rdma")] | length == 1 and (.[0].status == "ok"
         or (.[0].status == "warn" and (.[0].title | contains("本机不做 RDMA 实机测试"))))' \
        wmfs doctor --json
    check "wmfs ping rdma://127.0.0.1:19101：本机没有能用于这个地址的 RDMA 设备，10 s 内失败并报 EINVAL（不重试）" \
        m2_ping_rdma_fails_fast

    export WMFS_DEV_DIR="$LOGDIR/dev"
    trap 'wmfs dev down >/dev/null 2>&1' EXIT
    check "wmfs dev up --nodes 1（rdma 编进了二进制，但默认不监听、不碰设备）" wmfs dev up --nodes 1
    eval "$(wmfs dev env)"
    check_json "health 的地址只有 tcp（[plugin.rdma] listen 默认为空）" '.node.addrs == [["tcp","127.0.0.1:19100"]]' \
        curl --noproxy '*' -s "$M2_HTTP/api/v1/health"
    check "/metrics 里一启动就有 wmfs_rdma_ops_total{op=send|recv|read|write}（值为 0）" m2_metrics_registered
    check_json "只用 tcp 的 ping 照常" '.ok == 3 and .failed == 0' wmfs ping tcp://127.0.0.1:19100 --count 3 --json
    check "wmfs dev down" wmfs dev down

    local ip
    ip="$(m2_rdma_ip)"
    if [ -n "$ip" ]; then
        m2_device_checks "$ip"
        return
    fi
    skip "rdma ping 带 4 MiB bulk 200 次 CRC 全对，且服务端 /metrics 显示走了 RDMA READ/WRITE（wmfs dev up --rdma IP）" "$M2_NO_DEVICE"
    skip "rdma 的断开重连：30% 发送失败下靠重连重试 100/100 ok" "$M2_NO_DEVICE"
    skip "fail point plugin.rdma.transport 经签名的管理 API 生效（wmfs_failpoint_hits_total 增加）" "$M2_NO_DEVICE"
    skip "需要设备的 rdma 组件测试（cargo test -p wmfs-testkit --test net_rpc --test reconnect -- --ignored）" "$M2_NO_DEVICE"
}

# 有设备时（doctor 报告了 ACTIVE 端口上的 RoCE v2 IPv4 GID）真跑。
m2_device_checks() {
    local ip="$1"
    local rpc="rdma://$ip:19101"
    check "wmfs dev up --nodes 1 --rdma $ip（[plugin.rdma] listen = $ip:19101、max_qps = 32，数据族走 rdma）" \
        wmfs dev up --nodes 1 --rdma "$ip"
    check_json "health 的地址里有 rdma" '[.node.addrs[] | select(.[0] == "rdma")] | length == 1' \
        curl --noproxy '*' -s "$M2_HTTP/api/v1/health"
    check "rdma ping 带 4 MiB bulk 200 次 CRC 全对，且服务端 /metrics 的 READ、WRITE 各增加 ≥ 200" m2_rdma_bulk "$rpc"
    check "rdma 的断开重连：WMFS_FAILPOINTS='plugin.rdma.transport=pct(30,return(EIO))' 的 ping 靠重连重试 100/100 ok" \
        m2_rdma_fp_demo "$rpc"
    check "fail point plugin.rdma.transport 经签名的管理 API 生效：命中计数增加，clear 后清空" m2_rdma_fp_api "$rpc"
    check "wmfs dev down" wmfs dev down
    check "需要设备的 rdma 组件测试" cargo test --release -p wmfs-testkit --test net_rpc --test reconnect -- --ignored
}

# cargo build；cargo 与 rustc 的警告都打印在 stderr 上，有一行以 warning 开头就算失败。
m2_build() {
    local err rc
    err="$(mktemp)"
    cargo build --release --workspace 2>"$err"
    rc=$?
    cat "$err"
    if grep -q '^warning' "$err"; then
        echo "有编译警告（见上面）"
        rc=1
    fi
    rm -f "$err"
    return "$rc"
}

# 按完整名字只跑一个 rdma 单元测试，并确认它真的跑了（名字写错时 cargo 也退出 0）。
m2_one_test() {
    local out
    out="$(cargo test --release -p wmfs-plugins --features rdma "$1" -- --exact 2>&1)"
    printf '%s\n' "$out"
    printf '%s' "$out" | grep -q 'test result: ok. 1 passed'
}

m2_unit_tests_present() {
    local n
    n=$(cargo test --release -p wmfs-plugins --features rdma rdma -- --list 2>/dev/null | grep -c ': test$')
    echo "名字含 rdma 的测试：$n 个（至少要 $M2_MIN_UNIT_TESTS 个）"
    [ "$n" -ge "$M2_MIN_UNIT_TESTS" ]
}

m2_device_tests_ignored() {
    local got want
    got="$(cargo test --release -p wmfs-testkit --test net_rpc --test reconnect -- --ignored --list 2>/dev/null |
        sed -n 's/: test$//p' | sort | tr '\n' ' ')"
    # shellcheck disable=SC2086  # 故意按空白拆开
    want="$(printf '%s\n' $M2_DEVICE_TESTS | sort | tr '\n' ' ')"
    echo "ignored 的测试：$got"
    echo "期望：        $want"
    [ "$got" = "$want" ]
}

# git diff 看不到未跟踪的新文件（cargo 会自动编译 core/tests/*.rs、core/build.rs 这类新文件），所以另外查一遍。
m2_core_untouched() {
    git rev-parse -q --verify m2-base >/dev/null || {
        echo "没有 tag m2-base"
        return 1
    }
    git diff --stat m2-base -- core/
    if git diff --quiet m2-base -- core/ && [ -z "$(git ls-files --others --exclude-standard -- core/)" ]; then
        echo "core/ 没有任何改动"
        return 0
    fi
    git status --porcelain -- core/
    return 1
}

# roadmap 的命令原样。它的 grep -v '//' 去掉的是"含 // 的整行"，查不出 "rdma://…" 与带行尾注释的代码；更严的写法
# （先去掉每行 // 之后的注释再找）只写在 scripts/ci.sh rules 里，紧接着的那条检查跑它。
m2_core_no_rdma() {
    local a
    a="$(grep -rn --include='*.rs' -i rdma core/src | grep -v '//')"
    echo "roadmap 的命令：${a:-（没有输出）}"
    [ -z "$a" ]
}

m2_ping_rdma_fails_fast() {
    local key out rc t0 t1
    key="$LOGDIR/any.key"
    wmfs debug gen-key --out "$key" >/dev/null || return 1
    t0=$(date +%s)
    out="$(timeout 30 wmfs ping rdma://127.0.0.1:19101 --count 1 --key-file "$key" --json)"
    rc=$?
    t1=$(date +%s)
    printf '%s\n' "$out"
    echo "退出码 $rc，用时 $((t1 - t0)) s"
    [ "$rc" -ne 0 ] && [ $((t1 - t0)) -le 10 ] && printf '%s' "$out" | jq -e '.ok == 0 and .error == "EINVAL"'
}

m2_metrics_registered() {
    local m op
    m="$(curl --noproxy '*' -s "$M2_HTTP/metrics")" || return 1
    for op in send recv read write; do
        printf '%s\n' "$m" | grep -E "^wmfs_rdma_ops_total\{op=\"$op\"\} [0-9]" || {
            echo "缺少 wmfs_rdma_ops_total{op=\"$op\"}"
            return 1
        }
    done
}

# doctor --json 里第一个 ACTIVE 端口上的 RoCE v2 IPv4 地址（没有就是空）。
m2_rdma_ip() {
    wmfs doctor --json 2>/dev/null |
        jq -r '.checks[] | select(.id == "rdma" and .status == "ok") | .data.gids[]? | select(.active) | .ipv4' | head -1
}

# 服务端 /metrics 里 wmfs_rdma_ops_total 的 read 与 write（"read write" 两个数）。
m2_rdma_ops() {
    curl --noproxy '*' -s "$M2_HTTP/metrics" | awk '
        $1 == "wmfs_rdma_ops_total{op=\"read\"}" { r = $2 }
        $1 == "wmfs_rdma_ops_total{op=\"write\"}" { w = $2 }
        END { print (r + 0) " " (w + 0) }'
}

m2_rdma_bulk() {
    local before after
    before="$(m2_rdma_ops)"
    json_expect '.ok == 200 and .failed == 0 and .bulk_bytes == 4194304' wmfs ping "$1" --bulk 4M --count 200 --json || return 1
    after="$(m2_rdma_ops)"
    echo "服务端 READ/WRITE 计数：$before → $after"
    awk -v b="$before" -v a="$after" 'BEGIN { split(b, x, " "); split(a, y, " "); exit !(y[1] - x[1] >= 200 && y[2] - x[2] >= 200) }'
}

m2_rdma_fp_demo() {
    local out
    out="$(WMFS_FAILPOINTS='plugin.rdma.transport=pct(30,return(EIO))' wmfs ping "$1" --count 100 --json)" || {
        printf '%s\n' "$out"
        return 1
    }
    printf '%s\n' "$out"
    printf '%s' "$out" | jq -e '.ok == 100 and .failed == 0 and .retries > 0'
}

m2_rdma_fp_api() {
    wmfs failpoint set 1 plugin.rdma.transport 'times(1,return(EIO))' || return 1
    json_expect '.ok == 3 and .failed == 0 and .retries >= 1' wmfs ping "$1" --count 3 --json || return 1
    curl --noproxy '*' -s "$M2_HTTP/metrics" | grep -E '^wmfs_failpoint_hits_total\{name="plugin\.rdma\.transport"\} [1-9]' || return 1
    wmfs failpoint clear 1 plugin.rdma.transport || return 1
    wmfs failpoint list 1 --json | jq -e '.failpoints == []'
}
