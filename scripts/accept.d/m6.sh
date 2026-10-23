# M6 FUSE 挂载：逐条对应 roadmap §3 M6 的"验收标准"与"演示命令"。
# 由 scripts/accept.sh 加载（用法见那里的说明）；这里只定义函数，不直接运行。
#
# 验收标准：cp -r /usr/include 后 diff -r 完全一致（其中 135 个文件大于 64 KiB，走 chunk）；1 GiB 写入 + md5 在卸载重挂后不变；
#           两个挂载点之间：写者 close 之后读者 open 一定看到（包括在 1 秒属性缓存窗口内）；echo v > f（O_TRUNC）之后的写直接进新
#           epoch（日志里没有 STALE_LAYOUT 重写）；umask 027 下新建文件的权限是 640；符号链接、硬链接、xattr 正常；被删除但仍打开的
#           文件可以读完；dd oflag=direct 写入后 md5 一致；minimal profile 冒烟通过；容器内 pjdfstest 中不涉及 chown/权限的子集通过、
#           fsx 1 万次无不一致。另有 M5 的三处遗留（ADR-0035 第 9 条、ADR-0036、单飞建连用例的偶发失败）、新模块的单元测试与
#           fail point，以及演示命令。
#
# 开发集群与挂载点只用本次验收自己的目录（$LOGDIR/dev、$LOGDIR/mnt*，端口照常是 19100/19180）：通过时随日志一起删除，失败时保留
# 日志；无论成败，退出前都 wmfs dev down（它先卸载 wmfs dev mount 挂上的挂载点、再停节点），不留挂载、不留进程（m6_cleanup）。
# 只解析 --json 输出与命令的退出码；等待一律用 wmfs dev mount（它等到挂上才返回）与 wmfs dev wait leader，不用 sleep——唯一例外是
# "被删除但仍打开"：要让时间越过孤儿的 6 s 与 gc 的一轮，证明 gc 没有删它（python 里睡 9 s，见 m6_orphan）。
# cp -r /usr/include 的 diff 用 --no-dereference：/usr/include 里有指向树外的相对符号链接（clang/18/include → ../../../lib/…），
# 拷到任何地方（包括本地磁盘）之后都会悬空，不加它时 diff 在本地拷贝上也失败（roadmap 演示命令同步改了，design.md 的 M6 注记）。

accept_m6() {
    check "开发集群的端口 19100、19180 空闲（演示用的开发集群要先 wmfs dev down）" dev_ports_free M6
    check "没有残留的 wmfs FUSE 挂载（整台机器上的都算，包括别的开发集群与人类自己的挂载；有的话先 wmfs dev down --purge，或 fusermount3 -uz <挂载点>）" \
        m6_no_mounts
    check "cargo build --release 成功且零警告" build_no_warnings
    check "M5 遗留 ①（ADR-0035 第 9 条）：按旧的 inline 视图截断刚转成 chunk 的文件 → STALE_LAYOUT；重新 Open 按 chunk 截断、再扩长读到 0" \
        one_test --release -p wmfs-testkit --test meta_handle_authz probe_truncate_with_a_stale_inline_view_after_uninline
    check "M5 遗留 ②（ADR-0036）：属主经句柄提交、Uninline、续写 cap、TruncateHandle、Fallocate 不看权限位；按路径截断与写打开仍 EACCES；其他用户 chmod o-w 之后提交、续期、ftruncate → EACCES" \
        one_test --release -p wmfs-testkit --test meta_handle_authz owner_handles_ignore_mode_bits_while_others_get_eacces_after_chmod
    check "posix-mode 的 Handle 规则：root 放行、属主不看权限位、其他人与 Open 相同、扩展 ACL 弃权" \
        one_test --release -p wmfs-plugins --lib posix_mode::tests::handle_ops_trust_the_owner_like_nfs
    check "金样：MetaOp::TruncateHandle 追加在 MetaOp 末尾（只增不改）" cargo test --release -p wmfs-core --test golden_proto
    check "M5 遗留 ③：单飞建连——16 个线程同时第一次调用只建 1 次连接（tcp 与 mem，连跑 10 遍；判据是客户端自己的建连次数）" \
        m6_single_flight
    check "客户端文件层（审查后补）：截断按最新长度、按路径截断先查权限、fsync 不乱提交、重新 Open 不记「见过」、属性只前进、inline 副本带版本" \
        tests_at_least 6 --release -p wmfs-testkit --test client_files
    check "客户端 InodeState 的纯逻辑与读路径的块划分（chunk 小于 1 MiB 时块不跨 chunk）、inline 内容之后读 0" \
        tests_at_least 6 --release -p wmfs-core --lib client::
    check "写回缓冲：区间合并、本地末尾与读自己的写（在途与脏区）、槽位上限按池算" \
        tests_at_least 3 --release -p wmfs-core --lib client::writeback
    check "cache-memory：manifest、键里带 change_id、LRU 淘汰、超大的块不放、invalidate_ino" \
        tests_at_least 4 --release -p wmfs-plugins --lib cache_memory::
    check "FUSE 适配层：错误码映射、作业 id、时间换算、虚拟 xattr 的判定" \
        one_test --release -p wmfs --bin wmfs fuse::util::tests::job_ids_and_error_codes_and_times
    check "SetAtime（relatime 批量）：只往后走、不改 ctime 与 change_id、读不了的跳过；读打开按 relatime 给出 atime_due" \
        one_test --release -p wmfs-testkit --test meta_file_ops set_atime_is_best_effort_and_keeps_change_id
    check "FUSE：POSIX 基础、inline 与 chunk、umask 的 spike、虚拟 xattr、O_TRUNC 进新 epoch、删除后仍打开、O_DIRECT、inline 内容之后读 0、C4 重写、随机读写与模型一致" \
        tests_at_least 9 --release -p wmfs --test fuse_posix
    check "FUSE：两个挂载点之间 close-to-open（1 秒属性缓存窗口内、读者页缓存里有旧内容、读者只 stat 过、提交之间夹着别人的修改、只写打开、截断前别人写长了）" \
        tests_at_least 4 --release -p wmfs --test fuse_cto
    check "FUSE（ADR-0036）：属主 open(O_CREAT|O_WRONLY, 0444) 后写入并 close、fchmod 0444 之后继续写与 ftruncate、git 照常工作" \
        tests_at_least 1 --release -p wmfs --test fuse_handle
    check "fail point：client.flush.before_commit（报错不丢数据；崩溃时未提交的长度不可见）、plugin.cache-memory.cache_store（日志确认绕过了缓存）" \
        tests_at_least 3 --release -p wmfs --test failpoint_fuse

    export WMFS_DEV_DIR="$LOGDIR/dev"
    F="$LOGDIR/files"
    MA="$LOGDIR/mnt"
    MB="$LOGDIR/mnt2"
    mkdir -p "$F" "$MA" "$MB"
    WRITER_PID=""
    trap m6_cleanup EXIT
    check "wmfs dev up --nodes 1" wmfs dev up --nodes 1
    eval "$(wmfs dev env)"
    check "wmfs dev wait leader" wmfs dev wait leader --timeout 30
    check "wmfs dev mount 两个挂载点（两个客户端），打印集群警告「……新文件实际为 1 副本」" m6_mount_two
    check "wmfs mount 接受多个地址：第一个连不上（127.0.0.1:1）就换下一个，挂上、读写、卸载" m6_multi_addr
    check "cp -r /usr/include 后 diff -r --no-dereference 完全一致" m6_cp_include
    check "其中大于 64 KiB 的文件（本机 135 个）都是 chunk、stdio.h 是 inline（wmfs fs stat --json 逐个核对）" m6_include_layout
    check "dd 写 1 GiB、md5；wmfs dev umount / mount 重挂之后 md5 不变" m6_big
    check "两个挂载点之间：echo v1、v2 > cto.txt（写者 close）之后另一个挂载立即 cat 到（1 秒属性缓存窗口内；读者刚 stat 过、写者改长了也一样）" m6_cto
    check "echo v > f（O_TRUNC）之后的写直接进新 epoch：挂载日志里没有 STALE_LAYOUT 重写，服务端也没回过 STALE_LAYOUT" m6_no_stale
    check "(umask 027; touch um) 的权限是 640" m6_umask
    check "符号链接（cat link 读到 cto.txt 的内容）、硬链接（nlink 2）、xattr（b'hi'）" m6_links_xattr
    check "根目录的虚拟 xattr user.wmfs.mgmt = 管理地址（只读）" m6_mgmt_xattr
    check "被删除但仍打开的文件可以读完（另一个挂载上删掉，等过孤儿的 6 s 与 gc 的一轮）" m6_orphan
    check "dd oflag=direct 写入后 md5 一致（另一个挂载上读）" m6_direct
    if docker image inspect wmfs-tools >/dev/null 2>&1; then
        check "容器内 pjdfstest：不涉及 chown/权限的子集通过（失败都在 tests/pjdfstest-expected.txt 里，每条对应 semantics.md 的一条放松）" \
            scripts/container-tests.sh pjdfstest
        check "容器内 fsx（xfstests）1 万次无不一致" scripts/container-tests.sh fsx 10000
        check "容器内 fsx -l 65536（文件全程 inline、内容常比长度短）1 万次无不一致" scripts/container-tests.sh fsx 10000 -l 65536
    else
        skip "容器内 pjdfstest 子集与 fsx 1 万次" "没有镜像 wmfs-tools：先运行 scripts/build-tools-image.sh（要联网）"
    fi
    check "wmfs dev down 的安全清理顺序：一个挂载被冻住（SIGSTOP）、另一个上有写了没 close 的进程，照样卸载、停节点，不留挂载与进程" m6_down
    check "再 wmfs dev up：没 close 的那个文件的数据已经写出去了（dev down 先让挂载进程写出脏数据）" m6_after_down
    check "minimal profile 冒烟（scripts/profiles.sh minimal）" scripts/profiles.sh minimal
    check "结束：本次验收自己的挂载都已卸载（$LOGDIR 下没有 fuse.wmfs 挂载）" m6_no_own_mounts
    rm -rf "$F"
}

# 不论成败：结束写了没 close 的那个进程、卸载多地址挂载、停开发集群（它先卸载 dev mount 的挂载点）、删临时文件。
m6_cleanup() {
    [ -n "${WRITER_PID:-}" ] && kill "$WRITER_PID" 2>/dev/null
    grep -q " $LOGDIR/mnt3 " /proc/self/mountinfo && fusermount3 -uz "$LOGDIR/mnt3" 2>/dev/null
    wmfs dev down >/dev/null 2>&1
    rm -rf "$F"
}

m6_no_own_mounts() {
    local left
    left="$(grep ' - fuse.wmfs ' /proc/self/mountinfo | awk '{print $5}' | grep "^$LOGDIR/")"
    if [ -n "$left" ]; then
        printf '本次验收留下的 wmfs 挂载：\n%s\n' "$left"
        return 1
    fi
    echo "$LOGDIR 下没有 fuse.wmfs 挂载"
}

# wmfs mount <a,b> 挂载点：第一个地址连不上，换下一个。等挂上用一个有上限的轮询（wmfs dev mount 里也是这样等的）。
m6_multi_addr() {
    local mp="$LOGDIR/mnt3" pid i ok=1
    mkdir -p "$mp"
    wmfs mount "127.0.0.1:1,127.0.0.1:19100" "$mp" --key-file "$WMFS_DEV_DIR/n1/keys/client.key" \
        --log "$LOGDIR/mnt3.log" >"$LOGDIR/mnt3.out" 2>&1 &
    pid=$!
    for i in $(seq 1 300); do
        grep -q " $mp .* - fuse.wmfs " /proc/self/mountinfo && break
        kill -0 "$pid" 2>/dev/null || break
        sleep 0.1
    done
    cat "$LOGDIR/mnt3.out"
    if grep -q " $mp .* - fuse.wmfs " /proc/self/mountinfo; then
        echo multi >"$mp/multi.txt" && [ "$(cat "$mp/multi.txt")" = multi ] && ok=0
        fusermount3 -u "$mp" || fusermount3 -uz "$mp"
    fi
    wait "$pid"
    return "$ok"
}

m6_no_mounts() {
    local left
    left="$(grep ' - fuse.wmfs ' /proc/self/mountinfo | awk '{print $5}')"
    if [ -n "$left" ]; then
        printf '残留的 wmfs 挂载：\n%s\n' "$left"
        return 1
    fi
    echo "没有 fuse.wmfs 挂载"
}

m6_single_flight() {
    local i
    for i in 1 2 3 4 5 6 7 8 9 10; do
        echo "== 第 $i 遍"
        one_test --release -p wmfs-testkit --test net_rpc first_calls_from_16_threads_share_one_connection || return 1
    done
}

m6_mount_two() {
    local out
    out="$(wmfs dev mount "$MA")" || { printf '%s\n' "$out"; return 1; }
    printf '%s\n' "$out"
    printf '%s' "$out" | grep -q '新文件实际为 1 副本' || { echo "没有打印集群警告"; return 1; }
    wmfs dev mount "$MB" || return 1
    [ -n "$(wmfs dev client-pid "$MA")" ] && [ -n "$(wmfs dev client-pid "$MB")" ]
}

m6_cp_include() {
    cp -r /usr/include "$MA/include" || return 1
    diff -r --no-dereference /usr/include "$MA/include" && echo SAME
}

m6_include_layout() {
    local n=0 rel bad=0
    while IFS= read -r rel; do
        n=$((n + 1))
        wmfs fs stat "/include/$rel" --json | jq -e '.inline == false and .layout.chunk_size == 4194304' >/dev/null || {
            echo "/include/$rel 不是 chunk 文件"
            bad=1
        }
    done < <(cd "$MA/include" && find . -type f -size +64k | sed 's|^\./||')
    echo "大于 64 KiB 的文件：$n 个（本机 /usr/include 里也是 $(find /usr/include -type f -size +64k | wc -l) 个）"
    [ "$bad" -eq 0 ] && [ "$n" -eq "$(find /usr/include -type f -size +64k | wc -l)" ] || return 1
    json_expect '.inline == true' wmfs fs stat /include/stdio.h --json
}

m6_big() {
    local a b
    dd if=/dev/urandom of="$MA/big" bs=1M count=1024 status=none || return 1
    a="$(md5sum <"$MA/big" | awk '{print $1}')"
    echo "写入后：$a"
    wmfs dev umount "$MA" && wmfs dev mount "$MA" || return 1
    b="$(md5sum <"$MA/big" | awk '{print $1}')"
    echo "重挂后：$b"
    [ -n "$a" ] && [ "$a" = "$b" ]
}

m6_cto() {
    echo v1 >"$MA/cto.txt" && [ "$(cat "$MB/cto.txt")" = v1 ] || { echo "v1：读到 $(cat "$MB/cto.txt")"; return 1; }
    echo v2 >"$MA/cto.txt" && [ "$(cat "$MB/cto.txt")" = v2 ] || { echo "v2：读到 $(cat "$MB/cto.txt")"; return 1; }
    stat "$MB/cto.txt" >/dev/null # 读者刚 stat 过（属性新鲜），写者把文件改长：读者的 open 照样看到新长度与新内容
    echo v2-longer >"$MA/cto.txt" && [ "$(cat "$MB/cto.txt")" = v2-longer ] || { echo "v2-longer：读到 $(cat "$MB/cto.txt")"; return 1; }
    echo "v1、v2、v2-longer 都在另一个挂载上立即看到"
}

m6_no_stale() {
    local f
    echo v3 >"$MA/cto.txt" && echo v4 >"$MA/cto.txt" || return 1
    for f in "$WMFS_DEV_DIR"/mounts/*/mount.log; do
        if grep -n STALE_LAYOUT "$f"; then
            echo "$f 里有 STALE_LAYOUT 重写"
            return 1
        fi
    done
    if curl --noproxy '*' -s http://127.0.0.1:19180/metrics | grep 'code="STALE_LAYOUT"'; then
        echo "服务端回过 STALE_LAYOUT"
        return 1
    fi
    echo "挂载日志与服务端指标里都没有 STALE_LAYOUT"
}

m6_umask() {
    local m
    m="$(umask 027 && touch "$MA/um" && stat -c %a "$MA/um")" || return 1
    echo "权限：$m"
    [ "$m" = 640 ]
}

m6_links_xattr() {
    ln -s cto.txt "$MA/link" && [ "$(cat "$MA/link")" = v4 ] || return 1
    ln "$MA/cto.txt" "$MA/hard" && [ "$(stat -c %h "$MA/cto.txt")" = 2 ] || return 1
    local x
    x="$(python3 -c "import os,sys; p=sys.argv[1]; os.setxattr(p,'user.k',b'hi'); print(os.getxattr(p,'user.k'))" "$MA/cto.txt")"
    echo "xattr：$x"
    [ "$x" = "b'hi'" ]
}

m6_mgmt_xattr() {
    local v
    v="$(python3 -c "import os,sys; print(os.getxattr(sys.argv[1],'user.wmfs.mgmt').decode())" "$MA")" || return 1
    echo "user.wmfs.mgmt = $v（WMFS_CLUSTER = $WMFS_CLUSTER）"
    [ "$v" = "$WMFS_CLUSTER" ]
}

m6_orphan() {
    head -c 6M /dev/urandom >"$F/r6" && cp "$F/r6" "$MA/orphan" || return 1
    python3 - "$MA/orphan" "$MB/orphan" "$F/r6" <<'EOF'
import sys, time
f = open(sys.argv[1], "rb")
import os
os.unlink(sys.argv[2])
time.sleep(9)  # 越过孤儿的 6 s 与 gc 的一轮：打开着的文件不能被删
got = f.read()
want = open(sys.argv[3], "rb").read()
print("读到", len(got), "字节，与原文件一致" if got == want else "，不一致")
sys.exit(0 if got == want else 1)
EOF
}

m6_direct() {
    head -c 64M /dev/urandom >"$F/r64" || return 1
    dd if="$F/r64" of="$MA/direct" bs=1M oflag=direct status=none || return 1
    md5sum "$F/r64" "$MA/direct" "$MB/direct"
    [ "$(md5sum <"$F/r64")" = "$(md5sum <"$MB/direct")" ]
}

# 先制造"不好清理"的现场：$MB 的挂载进程被冻住（SIGSTOP），$MA 上有一个写了 1.5 MB、没 close 的进程（它占着挂载点，普通卸载会
# EBUSY）；再 wmfs dev down，之后既没有本次的挂载、也没有本次的 wmfs 进程。
m6_down() {
    python3 -c "import sys, time; f = open(sys.argv[1], 'wb'); f.write(b'u' * 1500000); f.flush(); time.sleep(120)" \
        "$MA/unclosed" &
    WRITER_PID=$!
    timeout 30 sh -c 'until [ "$(stat -c %s "$1" 2>/dev/null)" = 1500000 ]; do sleep 0.1; done' _ "$MA/unclosed" || return 1
    kill -STOP "$(wmfs dev client-pid "$MB")" || return 1
    local t0=$SECONDS
    wmfs dev down || return 1
    echo "wmfs dev down 用了 $((SECONDS - t0)) s"
    kill "$WRITER_PID" 2>/dev/null
    wait "$WRITER_PID" 2>/dev/null
    WRITER_PID=""
    m6_no_own_mounts || return 1
    if pgrep -f "wmfs (mount|server) .*$LOGDIR" >/dev/null; then
        pgrep -fa "wmfs (mount|server) .*$LOGDIR"
        echo "还有 wmfs 进程"
        return 1
    fi
    echo "没有残留的挂载与进程"
}

m6_after_down() {
    wmfs dev up --nodes 1 >/dev/null && wmfs dev wait leader --timeout 30 >/dev/null || return 1
    wmfs fs get /unclosed "$F/unclosed" || return 1
    python3 -c "import sys; d = open(sys.argv[1], 'rb').read(); print(len(d)); sys.exit(0 if d == b'u' * 1500000 else 1)" "$F/unclosed" || return 1
    wmfs dev down
}
