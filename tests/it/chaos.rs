//! chaos（roadmap M15 的发布门槛"所有故障注入测试中客户端除重试外无错误"与"Jepsen-lite 线性一致"；`scripts/accept.sh chaos` 跑）。
//!
//! 对着一个 3 节点开发集群（`wmfs dev up --nodes 3 --profile nas-safe` + 一个 `wmfs dev mount`；环境变量 `WMFS_DEV_DIR`，以及
//! `WMFS_CHAOS_DIR` = 挂载点下给这个用例用的目录），两份负载同时跑：
//! ① 寄存器（Jepsen-lite，`harness::register`）：3 个线程用 DataClient 直接读写一个 chunk 的前 8 字节——历史线性一致，而且**每个
//!    操作都成功**（DataClient 自己按 3.3.8 重试，结果未知的操作 = 客户端看到了错误）；
//! ② FUSE 写者：经挂载点一个接一个地写文件并 fsync（应用看到的就是普通的 write/fsync/close）——零错误；结束之后用参考客户端从数据节点
//!    逐个读回（不经 FUSE 与本机页缓存：fsync 过的数据在服务端没丢，9.5 检查器 ①）。
//!
//! 捣乱一次一种、按顺序轮两遍（每种至少两次），节点随机；每种都核对它真的生效了（`harness::dev` 的 `hits`、`serving_on`）：
//! - kill -9 再起来；
//! - SIGSTOP 一会儿（第二遍停 9–12 s，比 target 租约 8 s 还久：先自我隔离、再被移出链，回来以 Syncing 重同步）；
//! - 分区：一个节点与另外两个之间的请求与 raft 消息双向都丢（fail point `net.drop.to.*`，不用 root；客户端照样连得到它），两边的
//!   `net.drop.to.*` 都要在触发——它是元数据 leader 时让出 leader；第一遍分开 3–6 s；第二遍至少 12 s，一直分开到它的 target 至少被移出
//!   一条链（最多再等 60 s），恢复之后重同步（设置分区慢了，第一遍也按第二遍来，见 [`partition`]）；
//! - 延迟：一个节点发出的每一帧晚 50–200 ms（fail point `net.delay`，帧与帧之间不累加）3–6 s；
//! - 时钟：一个节点的单调时钟快或慢 10%、墙钟偏 ±30 s（fail point `clock.mono.rate_ppm`、`clock.wall.skew_ms`；租约只假设速率偏差
//!   有界，两个节点的速率比 ≤ 1.25 都安全，ADR-0023）。
//!
//! 坏盘（`chunk.commit.before_rename` 让一个 target 转只读、移出写路径，客户端的写照常成功）不在这里：只读要重启节点才清掉，多进程的
//! 用例是 `scripts/accept.sh M11` 的 m11_read_only（`accept.sh all` 跑），见 design.md 9.5 的 M15 注记。
//!
//! 失败时打印种子（`WMFS_JEPSEN_SEED=<种子>` 复现捣乱的顺序）。只对本开发集群的进程发信号（`harness::dev`）。

use std::io::Write;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use wmfs_core::base::rand::XorShift64;
use wmfs_testkit::harness::dev::Dev;
use wmfs_testkit::harness::node::{get, who};
use wmfs_testkit::harness::register::{seed, start};

/// 捣乱的种类（见文件头），每一遍按这个顺序各来一次。
const KILL: u64 = 0;
const STOP: u64 = 1;
const PARTITION: u64 = 2;
const DELAY: u64 = 3;
const CLOCK: u64 = 4;
const KINDS: u64 = 5;

/// 第 i 个文件的内容：256 KiB 多一点（大于 inline 的 64 KiB，走 chunk），每个文件不同。
fn content(i: usize) -> Vec<u8> {
    (0..(256 << 10) + i * 13).map(|j| (j.wrapping_mul(31) ^ (i * 7)) as u8).collect()
}

/// FUSE 写者：直到 `stop`，在 `dir` 里一个接一个地写文件并 fsync；返回写完的个数与看到的错误。
fn writer(dir: PathBuf, stop: Arc<AtomicBool>) -> std::thread::JoinHandle<(usize, Vec<String>)> {
    std::thread::spawn(move || {
        let (mut i, mut errors) = (0, Vec::new());
        while !stop.load(Ordering::Relaxed) && i < 3000 {
            let r = std::fs::File::create(dir.join(format!("f{i}"))).and_then(|mut f| {
                f.write_all(&content(i))?;
                f.sync_all()
            });
            if let Err(e) = r {
                println!("写者：f{i} 失败（应用看到了错误）：{e}");
                errors.push(format!("f{i}: {e}"));
            }
            i += 1;
            std::thread::sleep(Duration::from_millis(50));
        }
        (i, errors)
    })
}

/// 一次捣乱（见文件头），返回给人看的描述。`round` 0 是第一遍（短），1 是第二遍（越过 target 租约）。
fn fault(dev: &Dev, r: &mut XorShift64, kind: u64, round: u64) -> String {
    let n = 1 + r.below(3) as u32;
    let others: Vec<u32> = (1..=3).filter(|m| *m != n).collect();
    match kind {
        KILL => {
            dev.kill(n);
            std::thread::sleep(Duration::from_millis(500 + r.below(1500)));
            dev.start(n);
            format!("kill -9 node{n}")
        }
        STOP => {
            let ms = if round == 0 { 1000 + r.below(3000) } else { 9000 + r.below(3000) };
            dev.pause(n);
            std::thread::sleep(Duration::from_millis(ms));
            dev.resume(n);
            format!("SIGSTOP node{n} {ms} ms")
        }
        PARTITION => partition(dev, r, n, &others, round),
        DELAY => {
            let (ms, hold, hits) = (50 + r.below(150), 3000 + r.below(3000), dev.hits(n, "net.delay"));
            dev.delay(n, Some(ms));
            std::thread::sleep(Duration::from_millis(hold));
            assert!(dev.hits(n, "net.delay") > hits, "node{n} 的 net.delay 没有触发");
            dev.delay(n, None);
            format!("延迟 node{n} 发出的每一帧 {ms} ms，{hold} ms")
        }
        CLOCK => {
            let rate = if r.below(2) == 0 { 100_000 } else { -100_000 };
            let wall = if r.below(2) == 0 { 30_000 } else { -30_000 };
            let (ms, hits) = (5000 + r.below(5000), dev.hits(n, "clock.mono.rate_ppm"));
            dev.skew_clock(n, rate, wall);
            std::thread::sleep(Duration::from_millis(ms));
            assert!(dev.hits(n, "clock.mono.rate_ppm") > hits, "node{n} 的时钟 fail point 没有触发");
            dev.clear_clock(n);
            format!("时钟 node{n}：速率 {rate:+} ppm、墙钟 {wall:+} ms，{ms} ms")
        }
        _ => unreachable!("捣乱的种类只有 0..KINDS"),
    }
}

/// 分区 node `n` 与 `others`（见文件头）。设好之后核对两边的 `net.drop.to.*` 都在触发。第一遍的短分区只在设置得够快时才短：设置慢了，
/// "别人 → 它"那一半先断了这么久，转发失败可能先满 10 s、它被当作活着的成员移出（要过 repair_delay 才补回来，3.4.2.10）——这时让它分开得
/// 够久、先被判离线，回来时照常重新加入。长的分区一直分开到它的 target 至少被移出一条链（它本来就没有 Serving 的成员时只分开 12 s）。
fn partition(dev: &Dev, r: &mut XorShift64, n: u32, others: &[u32], round: u64) -> String {
    let drops = |from: &[u32], to: &[u32]| -> u64 {
        from.iter().flat_map(|x| to.iter().map(move |y| dev.hits(*x, &format!("net.drop.to.{y}")))).sum()
    };
    let (out0, in0, serving0) = (drops(&[n], others), drops(others, &[n]), dev.serving_on(n, others));
    let t = Instant::now();
    dev.partition(&[n], others);
    let setup = t.elapsed().as_millis() as u64; // 四个管理 API 调用，平时几毫秒
    let short = round == 0 && setup < 1000;
    let ms = if short { 3000 + r.below(3000) } else { 12_000 };
    std::thread::sleep(Duration::from_millis(ms));
    let mut removed = String::new();
    if let Some(s0) = serving0.filter(|s| !short && *s > 0) {
        let t = Instant::now();
        while !dev.serving_on(n, others).is_some_and(|s| s < s0) {
            assert!(
                t.elapsed() < Duration::from_secs(60),
                "分开 {ms} ms 又等了 60 s，node{n} 的 target 还在每一条链里"
            );
            std::thread::sleep(Duration::from_millis(200));
        }
        removed = format!("，之后又等了 {} ms 它的 target 被移出链", t.elapsed().as_millis());
    }
    assert!(drops(&[n], others) > out0, "node{n} 发往 {others:?} 的 net.drop.to.* 没有触发");
    assert!(drops(others, &[n]) > in0, "{others:?} 发往 node{n} 的 net.drop.to.{n} 没有触发");
    dev.heal(&[n], others);
    format!("分区 node{n} | {others:?} {ms} ms（设置用了 {setup} ms）{removed}")
}

#[test]
#[ignore = "要一个跑着的 3 节点开发集群与一个挂载点：由 scripts/accept.sh chaos 跑"]
fn clients_see_no_errors_and_a_register_stays_linearizable_under_kill_stop_partition_and_clock_skew() {
    // 先起 Dev、再打开挂载点上的文件：它跑捣乱命令的 shell 不能继承这些文件（harness::dev 的文件头）
    let (dev, seed) = (Dev::from_env(), seed());
    let dir = PathBuf::from(std::env::var("WMFS_CHAOS_DIR").expect("WMFS_CHAOS_DIR"));
    std::fs::create_dir_all(&dir).unwrap();
    // 最多 2000 个操作（足够撑过全部捣乱，之后用 check 停下）；不偏向 L；每个操作之后歇 300–1200 ms
    let reg = start(&dev.dir, seed, 2000, false, 300);
    let stop = Arc::new(AtomicBool::new(false));
    let w = writer(dir.clone(), stop.clone());
    let (mut r, t0, mut faults) = (XorShift64::new(seed), Instant::now(), Vec::new());
    for round in 0..2 {
        for kind in 0..KINDS {
            std::thread::sleep(Duration::from_millis(1500 + r.below(2500)));
            let what = fault(&dev, &mut r, kind, round);
            println!("{:>6.1} s 捣乱：{what}", t0.elapsed().as_secs_f64());
            faults.push(what);
            assert!(reg.running(), "寄存器的操作数用完了：捣乱还没做完");
        }
    }
    std::thread::sleep(Duration::from_secs(3)); // 最后一次捣乱之后再跑一会儿
    let reg_errors = reg.check(seed, &faults, 100);
    stop.store(true, Ordering::Relaxed);
    let (written, errors) = w.join().unwrap();
    println!("FUSE 写者：写了 {written} 个文件，{} 个错误；寄存器：{reg_errors} 个失败的操作", errors.len());
    assert_eq!(reg_errors, 0, "种子 {seed}：寄存器的客户端看到了错误（除重试外不该有）");
    assert!(errors.is_empty(), "种子 {seed}：FUSE 写者看到了错误：{errors:?}");
    assert!(dev.wmfs(&["dev", "wait", "chains-serving", "--timeout", "180"]), "捣乱之后链没有全部回到 Serving");
    read_back(&dev, &dir, written);
}

/// 已确认写不丢（9.5 检查器 ①）：fsync 过的文件逐个用参考客户端从数据节点读回（不经 FUSE 与本机页缓存），与写进去的相同。
fn read_back(dev: &Dev, dir: &Path, n: usize) {
    let c = dev.client();
    for i in 0..n {
        let ino = std::fs::metadata(dir.join(format!("f{i}"))).unwrap().ino();
        let got = get(&c, &who(0, 0), ino).unwrap_or_else(|e| panic!("f{i} 从数据节点读不回来：{e}"));
        assert!(got == content(i), "f{i} 从数据节点读回来不对（{} 字节）", got.len());
    }
    c.meta.close();
    println!("{n} 个文件从数据节点读回都一致");
}
