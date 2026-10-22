//! 读闸门的单元测试（design.md 3.4.1.6）：批量、"之后到达的等下一轮"、500 ms 重发、2 s 放弃、角色变化（坑 P6）。

use crossbeam_channel::{Receiver, bounded};

use super::*;

fn reader(g: &mut ReadGate) -> Receiver<Result<(), ErrCode>> {
    let (tx, rx) = bounded(1);
    g.add(tx);
    rx
}

/// 记录每次 read_index 的 ctx。
fn pump(g: &mut ReadGate, leader: bool, ready: bool, sent: &mut Vec<Vec<u8>>) {
    g.pump(leader, ready, &mut |c| sent.push(c));
}

#[test]
fn one_round_serves_all_current_waiters_after_apply() {
    let (mut g, mut sent) = (ReadGate::new(), Vec::new());
    let rs: Vec<_> = (0..3).map(|_| reader(&mut g)).collect();
    pump(&mut g, true, true, &mut sent);
    assert_eq!(sent, vec![1u64.to_be_bytes().to_vec()], "3 个读者只发一次 read_index");
    let late = reader(&mut g);
    pump(&mut g, true, true, &mut sent);
    assert_eq!(sent.len(), 1, "有在途轮次时不再发：之后到达的读者等下一轮");
    g.on_read_state(7, &1u64.to_be_bytes());
    g.on_applied(6);
    assert!(rs.iter().all(|r| r.try_recv().is_err()), "applied < read index：还不能读");
    g.on_applied(7);
    assert!(rs.iter().all(|r| r.try_recv() == Ok(Ok(()))));
    assert!(late.try_recv().is_err());
    pump(&mut g, true, true, &mut sent);
    assert_eq!(sent.len(), 2, "下一轮");
    g.on_read_state(9, &2u64.to_be_bytes());
    g.on_applied(9);
    assert_eq!(late.try_recv(), Ok(Ok(())));
    assert_eq!(g.pending(), 0);
}

#[test]
fn followers_answer_not_leader_and_unready_leaders_wait_then_retry() {
    let (mut g, mut sent) = (ReadGate::new(), Vec::new());
    let r = reader(&mut g);
    pump(&mut g, false, false, &mut sent);
    assert_eq!(r.try_recv(), Ok(Err(ErrCode::NOT_LEADER)), "follower 不服务读");
    let r = reader(&mut g);
    for _ in 0..19 {
        pump(&mut g, true, false, &mut sent);
        g.on_tick(true, &mut |c| sent.push(c));
    }
    assert!(
        sent.is_empty() && r.try_recv().is_err(),
        "leader 未就绪（本任期还没提交）：不发 read_index，继续等（坑 P6）"
    );
    g.on_tick(true, &mut |c| sent.push(c));
    assert_eq!(r.try_recv(), Ok(Err(ErrCode::RETRY)), "等待超过 2 s 的读者回 RETRY");
}

#[test]
fn lost_read_index_is_resent_with_a_new_ctx_and_gives_up_after_2s() {
    let (mut g, mut sent) = (ReadGate::new(), Vec::new());
    let r = reader(&mut g);
    pump(&mut g, true, true, &mut sent);
    for _ in 0..5 {
        g.on_tick(true, &mut |c| sent.push(c));
    }
    assert_eq!(sent, vec![1u64.to_be_bytes().to_vec(), 2u64.to_be_bytes().to_vec()], "500 ms 没有 ReadState → 重发");
    g.on_read_state(3, &1u64.to_be_bytes());
    g.on_applied(3);
    assert!(r.try_recv().is_err(), "旧 ctx 的 ReadState 不算");
    g.on_read_state(4, &2u64.to_be_bytes());
    g.on_applied(4);
    assert_eq!(r.try_recv(), Ok(Ok(())));

    let r = reader(&mut g);
    pump(&mut g, true, true, &mut sent);
    for _ in 0..20 {
        g.on_tick(true, &mut |c| sent.push(c));
    }
    assert_eq!(r.try_recv(), Ok(Err(ErrCode::RETRY)), "首次发出超过 2 s → RETRY");
    assert_eq!(g.pending(), 0);
}

#[test]
fn role_change_fails_everyone() {
    let (mut g, mut sent) = (ReadGate::new(), Vec::new());
    let a = reader(&mut g);
    pump(&mut g, true, true, &mut sent);
    g.on_read_state(5, &1u64.to_be_bytes());
    let b = reader(&mut g);
    pump(&mut g, true, true, &mut sent);
    let c = reader(&mut g);
    g.fail_all(ErrCode::NOT_LEADER);
    for r in [a, b, c] {
        assert_eq!(r.try_recv(), Ok(Err(ErrCode::NOT_LEADER)));
    }
    assert_eq!(g.pending(), 0);
}
