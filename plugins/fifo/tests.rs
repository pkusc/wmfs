//! fifo 的单元测试：manifest、先进先出、空队列回 Err(None)。

use wmfs_core::hooks::manifest::check_manifest_toml;
use wmfs_core::hooks::traits::QueueKey;

use super::*;

#[test]
fn manifest_toml_matches_code() {
    check_manifest_toml(&MANIFEST, include_str!("manifest.toml")).unwrap();
}

#[test]
fn first_in_first_out() {
    let f = Fifo::new();
    let key = QueueKey { tenant: 0, qos_id: 0, class_id: 1 };
    let spec = QosSpec { r: 0.0, w: 0.0, l: 0.0, bw_max: 0.0, burst_ms: 0, low_priority: false };
    assert_eq!(f.dequeue(Instant::now()), Err(None), "队列空");
    for token in [5, 3, 9] {
        let background = token == 3;
        f.enqueue(QosItem { key, job_id: 0, cost: 1, bytes: 0, background, token }, spec);
    }
    let got: Vec<u64> = (0..3).map(|_| f.dequeue(Instant::now()).unwrap()).collect();
    assert_eq!(got, vec![5, 3, 9]);
    assert_eq!(f.dequeue(Instant::now()), Err(None));
    assert!(f.stats().is_empty());
}
