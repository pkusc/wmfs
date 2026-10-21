//! 随机数（design.md 0.2 第 7 条）：所有随机数来自 [`fill`]（内核 `getrandom`），不引入 rand crate。
//!
//! 另有一个带种子的 xorshift 伪随机发生器 [`XorShift64`]：给"带种子的随机测试"（9.1，失败时打印种子以便复现）
//! 与 trace id 生成（3.11，每线程一个，种子来自 `fill`）用；它不是密码学安全的。

use super::sys;

/// 用内核随机数填满 `buf`（密钥、nonce、cluster_id、session id 都用它）。
pub fn fill(buf: &mut [u8]) {
    // getrandom 只在内核不支持（< 3.17）时失败；那样的机器跑不了 wmfs。
    if let Err(e) = sys::getrandom(buf) {
        panic!("不变量破坏：getrandom 失败（{e}），内核过旧？");
    }
}

/// 一个随机的 u64（来自 [`fill`]）。
pub fn random_u64() -> u64 {
    let mut b = [0u8; 8];
    fill(&mut b);
    u64::from_le_bytes(b)
}

/// 带种子的伪随机发生器（xorshift64）。同一种子产生同一序列。
#[derive(Clone, Debug)]
pub struct XorShift64 {
    state: u64,
}

impl XorShift64 {
    /// 种子为 0 时换成一个固定的非零值（xorshift 的状态不能为 0）。
    pub fn new(seed: u64) -> XorShift64 {
        XorShift64 { state: if seed == 0 { 0x9E37_79B9_7F4A_7C15 } else { seed } }
    }

    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    /// `[0, n)` 里的一个数（n 必须 > 0）。
    pub fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }

    /// 用伪随机字节填满 `buf`（测试里造随机输入用）。
    pub fn fill(&mut self, buf: &mut [u8]) {
        for chunk in buf.chunks_mut(8) {
            let v = self.next_u64().to_le_bytes();
            chunk.copy_from_slice(&v[..chunk.len()]);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fill_is_random() {
        let (mut a, mut b) = ([0u8; 32], [0u8; 32]);
        fill(&mut a);
        fill(&mut b);
        assert_ne!(a, b);
        assert_ne!(a, [0u8; 32]);
        assert_ne!(random_u64(), random_u64());
    }

    #[test]
    fn xorshift_is_deterministic() {
        let mut x = XorShift64::new(42);
        let mut y = XorShift64::new(42);
        let a: Vec<u64> = (0..10).map(|_| x.next_u64()).collect();
        let b: Vec<u64> = (0..10).map(|_| y.next_u64()).collect();
        assert_eq!(a, b);
        let mut z = XorShift64::new(0);
        assert_ne!(z.next_u64(), 0);
        assert!((0..1000).all(|_| z.below(7) < 7));
        let mut buf = [0u8; 13];
        z.fill(&mut buf);
        assert_ne!(buf, [0u8; 13]);
    }
}
