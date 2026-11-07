# 锁的计数器（M12：锁即同步点）：fd 一直开着，每轮 加锁 → 读 → 加一 → 写回 → 解锁。两个挂载点上的进程各跑一个，结果恰好是总次数
# 才说明锁跨挂载点互斥、拿到锁时读得到对方放锁之前提交的值。cli/tests/fuse_locks.rs（include_str!）与 scripts/accept.d/m12.sh 共用。
# 用法：python3 lock_counter.py <文件> fcntl|flock <次数>
import fcntl
import os
import sys

fd = os.open(sys.argv[1], os.O_RDWR)
lock = fcntl.flock if sys.argv[2] == "flock" else fcntl.lockf
for _ in range(int(sys.argv[3])):
    lock(fd, fcntl.LOCK_EX)
    v = int(os.pread(fd, 32, 0) or b"0")
    b = str(v + 1).encode()
    os.pwrite(fd, b, 0)
    os.ftruncate(fd, len(b))
    lock(fd, fcntl.LOCK_UN)
