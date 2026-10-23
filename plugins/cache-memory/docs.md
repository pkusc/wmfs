# cache-memory 客户端缓存插件

**钩子**：cache_store（客户端，不进集群插件清单）　**热路径**：是（每次读没命中页缓存时），预算 20 µs/次（另加拷贝）
**fail point**：`plugin.cache-memory.cache_store`（触发 = 这次读当作没命中、放的时候不放）　**指标**：`wmfs_cache_memory_hits_total`、
`wmfs_cache_memory_misses_total`、`wmfs_cache_memory_bytes`

## 它做什么

`wmfs mount` 进程读文件时，内核页缓存没有的部分先问它，没有再去数据节点读（按 1 MiB 对齐读回来，放进它里面）。它是挂载进程内存里的
一个 LRU：按 `(inode, change_id, 1 MiB 块号)` 存块，满了先扔最久没用过的（design.md 4.11）。

- **不会读到旧数据**：键里带着文件的 `change_id`。别的客户端改了文件、关闭之后，你下一次打开时拿到新的 change_id，旧的块就再也
  查不到了（close-to-open，semantics.md 2.2）；文件打开期间仍可能读到打开时的旧内容，这与 cto 的承诺一致。
- 只放已经提交到数据节点的数据；你自己还没写出去的数据在写回缓冲里，读的时候直接从那里拿。
- strong 目录里的文件与用 `O_DIRECT` 打开的文件不经过它。

## 配置

挂载参数 `--cache memory[:SIZE]`（默认 `memory:512M`），`--cache none` 关掉缓存；没有 node.toml 里的部署参数。local-nvme 缓存在 M13。

## 诚实说明

缓存在挂载进程的内存里，进程退出就没了（不做持久化，也就没有"重启后用到陈旧缓存"的问题）。容量是缓存数据的字节数，不含索引本身
的少量开销。
