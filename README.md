# minikv-lite

`minikv-lite` 是一个为了教学和演示目的用 Rust 编写的微型分布式 KV 存储。它主要包含：一个协调器节点 (Coordinator) 和多个存储卷节点 (Volume nodes)，实现了 HTTP 路由、一致性哈希以及同步复制机制。

```text
client/cli
   |
   v
coordinator :7000
   | 一致性哈希 + 副本复制
   +---- volume v1 :7001 -> 追加写入 data.log
   +---- volume v2 :7002 -> 追加写入 data.log
   +---- volume v3 :7003 -> 追加写入 data.log
```

## 核心特性

本项目虽然非常微型，但展示了几个分布式存储里最核心的设计权衡：

- 所有的写入请求都会带上 CRC 校验，以 append-only (追加方式) 记录到 `data.log` 中。
- 启动时会回放整个日志，构建出内存级别的对 live key (存活键) 索引。
- 读取操作通过索引直接 seek 到对应的字节偏移量后拉取最新 value。
- 删除操作写入一个 tombstone (墓碑) 记录，然后将这个 key 从内存中剔除。
- 压缩操作 (Compaction) 采用停顿世界 (stop-the-world) 的方式，将所有存活数据重写到一个新的 log 中并实现原子替换。

## 编译

```bash
cargo build
cargo test
cargo clippy -- -D warnings
```

## 本地运行

启动协调器：

```bash
cargo run --bin coord -- \
  --listen 127.0.0.1:7000 \
  --replicas 2 \
  --vnodes 64 \
  --dead-after-secs 6 \
  --meta ./coord-meta.json
```

启动三个 Volume 存储节点：

```bash
cargo run --bin volume -- --id v1 --listen 127.0.0.1:7001 --coord http://127.0.0.1:7000 --data ./data/v1 --heartbeat-secs 2
cargo run --bin volume -- --id v2 --listen 127.0.0.1:7002 --coord http://127.0.0.1:7000 --data ./data/v2 --heartbeat-secs 2
cargo run --bin volume -- --id v3 --listen 127.0.0.1:7003 --coord http://127.0.0.1:7000 --data ./data/v3 --heartbeat-secs 2
```

使用 CLI 交互：

```bash
cargo run --bin cli -- --coord http://127.0.0.1:7000 put k1 v1
cargo run --bin cli -- --coord http://127.0.0.1:7000 get k1
cargo run --bin cli -- --coord http://127.0.0.1:7000 del k1
cargo run --bin cli -- --coord http://127.0.0.1:7000 status
cargo run --bin cli -- --coord http://127.0.0.1:7000 cluster
cargo run --bin cli -- --coord http://127.0.0.1:7000 ring k1
cargo run --bin cli -- --coord http://127.0.0.1:7000 volume-stats
cargo run --bin cli -- health 127.0.0.1:7001
cargo run --bin cli -- keys 127.0.0.1:7001
cargo run --bin cli -- keys 127.0.0.1:7001 --prefix app: --limit 20
cargo run --bin cli -- compact 127.0.0.1:7001
```

## HTTP API 参考

### Volume API

| Method | Path | Body | Response |
|---|---|---|---|
| PUT | `/local/{key}` | 原始字节串 | `200` |
| GET | `/local/{key}` | 无 | `200` 原始字节串, 或 `404` |
| DELETE | `/local/{key}` | 无 | `200` |
| GET | `/healthz` | 无 | `"ok"` |
| GET | `/admin/stats` | 无 | 存储引擎数据统计 JSON |
| GET | `/admin/keys` | 可选前缀与限制 | 字典序排练的键列表 JSON |
| POST | `/admin/compact` | 无 | 压缩统计结果 JSON |

### Coordinator API

| Method | Path | Body | Response |
|---|---|---|---|
| POST | `/register` | `{"volume_id":"v1","addr":"...:7001"}` | `200` |
| POST | `/heartbeat` | `{"volume_id":"v1"}` | `200`, 或 `404` |
| PUT | `/kv/{key}` | 原始字节串 | `200`, 若可用副本不足会返回 `503` |
| GET | `/kv/{key}` | 无 | `200` 或 `404` |
| DELETE | `/kv/{key}` | 无 | `200`, 或 `503` |
| GET | `/status` | 无 | 注册表 JSON |
| GET | `/admin/ring/{key}` | 无 | 目标目标 volume ID 与地址 |
| GET | `/admin/cluster` | 无 | 集群摘要与注册表 |
| GET | `/admin/volumes/stats` | 无 | 所有节点的汇总数据 |

## Demo 脚本

```bash
bash scripts/demo.sh
```

或使用 Windows PowerShell：

```powershell
.\scripts\demo.ps1
```
