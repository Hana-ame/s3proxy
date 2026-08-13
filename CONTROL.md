# s3-proxy 冷热引擎外部控制协议

冷热判定引擎的**全部状态和控制面就是 `state_dir/tier.db`(SQLite)**。没有 HTTP 管理端口:外部程序(脚本、监控、任何语言的 sqlite 客户端)直接打开这个文件,查询随便读,控制写两张约定表。代理进程每 **1 秒**轮询一次控制表,指令即时生效。

```
┌──────────────┐  只读查询(实时镜像)   ┌──────────────────────────────┐
│  外部程序     │ ───────────────────▶ │  tier.db (proxy 同一文件)      │
│ sqlite3 /    │  控制(只写两张表)      │  control:  运行时策略覆盖      │
│ 任意语言      │ ───────────────────▶ │  commands: 一次性强制操作      │
└──────────────┘                      │  proxy 轮询消费,~1s 生效      │
                                      └──────────────────────────────┘
```

## 查询:随便读,只有读

数据表(`objects` / `resources` / `buckets`)是内存状态的**实时镜像**,每次变更(含 touch)都同步落盘,所以直接 SELECT 就是真值。

常用查询:

```sql
-- 每个资源的冷热状态:池、引用数、大小、idle 多久了(秒)
SELECT * FROM v_cold_status ORDER BY idle_seconds DESC;

-- 各池占用
SELECT pool, count(*) AS objects, sum(size) AS bytes FROM resources GROUP BY pool;

-- 名字层:key → 内容 id
SELECT fk, id, content_type, storage_class, last_access FROM objects;

-- 目标资源的两个依据(last_access 是全部引用 key 的最大值)
SELECT id, pool, refs, last_access,
       CAST(strftime('%s','now') AS INTEGER) - CAST(strftime('%s', last_access) AS INTEGER) AS idle_seconds
FROM resources;
```

`resources.refs` 归零且名字层无引用的资源是**孤儿**,当前版本不会自动回收(见"已知限制")。

## 控制:只允许写这两张表

> **警告**:绝对不要直接 UPDATE/DELETE/INSERT `objects` / `resources` / `buckets` 的行。代理进程的内存 map 才是权威,任何这类改动都会在下一笔业务写入(如 touch)时被**静默覆盖回原值**,等于没写,还可能造成瞬时不一致。

### 1. 运行时策略覆盖 — `control(k, v)`

`k` 是主键,`INSERT OR REPLACE` 覆盖,**`DELETE FROM control WHERE k=...` 即恢复配置文件默认值**(下一轮轮询生效)。

| k | v | 作用 | 示例 |
|---|---|---|---|
| `auto_enabled` | `"0"` / `"1"` | 暂停/恢复自动迁移循环(后台 RunOnce 被 gate)| `"0"` = 停机维护 |
| `cold_after_ms` | 毫秒数 | 热池资源 idle 多久算冷,覆盖 `cold_after` | `1800000` = 30 分钟 |
| `max_hot_bytes` | 字节数 | 热池配额,覆盖 `max_hot_bytes` | `107374182400` = 100GiB |
| `promote_on_access` | `"0"` / `"1"` | 冷读是否自动提升回热池,覆盖 `promote_on_access` | |
| *(其它 k 会被忽略并记录日志)* | | | |

示例(sqlite3 CLI):

```sh
# 停机维护:暂停自动迁移
sqlite3 state/tier.db "INSERT INTO control (k,v) VALUES ('auto_enabled','0')
                       ON CONFLICT(k) DO UPDATE SET v=excluded.v;"
# 改成 1 小时,帮它算毫秒
sqlite3 state/tier.db "INSERT INTO control (k,v) VALUES ('cold_after_ms','3600000')
                       ON CONFLICT(k) DO UPDATE SET v=excluded.v;"
# 恢复默认
sqlite3 state/tier.db "DELETE FROM control WHERE k='cold_after_ms';"
```

### 2. 一次性强制操作 — `commands(seq, verb, arg)`

只 `INSERT`。代理轮询时按 `seq` 依次执行,执行完(**成功或失败**)删除该行;失败会打印在代理日志里,不会重试。`verb` 必须时小写英文。

| verb | arg 格式 | 效果 |
|---|---|---|
| `migrate` | `bucket/key` 或内容 id(sha256hex) | 立即把该资源迁到冷池(从热池,round-robin 选目标);已在冷池则报错跳过 |
| `promote` | 同上 | 立即把该资源从冷池提升回热池;已在热池报错跳过 |

```sh
sqlite3 state/tier.db "INSERT INTO commands (verb, arg) VALUES ('migrate', 'mybucket/old.log');"
sqlite3 state/tier.db "INSERT INTO commands (verb, arg) VALUES ('promote', 'a1b2c3...');"
```

`migrate`/`promote` 绕过 idle/配额判定,是"手动控制引擎"的通道;与自动迁移的竞态由引擎内的内容锁 + 存储层 re-check 保证一致(输掉的一方干净报错,不会损坏)。

## 现成工具

仓库自带 Go 实现(也可当作参考写法,任何语言照抄两张表的 SQL 即可):

```sh
go run ./cmd/s3-admin state/tier.db status
go run ./cmd/s3-admin state/tier.db status --json
go run ./cmd/s3-admin state/tier.db pause        # 自动迁移暂停
go run ./cmd/s3-admin state/tier.db resume
go run ./cmd/s3-admin state/tier.db set --cold-after=30m --max-hot-bytes=100GiB
go run ./cmd/s3-admin state/tier.db migrate mybucket/old.log
go run ./cmd/s3-admin state/tier.db promote a1b2c3d4e5...
```

## 生效延迟

控制轮询固定 **1 秒**,`pause/resume`、覆盖值、指令最多 1 秒生效。重启代理时保留的 `control` 行会立即应用(例如维护中暂停,重启后依旧暂停)。

## 已知限制

- 孤儿资源(`refs=0` 且无 key 引用,如删桶后遗留)不会自动回收,需手动处理(池内按 id 直删,或留待重建)。
- 索引丢失重建后名字层不可恢复(见 `internal/tier` 包注释),控制表和池内容不受影响。