# s3-proxy

一个 Go 编写的 S3 兼容存储网关,前面是完整的 S3 HTTP API,后面是可插拔的存储后端,中间夹一层**热/冷分层策略**和**内容寻址去重**。

- 所有写入先落"热池"(通常是本地磁盘)
- 后台循环把空闲超过 `cold_after`(或超出热池字节配额)的对象迁到任意多个"冷池"(远程 S3 兼容服务,如 Cloudflare R2、MinIO,或额外的本地池)
- 冷对象的读取可以自动提升回热池(`promote_on_access`)

## 特性

- **完整 S3 API**:SigV4 签名认证、bucket/object CRUD、ListObjects(prefix/delimiter 分页)、CopyObject、Multipart 上传、批量删除,支持 path-style 与 virtual-host style 寻址
- **内容去重**:对象按 sha256 内容寻址,同一内容上传到不同 key 只存一份;Copy 是零字节移动的纯映射插入;同内容重传/自拷贝不增加引用计数,最终删除后字节被完整清除
- **完整性校验**:上传带 `Content-MD5` 时在**写入前**校验,不匹配直接拒绝(400 BadDigest)且不留下任何痕迹,旧对象不受影响——与 AWS 语义一致
- **热/冷分层**:本地磁盘当缓冲,远程对象存储当冷层,冷读自动提升(类缓存 read-through)
- **崩溃安全**:SQLite(WAL)索引实时落盘,重启不会瞬时清空热池;迁移中断可自愈(读时自动探测其他池);分片上传暂存超过 24h 自动清理,磁盘不会被中断的会话耗尽
- **无管理端口的控制面**:外部程序直接通过 SQLite 表控制引擎(暂停迁移、改策略、强制迁移/提升),哨兵文件触发毫秒级生效——详见 [CONTROL.md](CONTROL.md)
- **纯 Go、无 cgo**;CI 带 `-race` 全量测试,标签 `v*` 自动发布多平台二进制

## 架构

```
                     S3 客户端 (aws cli / rclone / 任意 SDK)
                                    │  SigV4
                                    ▼
                     ┌─────────────────────────────┐
                     │  internal/api  S3 前端       │
                     │  (对象/桶/列表/复制/分片上传)  │
                     └──────────────┬──────────────┘
                                    ▼
                     ┌─────────────────────────────┐
                     │  internal/tier 分层引擎      │
                     │  名字层 key→id + 内容层 id→池 │
                     │  SQLite 索引 + 去重 + 迁移    │
                     └──────┬──────────────┬───────┘
                            ▼              ▼
                  ┌──────────────┐  ┌──────────────┐
                  │ 热池 (local)  │  │ 冷池 (s3×N)   │
                  │  所有写入落点  │  │  迁移目标      │
                  └──────────────┘  └──────────────┘
```

**两层模型**(镜像 S3 对象模型,见 `internal/tier` 包注释):

- **objects(名字层)**:`bucket/key` → 内容 id(sha256)+ 每 key 元数据。多个 key 可以引用同一 id
- **resources(内容层)**:`id` → 物理持有字节的池、引用计数、大小、etag。每个 id 在全部池里至多存在一份

池只按内容 id 存字节,**只有名字层知道名字**。这带来两个设计后果:

- S3 冷池必须用 prefix 模式(单一远程 bucket 存所有 id),per-bucket 模式与全局去重冲突(启动校验会拒绝)
- 索引丢失重建后内容层可从池列表恢复,但名字层(桶/key 名)不可恢复——`Rebuild()` 会打日志声明名字层丢失

## 组件

| 命令 | 作用 |
|---|---|
| `s3-proxy` | 主进程:对外 S3 API + 分层引擎(`cmd/s3-proxy`) |
| `s3-store` | 独立的本地存储池:把本地文件系统池暴露为 S3 兼容端点,用于池在另一台机器时(`cmd/s3-store`) |
| `s3-admin` | 控制 CLI:直接读写 `tier.db` 的 control/commands 表(状态、暂停/恢复、改策略、强制迁移/提升)(`cmd/s3-admin`) |

## 快速开始

构建:

```sh
go build ./cmd/s3-proxy ./cmd/s3-store ./cmd/s3-admin
```

配置(`cmd/s3-proxy/example.json` 是带 Cloudflare R2 冷池的完整示例):

```json
{
  "listen": "0.0.0.0:9000",
  "region": "us-east-1",
  "state_dir": "/var/lib/s3-proxy",
  "client_creds": [
    { "ak": "your-access-key", "sk": "your-secret-key" }
  ],
  "pools": [
    {
      "name": "local-hot",
      "backend": "local",
      "path": "/var/lib/s3-proxy/hot"
    },
    {
      "name": "r2-cold",
      "backend": "s3",
      "endpoint": "https://ACCOUNT_ID.r2.cloudflarestorage.com",
      "region": "auto",
      "bucket": "s3proxy-archive",
      "ak": "r2-access-key",
      "sk": "r2-secret-key"
    }
  ],
  "tiering": {
    "hot": ["local-hot"],
    "cold": ["r2-cold"],
    "cold_after": "720h",
    "scan_interval": "1h",
    "max_hot_bytes": "0",
    "promote_on_access": true
  }
}
```

运行:

```sh
s3-proxy -config config.json
```

客户端测一下(任意 S3 客户端):

```sh
aws --endpoint-url http://localhost:9000 s3 mb s3://mybucket
aws --endpoint-url http://localhost:9000 s3 cp big-file.tar s3://mybucket/
s3-admin state/tier.db status
```

## 配置参考

| 字段 | 说明 | 默认 |
|---|---|---|
| `listen` | HTTP 监听地址 | `0.0.0.0:9000` |
| `tls_cert` / `tls_key` | 启用 HTTPS(必须成对给) | 空 = 纯 HTTP |
| `region` | 签名区域 | `us-east-1` |
| `base_host` | 开启 virtual-host 寻址(如 `s3.example.com`,bucket 走 `mybucket.s3.example.com`) | 空 = 仅 path-style |
| `state_dir` | 索引 `tier.db` + 分片上传暂存(暂存超过 24h 自动清理) | `state` |
| `client_creds` | 客户端 ak/sk 列表 | 必填,至少一对 |

**pools**(可多个,名字唯一):

| 字段 | 说明 |
|---|---|
| `name` | 池名,被 `tiering.hot/cold` 引用 |
| `backend` | `local` 或 `s3` |
| `path` | local:数据目录 |
| `endpoint` / `region` / `bucket` / `ak` / `sk` | s3:远程服务配置;`bucket` 必填(prefix 模式) |
| `insecure` / `timeout` | s3:跳过 TLS 校验 / 每请求超时 |

**tiering**:

| 字段 | 说明 | 默认 |
|---|---|---|
| `hot` | 接收写入的池,**必须恰好一个** | — |
| `cold` | 迁移目标池(round-robin),零个或多个 | — |
| `cold_after` | 热池资源 idle 多久算冷 | `168h` |
| `scan_interval` | 迁移循环周期(也兼临时文件清理) | `1h` |
| `max_hot_bytes` | 热池字节配额,超出按最久未访问驱逐;`0` = 不限 | `0` |
| `promote_on_access` | 冷读是否自动提升回热池 | `false` |

## 工作原理

- **写入**:字节流式写入热池临时 key 并同时算 sha256(带 `Content-MD5` 时先校验,不匹配删除临时字节并拒绝,不碰任何已存在状态)→ 已存在(id 命中)则丢弃临时字节、refcount++(同内容重传/自拷贝 net zero,不膨胀计数);新内容则重命名为内容 id、建资源行。新注册完成后,在**旧内容的锁下**释放旧映射引用,refcount 归零的内容从所有池清除
- **读取**:名字层解析出内容 id → 持有池直接服务 Range 请求 → 记录访问时间(去重内容取所有别名 key 访问的最大值)。池 miss 时自动探测其他池"自愈"(迁移中途崩溃的恢复路径)
- **迁移**:`RunOnce` 按资源粒度评估——idle 超过 `cold_after` 或超出热池配额的,按最久未访问排序迁往冷池(round-robin 选目标);写入热池的临时残留定期清理;`refs=0` 的孤儿字节(崩溃窗口遗留)由周期扫描自动回收
- **一致性**:名字/内容两级细粒度锁,任何释放/迁移/sweep 都在对应内容锁下进行并锁下 re-check——sweep 与并发重注册互斥,不可能误删刚注册的新字节;启动时按名字层重建引用计数(修复崩溃窗口残留的幻影引用);传输先拷贝后翻转索引再删源,读者永远读到完整字节

## 外部控制平面

代理**没有 HTTP 管理端口**。`state_dir/tier.db`(SQLite)就是全部状态和控制面:

- 查询随便读:数据表是内存状态的实时镜像,`v_cold_status` 视图直接给出每个资源的 idle 秒数
- 控制只写两张约定表:`control(k,v)` 运行时策略覆盖,`commands(seq,verb,arg)` 一次性强制操作;写完 `touch tier.db.ctl` 哨兵文件,代理毫秒级消费(兜底 60 秒 stat)
- 现成工具 `s3-admin`:`status` / `pause` / `resume` / `set --cold-after= --max-hot-bytes=` / `migrate` / `promote`

> 警告:不要直接 UPDATE/DELETE/INSERT `objects` / `resources` / `buckets` 表——代理内存 map 才是权威,任何这类改动会被下一笔业务写入静默覆盖。

详细协议见 **[CONTROL.md](CONTROL.md)**。

## 开发

```sh
go test -race -count=1 ./...
go vet ./...
```

回归测试约定:每个防御性/回归测试的 doc comment 都标注「发现背景」(Discovery background)——bug 如何被发现、修复方式是什么,见 `internal/tier/regression_test.go` 与 `internal/api/regression_test.go`。

CI(`.github/workflows/ci.yml`)在每个 push/PR 上跑 gofmt 校验、vet、`-race` 测试和构建;打 `v*` 标签触发 Release 工作流,产出 linux(amd64/arm64)、windows(amd64)、darwin(arm64) 的 s3-proxy 与 s3-store 二进制。

## 已知限制

- 索引丢失重建后名字层不可恢复(池内字节保留、标记 refs=-1 不会被误删,但桶/key 名无法还原)——**建议定期备份 `state_dir`** 中的 `tier.db`
- 无对象版本(覆盖即释放旧内容,最后一次引用消失即删除)
