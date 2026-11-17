<p align="center">
  <img src="docs/logo.svg" width="128" alt="cfs logo">
</p>

<h1 align="center">cfs</h1>

<p align="center">
  <b>为接口调用受限的网盘设计的文件系统。</b><br>
  小文件打包上传，大文件原样直传，<br>
  一批改动只需几次上传就能落到网盘上。
</p>

<p align="center">
  <a href="https://github.com/Ironboxplus/cfs/actions/workflows/test.yml"><img src="https://github.com/Ironboxplus/cfs/actions/workflows/test.yml/badge.svg?branch=main" alt="test"></a>
  <img src="https://img.shields.io/badge/go-1.27-00ADD8?logo=go&logoColor=white" alt="Go 1.27">
  <img src="https://img.shields.io/badge/built%20on-rclone-3F79AD" alt="built on rclone">
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-2B7A57" alt="MIT"></a>
</p>

<p align="center"><a href="README.md">English</a> | 中文</p>

---

## 为什么需要 cfs

115、百度网盘、阿里云盘这类网盘给的空间以 TB 计，给的接口调用却很少：列目录有频控，上传有限速，每个文件都要花好几次调用（查路径、初始化上传、传数据、确认）。一个装着 5 万篇笔记、源码或缩略图的文件夹，上传要走 5 万遍这套流程，浏览时每个目录还要列一次，账号就是在这里撞上风控的。

cfs 在这样的网盘上建一个卷，把接口调用当作最稀缺的资源来花：

- **小文件打包。** 小于 8 MiB 的文件写进最大 128 MiB 的 zip 包，一批写入变成一个包加一段日志。
- **大文件直传。** 8 MiB 及以上的文件按原路径一对一存放，读取时按范围请求直接转给网盘。
- **调用有预算。** 元数据调用同时只发 1 个，传输同时最多 8 个；小文件的目录列表来自本地索引。
- **数据用公开格式。** 包是 zip，日志是 gzip 压缩的 JSON Lines，检查点是 SQLite 数据库。

## 省下多少

同样 1564 个文件写进卷里，网盘上产生的对象数：

| 写入方式 | 网盘上的对象数 |
| --- | ---: |
| 直接复制 | 1564 |
| cfs，`ack=durable` | 270（132 个包、132 段日志、6 个大文件） |
| cfs，`ack=staged`（默认） | **8**（1 个包、1 段日志、6 个大文件） |

<sub>1564 个文件共 389,743,803 字节，其中 6 个大文件；经 OpenList 的 WebDAV 由 12 路并发上传写入，用本地目录代替网盘（`TestOpenListCfsDataset`，2026-10-28）。3 路并发时 `staged` 同样是 1 个包、1 段日志。</sub>

## 工作方式

### 写入

1. 小文件先落盘到本地暂存区（`state_dir`），写入随即返回。
2. 30 秒内没有新的小文件、距第一个文件满 10 分钟，或攒够 128 MiB 或 1 万个文件时，暂存的文件封成一个包。
3. 包上传到 `.cfs/packs/`，随后上传一段日志，记下每个文件在哪个包的哪个位置。

改一个文件，会把它完整地再写进后面的包；旧包里那份成为废数据，由维护任务回收。

### 读取

- **包内的小文件：** 对它所在的 512 KiB 窗口发一次范围请求，同一窗口里的相邻文件一并取回。
- **大文件：** 范围请求原样转给网盘。
- **列目录和 `stat`：** 小文件来自本地 SQLite 索引，大文件来自网盘的目录列表，列表在 `dir_cache`（5 分钟）内有效。

### 网盘上的布局

```text
cfs-volume/
├── .cfs/
│   ├── packs/  p-000001-0000000001.zip      小文件包，内含 .cfs-manifest.json
│   ├── log/    000001-0000000001.jsonl.gz   每次提交一段日志
│   ├── ckpt/   000001-0000000010.db.gz      索引的 SQLite 快照
│   └── lease/                               当前由哪台机器写入
└── videos/
    └── talk.mp4                             大文件放在原路径
```

包里每个成员的名字是文件被打包时的路径，之后的改名和删除都记在日志里。任何 zip 工具都能打开这些包。

### 多台机器

同一个卷可以被任意多台机器打开，列目录和读取不往网盘写任何东西。第一次改动时取得写入租约，写入方空闲 10 分钟后交还租约，于是各台机器轮流写入。只读的机器每隔 `dir_cache`（5 分钟）追一次日志；`ack=staged` 时写入方把已上传的包攒成一批再写日志，最晚在第一个包上传后 `log_batch_age`（10 分钟）写出。

### 恢复

本地索引是网盘内容的缓存。删掉 `state_dir`，cfs 会从最新的检查点和日志重建索引，也能只靠各个包里的清单重建。`ack=staged` 时，已确认的小文件在所在的包上传之前只存在本地磁盘上；`ack=durable` 在包上传到网盘之后才确认写入。

## 快速开始

### 编译

```sh
git clone https://github.com/Ironboxplus/cfs
cd cfs
go build ./cmd/cfs        # Go 1.27，无需 cgo
```

`cfs` 就是内置了 cfs 后端的 rclone，所有 rclone 命令都能用在 cfs 远端上。

### 配置

在 `rclone.conf` 里加上网盘和建在它上面的 cfs 卷。下面的例子通过 OpenList 的 WebDAV 访问 115：

```ini
[drive]
type = webdav
url = http://127.0.0.1:5244/dav
vendor = other
user = admin
pass = <rclone obscure 的输出>

[vol]
type = cfs
remote = drive:115/cfs-volume
```

### 使用

```sh
cfs copy ~/notes vol:notes                       # 上传一个文件夹
cfs serve webdav vol: --addr 127.0.0.1:8080      # 用 WebDAV 浏览和编辑
cfs mount vol: /mnt/vol --vfs-cache-mode full    # 挂载（即 rclone mount）

cfs backend maintain vol:                        # 压缩包、写检查点、清理
cfs backend fsck vol: -o deep=true               # 校验每个包内文件的 SHA-1
cfs backend stats vol:                           # 调用次数、字节数、并发峰值
```

### 在 OpenList 中使用

OpenList 分支 [Ironboxplus/OpenList](https://github.com/Ironboxplus/OpenList) 带有 **Cfs** 存储。把 `remote_path` 设为另一个存储里的文件夹，例如 `/115/cfs`，这个卷就作为普通存储出现，可以用 WebDAV、分享和预览。`openlist cfs mount` 和 `openlist cfs serve webdav` 把 rclone 的 VFS 挂到同一个卷上。

### 作为 Go 库

```go
import (
    "github.com/Ironboxplus/cfs/aspect"
    "github.com/Ironboxplus/cfs/core"
    "github.com/Ironboxplus/cfs/store/rclonestore"
    _ "github.com/Ironboxplus/cfs/sqlitedrv"
)

st, _ := aspect.Stack(rclonestore.New(f), aspect.Options{}) // f：任意 rclone fs.Fs
vol, err := core.Open(ctx, st, core.Config{StateDir: "/var/lib/cfs/vol"})
defer vol.Close(ctx)

vol.Put(ctx, "notes/a.md", r, size, mtime)
rc, err := vol.Open(ctx, "notes/a.md", 0, -1)
```

## 常用设置

`cfs help backend cfs` 列出全部选项。

| 选项 | 默认值 | 含义 |
| --- | --- | --- |
| `threshold` | 8 MiB | 小于它的文件打包，其余一对一存放。 |
| `pack_max_bytes` | 128 MiB | 包达到这个大小（或 1 万个文件）时封包。 |
| `ack` | `staged` | `staged`：落到本地磁盘即返回。`durable`：所在的包上传到网盘后返回。 |
| `seal_quiet` / `seal_max` | 30 秒 / 10 分钟 | 安静这么久后封包 / 第一个文件进包这么久后封包。 |
| `meta_inflight` | 1 | 同时进行的元数据调用数。 |
| `transfers` | 8 | 同时进行的上传数；读取另有 `read_transfers`。 |
| `dir_cache` | 5 分钟 | 网盘目录列表的有效期。 |
| `lease_idle` | 10 分钟 | 写入方空闲这么久后交还租约。 |
| `gc_grace` | 24 小时 | 不再被引用的包和旧日志保留多久。 |

## 维护

`cfs backend maintain` 重写废数据占一半以上的包，上传检查点，并在 24 小时宽限期过后删除不再被引用的内容。在负责写入的机器上用 cron 定期运行。

## 测试

每次推送都在 Linux 和 Windows 上跑 CI：

- 两种 `ack` 模式下的崩溃点和随机杀进程，重启后逐一核对每个已确认的写入是否在网盘上；
- TCP 故障代理在上传和读取中途切断连接；
- 模型测试对随机操作序列与参照模型逐步比对（长任务跑 3 万次操作）；
- 通过 `cfs serve webdav` 跑 rclone 自带的 fstests 和 VFS 测试；
- 经由真实 OpenList 进程的黑盒测试。

## 路线图

- **自动维护：** 在调用预算内后台运行，rclone 和 OpenList 都支持。
- **按目录装包：** 一个文件夹用一个窗口就能读完。
- **快照：** 浏览卷在过去某一时刻的样子。日志和从不修改的包已经保存了这段历史，直到被 GC 清理。
- **预编译二进制：** Linux、macOS、Windows。

## 基于

[rclone](https://github.com/rclone/rclone)（使用修复了 VFS 改名问题的[分支](https://github.com/Ironboxplus/rclone)）和 [modernc.org/sqlite](https://gitlab.com/cznic/sqlite)。包、锁和 prune 的设计参考 [restic](https://github.com/restic/restic)，压缩以及上传、下载分开限流参考 [JuiceFS](https://github.com/juicedata/juicefs)。

## 许可证

[MIT](LICENSE)
