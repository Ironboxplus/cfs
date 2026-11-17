<p align="center">
  <img src="docs/logo.svg" width="128" alt="cfs logo">
</p>

<h1 align="center">cfs</h1>

<p align="center">
  <b>A filesystem for cloud drives that ration API calls.</b><br>
  Small files travel in packs, large files go straight through,<br>
  and a burst of changes reaches the drive as a handful of uploads.
</p>

<p align="center">
  <a href="https://github.com/Ironboxplus/cfs/actions/workflows/test.yml"><img src="https://github.com/Ironboxplus/cfs/actions/workflows/test.yml/badge.svg?branch=main" alt="test"></a>
  <img src="https://img.shields.io/badge/go-1.27-00ADD8?logo=go&logoColor=white" alt="Go 1.27">
  <img src="https://img.shields.io/badge/built%20on-rclone-3F79AD" alt="built on rclone">
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-2B7A57" alt="MIT"></a>
</p>

<p align="center">English | <a href="README.zh-CN.md">中文</a></p>

---

## Why

Consumer cloud drives such as 115, Baidu Netdisk and Aliyun Drive give you terabytes of space and very few API calls. They throttle listings, rate-limit uploads, and charge every file several calls: look up the path, start the upload, send the bytes, confirm. A folder of 50,000 notes, source files or thumbnails costs 50,000 of those sequences to upload and one listing per directory to browse, and that is where accounts run into the throttle.

cfs keeps a volume on such a drive and spends API calls as the scarce resource:

- **Small files are packed.** Files under 8 MiB are written into zip packs of up to 128 MiB, so a burst of writes becomes one pack and one log segment.
- **Large files go straight through.** Files of 8 MiB and more are stored 1:1 at their own path, and reads are forwarded as ranged requests.
- **Calls are budgeted.** Metadata calls run one at a time and transfers eight at a time, and small files are listed from a local index.
- **Data stays in open formats.** Packs are zip files, the log is gzipped JSON lines, and checkpoints are SQLite databases.

## What it saves

The same 1564 files written to a volume, counted as objects on the drive:

| Written as | Objects on the drive |
| --- | ---: |
| plain copy | 1564 |
| cfs, `ack=durable` | 270 &nbsp;(132 packs, 132 log segments, 6 large files) |
| cfs, `ack=staged` (default) | **8** &nbsp;(1 pack, 1 log segment, 6 large files) |

<sub>1564 files, 389,743,803 bytes, 6 of them large, written through OpenList's WebDAV by 12 parallel uploaders; a local directory stood in for the drive (`TestOpenListCfsDataset`, 2026-10-28). With 3 uploaders `staged` also produced 1 pack and 1 segment.</sub>

## How it works

### Writing

1. A small file is fsynced into the local staging area (`state_dir`) and the write returns.
2. After 30 s without a new small file, 10 min after the first one, or when 128 MiB or 10,000 files have gathered, the staged files are sealed into one pack.
3. The pack is uploaded to `.cfs/packs/`, followed by a log segment that records where each file lives.

Changing a file writes it again in full into a later pack. Its copy in the older pack becomes dead space, which maintenance reclaims.

### Reading

- **Packed file:** one ranged request for the 512 KiB window around it; neighbours in the same window come with it.
- **Large file:** the ranged request goes to the drive as it is.
- **Listings and `stat`:** small files come from the local SQLite index, large files from a listing of the drive that is trusted for `dir_cache` (5 min).

### On the drive

```text
cfs-volume/
├── .cfs/
│   ├── packs/  p-000001-0000000001.zip      small files, plus .cfs-manifest.json
│   ├── log/    000001-0000000001.jsonl.gz   one segment per commit
│   ├── ckpt/   000001-0000000010.db.gz      SQLite snapshot of the index
│   └── lease/                               which machine is writing
└── videos/
    └── talk.mp4                             large files at their own path
```

A pack member is named by the path its file had when it was packed, and the log records every later rename and removal. Any zip tool opens the packs.

### Several machines

Any number of machines can open the same volume. Listing and reading write nothing to the drive. The first change takes the writer lease, and the writer hands it back after 10 minutes without changes, so machines take turns writing. Readers catch up with the log every `dir_cache` (5 min). With `ack=staged` the writer logs its uploaded packs in batches, at the latest `log_batch_age` (10 min) after the first of them.

### Recovery

The local index is a cache of the drive. Delete `state_dir` and cfs rebuilds it from the newest checkpoint and the log, or from the pack manifests alone. With `ack=staged` an acknowledged small file lives on the local disk until its pack is uploaded; `ack=durable` acknowledges a write once its pack is on the drive.

## Getting started

### Build

```sh
git clone https://github.com/Ironboxplus/cfs
cd cfs
go build ./cmd/cfs        # Go 1.27, no cgo
```

`cfs` is rclone with the cfs backend built in, so every rclone command works on cfs remotes.

### Configure

Add the drive and a cfs volume on it to `rclone.conf`. Here the drive is 115, reached through OpenList's WebDAV:

```ini
[drive]
type = webdav
url = http://127.0.0.1:5244/dav
vendor = other
user = admin
pass = <rclone obscure output>

[vol]
type = cfs
remote = drive:115/cfs-volume
```

### Use

```sh
cfs copy ~/notes vol:notes                       # upload a folder
cfs serve webdav vol: --addr 127.0.0.1:8080      # browse and edit over WebDAV
cfs mount vol: /mnt/vol --vfs-cache-mode full    # mount it (rclone mount)

cfs backend maintain vol:                        # compact packs, checkpoint, clean up
cfs backend fsck vol: -o deep=true               # verify every packed file's SHA-1
cfs backend stats vol:                           # calls, bytes, in-flight peaks
```

### In OpenList

The OpenList fork [Ironboxplus/OpenList](https://github.com/Ironboxplus/OpenList) has a **Cfs** storage. Set `remote_path` to a folder of another storage, for example `/115/cfs`, and the volume appears as an ordinary storage with WebDAV, sharing and previews. `openlist cfs mount` and `openlist cfs serve webdav` attach rclone's VFS to the same volume.

### As a Go library

```go
import (
    "github.com/Ironboxplus/cfs/aspect"
    "github.com/Ironboxplus/cfs/core"
    "github.com/Ironboxplus/cfs/store/rclonestore"
    _ "github.com/Ironboxplus/cfs/sqlitedrv"
)

st, _ := aspect.Stack(rclonestore.New(f), aspect.Options{}) // f: any rclone fs.Fs
vol, err := core.Open(ctx, st, core.Config{StateDir: "/var/lib/cfs/vol"})
defer vol.Close(ctx)

vol.Put(ctx, "notes/a.md", r, size, mtime)
rc, err := vol.Open(ctx, "notes/a.md", 0, -1)
```

## Settings

The ones most often changed; `cfs help backend cfs` lists all of them.

| Option | Default | Meaning |
| --- | --- | --- |
| `threshold` | 8 MiB | Files below this are packed; others are stored 1:1. |
| `pack_max_bytes` | 128 MiB | A pack is sealed at this size (or at 10,000 files). |
| `ack` | `staged` | `staged`: a write returns once it is on the local disk. `durable`: once its pack is on the drive. |
| `seal_quiet` / `seal_max` | 30 s / 10 min | Seal the open pack after this much quiet / this long after its first file. |
| `meta_inflight` | 1 | Simultaneous metadata calls to the drive. |
| `transfers` | 8 | Simultaneous uploads; reads have their own `read_transfers`. |
| `dir_cache` | 5 min | How long listings of the drive are trusted. |
| `lease_idle` | 10 min | An idle writer hands the lease back after this long. |
| `gc_grace` | 24 h | How long unreferenced packs and old log segments are kept. |

## Maintenance

`cfs backend maintain` rewrites the packs that are at least half dead space, uploads a checkpoint, and deletes what nothing refers to once the 24 h grace period has passed. Run it from cron on the machine that writes.

## Tests

CI runs on Linux and Windows on every push:

- crash points and random process kills in both `ack` modes, with every acknowledged write checked on the drive after the restart;
- a TCP fault proxy that cuts connections in the middle of uploads and reads;
- a model test that runs random operations against a reference model (30,000 operations in the long job);
- rclone's own fstests and VFS tests over `cfs serve webdav`;
- black-box runs through a real OpenList process.

## Roadmap

- **Automatic maintenance** within the call budget, in rclone and in OpenList.
- **Packing by directory**, so a folder reads in one window.
- **Snapshots:** browse the volume as it was at an earlier time. The log and the packs, which are never modified, already hold that history until GC.
- **Release binaries** for Linux, macOS and Windows.

## Built on

[rclone](https://github.com/rclone/rclone) (a [fork](https://github.com/Ironboxplus/rclone) with VFS rename fixes) and [modernc.org/sqlite](https://gitlab.com/cznic/sqlite). The design follows [restic](https://github.com/restic/restic) for packs, locks and prune, and [JuiceFS](https://github.com/juicedata/juicefs) for compaction and separate upload and download limits.

## License

[MIT](LICENSE)
