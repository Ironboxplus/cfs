# cfs status

Last full runs: aspect 7/7, core 28/28 (own run after the last fix), frontend/rclone 6/6, internal/testutil 2/2, test 10/11 + 1 skipped (the WinFsp mount test). One earlier full run had a single failure, TestRandomKills, when a random kill came before the writer logged anything and the test treated the missing events file as an error; it now means "nothing acknowledged" (fixed, 8-round run passes).
Tests use no fakes: local directories, `cfs serve webdav` subprocesses over real HTTP, a real TCP fault proxy, real process kills (`CFS_CRASHPOINT`, taskkill).

## Done

- C1 store/aspect/metalog/core/frontend skeleton: Put/Get/List/Stat/Remove/Move/DirMove/Mkdir/Rmdir/SetModTime, small files in zip packs with a SQLite member index, large files 1:1 at the real path, hysteresis at 0.5 x threshold.
- C2 durability: small Put returns after pack + log segment are on the drive (group commit, pipelined); metadata ops ack after a local SQLite fsync and commit asynchronously; recovery from local state, from the drive log (`catchUp`), from checkpoints, and from pack manifests only; lease/epoch fencing.
- C3 maintenance: compaction (CAS records), GC with grace, checkpoints, `fsck` (deep/repair), `RemoveAll` via `rmtree` + `Store.Purge`.
- C4 rclone front end (`backend cfs`, `cmd/cfs`): fstests over WebDAV, VFS model test (rclone mount/serve code path), backend commands fsck/checkpoint/maintain/flush/stats, `CFS_STATS_FILE`.
- Shared instance API for in-process front ends: `Acquire/Lookup/Release`, `Shared.NewFs` (one Volume per key per process).

## Concurrency (decision, replaces "max_inflight=1 for everything")

Two classes in `aspect.Gate`:
- metadata (List/Stat/Remove/Move/DirMove/Mkdir/Rmdir/SetModTime/Hash/Purge): `meta_inflight`, default 1;
- transfer (Put and ranged reads): `transfers`, default 8. A slot is held until Put returns or the reader reaches EOF/Close, never while only waiting for a commit.
Files are independent of each other, group commit is pipelined (`Pipeline = transfers`), parts of ONE big file are sequential. No second QPS limiter in cfs: API QPS belongs to the OpenList driver layer.
`aspect.Meter` records calls, bytes and peak in-flight per class; tests assert `PeakMeta <= meta_inflight`, `PeakTransfer <= transfers`, and `TestTransfersOverlap` proves peak > 1 with transfers=8.

## Test runs (this machine)

| suite | result | notes |
| --- | --- | --- |
| aspect | 7/7 | gate classes, retry (transient, give-up, landed upload, idempotent mutations), redact |
| core | 28/28 | basic, metadata, reopen/rebuild, overlap, crash points + random kills, faulty drive, lease/fence, corruption, RemoveAll, cancelled put, model |
| core model `CFS_MODEL_OPS=30000` | pass, 581s | put 7546, append 2894, rename 5083, remove 3868, truncate 2123, dirmove 69, rmdir 227, maintain 60, rebuild 60, compacted packs 745, packs deleted 3112 |
| frontend/rclone | 6/6 | rclone fstests over cfs on WebDAV, VFS model (3 thresholds, 400 steps), shared-volume test |
| internal/testutil | 2/2 | the fault proxy does what it says |
| test (black box) | 7/7 | dataset copy, threshold edges, server crash, faulty drive, 1 GiB and 4 GiB through `cfs serve webdav` |
| 1 GiB via WebDAV | pass | PUT returned 3.4 s, on the drive 11.6 s, download+verify 2.5 s; underlying 21 calls, 1073742169 B written, peak transfer 2 |
| 4 GiB via WebDAV | pass | PUT returned 16.7 s, on the drive 45.8 s, download+verify 15.0 s; underlying 37 calls, 4294968561 B written, peak transfer 2 |

## Bugs found by the tests and fixed in this stretch

- A Put whose context ended could still be committed afterwards (rclone VFS then lost track of the file and a deleted file reappeared after a directory rename). Now a member is either taken along by the commit or cancelled (CAS); if the commit already took it, the write is taken back (`undoPut`, restores the previous version) before Put returns the error. Test: `TestCancelledPutLeavesNoTrace` (fails without the fix). Large-file Puts cancelled after the upload finished are not taken back.
- rclone's WebDAV client deletes an upload that errored; the retry test now also drops that DELETE to model a dead network.
- Redact leaked `Authorization: Bearer`; lease JSON read while rewritten; concurrent `VACUUM INTO`; `Close` racing metadata timers.

## Known gaps and risks

- Metadata ops are acknowledged after a local fsync and reach the drive asynchronously: losing the local state directory loses the most recent renames/deletes (not the data). Rebuilding from pack manifests alone loses deletes/renames.
- The lease has no compare-and-swap on the drive; two writers racing at the same instant could both believe they hold it (fencing by epoch catches it on the next segment).
- Case-insensitive underlying stores are not handled.
- Reading a small file costs one windowed (512 KiB) range request.
- rclone VFS quirks, reproduced with rclone's own local backend and not cfs bugs: directory rename loses the destination's siblings from VFS memory; rename over an existing file serves stale cached bytes; a half-written file may be re-uploaded as a prefix after a crash. rclone 1.75 `--vfs-handle-caching` (grace period) makes a directory rename inside the grace fail the delayed close; the model test sets it to 0.
- Windows: a TempDir cleanup flake (index.db still open) was seen occasionally; `removeAll` retries.
- Time-dependent tests were run 8-way parallel to expose races; `TestVFSModel` passes 8/8 under that load.

## C5: OpenList adapters (OP-sync, branch feat/sync)

Thin adapters only; cfs itself has no OpenList dependency.

- `drivers/cfs` (modelled on `drivers/crypt`): storage "Cfs" with `remote_path` (mount path of a folder of another storage), `state_dir`, `threshold`, `meta_inflight`, `transfers`, `seal_quiet`, `seal_max`, `dir_cache`, `gc_grace`. The underlying storage is reached with `internal/rclonefs` (so OpenList's credentials, caches and rate limiter apply), wrapped by `store/rclonestore`. Big files are cached by OpenList (`CacheFullAndWriter`) before the 1:1 upload so a failed upload can be repeated. `Copy` is left to OpenList.
- `cmd/cfs.go`: `openlist cfs mount <storage-mount-path> <mount-point>` and `openlist cfs serve webdav <storage-mount-path> --addr ...`. Both start the whole server (volume has one writer) and then attach rclone's VFS to the volume the storage already opened: ONE instance, shared through `frontend/rclone.Acquire/Lookup`. `mount` needs `-tags cmount` (and WinFsp on Windows); without it the command says so and exits 2. `serve webdav` is the same VFS layer without WinFsp.
- `go.mod`: rclone v1.70.3 -> v1.75.1, go 1.27, `replace github.com/Ironboxplus/cfs => ../cfs`. `go mod tidy` raised many other modules (see the OP-sync commit). One OP-sync file touched besides go.mod/go.sum: one import line in `drivers/all.go`.
- Driver conflict found and fixed: cfs used to register the `sqlite` database/sql driver itself, which panics next to OpenList's glebarez/sqlite. `core` no longer imports a driver; programs that have none (cmd/cfs, tests) import `github.com/Ironboxplus/cfs/sqlitedrv`.
- Links cached by OpenList outlive an edited storage; the driver reads from whatever volume is registered for the mount path when the link is used.

### C5 tests

OP-sync (`CGO_ENABLED=0`; with cgo the repo's `internal/fuse` needs WinFsp headers, as before):
- `go test ./internal/rclonefs/... ./internal/syncjob/... ./drivers/crypt/... ./drivers/cfs/...` ok (crypt has no tests; drivers/cfs: 2/2: storage on a Local storage with pack/1:1 layout check, rename, move, dir move, remove, drop and re-create the storage; refusing a remote_path inside itself).
- `go build ./...` ok, `GOOS=windows go build ./...` ok. `GOOS=linux go build ./...` fails with CGO_ENABLED=0 in cgofuse (needs cgo on linux; unrelated to cfs).

cfs repo, against a real OpenList process (`CFS_OP_BIN=<openlist.exe>`; scratch in TempDir, port from a free-port probe, never 5244):
- `TestOpenListCfsDataset`: the 1564-file OP-sync copy (389 743 803 B) through OpenList WebDAV, 12 parallel uploads; the underlying Local storage ends with 131 packs, 131 log segments and 6 large files; modify/grow/shrink/delete/dir rename; kill -9 and restart; delete the local index and rebuild from the drive; every step verified by SHA-256 against a local reference. Peaks: meta 1, transfer 7 (limit 8).
- `TestOpenListCfsOneInstanceForTwoFrontEnds`: OpenList's own WebDAV and `openlist cfs serve webdav` in one process; files written by either are read by the other (small and 9 MiB).
- `TestOpenListCfsOnWebDavStorage`: the volume on OpenList's "WebDav" driver through the fault proxy (503s, dropped connections on PUT and PROPFIND): 120 files verified, then after kill -9. With `CFS_OP_UNDER_URL`, `CFS_OP_UNDER_USER`, `CFS_OP_UNDER_PASS` (and optional `CFS_OP_UNDER_DIR`, default /cfs-test) it targets a real remote WebDAV instead and injects nothing.
- `TestOpenListCfsMount`: skipped here (no WinFsp, no cmount build); runs with `CFS_OP_MOUNT_BIN` and `CFS_OP_MOUNT_POINT` set when WinFsp/FUSE exists.

### For C6 (real 115, not done here)

Pointing the C5 instance at 115 through an OpenList storage needs no cfs change: create the 115 storage in OpenList (mount e.g. /115), then a Cfs storage with `remote_path` `/115/cfs-volume`. For the black-box suite use an OpenList WebDAV storage in front of it (OpenList's own `/dav`), or set the three `CFS_OP_UNDER_*` variables above to a WebDAV endpoint (for example an OpenList instance's `/dav`) and run `CFS_OP_BIN=... go test ./test -run TestOpenListCfsOnWebDavStorage -v`. Not tried: real drive latency, 115 rapid-upload interplay with packs, real API rate limits.

### Known C5 gaps

- The mount itself (WinFsp) was not run on this machine; only the VFS layer over WebDAV was.
- rclone logs `internal error: no overview data found for "cfs"` once at start (rclone wants docs metadata for every registered backend); harmless.
- The dataset test lets OpenList's Local storage stand in for a drive: it proves the plumbing and the layout, not drive behaviour.
