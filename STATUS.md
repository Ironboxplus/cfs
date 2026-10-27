# cfs status

Last full run: all packages `ok` (aspect 7/7, core 28/28, frontend/rclone 6/6 + shared, internal/testutil 2/2, test 7/7 top-level tests).
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
