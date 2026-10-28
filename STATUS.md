# cfs status

Rework round 2 (2026-10-28): default `ack=staged`, segment-upload failures no longer reorder metadata, rclone fork verified, every suite re-run. Numbers in the tables below are from that run.
Tests use no fakes: local directories, `cfs serve webdav` subprocesses over real HTTP, a real TCP fault proxy, real process kills (`CFS_CRASHPOINT`, taskkill), a real OpenList process.

## Done

- C1 store/aspect/metalog/core/frontend skeleton: Put/Get/List/Stat/Remove/Move/DirMove/Mkdir/Rmdir/SetModTime, small files in zip packs with a SQLite member index, large files 1:1 at the real path, hysteresis at 0.5 x threshold.
- C2 durability: recovery from local state, from the drive log (`catchUp`), from checkpoints, and from pack manifests only; lease/epoch fencing. Metadata ops ack after a local SQLite fsync and commit asynchronously.
- C3 maintenance: compaction (CAS records), GC with grace, checkpoints, `fsck` (deep/repair), `RemoveAll` via `rmtree` + `Store.Purge`.
- C4 rclone front end (`backend cfs`, `cmd/cfs`): fstests over WebDAV, VFS model test, backend commands fsck/checkpoint/maintain/flush/stats, `CFS_STATS_FILE`.
- Shared instance API for in-process front ends: `Acquire/Lookup/Release`, `Shared.NewFs` (one Volume per key per process).
- **Round 2, item 1: `ack=staged` (default).** See "Staging" below.
- **Round 2, item 3: a failed log-segment upload never moves metadata records to a later sequence number.** See "Log segments".

## Ack modes (`Config.Ack`, rclone option `ack`, OpenList storage option `ack`)

- `staged` (default): a small Put fsyncs the bytes to `<state>/stage/<id>.dat`, records the file in the local SQLite `staged` table, and returns. Packing and the commit to the drive run in the background.
- `durable`: the old semantics. Put returns when the pack and the log segment describing it are on the drive. Files per pack are about the number of concurrent callers.

Large files (>= threshold) are not staged in either mode: Put returns after the 1:1 upload.

### Staging (what the code does)

- **Visibility.** Readers see committed rows overlaid with staged ones: `Stat`, `List` (also directories that only staged files live in), `Open`, `Hash`, shape checks. A read-only follower sees only what is on the drive.
- **Commit.** One packer goroutine feeds staged files, in acknowledgement order, into the existing pack pipeline (`Pipeline = transfers`). The local transaction that applies a commit's records also deletes its staged rows and files. Seal on `pack_max_bytes` / `pack_max_files` / `seal_max`, or on `seal_quiet` of silence.
- **`seal_quiet` / `seal_max` defaults.** `ack=durable`: 2s / 15s (callers wait for the commit). `ack=staged`: 10s / 60s. Nobody waits for a pack in staged mode, so a 2s quiet period only cut tiny packs out of slow streams; 10s still ends a burst, 60s bounds how long a file waits (and how much a lost local disk could lose). `Flush`, `Close`, any metadata operation on a staged name, and a full staging area seal at once. Explicit settings win.
- **Restart.** Open replays the staged rows (and drops stage files nothing refers to). Verified by kill tests: every acknowledged write ends up on the drive with the right content (`TestStagedReplayReachesTheDrive`, crash points in both modes).
- **Backpressure.** `stage_max_bytes` (default 1 GiB): Put blocks, never errors, while the area is full, and asks for a seal so it drains (`TestStagedBackpressure`).
- **Failed commit.** The staged files go back to the queue after a back-off (250 ms doubling to 10 s). `Flush` and metadata operations that need those files report the failure instead of hanging. `Close` with the drive down returns the error and leaves the staging area for the next start.
- **Same name twice.** A newer Put replaces the staged version; an older version already in a sealed pack is committed first, so two versions of a name always reach the log in the order they were written.
- **Metadata operations on staged names** (Remove, Move, DirMove, RemoveAll, SetModTime, a large Put of the same name) first force those files out to the drive. Cost: a rename right after a write seals the open pack early (many such renames make small packs). Correct, not free.
- **The one thing staged mode can lose:** the local disk dying before the upload. Acknowledged = fsynced on the local disk. A fenced writer (another writer took over) keeps its acknowledged files only locally and refuses new Puts.

### Log segments (item 3)

- `sealPending` reserves the sequence number and seals the queued metadata rows in one transaction, before the pack is uploaded. (Before: a flush running between a commit's sequence allocation and its sealing could put an older row into a higher-numbered segment.)
- If a commit fails, its rows stay under that segment name and the metadata flush uploads them under it. `appendSegment` retries the identical segment up to 4 times and asks the drive (list + read-back, compared record by record) whether it landed.
- `TestSegmentFailureKeepsMetadataOrder` (real fault proxy: the segment PUT lands, the connection drops, probes of that name answer 404 so the volume can't confirm) runs in both modes, and fails with the old `unseal` behaviour (log_mismatch: "remove x" replayed after "put x") and passes now.

## Concurrency (unchanged)

Two classes in `aspect.Gate`: metadata (List/Stat/Remove/Move/DirMove/Mkdir/Rmdir/SetModTime/Hash/Purge), `meta_inflight` default 1; transfer (Put and ranged reads), `transfers` default 8. A slot is never held while only waiting for a commit; a staged Put holds none at all. Files are independent of each other, group commit is pipelined, parts of ONE big file are sequential. No second QPS limiter in cfs. `aspect.Meter` records calls, bytes and peak in-flight per class; tests assert `PeakMeta <= meta_inflight`, `PeakTransfer <= transfers`, and `TestTransfersOverlap` proves peak > 1 with transfers=8 (its large files are now 4 MiB: staged small Puts no longer slow the callers down enough to overlap by themselves).

## Test runs (this machine, 2026-10-28)

| suite | result | notes |
| --- | --- | --- |
| aspect | 7/7 | |
| internal/testutil | 2/2 | the fault proxy |
| core, default (staged) | 37/37, 408s | basic, metadata, reopen/rebuild, overlap, crash points x2 modes, random kills x2 modes, staged replay, faulty drive, segment failure, lease/fence, corruption, RemoveAll, cancelled put, staged visibility/metadata/backpressure/drive-down/concurrent, pack efficiency, model |
| core, `CFS_TEST_ACK=durable` | 37/37, 407s | the same suite with every volume in durable mode (tests that need a mode set it themselves) |
| core model `CFS_MODEL_OPS=30000`, staged | pass, 660s | put 7546, append 2894, rename 5083, remove 3868, truncate 2123, dirmove 69, rmdir 227, maintain 60, rebuild 60, compacted packs 742, packs deleted 3265, peak in-flight meta 1 / transfer 1 |
| core model `CFS_MODEL_OPS=30000`, `CFS_TEST_ACK=durable` | pass, 910s | same step counts, compacted packs 745, packs deleted 3705 |
| core `-race` (staged tests, pack efficiency, segment failure, fault upload, cancelled put, metadata ops, transfers overlap) | 13/13 on the last 4 runs | one earlier batch run of this set had one failure of `TestPackEfficiency` whose output I did not capture; 11 reruns (3 batches, 8 alone) were clean. Not understood: treat as an open risk |
| frontend/rclone | 9/9, 124s | rclone fstests over cfs on WebDAV, VFS model (3 thresholds), atomic save, rename quirks, shared volume, VFS pack efficiency |
| test (black box + OpenList process, `CFS_OP_BIN`) | 11 pass, 1 skip (the WinFsp mount), 610s | dataset local/WebDAV, 1 GiB and 4 GiB through `cfs serve webdav`, server crash, faulty drive, OpenList dataset in 3 variants, two front ends, WebDav storage, API put in 2 modes. Peak in-flight in every one: meta 1, transfer <= 8 (largest seen: 8, in the faulty-drive run and the rebuilds) |
| OP-sync `internal/rclonefs`, `internal/syncjob`, `drivers/cfs` | 67/67 | |
| rclone fork `go test ./vfs/... ./lib/file/...` | all ok | vfs 29s, vfscache 20s, downloaders 13s, writeback 3s, vfscommon 1s, lib/file 1s |

### Pack and segment counts

OP-sync dataset (1564 files, 389 743 803 B, 6 large) through OpenList WebDAV into a Cfs storage on a Local storage:

| ack | uploaders | packs | log segments | large files |
| --- | --- | --- | --- | --- |
| staged | 3 | 1 | 1 | 6 |
| staged | 12 | 1 | 1 | 6 |
| durable | 12 | 132 | 132 | 6 |

(before this round: 131 / 131 with 12 uploaders.) Underlying calls after the upload: 546 staged vs 1140 durable (12 uploaders).

Other measurements (small files, random bytes, `pack_max_bytes` set so that bytes / pack size is the expected pack count):

| path | files | bytes / pack size | staged packs / segments | durable packs / segments |
| --- | --- | --- | --- | --- |
| core, 3 callers | 2000 | 9 | 9 / 9 | 667 / 667 |
| VFS (rclone write-back, 3 callers) | 1500 | 7 | 7 / 7 | 375 / 375 |
| OpenList `/api/fs/put`, 3 callers | 900 | 3 | 3 / 3 | 300 / 301 |

### rclone fork (item 2, verified, nothing to change)

- fork = v1.75.2 + upstream PRs #9574/#9975 + c6c227a3f (directory move keeps the destination's entry of the old name). cfs and OP-sync both `replace` rclone with `../rclone-fork`.
- `TestVFSAtomicSave` on unpatched v1.75.2 (a `-modfile` without the replace, kept outside the repo): FAILS on Windows for `local/write_back=5s` and `cfs/write_back=5s` ("read right after the rename" returns an empty file); the `write_back=0s` variants pass. With the fork: 4/4 pass, and `TestVFSRenameQuirks` 4/4.

## Bugs found by the tests and fixed this round

- A pending metadata row could land in a segment numbered below an older row (flush between a commit's sequence allocation and its sealing). Fixed by `sealPending`.
- A move onto an existing name (large over small, small over large) was several steps with a crash window that lost the target. Journaled (`mv:` keys) and completed at the next start.
- A large file shadowed by a packed file of the same name could survive a crash between the commit and its removal (found by random kills in staged mode). Journaled before the commit uploads anything (`stray:` keys).
- A large upload over a small file: a crash between upload and index change left the small file shadowing a loose one. Journaled (`putbig:` keys); an unacknowledged upload is taken back at the next start.
- A lease file truncated by a killed writer made the volume unopenable ("unreadable lease: EOF"). Now taken over like a stale lease.
- Directories that only staged files live in don't exist on the drive; every Put under one listed it again (507 lists for 2000 files). Negative listing cache.
- `Close` with a pack still filling and the drive down left its spool file open.

## Known gaps and risks

- **Staged mode: the local disk is the only copy until the upload.** Documented above; the staging area is capped at `stage_max_bytes`.
- Metadata ops are acknowledged after a local fsync and reach the drive asynchronously: losing the local state directory loses the most recent renames/deletes (not the data). Rebuilding from pack manifests alone loses deletes/renames.
- Renames/removes of freshly staged names commit their packs early (see Staging).
- A segment whose upload landed but was reported failed carries puts of a commit whose callers saw an error (durable) or whose files were re-queued (staged); the retry under the same name carries only the metadata rows and overwrites it. Until then a rebuild from the drive could show those puts.
- The crash model accepts a prefix of an unacknowledged large upload because rclone's local backend writes the final name in place (a property of the test drive; 115 and WebDAV servers show a file only when it is complete). Not proven for a real drive.
- The lease has no compare-and-swap on the drive; two writers racing at the same instant could both believe they hold it (fencing by epoch catches it on the next segment).
- Case-insensitive underlying stores are not handled.
- Reading a small file from a pack costs one windowed (512 KiB) range request.
- rclone VFS quirks, reproduced with rclone's own local backend and not cfs bugs: directory rename loses the destination's siblings from VFS memory (fixed in the fork); rename over an existing file served stale bytes on Windows (fixed in the fork). rclone 1.75 `--vfs-handle-caching` (grace period) makes a directory rename inside the grace fail the delayed close; the model test sets it to 0.
- Windows: test TempDir cleanup occasionally fails with "index.db in use" after a kill; a timed-out `go test` leaves `cfs serve webdav` subprocesses behind (found 7, killed by PID); `go build ./...` in OP-sync sometimes exits 1 only because Windows can't remove Go's temp exe directory ("unlinkat ... exe: directory not empty": three failures in a row, then a pass; `-work` runs exit 0). Not a code problem.
- One `-race` failure not understood (see the table).

## C5: OpenList adapters (OP-sync, branch feat/sync)

Thin adapters only; cfs itself has no OpenList dependency.

- `drivers/cfs` (modelled on `drivers/crypt`): storage "Cfs" with `remote_path`, `state_dir`, `threshold`, `meta_inflight`, `transfers`, `ack` (select staged/durable, default staged), `stage_max_bytes`, `pack_max_bytes`, `seal_quiet`, `seal_max` (empty = mode defaults), `dir_cache`, `gc_grace`. Big files are cached by OpenList (`CacheFullAndWriter`) before the 1:1 upload so a failed upload can be repeated. `Copy` is left to OpenList.
- `cmd/cfs.go`: `openlist cfs mount <storage-mount-path> <mount-point>` and `openlist cfs serve webdav <storage-mount-path> --addr ...`: both attach rclone's VFS to the volume the storage already opened (one instance, shared through `frontend/rclone.Acquire/Lookup`). `mount` needs `-tags cmount` (and WinFsp on Windows).
- `go.mod`: rclone v1.75.2 via `replace => ../rclone-fork`, `replace github.com/Ironboxplus/cfs => ../cfs`.
- OP-sync builds: `go build ./...` ok, `GOOS=windows go build ./...` ok (see the flake above), `CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -buildvcs=false .` ok. `./...` for linux fails in cgofuse (needs cgo), known.

### C5 tests

- OP-sync (`CGO_ENABLED=0`): `go test ./internal/rclonefs/... ./internal/syncjob/... ./drivers/cfs/...` 67/67. `drivers/cfs` flushes the volume before it looks at the packs.
- cfs repo against a real OpenList process (`CFS_OP_BIN=<openlist.exe>`; scratch in TempDir, port from a free-port probe, never 5244):
  - `TestOpenListCfsDataset`: the 1564-file OP-sync copy through OpenList WebDAV, 3 variants (staged 3 uploaders, staged 12, durable 12; counts above), then modify/grow/shrink/delete/dir rename, kill -9 and restart, delete the local index and rebuild from the drive, every step verified by SHA-256 against a local reference. In staged mode the test waits for the staging area to drain (`staged_files` in the stats file) before it throws the local index away.
  - `TestOpenListCfsAPIPut`: 900 small files by `/api/fs/put` from 3 callers, both modes.
  - `TestOpenListCfsOneInstanceForTwoFrontEnds`, `TestOpenListCfsOnWebDavStorage` (fault proxy), `TestOpenListCfsMount` (skipped here: no WinFsp).
- The sync job path of OP-sync ends in the same driver `Put`; it is not measured separately.

### For C6 (real 115, not done here)

Pointing the C5 instance at 115 through an OpenList storage needs no cfs change: create the 115 storage in OpenList (mount e.g. /115), then a Cfs storage with `remote_path` `/115/cfs-volume`. For the black-box suite use `CFS_OP_UNDER_URL` / `CFS_OP_UNDER_USER` / `CFS_OP_UNDER_PASS` (optional `CFS_OP_UNDER_DIR`) with `TestOpenListCfsOnWebDavStorage`. Not tried: real drive latency, 115 rapid-upload interplay with packs, real API rate limits. With `ack=staged` the commit to 115 happens seconds to a minute after the upload returned: wait for `staged_files` to reach 0 (or call Flush) before comparing what is on the drive.

### Known C5 gaps

- The mount itself (WinFsp) was not run on this machine; only the VFS layer over WebDAV was.
- rclone logs `internal error: no overview data found for "cfs"` once at start; harmless.
- The dataset test lets OpenList's Local storage stand in for a drive: it proves the plumbing and the layout, not drive behaviour.
