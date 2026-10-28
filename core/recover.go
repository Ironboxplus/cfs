package core

import (
	"archive/zip"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/Ironboxplus/cfs/metalog"
	"github.com/Ironboxplus/cfs/store"
)

// ErrEpochConflict is returned when the drive holds two different segments
// with the same sequence number: two writers wrote at once. cfs reports it
// and never merges silently.
var ErrEpochConflict = errors.New("cfs: two segments with the same sequence number (writer conflict)")

var packRE = regexp.MustCompile(`^p-(\d+)-(\d+)\.zip$`)

func (v *Volume) segRefs(ctx context.Context) ([]metalog.Ref, error) {
	refs, err := v.log.ListSegments(ctx)
	if err != nil {
		return nil, err
	}
	for i := 1; i < len(refs); i++ {
		if refs[i].Seq == refs[i-1].Seq {
			return nil, fmt.Errorf("%w: seq %d in epochs %d and %d", ErrEpochConflict, refs[i].Seq, refs[i-1].Epoch, refs[i].Epoch)
		}
	}
	return refs, nil
}

// fetchBaseCheckpoint puts the newest readable checkpoint of the drive in
// place of a missing local index. With no usable checkpoint the index starts
// empty and catchUp replays every segment.
func (v *Volume) fetchBaseCheckpoint(ctx context.Context, dbPath string) error {
	cks, err := v.log.ListCheckpoints(ctx)
	if err != nil {
		return err
	}
	for i := len(cks) - 1; i >= 0; i-- {
		tmp := dbPath + ".fetch"
		os.Remove(tmp)
		if err := v.log.FetchCheckpoint(ctx, cks[i], tmp); err != nil {
			v.logf("cfs: checkpoint %s unusable, trying an older one: %v", cks[i].Name, err)
			continue
		}
		ix, err := openIndexFile(tmp)
		if err == nil {
			var n int
			err = ix.db.QueryRow("SELECT count(*) FROM files").Scan(&n)
			if err == nil {
				err = ix.tx(func(tx *sql.Tx) error {
					for _, q := range []string{
						"DELETE FROM kv WHERE k IN ('writer_id','epoch','inited','dmv_intent')",
						"DELETE FROM pending", "DELETE FROM inflight", "DELETE FROM staged",
					} {
						if _, err := tx.Exec(q); err != nil {
							return err
						}
					}
					return kvSet(tx, "base_seq", strconv.FormatUint(cks[i].Seq, 10))
				})
			}
			ix.close()
		}
		if err != nil {
			v.logf("cfs: checkpoint %s unusable, trying an older one: %v", cks[i].Name, err)
			os.Remove(tmp)
			continue
		}
		for _, ext := range []string{"", "-wal", "-shm"} {
			os.Remove(dbPath + ext)
		}
		return os.Rename(tmp, dbPath)
	}
	return nil
}

// applySegment applies a segment to the local index and records its name.
func (v *Volume) applySegment(seg metalog.Segment) error {
	key := segKeyOf(seg.Epoch, seg.Seq)
	return v.ix.tx(func(tx *sql.Tx) error {
		for _, r := range seg.Records {
			if err := applyRecord(tx, r, seg.Seq); err != nil {
				return fmt.Errorf("segment %s: %w", key, err)
			}
		}
		if seg.Pack != "" {
			if _, err := tx.Exec("INSERT OR REPLACE INTO packs(name,epoch,seq,bytes,sha1,members,dead_at) VALUES(?,?,?,?,?,?,0)",
				seg.Pack, seg.Epoch, seg.Seq, seg.PackBytes, seg.PackSHA1, seg.PackN); err != nil {
				return err
			}
		}
		_, err := tx.Exec("INSERT OR IGNORE INTO segs(name) VALUES(?)", key)
		return err
	})
}

// catchUp applies the drive's segments this index hasn't seen, in sequence
// order, and raises the sequence counter above everything on the drive.
func (v *Volume) catchUp(ctx context.Context) error {
	refs, err := v.segRefs(ctx)
	if err != nil {
		return err
	}
	base := v.ix.kvUint("base_seq")
	if len(refs) == 0 && base == 0 {
		var n int
		v.ix.db.QueryRow("SELECT count(*) FROM packs").Scan(&n)
		if cks, _ := v.log.ListCheckpoints(ctx); n == 0 && len(cks) == 0 {
			if err := v.recoverFromPacks(ctx); err != nil {
				return err
			}
		}
	}
	known := map[string]bool{}
	rows, err := v.ix.db.Query("SELECT name FROM segs")
	if err != nil {
		return err
	}
	for rows.Next() {
		var n string
		rows.Scan(&n)
		known[n] = true
	}
	rows.Close()
	var top uint64
	for i, ref := range refs {
		top = max(top, ref.Seq)
		if ref.Seq <= base || known[segKeyOf(ref.Epoch, ref.Seq)] {
			continue
		}
		seg, err := v.log.ReadSegment(ctx, ref)
		if err != nil {
			// The newest segment can be the leftover of an upload that was cut
			// short: it was never verified, so nothing was acknowledged for it.
			// Anywhere else an unreadable segment is real damage.
			if i == len(refs)-1 && !errors.Is(err, store.ErrNotFound) && ctx.Err() == nil {
				v.logf("cfs: ignoring unreadable newest log segment %s (an interrupted upload): %v", ref.Name, err)
				if !v.ReadOnly() && !v.fenced.Load() {
					_ = v.log.DeleteSegment(ctx, ref)
				}
				continue
			}
			return fmt.Errorf("cfs: replay %s: %w", ref.Name, err)
		}
		if err := v.applySegment(seg); err != nil {
			return err
		}
	}
	cks, err := v.log.ListCheckpoints(ctx)
	if err != nil {
		return err
	}
	for _, c := range cks {
		top = max(top, c.Seq)
	}
	if ents, err := v.st.List(ctx, CtlDir+"/packs"); err == nil {
		for _, e := range ents {
			if m := packRE.FindStringSubmatch(e.Name()); m != nil {
				s, _ := strconv.ParseUint(m[2], 10, 64)
				top = max(top, s)
			}
		}
	}
	return v.ix.raiseSeq(top)
}

// recoverLocal finishes what a crash of this process interrupted: a directory
// move that was only half done, and log segments that were sealed locally.
func (v *Volume) recoverLocal(ctx context.Context) error {
	if s, ok := v.ix.kvGet("dmv_intent"); ok {
		var rec metalog.Record
		if err := json.Unmarshal([]byte(s), &rec); err != nil {
			return err
		}
		_, dstErr := v.st.Stat(ctx, rec.To)
		_, srcErr := v.st.Stat(ctx, rec.Path)
		switch {
		case dstErr == nil && errors.Is(srcErr, store.ErrNotFound): // the drive did move it
			err := v.ix.tx(func(tx *sql.Tx) error {
				if err := applyRecord(tx, rec, 0); err != nil {
					return err
				}
				if err := addPending(tx, rec); err != nil {
					return err
				}
				_, err := tx.Exec("DELETE FROM kv WHERE k='dmv_intent'")
				return err
			})
			if err != nil {
				return err
			}
			v.logf("cfs: completed interrupted directory move %s -> %s", rec.Path, rec.To)
		case srcErr == nil && errors.Is(dstErr, store.ErrNotFound): // it never happened
			if err := v.ix.tx(func(tx *sql.Tx) error { _, e := tx.Exec("DELETE FROM kv WHERE k='dmv_intent'"); return e }); err != nil {
				return err
			}
		default:
			return fmt.Errorf("cfs: interrupted directory move %s -> %s found in an unclear state (src: %v, dst: %v); run fsck",
				rec.Path, rec.To, srcErr, dstErr)
		}
	}
	refs, err := v.segRefs(ctx)
	if err != nil {
		return err
	}
	onDrive := map[string]metalog.Ref{}
	for _, r := range refs {
		onDrive[segKeyOf(r.Epoch, r.Seq)] = r
	}
	// Packs whose commit was interrupted after the segment reached the drive:
	// the puts are applied now. Their pending rows ride in the same segment.
	rows, err := v.ix.db.Query("SELECT name FROM inflight")
	if err != nil {
		return err
	}
	var names []string
	for rows.Next() {
		var n string
		rows.Scan(&n)
		names = append(names, n)
	}
	rows.Close()
	sort.Strings(names)
	for _, key := range names {
		if ref, ok := onDrive[key]; ok {
			seg, err := v.log.ReadSegment(ctx, ref)
			if err != nil {
				// Cut short by the crash: it was never complete, so the batch was
				// never acknowledged. Remove the debris and give the rows back.
				v.logf("cfs: log segment %s of an interrupted commit is unreadable, discarding it: %v", ref.Name, err)
				if !v.ReadOnly() {
					_ = v.log.DeleteSegment(ctx, ref)
				}
				v.releaseInflight(key)
				continue
			}
			err = v.ix.tx(func(tx *sql.Tx) error {
				for _, r := range seg.Records {
					if r.Op == metalog.OpPut {
						if err := applyRecord(tx, r, seg.Seq); err != nil {
							return err
						}
						// The staged copy is on the drive now: nothing to replay.
						if _, err := tx.Exec("DELETE FROM staged WHERE path=? AND sha1=? AND size=? AND mtime=?",
							r.Path, r.SHA1, r.Size, r.MTime); err != nil {
							return err
						}
					}
				}
				if _, err := tx.Exec("INSERT OR REPLACE INTO packs(name,epoch,seq,bytes,sha1,members,dead_at) VALUES(?,?,?,?,?,?,0)",
					seg.Pack, seg.Epoch, seg.Seq, seg.PackBytes, seg.PackSHA1, seg.PackN); err != nil {
					return err
				}
				if _, err := tx.Exec("DELETE FROM pending WHERE seg=?", key); err != nil {
					return err
				}
				_, err := tx.Exec("DELETE FROM inflight WHERE name=?", key)
				return err
			})
			if err != nil {
				return err
			}
			v.logf("cfs: recovered pack %s committed before the crash", seg.Pack)
		} else {
			v.releaseInflight(key)
		}
	}
	// Sealed metadata segments that did reach the drive need no more upload.
	sealed, err := readPending(v.ix.db, "WHERE seg IS NOT NULL")
	if err != nil {
		return err
	}
	for _, p := range sealed {
		if _, ok := onDrive[p.Seg]; ok {
			if _, err := v.ix.db.Exec("DELETE FROM pending WHERE seg=?", p.Seg); err != nil {
				return err
			}
		}
	}
	// With the index complete: what was half done on the drive.
	return v.finishInterrupted(ctx)
}

// Refresh applies segments written to the drive by the writer since this
// volume last looked. A read-only volume calls it to follow the writer.
func (v *Volume) Refresh(ctx context.Context) error {
	if !v.ReadOnly() {
		return nil
	}
	v.dc.reset()
	return v.catchUp(ctx)
}

// Checkpoint uploads a snapshot of the index. Rebuilding then needs only that
// snapshot and the segments after it. It waits for packs in commit, so the
// snapshot is a consistent cut of the log.
func (v *Volume) Checkpoint(ctx context.Context) error {
	if err := v.writable(); err != nil {
		return err
	}
	if err := v.flushMeta(ctx); err != nil {
		return err
	}
	v.ckptOne.Lock()
	defer v.ckptOne.Unlock()
	tf, err := os.CreateTemp(filepath.Join(v.cfg.StateDir, "spool"), "ckpt-*.db")
	if err != nil {
		return err
	}
	tmp := tf.Name()
	tf.Close()
	os.Remove(tmp) // VACUUM INTO wants to create the file itself
	defer os.Remove(tmp)
	v.structMu.Lock()
	v.ckptMu.Lock()
	seq := v.ix.kvUint("next_seq") - 1
	_, err = v.ix.db.Exec("VACUUM INTO ?", tmp)
	v.ckptMu.Unlock()
	v.structMu.Unlock()
	if err != nil {
		return fmt.Errorf("cfs: snapshot index: %w", err)
	}
	if err := v.checkLease(ctx); err != nil {
		return err
	}
	if err := v.log.PutCheckpoint(ctx, v.epoch, seq, tmp); err != nil {
		return err
	}
	v.segsNew.Store(0)
	_ = v.ix.kvSet("last_ckpt_seq", strconv.FormatUint(seq, 10))
	_ = v.ix.kvSet("last_ckpt_at", strconv.FormatInt(nowUnix(), 10))
	return nil
}

func (v *Volume) maybeCheckpoint() {
	if !v.ckptRun.CompareAndSwap(false, true) {
		return
	}
	v.wg.Add(1)
	go func() {
		defer v.wg.Done()
		defer v.ckptRun.Store(false)
		if err := v.Checkpoint(v.ctx); err != nil && v.ctx.Err() == nil {
			v.logf("cfs: checkpoint: %v", err)
		}
	}()
}

// packReaderAt reads a pack on the drive through ranged requests, so that
// archive/zip can find the central directory at the end without downloading it.
type packReaderAt struct {
	ctx  context.Context
	st   store.Store
	path string
}

func (r *packReaderAt) ReadAt(p []byte, off int64) (int, error) {
	rc, err := r.st.Open(r.ctx, r.path, off, int64(len(p)))
	if err != nil {
		return 0, err
	}
	defer rc.Close()
	return io.ReadFull(rc, p)
}

// recoverFromPacks rebuilds the index from the manifests the packs carry
// themselves. It is the last resort when the drive has packs but no usable log
// or checkpoint. Packs are applied in sequence order, so the newest version of
// each path wins; what the log would have said about deletions and renames made
// after a pack was written is gone, so such files come back under the name they
// had when they were packed.
func (v *Volume) recoverFromPacks(ctx context.Context) error {
	ents, err := v.st.List(ctx, CtlDir+"/packs")
	if err != nil {
		if isNotFound(err) {
			return nil
		}
		return err
	}
	type pk struct {
		name       string
		epoch, seq uint64
		size       int64
	}
	var packs []pk
	for _, e := range ents {
		if m := packRE.FindStringSubmatch(e.Name()); m != nil && !e.IsDir {
			ep, _ := strconv.ParseUint(m[1], 10, 64)
			sq, _ := strconv.ParseUint(m[2], 10, 64)
			packs = append(packs, pk{e.Name(), ep, sq, e.Size})
		}
	}
	sort.Slice(packs, func(i, j int) bool { return packs[i].seq < packs[j].seq })
	if len(packs) == 0 {
		return nil
	}
	v.logf("cfs: no log or checkpoint on the drive: rebuilding the index from the manifests of %d packs", len(packs))
	for _, p := range packs {
		ra := &packReaderAt{ctx: ctx, st: v.st, path: CtlDir + "/packs/" + p.name}
		zr, err := zip.NewReader(ra, p.size)
		if err != nil {
			v.logf("cfs: pack %s unreadable, skipped: %v", p.name, err)
			continue
		}
		var man *zip.File
		for _, f := range zr.File {
			if f.Name == manifestName {
				man = f
			}
		}
		if man == nil {
			v.logf("cfs: pack %s has no manifest, skipped", p.name)
			continue
		}
		rc, err := man.Open()
		if err != nil {
			continue
		}
		var pm packManifest
		err = json.NewDecoder(rc).Decode(&pm)
		rc.Close()
		if err != nil {
			v.logf("cfs: pack %s manifest unreadable, skipped: %v", p.name, err)
			continue
		}
		seg := metalog.Segment{Epoch: p.epoch, Seq: p.seq, Pack: p.name, PackBytes: p.size, PackN: len(pm.Members), Records: pm.Members}
		if err := v.applySegment(seg); err != nil {
			return err
		}
	}
	return nil
}

// finishInterrupted completes what a crash left half done on the drive and in
// the index: removals of large files shadowed by a packed file, and moves.
func (v *Volume) finishInterrupted(ctx context.Context) error {
	if v.ReadOnly() {
		return nil
	}
	rows, err := v.ix.db.Query("SELECT k, v FROM kv WHERE k LIKE 'stray:%' OR k LIKE 'mv:%' OR k LIKE 'putbig:%'")
	if err != nil {
		return err
	}
	type kvp struct{ k, v string }
	var todo []kvp
	for rows.Next() {
		var x kvp
		if err := rows.Scan(&x.k, &x.v); err != nil {
			rows.Close()
			return err
		}
		todo = append(todo, x)
	}
	rows.Close()
	for _, x := range todo {
		switch {
		case strings.HasPrefix(x.k, strayKey):
			p := strings.TrimPrefix(x.k, strayKey)
			// Only while the packed file is still the one the name stands for:
			// a large file written since is the new version.
			if _, ok, err := v.ix.getCommitted(p); err != nil {
				return err
			} else if ok {
				if err := v.st.Remove(ctx, p); err != nil && !isNotFound(err) {
					return fmt.Errorf("cfs: remove shadowed large file %s: %w", p, err)
				}
			}
		case strings.HasPrefix(x.k, putBigKey):
			// The large upload was never acknowledged (the key goes before the
			// acknowledgement), and it may be whole, partial or absent: take it
			// back, the small version stays what the name stands for.
			p := strings.TrimPrefix(x.k, putBigKey)
			if _, ok, err := v.ix.getCommitted(p); err != nil {
				return err
			} else if ok {
				if err := v.st.Remove(ctx, p); err != nil && !isNotFound(err) {
					return fmt.Errorf("cfs: remove unacknowledged large file %s: %w", p, err)
				}
			}
		case strings.HasPrefix(x.k, mvKey):
			var in mvIntent
			if err := json.Unmarshal([]byte(x.v), &in); err != nil {
				return err
			}
			if err := v.finishMove(ctx, in); err != nil {
				return fmt.Errorf("cfs: finish interrupted move %s -> %s: %w", in.Src, in.Dst, err)
			}
			v.logf("cfs: completed interrupted move %s -> %s", in.Src, in.Dst)
		}
		if _, err := v.ix.db.Exec("DELETE FROM kv WHERE k=?", x.k); err != nil {
			return err
		}
	}
	return nil
}
