package core

import (
	"bufio"
	"bytes"
	"compress/flate"
	"context"
	"crypto/sha1"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/Ironboxplus/cfs/store"
)

func nowUnix() int64 { return time.Now().Unix() }

// ---- compaction ----

// CompactReport says what Compact did.
type CompactReport struct {
	PacksCompacted int
	MembersMoved   int
	BytesMoved     int64
}

// Compact rewrites the live members of packs in which at least minDead (0..1,
// default 0.5) of the bytes are dead into new packs. The log records are
// conditional on the member still being where it was read from, so a
// concurrent write is never overridden. The emptied packs are deleted later by
// GC, after a checkpoint and a grace period.
func (v *Volume) Compact(ctx context.Context, minDead float64) (CompactReport, error) {
	var rep CompactReport
	end, werr := v.beginWrite(ctx)
	if werr != nil {
		return rep, werr
	}
	defer end()
	if minDead <= 0 {
		minDead = 0.5
	}
	rows, err := v.ix.db.Query(`SELECT p.name, p.bytes, COALESCE(SUM(f.csize),0), COUNT(f.path)
		FROM packs p LEFT JOIN files f ON f.pack=p.name GROUP BY p.name ORDER BY p.seq`)
	if err != nil {
		return rep, err
	}
	var cands []string
	for rows.Next() {
		var name string
		var total, live, n int64
		if err := rows.Scan(&name, &total, &live, &n); err != nil {
			rows.Close()
			return rep, err
		}
		if n > 0 && total > 0 && 1-float64(live)/float64(total) >= minDead {
			cands = append(cands, name)
		}
	}
	rows.Close()
	for _, pack := range cands {
		frows, err := v.ix.db.Query("SELECT "+fileCols+" FROM files WHERE pack=?", pack)
		if err != nil {
			return rep, err
		}
		var live []fileRow
		for frows.Next() {
			r, err := scanFile(frows)
			if err != nil {
				frows.Close()
				return rep, err
			}
			live = append(live, r)
		}
		frows.Close()
		var waits []*batch
		for _, r := range live {
			data, err := v.memberBytes(ctx, r)
			if err != nil {
				return rep, fmt.Errorf("cfs: compact %s: %w", pack, err)
			}
			m := &member{path: r.Path, size: r.Size, mtime: r.MTime, sha1: r.SHA1, expect: fmt.Sprintf("%s@%d", r.Pack, r.Off)}
			stored, meth, crc := v.encodeMember(r.Path, data)
			m.meth = meth
			v.structMu.RLock()
			b, err := v.packAdd(ctx, m, stored, crc)
			v.structMu.RUnlock()
			if err != nil {
				return rep, err
			}
			waits = append(waits, b)
			rep.MembersMoved++
			rep.BytesMoved += r.Size
		}
		for _, b := range waits {
			select {
			case <-b.done:
				if b.err != nil {
					return rep, b.err
				}
			case <-ctx.Done():
				return rep, ctx.Err()
			}
		}
		rep.PacksCompacted++
	}
	return rep, nil
}

// ---- garbage collection ----

// GCReport says what GC removed.
type GCReport struct {
	PacksDeleted, OrphansDeleted, SegmentsDeleted, CheckpointsDeleted int
	DeadPacks                                                         int // empty packs waiting for a checkpoint / the grace period
}

// GC deletes what nothing refers to any more, but only when it is safe:
//   - a pack with no live member, once a checkpoint newer than the moment it
//     became empty is on the drive and the grace period has passed;
//   - a pack nothing in the log refers to (left by a crash before the segment
//     was written), once it is older than the grace period;
//   - log segments covered by the second newest checkpoint, and checkpoints
//     older than the two newest, once older than the grace period.
func (v *Volume) GC(ctx context.Context) (GCReport, error) {
	var rep GCReport
	end, werr := v.beginWrite(ctx)
	if werr != nil {
		return rep, werr
	}
	defer end()
	now := nowUnix()
	grace := int64(v.cfg.GCGrace / time.Second)
	graceDur := v.cfg.GCGrace
	err := v.ix.tx(func(tx *sql.Tx) error {
		if _, err := tx.Exec(`UPDATE packs SET dead_at=? WHERE dead_at=0 AND NOT EXISTS (SELECT 1 FROM files f WHERE f.pack=packs.name)`, now); err != nil {
			return err
		}
		_, err := tx.Exec(`UPDATE packs SET dead_at=0 WHERE dead_at<>0 AND EXISTS (SELECT 1 FROM files f WHERE f.pack=packs.name)`)
		return err
	})
	if err != nil {
		return rep, err
	}
	lastCkptAt := int64(v.ix.kvUint("last_ckpt_at"))
	rows, err := v.ix.db.Query("SELECT name, dead_at FROM packs WHERE dead_at<>0")
	if err != nil {
		return rep, err
	}
	var del []string
	for rows.Next() {
		var name string
		var dead int64
		rows.Scan(&name, &dead)
		rep.DeadPacks++
		if now-dead >= grace && lastCkptAt > dead {
			del = append(del, name)
		}
	}
	rows.Close()
	for _, name := range del {
		if err := v.st.Remove(ctx, CtlDir+"/packs/"+name); err != nil && !isNotFound(err) {
			return rep, err
		}
		if _, err := v.ix.db.Exec("DELETE FROM packs WHERE name=?", name); err != nil {
			return rep, err
		}
		rep.PacksDeleted++
		rep.DeadPacks--
	}
	// Orphans.
	ents, err := v.st.List(ctx, CtlDir+"/packs")
	if err != nil && !isNotFound(err) {
		return rep, err
	}
	busy := map[string]bool{}
	v.batchMu.Lock()
	for b := range v.inflight {
		if b.pack != "" {
			busy[b.pack] = true
		}
	}
	v.batchMu.Unlock()
	for _, e := range ents {
		name := e.Name()
		if e.IsDir || packRE.FindString(name) == "" || busy[name] {
			continue
		}
		var one int
		if v.ix.db.QueryRow("SELECT 1 FROM packs WHERE name=?", name).Scan(&one) == nil {
			continue
		}
		if e.ModTime.IsZero() || time.Since(e.ModTime) < graceDur {
			continue
		}
		if err := v.st.Remove(ctx, CtlDir+"/packs/"+name); err != nil && !isNotFound(err) {
			return rep, err
		}
		rep.OrphansDeleted++
	}
	// Old segments and checkpoints.
	segs, cks, err := v.cleanLog(ctx)
	rep.SegmentsDeleted, rep.CheckpointsDeleted = segs, cks
	return rep, err
}

// cleanLog deletes the log the newest checkpoint that has been on the drive for
// CleanupSafety makes garbage: the segments it covers, and the checkpoints
// before it (Kopia deletes what a compaction replaced after its
// CleanupSafetyMargin). Only the writer does it, and the deletes go one after
// the other through the store's metadata gate. Volumes that are following the
// writer find a segment missing and start again from the newest checkpoint.
func (v *Volume) cleanLog(ctx context.Context) (segments, checkpoints int, err error) {
	if v.ReadOnly() || v.fenced.Load() {
		return 0, 0, nil
	}
	cks, err := v.log.ListCheckpoints(ctx)
	if err != nil {
		return 0, 0, err
	}
	keep := -1
	for i := len(cks) - 1; i >= 0; i-- {
		if !cks[i].ModTime.IsZero() && time.Since(cks[i].ModTime) >= v.cfg.CleanupSafety {
			keep = i
			break
		}
	}
	if keep < 0 {
		return 0, 0, nil
	}
	segs, err := v.log.ListSegments(ctx)
	if err != nil {
		return 0, 0, err
	}
	for _, s := range segs {
		if s.Seq <= cks[keep].Seq {
			if err := v.log.DeleteSegment(ctx, s); err != nil && !isNotFound(err) {
				return segments, checkpoints, err
			}
			segments++
		}
	}
	for _, c := range cks[:keep] {
		if err := v.log.DeleteCheckpoint(ctx, c); err != nil && !isNotFound(err) {
			return segments, checkpoints, err
		}
		checkpoints++
	}
	return segments, checkpoints, nil
}

// MaintainReport is what MaintainLog did.
type MaintainReport struct {
	// CheckpointSeq is the sequence number the new checkpoint covers.
	CheckpointSeq      uint64
	SegmentsDeleted    int
	CheckpointsDeleted int
	Elapsed            time.Duration
}

// MaintainLog makes the next volume that opens cheap: it takes the lease, writes
// the log of everything committed, uploads a checkpoint, cleans up what an old
// enough checkpoint makes garbage, and hands the lease back. It fails with
// ErrReadOnly while another writer holds the lease.
func (v *Volume) MaintainLog(ctx context.Context) (MaintainReport, error) {
	start := time.Now()
	var rep MaintainReport
	err := func() error {
		end, err := v.beginWrite(ctx)
		if err != nil {
			return err
		}
		defer end()
		if err := v.Flush(ctx); err != nil {
			return err
		}
		if err := v.Checkpoint(ctx); err != nil {
			return err
		}
		rep.CheckpointSeq = v.ix.kvUint("last_ckpt_seq")
		segs, cks, err := v.cleanLog(ctx)
		rep.SegmentsDeleted, rep.CheckpointsDeleted = segs, cks
		return err
	}()
	if err == nil {
		v.handBack(true)
	}
	rep.Elapsed = time.Since(start)
	return rep, err
}

// Maintain runs a compaction, a checkpoint and a GC pass.
func (v *Volume) Maintain(ctx context.Context) (CompactReport, GCReport, error) {
	if err := v.Flush(ctx); err != nil {
		return CompactReport{}, GCReport{}, err
	}
	cr, err := v.Compact(ctx, 0.5)
	if err != nil {
		return cr, GCReport{}, err
	}
	if err := v.Flush(ctx); err != nil {
		return cr, GCReport{}, err
	}
	g1, err := v.GC(ctx) // marks the emptied packs
	if err != nil {
		return cr, g1, err
	}
	if err := v.Checkpoint(ctx); err != nil {
		return cr, g1, err
	}
	g2, err := v.GC(ctx)
	g2.DeadPacks += 0
	return cr, g2, err
}

// ---- fsck ----

// Problem is one inconsistency found by Fsck.
type Problem struct {
	Kind   string
	Path   string
	Detail string
}

func (p Problem) String() string { return fmt.Sprintf("%s: %s: %s", p.Kind, p.Path, p.Detail) }

// FsckReport is the result of Fsck.
type FsckReport struct {
	Packs, Members, Segments int
	Problems                 []Problem
	Repaired                 int
}

// OK reports whether nothing was found.
func (r FsckReport) OK() bool { return len(r.Problems) == 0 }

// FsckOpts select how thorough Fsck is.
type FsckOpts struct {
	// Deep reads every pack and verifies every member's size and SHA-1.
	Deep bool
	// Repair fixes what is safe to fix: removes large files shadowed by a packed
	// file of the same name, and unreferenced packs.
	Repair bool
}

// Fsck compares the index, the packs and the large files, and the index with
// what the drive's log and checkpoints replay to. It only reports unless
// opts.Repair is set.
func (v *Volume) Fsck(ctx context.Context, opts FsckOpts) (FsckReport, error) {
	var rep FsckReport
	add := func(kind, p, format string, args ...any) {
		rep.Problems = append(rep.Problems, Problem{kind, p, fmt.Sprintf(format, args...)})
	}
	if !v.ReadOnly() && v.writable() == nil {
		if err := v.Flush(ctx); err != nil {
			return rep, err
		}
	}
	// Packs on the drive.
	ents, err := v.st.List(ctx, CtlDir+"/packs")
	if err != nil && !isNotFound(err) {
		return rep, err
	}
	onDrive := map[string]store.Entry{}
	for _, e := range ents {
		if !e.IsDir {
			onDrive[e.Name()] = e
		}
	}
	type packInfo struct {
		bytes int64
		sha1  string
	}
	packs := map[string]packInfo{}
	prow, err := v.ix.db.Query("SELECT name, bytes, sha1 FROM packs")
	if err != nil {
		return rep, err
	}
	for prow.Next() {
		var n string
		var pi packInfo
		prow.Scan(&n, &pi.bytes, &pi.sha1)
		packs[n] = pi
	}
	prow.Close()
	rep.Packs = len(packs)
	for n, pi := range packs {
		e, ok := onDrive[n]
		switch {
		case !ok:
			add("missing_pack", n, "the index knows this pack but the drive doesn't have it")
		case e.Size != pi.bytes:
			add("pack_size", n, "drive has %d bytes, index says %d", e.Size, pi.bytes)
		}
	}
	busy := map[string]bool{}
	v.batchMu.Lock()
	for b := range v.inflight {
		busy[b.pack] = true
	}
	v.batchMu.Unlock()
	for n := range onDrive {
		if _, ok := packs[n]; !ok && !busy[n] && packRE.MatchString(n) {
			if opts.Repair && v.writable() == nil {
				if err := v.st.Remove(ctx, CtlDir+"/packs/"+n); err == nil {
					rep.Repaired++
					continue
				}
			}
			add("orphan_pack", n, "no log segment refers to this pack")
		}
	}
	// Members.
	byPack := map[string][]fileRow{}
	dirs := map[string]bool{}
	err = v.ix.all(func(r fileRow) error {
		rep.Members++
		byPack[r.Pack] = append(byPack[r.Pack], r)
		dirs[r.Dir] = true
		pi, ok := packs[r.Pack]
		if !ok {
			add("dangling_member", r.Path, "pack %s is not in the index", r.Pack)
		} else if r.Off < 0 || r.Off+r.CSize > pi.bytes {
			add("member_range", r.Path, "bytes %d..%d are outside pack %s (%d bytes)", r.Off, r.Off+r.CSize, r.Pack, pi.bytes)
		}
		return nil
	})
	if err != nil {
		return rep, err
	}
	if opts.Deep {
		names := make([]string, 0, len(byPack))
		for n := range byPack {
			names = append(names, n)
		}
		sort.Strings(names)
		for _, n := range names {
			if _, ok := onDrive[n]; !ok {
				continue
			}
			v.deepCheckPack(ctx, n, packs[n].sha1, byPack[n], add)
		}
	}
	// Names that exist twice.
	for d := range dirs {
		lm, err := v.listStore(ctx, d)
		if err != nil {
			if isNotFound(err) { // packed files need no folder on the drive
				continue
			}
			return rep, err
		}
		rows, _ := v.ix.listDirCommitted(d)
		for _, r := range rows {
			e, ok := lm[r.Name]
			if !ok {
				continue
			}
			if e.IsDir {
				add("name_conflict", r.Path, "a packed file and a directory share this name")
				continue
			}
			if opts.Repair && v.writable() == nil {
				if err := v.st.Remove(ctx, r.Path); err == nil {
					v.dc.cacheDel(r.Path)
					rep.Repaired++
					continue
				}
			}
			add("shadowed_large", r.Path, "a large file of the same name is hidden by the packed file")
		}
	}
	// Control directory.
	if cents, err := v.st.List(ctx, CtlDir); err == nil {
		for _, e := range cents {
			switch e.Name() {
			case "lease.json", "packs", "log", "ckpt":
			default:
				add("stray_ctl", CtlDir+"/"+e.Name(), "unexpected entry in the control directory")
			}
		}
	}
	// Index against what the drive replays to.
	if segs, err := v.segRefs(ctx); err != nil {
		add("log_conflict", "", "%v", err)
	} else {
		rep.Segments = len(segs)
		if mism, err := v.compareWithDrive(ctx); err != nil {
			add("log_unreadable", "", "%v", err)
		} else {
			for _, m := range mism {
				add("log_mismatch", m.path, "%s", m.detail)
			}
		}
	}
	return rep, nil
}

type mismatch struct{ path, detail string }

// compareWithDrive replays checkpoint + log into a scratch index and compares
// it with the live one. Only meaningful when no operation is pending.
func (v *Volume) compareWithDrive(ctx context.Context) ([]mismatch, error) {
	var pend int
	if err := v.ix.db.QueryRow("SELECT count(*) FROM pending").Scan(&pend); err != nil {
		return nil, err
	}
	var def int
	if err := v.ix.db.QueryRow("SELECT count(*) FROM deferred").Scan(&def); err != nil {
		return nil, err
	}
	if pend > 0 || def > 0 {
		return nil, nil // the drive is legitimately behind
	}
	dir, err := os.MkdirTemp(v.cfg.StateDir, "fsck-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)
	dbPath := filepath.Join(dir, "index.db")
	if err := v.fetchBaseCheckpoint(ctx, dbPath); err != nil {
		return nil, err
	}
	sx, err := openIndexFile(dbPath)
	if err != nil {
		return nil, err
	}
	defer sx.close()
	sv := &Volume{cfg: v.cfg, st: v.st, log: v.log, ix: sx}
	if err := sv.catchUp(ctx); err != nil {
		return nil, err
	}
	if err := sx.backfillDirs(); err != nil {
		return nil, err
	}
	load := func(ix *index) (map[string]fileRow, error) {
		m := map[string]fileRow{}
		err := ix.all(func(r fileRow) error { m[r.Path] = r; return nil })
		return m, err
	}
	live, err := load(v.ix)
	if err != nil {
		return nil, err
	}
	drive, err := load(sx)
	if err != nil {
		return nil, err
	}
	var out []mismatch
	for p, a := range live {
		b, ok := drive[p]
		switch {
		case !ok:
			out = append(out, mismatch{p, "in the index but not in the log"})
		case a.Pack != b.Pack || a.Off != b.Off || a.CSize != b.CSize || a.Size != b.Size || a.SHA1 != b.SHA1 || a.MTime != b.MTime:
			out = append(out, mismatch{p, "index and log disagree on the member"})
		}
	}
	for p := range drive {
		if _, ok := live[p]; !ok {
			out = append(out, mismatch{p, "in the log but not in the index"})
		}
	}
	// The directories of the namespace.
	loadDirs := func(ix *index) (map[string]bool, error) {
		rows, err := ix.db.Query("SELECT path FROM dirs")
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		m := map[string]bool{}
		for rows.Next() {
			var p string
			if err := rows.Scan(&p); err != nil {
				return nil, err
			}
			m[p] = true
		}
		return m, rows.Err()
	}
	ld, err := loadDirs(v.ix)
	if err != nil {
		return nil, err
	}
	dd, err := loadDirs(sx)
	if err != nil {
		return nil, err
	}
	for p := range ld {
		if !dd[p] {
			out = append(out, mismatch{p, "directory in the index but not in the log"})
		}
	}
	for p := range dd {
		if !ld[p] {
			out = append(out, mismatch{p, "directory in the log but not in the index"})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].path < out[j].path })
	return out, nil
}

// maxStreamBufferSize is the read-ahead buffer of the pack check, restic's
// value (internal/repository/checker.go).
const maxStreamBufferSize = 4 << 20

// deepCheckPack checks a pack the way restic's checkPackInner does
// (internal/repository/checker.go): the pack is read once, front to back, as one
// stream; its hash is computed on the fly; a 4 MiB bufio.Reader sits on it; the
// members are read one by one, in order of offset, into a buffer that is as big
// as the largest member, and verified; the gaps between them are skipped with
// Discard; the rest of the stream is read, and the hash of the whole pack is
// compared with the index. So memory use is the read-ahead buffer plus one
// member, whatever the size of the pack. A check that finds anything is run once
// more from the start (restic's checkPack): only what the second run finds is
// reported, which tells a transient error from damage.
func (v *Volume) deepCheckPack(ctx context.Context, name, wantSHA string, rows []fileRow, add func(kind, p, format string, args ...any)) {
	e, err := v.st.Stat(ctx, CtlDir+"/packs/"+name)
	if err != nil {
		add("pack_unreadable", name, "%v", err)
		return
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Off < rows[j].Off })
	type finding struct{ kind, path, msg string }
	run := func() (found []finding, err error) {
		note := func(kind, p, format string, args ...any) {
			found = append(found, finding{kind, p, fmt.Sprintf(format, args...)})
		}
		var biggest int64
		for _, r := range rows {
			biggest = max(biggest, r.CSize)
		}
		rc, err := v.st.Open(ctx, CtlDir+"/packs/"+name, 0, e.Size)
		if err != nil {
			return nil, err
		}
		defer rc.Close()
		h := sha1.New()
		br := bufio.NewReaderSize(io.TeeReader(rc, h), maxStreamBufferSize)
		buf := make([]byte, biggest)
		var pos int64
		for _, r := range rows {
			if r.Off < pos || r.Off+r.CSize > e.Size {
				note("member_checksum", r.Path, "stored at %d+%d, which overlaps the member before it or lies beyond the end of the pack (%d bytes)", r.Off, r.CSize, e.Size)
				continue
			}
			if _, err := br.Discard(int(r.Off - pos)); err != nil {
				return found, fmt.Errorf("skip to %d: %w", r.Off, err)
			}
			if _, err := io.ReadFull(br, buf[:r.CSize]); err != nil {
				return found, fmt.Errorf("read %s at %d: %w", r.Path, r.Off, err)
			}
			pos = r.Off + r.CSize
			var plain io.Reader = bytes.NewReader(buf[:r.CSize])
			if r.Meth == 8 {
				plain = flate.NewReader(plain)
			}
			ms := sha1.New()
			n, derr := io.Copy(ms, plain)
			switch {
			case derr != nil:
				note("member_checksum", r.Path, "cannot inflate: %v", derr)
			case n != r.Size || hex.EncodeToString(ms.Sum(nil)) != r.SHA1:
				note("member_checksum", r.Path, "content doesn't match the recorded size/sha1")
			}
		}
		// The rest: what follows the last member (the manifest, the directory).
		if _, err := br.Discard(int(e.Size - pos)); err != nil {
			return found, fmt.Errorf("read the end of the pack: %w", err)
		}
		if _, err := br.ReadByte(); err != io.EOF {
			note("pack_checksum", name, "the pack is longer than the %d bytes the drive lists", e.Size)
		}
		if got := hex.EncodeToString(h.Sum(nil)); wantSHA != "" && got != wantSHA {
			note("pack_checksum", name, "sha1 is %s, the index says %s", got, wantSHA)
		}
		return found, nil
	}
	found, err := run()
	if err != nil || len(found) > 0 {
		first := err
		if err == nil {
			first = fmt.Errorf("%d problems, the first: %s: %s", len(found), found[0].path, found[0].msg)
		}
		found, err = run()
		if err == nil && len(found) == 0 {
			v.logf("cfs: check of pack %s passed on the second attempt, the first found: %v", name, first)
		}
	}
	for _, f := range found {
		add(f.kind, f.path, "%s", f.msg)
	}
	if err != nil {
		add("pack_unreadable", name, "%v", err)
	}
}
