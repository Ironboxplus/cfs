package core

import (
	"archive/zip"
	"bytes"
	"compress/flate"
	"context"
	"crypto/sha1"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Ironboxplus/cfs/internal/crash"
	"github.com/Ironboxplus/cfs/metalog"
	"github.com/Ironboxplus/cfs/store"
)

// manifestName is the last member of every pack: the pack describes itself, so
// the index can be recovered from the packs alone as a last resort.
const manifestName = ".cfs-manifest.json"

type member struct {
	path       string
	size       int64
	mtime      int64
	sha1       string
	off, csize int64
	meth       uint16
	expect     string // compaction: only apply while the path still points here
	strayBig   bool   // a large file of the same name must be removed after commit
	id         int64  // stage id of an acknowledged-but-uncommitted file (AckStaged), else 0
	superseded bool
	// state is memberPending until the commit takes the member along
	// (memberLocked) or its caller gives up first (memberCancelled). Whoever
	// moves it out of pending first wins, so a Put that returns an error because
	// its context ended never becomes visible afterwards.
	state atomic.Int32
	// prev is what the path held before this member's commit, kept so that a
	// put whose caller gave up too late can be taken back.
	prev    fileRow
	hadPrev bool
}

const (
	memberPending int32 = iota
	memberLocked
	memberCancelled
)

type packManifest struct {
	Pack    string           `json:"pack"`
	Epoch   uint64           `json:"epoch"`
	Seq     uint64           `json:"seq"`
	Members []metalog.Record `json:"members"`
}

type countWriter struct {
	w io.Writer
	n int64
}

func (c *countWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

// batch is one pack being filled, then committed: the group commit unit.
type batch struct {
	v      *Volume
	spool  string
	f      *os.File
	cw     *countWriter
	zw     *zip.Writer
	sum    hash.Hash
	mem    []*member
	byPath map[string]int
	bytes  int64
	first  time.Time
	quiet  *time.Timer
	max    *time.Timer
	sealed bool

	done chan struct{}
	err  error
	pack string // pack name, once allocated
	seq  uint64
}

func (v *Volume) newBatch() (*batch, error) {
	f, err := os.CreateTemp(filepath.Join(v.cfg.StateDir, "spool"), "pack-*.zip")
	if err != nil {
		return nil, err
	}
	b := &batch{v: v, spool: f.Name(), f: f, sum: sha1.New(), byPath: map[string]int{}, done: make(chan struct{})}
	b.cw = &countWriter{w: io.MultiWriter(f, b.sum)}
	b.zw = zip.NewWriter(b.cw)
	return b, nil
}

// encodeMember compresses a small file for storing in a pack.
func (v *Volume) encodeMember(name string, data []byte) (stored []byte, meth uint16, crc uint32) {
	crc = crc32.ChecksumIEEE(data)
	if len(data) < 128 || v.cfg.stores(name) {
		return data, zip.Store, crc
	}
	var buf bytes.Buffer
	fw, _ := flate.NewWriter(&buf, flate.BestSpeed)
	fw.Write(data)
	fw.Close()
	if buf.Len() >= len(data) {
		return data, zip.Store, crc
	}
	return buf.Bytes(), zip.Deflate, crc
}

func (b *batch) append(m *member, stored []byte, crc uint32) error {
	hdr := &zip.FileHeader{
		Name: m.path, Method: m.meth, CRC32: crc,
		CompressedSize64: uint64(len(stored)), UncompressedSize64: uint64(m.size),
		Modified: time.Unix(0, m.mtime),
	}
	w, err := b.zw.CreateRaw(hdr)
	if err != nil {
		return err
	}
	if err := b.zw.Flush(); err != nil {
		return err
	}
	m.off = b.cw.n
	m.csize = int64(len(stored))
	if _, err := w.Write(stored); err != nil {
		return err
	}
	b.bytes += int64(len(stored))
	return nil
}

// packAdd appends a member to the open pack, sealing the pack when it is
// full, and returns the batch to wait on. It never holds a gate slot: it only
// waits for room in the commit pipeline.
func (v *Volume) packAdd(ctx context.Context, m *member, stored []byte, crc uint32) (*batch, error) {
	for {
		v.batchMu.Lock()
		if v.closed.Load() {
			v.batchMu.Unlock()
			return nil, ErrClosed
		}
		// An older version of this name in a pack that is already sealed must be
		// committed first. Checked here, under the lock that also puts the member
		// into a pack, so no pack can seal in between and let the two commit in
		// either order: a commit applies its records over whatever the index holds.
		if w := v.sealedHolderLocked(m.path); w != nil {
			v.batchMu.Unlock()
			select {
			case <-w.done:
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-v.ctx.Done():
				return nil, ErrClosed
			}
			continue
		}
		if b := v.cur; b != nil {
			if len(b.mem) > 0 && (b.bytes+int64(len(stored)) > v.cfg.PackMaxBytes || len(b.mem) >= v.cfg.PackMaxFiles) {
				v.sealLocked(b)
				v.batchMu.Unlock()
				continue
			}
			if err := b.append(m, stored, crc); err != nil {
				// A write error to the spool poisons the pack: fail it.
				v.sealLocked(b)
				v.batchMu.Unlock()
				return nil, err
			}
			if i, dup := b.byPath[m.path]; dup {
				b.mem[i].superseded = true
			}
			b.byPath[m.path] = len(b.mem)
			b.mem = append(b.mem, m)
			if len(b.mem) == 1 {
				b.first = time.Now()
				b.max = time.AfterFunc(v.cfg.SealMax, func() { v.sealIf(b) })
				b.quiet = time.AfterFunc(v.cfg.SealQuiet, func() { v.sealIf(b) })
			} else {
				b.quiet.Reset(v.cfg.SealQuiet)
			}
			if b.bytes >= v.cfg.PackMaxBytes || len(b.mem) >= v.cfg.PackMaxFiles {
				v.sealLocked(b)
			}
			v.batchMu.Unlock()
			return b, nil
		}
		v.batchMu.Unlock()
		// Need a new pack: wait for a pipeline slot without holding batchMu.
		select {
		case v.slots <- struct{}{}:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		v.batchMu.Lock()
		if v.cur == nil {
			nb, err := v.newBatch()
			if err != nil {
				v.batchMu.Unlock()
				<-v.slots
				return nil, err
			}
			v.cur = nb
			v.inflight[nb] = struct{}{}
		} else {
			<-v.slots
		}
		v.batchMu.Unlock()
	}
}

// sealedHolderLocked returns a sealed pack, still in commit, that holds a live
// version of p. batchMu held.
func (v *Volume) sealedHolderLocked(p string) *batch {
	for b := range v.inflight {
		if b == v.cur {
			continue
		}
		for _, m := range b.mem {
			if m.path == p && !m.superseded {
				return b
			}
		}
	}
	return nil
}

func (v *Volume) sealIf(b *batch) {
	v.batchMu.Lock()
	if v.cur == b {
		v.sealLocked(b)
	}
	v.batchMu.Unlock()
}

// sealLocked closes b for new members and starts its commit. batchMu held.
func (v *Volume) sealLocked(b *batch) {
	if v.cur == b {
		v.cur = nil
	}
	if b.sealed {
		return
	}
	b.sealed = true
	if b.quiet != nil {
		b.quiet.Stop()
	}
	if b.max != nil {
		b.max.Stop()
	}
	v.wg.Add(1)
	go func() {
		defer v.wg.Done()
		v.commit(b)
	}()
}

// flushBatches seals the open pack and waits for every pack in commit.
func (v *Volume) flushBatches(ctx context.Context) error {
	v.batchMu.Lock()
	if v.cur != nil {
		v.sealLocked(v.cur)
	}
	var bs []*batch
	for b := range v.inflight {
		bs = append(bs, b)
	}
	v.batchMu.Unlock()
	var first error
	for _, b := range bs {
		select {
		case <-b.done:
			if b.err != nil && first == nil {
				first = b.err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return first
}

// waitConflicts forces the packs holding any path for which match is true to
// commit, and waits for them. Metadata operations call it first so that the
// log order of a rename or delete and an earlier write to the same name is the
// order in which the caller issued them.
func (v *Volume) waitConflicts(ctx context.Context, match func(string) bool) error {
	v.batchMu.Lock()
	var wait []*batch
	for b := range v.inflight {
		hit := false
		for _, m := range b.mem {
			if !m.superseded && match(m.path) {
				hit = true
				break
			}
		}
		if hit {
			wait = append(wait, b)
			if v.cur == b {
				v.sealLocked(b)
			}
		}
	}
	v.batchMu.Unlock()
	for _, b := range wait {
		select {
		case <-b.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return v.waitStaged(ctx, match)
}

func (v *Volume) finishBatch(b *batch, err error) {
	b.err = err
	b.f.Close()
	os.Remove(b.spool)
	v.batchMu.Lock()
	delete(v.inflight, b)
	v.batchMu.Unlock()
	close(b.done)
	<-v.slots
}

// commit makes a sealed pack durable and visible. The order is fixed:
//
//  1. upload the pack            (crash point pack_uploaded)
//  2. verify it on the drive     (crash point pack_verified)
//  3. upload the log segment     (crash point log_uploaded)
//  4. mark applied locally       (crash point local_marked)
//
// so the log never refers to a pack that isn't there. A crash between 1 and 3
// leaves an unreferenced pack (collected after a grace period) and nothing
// acknowledged; a crash after 3 is repaired at the next start by applying the
// segment found on the drive. Names are deterministic (epoch, seq), and the
// sequence number is reserved durably before the pack is uploaded.
func (v *Volume) commit(b *batch) {
	err := v.commitInner(b)
	if err != nil {
		v.logf("cfs: commit of pack %s failed: %v", b.pack, err)
	}
	v.finishBatch(b, err)
	if err != nil {
		v.stageFailed(b, err)
	}
	if err == nil && v.cfg.CheckpointEvery > 0 && v.segsNew.Add(1) >= int64(v.cfg.CheckpointEvery) {
		v.maybeCheckpoint()
	}
}

func (v *Volume) commitInner(b *batch) (err error) {
	ctx := v.ctx
	if err := v.writable(); err != nil {
		return err
	}
	// Finish the zip: the self-describing manifest member, then the directory.
	var live []*member
	for _, m := range b.mem {
		if !m.superseded && m.state.CompareAndSwap(memberPending, memberLocked) {
			live = append(live, m)
		}
	}
	if len(live) == 0 {
		return nil
	}
	v.ckptMu.RLock()
	defer v.ckptMu.RUnlock()
	// The sequence number and the metadata rows that ride in this segment are
	// fixed together, before anything is uploaded (see sealPending). From here
	// on the rows belong to this segment name for good: if the commit fails they
	// are uploaded under the same name by the metadata flush, never under a later
	// one.
	seq, pend, err := v.ix.sealPending(v.epoch, true, false)
	if err != nil {
		return err
	}
	segKey := segKeyOf(v.epoch, seq)
	landed := false
	defer func() {
		if err != nil && !landed {
			v.releaseInflight(segKey)
		}
	}()
	b.seq = seq
	b.pack = packName(v.epoch, seq)
	// Journal the removal of the loose files these members will shadow, before
	// anything is uploaded: whichever step a crash interrupts, the next start
	// removes them once the packed file is in the index (finishInterrupted). A
	// key whose commit never happened does nothing: there is no packed file.
	if err := v.ix.tx(func(tx *sql.Tx) error {
		for _, m := range live {
			if m.strayBig {
				if err := kvSet(tx, strayKey+m.path, "1"); err != nil {
					return err
				}
			}
		}
		return nil
	}); err != nil {
		return err
	}
	pm := packManifest{Pack: b.pack, Epoch: v.epoch, Seq: seq}
	for _, m := range live {
		pm.Members = append(pm.Members, metalog.Record{Op: metalog.OpPut, Path: m.path, Size: m.size, MTime: m.mtime,
			SHA1: m.sha1, Pack: b.pack, Off: m.off, CSize: m.csize, Meth: m.meth})
	}
	mj, _ := json.Marshal(pm)
	mw, err := b.zw.CreateHeader(&zip.FileHeader{Name: manifestName, Method: zip.Deflate})
	if err != nil {
		return err
	}
	if _, err := mw.Write(mj); err != nil {
		return err
	}
	if err := b.zw.Close(); err != nil {
		return err
	}
	if err := b.f.Sync(); err != nil {
		return err
	}
	size := b.cw.n
	packSHA := hex.EncodeToString(b.sum.Sum(nil))

	if err := v.checkLease(ctx); err != nil {
		return err
	}
	pp := CtlDir + "/packs/" + b.pack
	if err := v.st.Put(ctx, pp, store.File(b.spool), store.PutOpts{Size: size, SHA1: packSHA, ModTime: time.Now(), Unique: true}); err != nil {
		return fmt.Errorf("upload pack: %w", err)
	}
	crash.Point("pack_uploaded")
	e, err := v.st.Stat(ctx, pp)
	if err != nil {
		return fmt.Errorf("verify pack: %w", err)
	}
	if e.Size != size {
		return fmt.Errorf("verify pack: drive has %d bytes, uploaded %d", e.Size, size)
	}
	if e.SHA1 != "" && e.SHA1 != packSHA {
		return fmt.Errorf("verify pack: drive sha1 %s, uploaded %s", e.SHA1, packSHA)
	}
	crash.Point("pack_verified")

	for _, m := range live {
		if r, ok, err := v.ix.getCommitted(m.path); err == nil {
			m.prev, m.hadPrev = r, ok
		}
	}
	seg := metalog.Segment{Epoch: v.epoch, Seq: seq, Pack: b.pack, PackBytes: size, PackSHA1: packSHA, PackN: len(live)}
	for _, p := range pend {
		seg.Records = append(seg.Records, p.Rec)
	}
	puts := make([]metalog.Record, 0, len(live))
	for _, m := range live {
		r := metalog.Record{Op: metalog.OpPut, Path: m.path, Size: m.size, MTime: m.mtime, SHA1: m.sha1,
			Pack: b.pack, Off: m.off, CSize: m.csize, Meth: m.meth, Expect: m.expect}
		puts = append(puts, r)
	}
	seg.Records = append(seg.Records, puts...)
	if err := v.appendSegment(ctx, seg); err != nil {
		return err
	}
	landed = true
	crash.Point("log_uploaded")
	err = v.ix.tx(func(tx *sql.Tx) error {
		for _, r := range puts {
			if err := applyRecord(tx, r, seq); err != nil {
				return err
			}
		}
		for _, m := range live {
			if m.id != 0 {
				if _, err := tx.Exec("DELETE FROM staged WHERE path=? AND id=?", m.path, m.id); err != nil {
					return err
				}
			}

		}
		if _, err := tx.Exec("INSERT OR REPLACE INTO packs(name,epoch,seq,bytes,sha1,members,dead_at) VALUES(?,?,?,?,?,?,0)",
			b.pack, v.epoch, seq, size, packSHA, len(live)); err != nil {
			return err
		}
		if _, err := tx.Exec("DELETE FROM pending WHERE seg=?", segKey); err != nil {
			return err
		}
		_, err := tx.Exec("DELETE FROM inflight WHERE name=?", segKey)
		return err
	})
	if err != nil {
		return err
	}
	crash.Point("local_marked")
	for _, m := range live {
		if m.strayBig {
			if err := v.st.Remove(ctx, m.path); err != nil && !isNotFound(err) {
				v.logf("cfs: remove shadowed large file %s: %v (will be repeated at the next start)", m.path, err)
			} else {
				v.ix.db.Exec("DELETE FROM kv WHERE k=?", strayKey+m.path)
			}
		}
		v.dc.cacheDel(m.path)
	}
	v.stageDone(live)
	return nil
}

// segmentTries is how often a commit uploads one and the same segment before it
// gives up on it.
const segmentTries = 4

// appendSegment uploads seg. A failed upload may still have landed (the answer
// was lost), so after a failure the drive is asked what it holds under that
// name; when that isn't conclusive the very same segment is uploaded again.
// Uploading a segment is idempotent. What must never happen is that the
// records of seg reappear under a later sequence number: they could then be
// replayed after writes that came after them.
func (v *Volume) appendSegment(ctx context.Context, seg metalog.Segment) error {
	var err error
	for try := 0; try < segmentTries; try++ {
		if try > 0 {
			select {
			case <-time.After(time.Duration(200<<(try-1)) * time.Millisecond):
			case <-ctx.Done():
				return err
			}
		}
		if err = v.log.AppendSegment(ctx, seg); err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return err
		}
		if v.segmentLanded(ctx, seg) {
			v.logf("cfs: log segment %d-%d landed although its upload reported: %v", seg.Epoch, seg.Seq, err)
			return nil
		}
	}
	return err
}

// segmentLanded reports whether the drive holds exactly seg.
func (v *Volume) segmentLanded(ctx context.Context, seg metalog.Segment) bool {
	refs, err := v.log.ListSegments(ctx)
	if err != nil {
		return false
	}
	for _, r := range refs {
		if r.Epoch != seg.Epoch || r.Seq != seg.Seq {
			continue
		}
		got, err := v.log.ReadSegment(ctx, r)
		if err != nil || got.Pack != seg.Pack || len(got.Records) != len(seg.Records) {
			return false
		}
		for i := range got.Records {
			if got.Records[i] != seg.Records[i] {
				return false
			}
		}
		return true
	}
	return false
}

// releaseInflight gives up the commit of a segment that did not (verifiably)
// reach the drive. Its pending metadata rows stay sealed under the SAME segment
// key and are sent again, under that name, by the metadata flush: the upload
// is idempotent, and a record is never re-emitted under a later sequence number
// where it could be replayed after the writes that came after it. The retry
// carries the metadata rows only: the puts of the failed commit were either
// reported to their callers as failed (AckDurable) or are staged and go out
// again in a later pack (AckStaged), and a copy of the segment that landed
// although the call failed is overwritten by the retry.
func (v *Volume) releaseInflight(segKey string) {
	if _, err := v.ix.db.Exec("DELETE FROM inflight WHERE name=?", segKey); err != nil {
		v.logf("cfs: release %s: %v", segKey, err)
		return
	}
	v.kickMeta()
}

func packName(epoch, seq uint64) string { return fmt.Sprintf("p-%06d-%010d.zip", epoch, seq) }

func segKeyOf(epoch, seq uint64) string { return fmt.Sprintf("%d-%d", epoch, seq) }

func parseSegKey(k string) (epoch, seq uint64, ok bool) {
	e, s, found := strings.Cut(k, "-")
	if !found {
		return 0, 0, false
	}
	_, err1 := fmt.Sscanf(e, "%d", &epoch)
	_, err2 := fmt.Sscanf(s, "%d", &seq)
	return epoch, seq, err1 == nil && err2 == nil
}

// abandonBatch drops the pack that is still filling when the volume closes
// without having been able to flush it (the drive refused). Whatever went into
// it is staged (it is replayed at the next start) or was never acknowledged.
func (v *Volume) abandonBatch() {
	v.batchMu.Lock()
	defer v.batchMu.Unlock()
	b := v.cur
	if b == nil {
		return
	}
	v.cur = nil
	b.sealed = true
	if b.quiet != nil {
		b.quiet.Stop()
	}
	if b.max != nil {
		b.max.Stop()
	}
	b.f.Close()
	os.Remove(b.spool)
	delete(v.inflight, b)
	close(b.done)
}

// strayKey prefixes the kv rows that say a large file of that name still has to
// be removed because a packed file of the same name has replaced it.
const strayKey = "stray:"
