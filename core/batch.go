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
	superseded bool
}

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
	return nil
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
	if err == nil && v.cfg.CheckpointEvery > 0 && v.segsNew.Add(1) >= int64(v.cfg.CheckpointEvery) {
		v.maybeCheckpoint()
	}
}

func (v *Volume) commitInner(b *batch) error {
	ctx := v.ctx
	if err := v.writable(); err != nil {
		return err
	}
	// Finish the zip: the self-describing manifest member, then the directory.
	var live []*member
	for _, m := range b.mem {
		if !m.superseded {
			live = append(live, m)
		}
	}
	if len(live) == 0 {
		return nil
	}
	v.ckptMu.RLock()
	defer v.ckptMu.RUnlock()
	seq, err := v.ix.allocSeq()
	if err != nil {
		return err
	}
	b.seq = seq
	b.pack = packName(v.epoch, seq)
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
	// Directories of all members must exist before the pack lands: the log
	// says the files are there, and listings come from the drive.
	seen := map[string]bool{}
	for _, m := range live {
		d := parentOf(m.path)
		if !seen[d] {
			seen[d] = true
			if err := v.mkdirAll(ctx, d); err != nil {
				return fmt.Errorf("create directory %q: %w", d, err)
			}
		}
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

	// Take the pending metadata operations along: they go into this segment
	// ahead of the puts. Sealing and the in-flight marker are one transaction.
	segKey := segKeyOf(v.epoch, seq)
	var pend []pendingRow
	err = v.ix.tx(func(tx *sql.Tx) error {
		var err error
		if pend, err = readPending(tx, "WHERE seg IS NULL"); err != nil {
			return err
		}
		if _, err = tx.Exec("UPDATE pending SET seg=? WHERE seg IS NULL", segKey); err != nil {
			return err
		}
		if _, err = tx.Exec("INSERT OR IGNORE INTO segs(name) VALUES(?)", segKey); err != nil {
			return err
		}
		_, err = tx.Exec("INSERT OR IGNORE INTO inflight(name) VALUES(?)", segKey)
		return err
	})
	if err != nil {
		return err
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
	if err := v.log.AppendSegment(ctx, seg); err != nil {
		v.unseal(segKey)
		return err
	}
	crash.Point("log_uploaded")
	err = v.ix.tx(func(tx *sql.Tx) error {
		for _, r := range puts {
			if err := applyRecord(tx, r, seq); err != nil {
				return err
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
				v.logf("cfs: remove shadowed large file %s: %v", m.path, err)
			}
		}
		v.dc.cacheDel(m.path)
	}
	return nil
}

// unseal gives the pending rows of a segment that failed to upload back to
// the unsealed queue.
func (v *Volume) unseal(segKey string) {
	err := v.ix.tx(func(tx *sql.Tx) error {
		if _, err := tx.Exec("UPDATE pending SET seg=NULL WHERE seg=?", segKey); err != nil {
			return err
		}
		if _, err := tx.Exec("DELETE FROM segs WHERE name=?", segKey); err != nil {
			return err
		}
		_, err := tx.Exec("DELETE FROM inflight WHERE name=?", segKey)
		return err
	})
	if err != nil {
		v.logf("cfs: unseal %s: %v", segKey, err)
	}
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
