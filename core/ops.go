package core

import (
	"bytes"
	"context"
	"crypto/sha1"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/Ironboxplus/cfs/internal/crash"
	"github.com/Ironboxplus/cfs/metalog"
	"github.com/Ironboxplus/cfs/store"
)

// checkShape rejects a file at p when an ancestor is a packed file or p is a
// directory that holds packed files.
func (v *Volume) checkShape(p string) error {
	for d := parentOf(p); d != ""; d = parentOf(d) {
		if _, ok, err := v.ix.get(d); err != nil {
			return err
		} else if ok {
			return store.ErrNotDir
		}
	}
	if any, err := v.ix.anyUnder(p); err != nil {
		return err
	} else if any {
		return store.ErrIsDir
	}
	return nil
}

// bigAt reports the entry the drive lists at p, if any.
func (v *Volume) bigAt(ctx context.Context, p string) (store.Entry, bool, error) {
	dir, name := splitPath(p)
	ents, err := v.listStore(ctx, dir)
	if err != nil {
		if isNotFound(err) {
			return store.Entry{}, false, nil
		}
		return store.Entry{}, false, err
	}
	e, ok := ents[name]
	return e, ok, nil
}

// Put writes a file. size is the exact length of r, or -1 if unknown. A file
// below the threshold goes into a pack and Put returns once the pack and the
// log segment describing it are durable on the drive (and are visible); a
// larger file is uploaded to its own path directly. Put returns only when the
// data is where it will be read from.
func (v *Volume) Put(ctx context.Context, p string, r io.Reader, size int64, mtime time.Time) (Entry, error) {
	p, err := clean(p)
	if err != nil {
		return Entry{}, err
	}
	if p == "" {
		return Entry{}, store.ErrIsDir
	}
	if reserved(p) {
		return Entry{}, ErrReserved
	}
	if err := v.writable(); err != nil {
		return Entry{}, err
	}
	if mtime.IsZero() {
		mtime = time.Now()
	}
	if err := v.checkShape(p); err != nil {
		return Entry{}, err
	}
	big, bigExists, err := v.bigAt(ctx, p)
	if err != nil {
		return Entry{}, err
	}
	if bigExists && big.IsDir {
		return Entry{}, store.ErrIsDir
	}
	cfg := &v.cfg
	shrinkMin := int64(float64(cfg.Threshold) * cfg.Shrink)
	var head []byte
	small := false
	switch {
	case size >= cfg.Threshold, size >= 0 && bigExists && size >= shrinkMin:
		// stays a file of its own
	case size >= 0:
		small = true
		head = make([]byte, size)
		if _, err := io.ReadFull(r, head); err != nil {
			if errors.Is(err, io.EOF) {
				err = io.ErrUnexpectedEOF
			}
			return Entry{}, err
		}
	default: // unknown length: read up to the threshold to find out
		head, err = io.ReadAll(io.LimitReader(r, cfg.Threshold))
		if err != nil {
			return Entry{}, err
		}
		if int64(len(head)) >= cfg.Threshold {
			r = io.MultiReader(bytes.NewReader(head), r)
		} else {
			size = int64(len(head))
			if bigExists && size >= shrinkMin {
				r = bytes.NewReader(head)
			} else {
				small = true
			}
		}
	}
	if small {
		return v.putSmall(ctx, p, head, mtime, bigExists)
	}
	return v.putLarge(ctx, p, r, size, mtime)
}

func (v *Volume) putSmall(ctx context.Context, p string, data []byte, mtime time.Time, stray bool) (Entry, error) {
	sum := sha1.Sum(data)
	m := &member{path: p, size: int64(len(data)), mtime: mtime.UnixNano(), sha1: hex.EncodeToString(sum[:]), strayBig: stray}
	stored, meth, crc := v.encodeMember(p, data)
	m.meth = meth
	v.structMu.RLock()
	b, err := v.packAdd(ctx, m, stored, crc)
	v.structMu.RUnlock()
	if err != nil {
		return Entry{}, err
	}
	select {
	case <-b.done:
	case <-ctx.Done():
		return Entry{}, ctx.Err()
	}
	if b.err != nil {
		return Entry{}, b.err
	}
	return Entry{Path: p, Size: m.size, ModTime: time.Unix(0, m.mtime), Packed: true, SHA1: m.sha1}, nil
}

func (v *Volume) putLarge(ctx context.Context, p string, r io.Reader, size int64, mtime time.Time) (Entry, error) {
	if err := v.mkdirAll(ctx, parentOf(p)); err != nil {
		return Entry{}, err
	}
	if err := v.st.Put(ctx, p, store.Once(r), store.PutOpts{Size: size, ModTime: mtime}); err != nil {
		v.dc.cacheDel(p)
		return Entry{}, err
	}
	if size < 0 {
		e, err := v.st.Stat(ctx, p)
		if err != nil {
			return Entry{}, err
		}
		size = e.Size
	}
	// A small version of the same name would shadow the new file: log its removal.
	if _, ok, err := v.ix.get(p); err != nil {
		return Entry{}, err
	} else if ok {
		if err := v.localOp(metalog.Record{Op: metalog.OpDel, Path: p}); err != nil {
			return Entry{}, err
		}
	}
	e := store.Entry{Path: p, Size: size, ModTime: mtime}
	v.dc.cacheAdd(e)
	return bigEntry(e), nil
}

// localOp applies a metadata record to the index and queues it for the log in
// one transaction: when it returns, the change survives a crash of this
// process (SQLite synchronous=FULL) and will be logged to the drive soon.
func (v *Volume) localOp(recs ...metalog.Record) error {
	err := v.ix.tx(func(tx *sql.Tx) error {
		for _, r := range recs {
			if err := applyRecord(tx, r, 0); err != nil {
				return err
			}
			if err := addPending(tx, r); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	crash.Point("meta_fsynced")
	v.kickMeta()
	return nil
}

// Remove deletes a file.
func (v *Volume) Remove(ctx context.Context, p string) error {
	p, err := clean(p)
	if err != nil {
		return err
	}
	if p == "" || reserved(p) {
		return ErrReserved
	}
	if err := v.writable(); err != nil {
		return err
	}
	if err := v.waitConflicts(ctx, func(q string) bool { return q == p }); err != nil {
		return err
	}
	v.structMu.RLock()
	defer v.structMu.RUnlock()
	removed := false
	if _, ok, err := v.ix.get(p); err != nil {
		return err
	} else if ok {
		if err := v.localOp(metalog.Record{Op: metalog.OpDel, Path: p}); err != nil {
			return err
		}
		removed = true
	}
	big, ok, err := v.bigAt(ctx, p)
	if err != nil {
		return err
	}
	if ok {
		if big.IsDir {
			if removed {
				return nil
			}
			return store.ErrIsDir
		}
		if err := v.st.Remove(ctx, p); err != nil && !isNotFound(err) {
			return err
		}
		v.dc.cacheDel(p)
		removed = true
	}
	if !removed {
		return store.ErrNotFound
	}
	return nil
}

// Move renames a file. An existing file at dst is replaced.
func (v *Volume) Move(ctx context.Context, src, dst string) error {
	src, err := clean(src)
	if err != nil {
		return err
	}
	dst, err = clean(dst)
	if err != nil {
		return err
	}
	if src == "" || dst == "" || reserved(src) || reserved(dst) {
		return ErrReserved
	}
	if err := v.writable(); err != nil {
		return err
	}
	if src == dst {
		if _, err := v.Stat(ctx, src); err != nil {
			return err
		}
		return nil
	}
	if err := v.waitConflicts(ctx, func(q string) bool { return q == src || q == dst }); err != nil {
		return err
	}
	if err := v.checkShape(dst); err != nil {
		return err
	}
	v.structMu.RLock()
	defer v.structMu.RUnlock()
	srcRow, srcSmall, err := v.ix.get(src)
	if err != nil {
		return err
	}
	srcBig, srcIsBig, err := v.bigAt(ctx, src)
	if err != nil {
		return err
	}
	if srcIsBig && srcBig.IsDir {
		srcIsBig = false
	}
	if !srcSmall && !srcIsBig {
		return store.ErrNotFound
	}
	if err := v.mkdirAll(ctx, parentOf(dst)); err != nil {
		return err
	}
	dstBig, dstIsBig, err := v.bigAt(ctx, dst)
	if err != nil {
		return err
	}
	if dstIsBig && dstBig.IsDir {
		return store.ErrIsDir
	}
	_ = srcRow
	if srcSmall {
		if dstIsBig {
			if err := v.st.Remove(ctx, dst); err != nil && !isNotFound(err) {
				return err
			}
			v.dc.cacheDel(dst)
		}
		if err := v.localOp(metalog.Record{Op: metalog.OpMv, Path: src, To: dst}); err != nil {
			return err
		}
	}
	if srcIsBig {
		if _, ok, err := v.ix.get(dst); err != nil {
			return err
		} else if ok {
			if err := v.localOp(metalog.Record{Op: metalog.OpDel, Path: dst}); err != nil {
				return err
			}
		}
		if dstIsBig {
			if err := v.st.Remove(ctx, dst); err != nil && !isNotFound(err) {
				return err
			}
			v.dc.cacheDel(dst)
		}
		if err := v.st.Move(ctx, src, dst); err != nil {
			return err
		}
		v.dc.cacheDel(src)
		ne := srcBig
		ne.Path = dst
		v.dc.cacheAdd(ne)
	}
	return nil
}

// SetModTime sets the modification time of a file.
func (v *Volume) SetModTime(ctx context.Context, p string, t time.Time) error {
	p, err := clean(p)
	if err != nil {
		return err
	}
	if p == "" || reserved(p) {
		return ErrReserved
	}
	if err := v.writable(); err != nil {
		return err
	}
	if err := v.waitConflicts(ctx, func(q string) bool { return q == p }); err != nil {
		return err
	}
	if _, ok, err := v.ix.get(p); err != nil {
		return err
	} else if ok {
		return v.localOp(metalog.Record{Op: metalog.OpTouch, Path: p, MTime: t.UnixNano()})
	}
	big, ok, err := v.bigAt(ctx, p)
	if err != nil {
		return err
	}
	if !ok || big.IsDir {
		return store.ErrNotFound
	}
	if err := v.st.SetModTime(ctx, p, t); err != nil {
		return err
	}
	big.ModTime = t
	v.dc.cacheAdd(big)
	return nil
}

// DirMove renames a directory with everything in it. dst must not exist.
func (v *Volume) DirMove(ctx context.Context, src, dst string) error {
	src, err := clean(src)
	if err != nil {
		return err
	}
	dst, err = clean(dst)
	if err != nil {
		return err
	}
	if src == "" || dst == "" || reserved(src) || reserved(dst) {
		return ErrReserved
	}
	if err := v.writable(); err != nil {
		return err
	}
	if dst == src {
		return store.ErrExists
	}
	if strings.HasPrefix(dst, src+"/") {
		return fmt.Errorf("cfs: can't move %q into itself", src)
	}
	v.structMu.Lock()
	defer v.structMu.Unlock()
	match := func(q string) bool {
		return q == src || strings.HasPrefix(q, src+"/") || q == dst || strings.HasPrefix(q, dst+"/")
	}
	if err := v.waitConflicts(ctx, match); err != nil {
		return err
	}
	if e, err := v.Stat(ctx, src); err != nil {
		return err
	} else if !e.IsDir {
		return store.ErrNotDir
	}
	if _, err := v.Stat(ctx, dst); err == nil {
		return store.ErrExists
	} else if !isNotFound(err) {
		return err
	}
	if err := v.checkShape(dst); err != nil {
		return err
	}
	if err := v.mkdirAll(ctx, parentOf(dst)); err != nil {
		return err
	}
	rec := metalog.Record{Op: metalog.OpDmv, Path: src, To: dst}
	intent, _ := json.Marshal(rec)
	if err := v.ix.kvSet("dmv_intent", string(intent)); err != nil {
		return err
	}
	crash.Point("dirmove_intent")
	if err := v.st.DirMove(ctx, src, dst); err != nil {
		_ = v.ix.tx(func(tx *sql.Tx) error { _, e := tx.Exec("DELETE FROM kv WHERE k='dmv_intent'"); return e })
		return err
	}
	crash.Point("dirmove_done")
	err = v.ix.tx(func(tx *sql.Tx) error {
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
	v.dc.dropTree(src)
	v.dc.dropTree(dst)
	v.dc.cacheDel(src)
	v.dc.cacheAdd(store.Entry{Path: dst, IsDir: true, ModTime: time.Now()})
	v.kickMeta()
	return nil
}

// ---- metadata log ----

func (v *Volume) kickMeta() {
	if v.closed.Load() || v.ReadOnly() {
		return
	}
	v.metaMu.Lock()
	defer v.metaMu.Unlock()
	if v.metaQuiet == nil {
		v.metaMax = time.AfterFunc(v.cfg.SealMax, v.fireMeta)
		v.metaQuiet = time.AfterFunc(v.cfg.SealQuiet, v.fireMeta)
		return
	}
	v.metaQuiet.Reset(v.cfg.SealQuiet)
}

func (v *Volume) fireMeta() {
	v.metaMu.Lock()
	if v.metaQuiet != nil {
		v.metaQuiet.Stop()
		v.metaMax.Stop()
		v.metaQuiet, v.metaMax = nil, nil
	}
	v.metaMu.Unlock()
	if v.closed.Load() {
		return
	}
	v.wg.Add(1)
	go func() {
		defer v.wg.Done()
		if err := v.flushMeta(v.ctx); err != nil && !errors.Is(err, ErrClosed) && v.ctx.Err() == nil {
			v.logf("cfs: metadata log flush: %v (will retry)", err)
			time.AfterFunc(5*time.Second, v.kickMeta)
		}
	}()
}

// flushMeta uploads pending metadata operations as log segments: first the
// segments sealed by an earlier attempt (same names, same content), then one
// new segment for everything queued since.
func (v *Volume) flushMeta(ctx context.Context) error {
	v.metaFlush.Lock()
	defer v.metaFlush.Unlock()
	if err := v.writable(); err != nil {
		return err
	}
	left, err := readPending(v.ix.db, "WHERE seg IS NOT NULL AND seg NOT IN (SELECT name FROM inflight)")
	if err != nil {
		return err
	}
	groups := map[string][]pendingRow{}
	var order []string
	for _, p := range left {
		if _, ok := groups[p.Seg]; !ok {
			order = append(order, p.Seg)
		}
		groups[p.Seg] = append(groups[p.Seg], p)
	}
	for _, k := range order {
		if err := v.uploadMetaSeg(ctx, k, groups[k]); err != nil {
			return err
		}
	}
	n := 0
	if err := v.ix.db.QueryRow("SELECT count(*) FROM pending WHERE seg IS NULL").Scan(&n); err != nil {
		return err
	}
	if n == 0 {
		return nil
	}
	v.ckptMu.RLock()
	defer v.ckptMu.RUnlock()
	seq, err := v.ix.allocSeq()
	if err != nil {
		return err
	}
	key := segKeyOf(v.epoch, seq)
	var rows []pendingRow
	err = v.ix.tx(func(tx *sql.Tx) error {
		var err error
		if rows, err = readPending(tx, "WHERE seg IS NULL"); err != nil {
			return err
		}
		if _, err = tx.Exec("UPDATE pending SET seg=? WHERE seg IS NULL", key); err != nil {
			return err
		}
		_, err = tx.Exec("INSERT OR IGNORE INTO segs(name) VALUES(?)", key)
		return err
	})
	if err != nil || len(rows) == 0 {
		return err
	}
	return v.uploadMetaSeg(ctx, key, rows)
}

func (v *Volume) uploadMetaSeg(ctx context.Context, key string, rows []pendingRow) error {
	epoch, seq, ok := parseSegKey(key)
	if !ok {
		return fmt.Errorf("cfs: bad sealed segment key %q", key)
	}
	if err := v.checkLease(ctx); err != nil {
		return err
	}
	seg := metalog.Segment{Epoch: epoch, Seq: seq}
	for _, r := range rows {
		seg.Records = append(seg.Records, r.Rec)
	}
	if err := v.log.AppendSegment(ctx, seg); err != nil {
		return err
	}
	crash.Point("meta_log_uploaded")
	if _, err := v.ix.db.Exec("DELETE FROM pending WHERE seg=?", key); err != nil {
		return err
	}
	v.segsNew.Add(1)
	return nil
}
