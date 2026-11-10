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

// checkAncestors rejects p when one of its ancestors is a file, packed or large.
func (v *Volume) checkAncestors(ctx context.Context, p string) error {
	for d := parentOf(p); d != ""; d = parentOf(d) {
		if _, ok, err := v.ix.get(d); err != nil {
			return err
		} else if ok {
			return store.ErrNotDir
		}
		if isDir, err := v.ix.isDir(d); err != nil {
			return err
		} else if isDir {
			return nil // a directory, and so are all its ancestors
		}
		if big, ok, err := v.bigAt(ctx, d); err != nil {
			return err
		} else if ok && !big.IsDir {
			return store.ErrNotDir
		}
	}
	return nil
}

// checkShape rejects a file at p when an ancestor is a file or p is a
// directory.
func (v *Volume) checkShape(ctx context.Context, p string) error {
	if err := v.checkAncestors(ctx, p); err != nil {
		return err
	}
	if any, err := v.ix.anyUnder(p); err != nil {
		return err
	} else if any {
		return store.ErrIsDir
	}
	if isDir, err := v.ix.isDir(p); err != nil {
		return err
	} else if isDir {
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
// below the threshold goes into a pack. With AckStaged (the default) Put
// returns once the data is fsynced in the local staging area, from where it is
// readable at once and packed and uploaded in the background; with AckDurable
// it returns once the pack and the log segment describing it are on the drive.
// A larger file is uploaded to its own path directly and Put returns when that
// is done.
func (v *Volume) Put(ctx context.Context, p string, r io.Reader, size int64, mtime time.Time) (Entry, error) {
	return v.PutWith(ctx, p, r, PutOpts{Size: size, MTime: mtime})
}

// PutOpts describe the file handed to PutWith.
type PutOpts struct {
	// Size is the exact length of the stream, or -1 if unknown.
	Size int64
	// MTime is the modification time; zero means now.
	MTime time.Time
	// SHA1 is the hex SHA-1 of the content when the caller already knows it. It
	// goes to the drive with a large file, which may then take the file without a
	// transfer (rapid upload). It is not checked here.
	SHA1 string
	// Ranged reads the same bytes as the stream by range, if the caller can. A
	// large file is then handed to the drive as that, and the stream is not read:
	// a drive that hashes and uploads by range makes no copy of the file. It is
	// only used when Size is known.
	Ranged store.RangeOpener
}

// PutWith is Put with the facts the caller knows about the file.
func (v *Volume) PutWith(ctx context.Context, p string, r io.Reader, o PutOpts) (Entry, error) {
	size, mtime := o.Size, o.MTime
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
	end, werr := v.beginWrite(ctx)
	if werr != nil {
		return Entry{}, werr
	}
	defer end()
	if mtime.IsZero() {
		mtime = time.Now()
	}
	if err := v.checkShape(ctx, p); err != nil {
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
	return v.putLarge(ctx, p, r, size, mtime, o.SHA1, o.Ranged)
}

func (v *Volume) putSmall(ctx context.Context, p string, data []byte, mtime time.Time, stray bool) (Entry, error) {
	if v.cfg.Ack == AckStaged {
		return v.putStaged(ctx, p, data, mtime, stray)
	}
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
		if m.state.CompareAndSwap(memberPending, memberCancelled) {
			return Entry{}, ctx.Err()
		}
		// The commit already took the file along. The caller will treat this as
		// a failed write, so once it is through the write is taken back.
		<-b.done
		if b.err == nil {
			v.undoPut(m, b.pack)
		}
		return Entry{}, ctx.Err()
	}
	if m.state.Load() == memberCancelled {
		return Entry{}, ctx.Err()
	}
	if b.err != nil {
		return Entry{}, b.err
	}
	return Entry{Path: p, Size: m.size, ModTime: time.Unix(0, m.mtime), Packed: true, SHA1: m.sha1}, nil
}

func (v *Volume) putLarge(ctx context.Context, p string, r io.Reader, size int64, mtime time.Time, sha1 string, ranged store.RangeOpener) (Entry, error) {
	// A small version of the name that is still staged has to be in the log
	// before this write removes it (below).
	if err := v.waitConflicts(ctx, func(q string) bool { return q == p }); err != nil {
		return Entry{}, err
	}
	if err := v.mkdirAll(ctx, parentOf(p)); err != nil {
		return Entry{}, err
	}
	// If a small version of the name exists, the upload makes it obsolete, and
	// the index has to follow: journal that, so a crash between the upload and
	// the index change doesn't leave the small file shadowing the new one.
	_, hadSmall, err := v.ix.get(p)
	if err != nil {
		return Entry{}, err
	}
	if hadSmall {
		if err := v.ix.kvSet(putBigKey+p, "1"); err != nil {
			return Entry{}, err
		}
	}
	var src store.Source = store.Once(r)
	if rs, ok := r.(io.ReadSeeker); ok {
		if ss, err := store.Seekable(rs); err == nil {
			src = ss // a seekable stream can be uploaded again after a transient failure
		}
	}
	if size < 0 {
		ranged = nil
	}
	if ranged != nil {
		src = store.Ranged(ranged) // the stream is left unread, and a retry can read again
	}
	if err := v.st.Put(ctx, p, src, store.PutOpts{Size: size, ModTime: mtime, SHA1: sha1, Ranged: ranged}); err != nil {
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
	if hadSmall {
		v.ix.db.Exec("DELETE FROM kv WHERE k=?", putBigKey+p)
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
	end, werr := v.beginWrite(ctx)
	if werr != nil {
		return werr
	}
	defer end()
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
		if isDir, err := v.ix.isDir(p); err != nil {
			return err
		} else if isDir {
			return store.ErrIsDir
		}
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
	end, werr := v.beginWrite(ctx)
	if werr != nil {
		return werr
	}
	defer end()
	if src == dst {
		if _, err := v.Stat(ctx, src); err != nil {
			return err
		}
		return nil
	}
	if err := v.waitConflicts(ctx, func(q string) bool { return q == src || q == dst }); err != nil {
		return err
	}
	if err := v.checkShape(ctx, dst); err != nil {
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
	if srcIsBig {
		if err := v.mkdirAll(ctx, parentOf(dst)); err != nil {
			return err
		}
	}
	dstBig, dstIsBig, err := v.bigAt(ctx, dst)
	if err != nil {
		return err
	}
	if dstIsBig && dstBig.IsDir {
		return store.ErrIsDir
	}
	_ = srcRow
	// The move takes several steps on the drive and in the index. Journal it
	// first: after a crash in the middle, the next start completes it.
	if err := v.setMvIntent(src, dst); err != nil {
		return err
	}
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
	v.clearMvIntent(dst)
	return nil
}

// mvIntent is a Move that has started.
type mvIntent struct{ Src, Dst string }

// mvKey prefixes the kv rows that journal moves in progress, one per target.
const mvKey = "mv:"

func (v *Volume) setMvIntent(src, dst string) error {
	b, _ := json.Marshal(mvIntent{src, dst})
	return v.ix.kvSet(mvKey+dst, string(b))
}

func (v *Volume) clearMvIntent(dst string) {
	if _, err := v.ix.db.Exec("DELETE FROM kv WHERE k=?", mvKey+dst); err != nil {
		v.logf("cfs: clear move journal for %s: %v", dst, err)
	}
}

// finishMove carries an interrupted Move to its end. Every step is safe to
// repeat: a source that is already gone means the move is done.
func (v *Volume) finishMove(ctx context.Context, in mvIntent) error {
	if _, ok, err := v.ix.getCommitted(in.Src); err != nil {
		return err
	} else if ok { // a small source
		if e, ok, err := v.bigAt(ctx, in.Dst); err != nil {
			return err
		} else if ok && !e.IsDir {
			if err := v.st.Remove(ctx, in.Dst); err != nil && !isNotFound(err) {
				return err
			}
			v.dc.cacheDel(in.Dst)
		}
		return v.localOp(metalog.Record{Op: metalog.OpMv, Path: in.Src, To: in.Dst})
	}
	srcBig, ok, err := v.bigAt(ctx, in.Src)
	if err != nil {
		return err
	} else if !ok || srcBig.IsDir { // gone: it was done
		return nil
	}
	if _, ok, err := v.ix.getCommitted(in.Dst); err != nil {
		return err
	} else if ok {
		if err := v.localOp(metalog.Record{Op: metalog.OpDel, Path: in.Dst}); err != nil {
			return err
		}
	}
	if e, ok, err := v.bigAt(ctx, in.Dst); err != nil {
		return err
	} else if ok && !e.IsDir {
		if err := v.st.Remove(ctx, in.Dst); err != nil && !isNotFound(err) {
			return err
		}
		v.dc.cacheDel(in.Dst)
	}
	if err := v.mkdirAll(ctx, parentOf(in.Dst)); err != nil {
		return err
	}
	if err := v.st.Move(ctx, in.Src, in.Dst); err != nil {
		return err
	}
	// The looks above (bigAt) filled the listing cache with both folders as they
	// were before the move: bring it up to date, as Move does.
	v.dc.cacheDel(in.Src)
	ne := srcBig
	ne.Path = in.Dst
	v.dc.cacheAdd(ne)
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
	end, werr := v.beginWrite(ctx)
	if werr != nil {
		return werr
	}
	defer end()
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
	end, werr := v.beginWrite(ctx)
	if werr != nil {
		return werr
	}
	defer end()
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
	if err := v.checkShape(ctx, dst); err != nil {
		return err
	}
	rec := metalog.Record{Op: metalog.OpDmv, Path: src, To: dst}
	// The drive only moves a folder it has: a tree of small files and empty
	// directories exists in the index alone.
	onDriveTree, err := v.ix.anyDDirUnder(src)
	if err != nil {
		return err
	}
	if onDriveTree {
		if err := v.mkdirAll(ctx, parentOf(dst)); err != nil {
			return err
		}
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
	}
	err = v.ix.tx(func(tx *sql.Tx) error {
		if err := applyRecord(tx, rec, 0); err != nil {
			return err
		}
		if err := addPending(tx, rec); err != nil {
			return err
		}
		if onDriveTree {
			if err := moveDDirs(tx, src, dst); err != nil {
				return err
			}
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
	if onDriveTree {
		v.dc.cacheAdd(store.Entry{Path: dst, IsDir: true, ModTime: time.Now()})
	}
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
	if v.closed.Load() || v.closing.Load() {
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
	if v.ReadOnly() { // a reader has handed back everything it wrote
		return nil
	}
	if err := v.writable(); err != nil {
		return err
	}
	// The packs waiting for their segment come first, so that no segment of a
	// later commit is written before theirs; their sealed rows go with them.
	if err := v.logDeferred(ctx); err != nil {
		return err
	}
	left, err := readPending(v.ix.db, "WHERE seg IS NOT NULL AND seg NOT IN (SELECT name FROM inflight) AND seg NOT IN (SELECT name FROM deferred)")
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
	v.ckptMu.RLock()
	defer v.ckptMu.RUnlock()
	seq, rows, err := v.ix.sealPending(v.epoch, false, true)
	if err != nil || len(rows) == 0 {
		return err
	}
	return v.uploadMetaSeg(ctx, segKeyOf(v.epoch, seq), rows)
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
	size, err := v.log.AppendSegment(ctx, seg)
	if err != nil {
		return err
	}
	crash.Point("meta_log_uploaded")
	if _, err := v.ix.db.Exec("DELETE FROM pending WHERE seg=?", key); err != nil {
		return err
	}
	v.segLogged(size)
	return nil
}

// RemoveAll deletes a directory with everything below it: packed files by a
// single logged record, large files and directories with as few calls to the
// drive as it allows (one purge where it has one).
func (v *Volume) RemoveAll(ctx context.Context, dir string) error {
	dir, err := clean(dir)
	if err != nil {
		return err
	}
	if dir == "" || reserved(dir) {
		return ErrReserved
	}
	end, werr := v.beginWrite(ctx)
	if werr != nil {
		return werr
	}
	defer end()
	v.structMu.Lock()
	defer v.structMu.Unlock()
	if err := v.waitConflicts(ctx, func(q string) bool { return q == dir || strings.HasPrefix(q, dir+"/") }); err != nil {
		return err
	}
	e, err := v.Stat(ctx, dir)
	if err != nil {
		return err
	}
	if !e.IsDir {
		return store.ErrNotDir
	}
	any, err := v.ix.anyUnder(dir)
	if err != nil {
		return err
	}
	if !any {
		if any, err = v.ix.anyDirsUnder(dir); err != nil {
			return err
		}
	}
	if !any {
		if _, any, err = v.ix.dirMTime(dir); err != nil {
			return err
		}
	}
	if any {
		if err := v.localOp(metalog.Record{Op: metalog.OpRmTree, Path: dir}); err != nil {
			return err
		}
	}
	if v.ix.hasDDir(dir) {
		err = v.st.Purge(ctx, dir)
		if errors.Is(err, store.ErrNotSupported) {
			err = v.purgeStore(ctx, dir)
		}
		if err != nil && !isNotFound(err) {
			return err
		}
		if err := v.ix.tx(func(tx *sql.Tx) error { return dropDDirs(tx, dir) }); err != nil {
			return err
		}
	}
	v.dc.dropTree(dir)
	v.dc.cacheDel(dir)
	return nil
}

func (v *Volume) purgeStore(ctx context.Context, dir string) error {
	ents, err := v.st.List(ctx, dir)
	if err != nil {
		return err
	}
	for _, e := range ents {
		if e.IsDir {
			if err := v.purgeStore(ctx, e.Path); err != nil {
				return err
			}
		} else if err := v.st.Remove(ctx, e.Path); err != nil && !isNotFound(err) {
			return err
		}
	}
	return v.st.Rmdir(ctx, dir)
}

// undoPut takes back a small-file write that was committed after its caller
// had already given up (context cancelled): a caller that saw an error must not
// find the file changed. The previous version, if any, is put back; the record
// only applies while the path still points at the write being undone.
func (v *Volume) undoPut(m *member, pack string) {
	exp := fmt.Sprintf("%s@%d", pack, m.off)
	rec := metalog.Record{Op: metalog.OpDel, Path: m.path, Expect: exp}
	if m.hadPrev {
		p := m.prev
		rec = metalog.Record{Op: metalog.OpPut, Path: m.path, Size: p.Size, MTime: p.MTime, SHA1: p.SHA1,
			Pack: p.Pack, Off: p.Off, CSize: p.CSize, Meth: p.Meth, Expect: exp}
	}
	if err := v.localOp(rec); err != nil {
		v.logf("cfs: take back cancelled write of %s: %v", m.path, err)
		return
	}
	v.dc.cacheDel(m.path)
}

// putBigKey prefixes the kv rows that say a large file is being uploaded over a
// name that has a small version.
const putBigKey = "putbig:"
