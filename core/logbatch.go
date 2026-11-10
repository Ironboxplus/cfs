package core

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Ironboxplus/cfs/internal/crash"
	"github.com/Ironboxplus/cfs/metalog"
)

// Batched logging (AckStaged). A pack that is on the drive and verified is
// applied to the local index at once, and the log segment that makes it visible
// to the other volumes of the drive is written later, together with the other
// packs that came in the meantime: until the first of them has waited
// Config.LogBatchAge, or they hold Config.LogBatchMembers members, or something
// needs the log to be complete (Flush, a hand-back of the lease, Close, any
// other segment). restic does the same with its index files (indexMaxAge,
// indexMaxBlobs). Until then the packs are listed in the table "deferred" of
// the local index, in the same transaction that applied them, so that a crash
// loses nothing and uploads nothing twice: the next start finds the packs on
// the drive (their manifest says what is in them) and only writes the segment.
//
// The segment of several commits is named after the highest sequence number
// among them and carries the others as Extra packs; its records are in the order
// of the sequence numbers, so replaying it is replaying the commits one after
// the other. AckDurable commits each pack with its own segment at once.

// logBatched reports whether a committed pack waits for others before its
// segment is written.
func (v *Volume) logBatched() bool {
	return v.cfg.Ack == AckStaged && v.cfg.LogBatchAge > 0
}

// noteDeferred is called when a pack of n members has been added to the table.
func (v *Volume) noteDeferred(n int) {
	v.defMu.Lock()
	v.defMembers += n
	full := v.defMembers >= v.cfg.LogBatchMembers
	if v.defTimer == nil && !full {
		v.defTimer = time.AfterFunc(v.cfg.LogBatchAge, v.fireDrain)
	}
	v.defMu.Unlock()
	if full {
		v.fireDrain()
	}
}

// fireDrain writes the log of the deferred packs in the background.
func (v *Volume) fireDrain() {
	v.defMu.Lock()
	v.defTimer = nil
	v.defMu.Unlock()
	if v.closed.Load() || v.closing.Load() || v.ReadOnly() {
		return
	}
	v.wg.Add(1)
	go func() {
		defer v.wg.Done()
		if err := v.logDeferred(v.ctx); err != nil && v.ctx.Err() == nil {
			v.logf("cfs: writing the log of committed packs: %v (will retry)", err)
			v.defMu.Lock()
			if v.defTimer == nil {
				v.defTimer = time.AfterFunc(5*time.Second, v.fireDrain)
			}
			v.defMu.Unlock()
		}
	}()
}

type deferredRow struct {
	name       string
	epoch, seq uint64
	pack, sha1 string
	bytes      int64
	members    int
	recs       string
}

// logDeferred writes one segment for every pack waiting for one.
func (v *Volume) logDeferred(ctx context.Context) error {
	v.drainMu.Lock()
	defer v.drainMu.Unlock()
	v.defMu.Lock()
	if v.defTimer != nil {
		v.defTimer.Stop()
		v.defTimer = nil
	}
	v.defMu.Unlock()
	rows, err := v.ix.db.Query("SELECT name, epoch, seq, pack, bytes, sha1, members, recs FROM deferred ORDER BY seq")
	if err != nil {
		return err
	}
	var all []deferredRow
	for rows.Next() {
		var r deferredRow
		if err := rows.Scan(&r.name, &r.epoch, &r.seq, &r.pack, &r.bytes, &r.sha1, &r.members, &r.recs); err != nil {
			rows.Close()
			return err
		}
		all = append(all, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	if len(all) == 0 {
		v.defMu.Lock()
		v.defMembers = 0
		v.defMu.Unlock()
		return nil
	}
	last := all[len(all)-1]
	seg := metalog.Segment{Epoch: last.epoch, Seq: last.seq, Pack: last.pack, PackBytes: last.bytes, PackSHA1: last.sha1, PackN: last.members}
	names := make([]string, 0, len(all))
	members := 0
	for _, r := range all {
		names = append(names, r.name)
		members += r.members
		var recs []metalog.Record
		if err := json.Unmarshal([]byte(r.recs), &recs); err != nil {
			return fmt.Errorf("cfs: records of deferred commit %s: %w", r.name, err)
		}
		if r != last {
			seg.Extra = append(seg.Extra, metalog.PackFact{Epoch: r.epoch, Seq: r.seq, Name: r.pack, Bytes: r.bytes, SHA1: r.sha1, N: r.members, R: len(recs)})
		}
		seg.Records = append(seg.Records, recs...)
	}
	if err := v.checkLease(ctx); err != nil {
		return err
	}
	if err := v.appendSegment(ctx, seg); err != nil {
		return err
	}
	crash.Point("log_uploaded")
	err = v.ix.tx(func(tx *sql.Tx) error {
		for _, n := range names {
			if _, err := tx.Exec("DELETE FROM pending WHERE seg=?", n); err != nil {
				return err
			}
			if _, err := tx.Exec("DELETE FROM deferred WHERE name=?", n); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	v.removeStrays(ctx, seg.Records)
	v.defMu.Lock()
	v.defMembers = max(0, v.defMembers-members)
	again := v.defMembers > 0 && v.defTimer == nil
	if again {
		v.defTimer = time.AfterFunc(v.cfg.LogBatchAge, v.fireDrain)
	}
	v.defMu.Unlock()
	return nil
}

// removeStrays removes the large files that packed files of the same name have
// replaced, now that the log says so.
func (v *Volume) removeStrays(ctx context.Context, recs []metalog.Record) {
	for _, r := range recs {
		if r.Op != metalog.OpPut {
			continue
		}
		if _, ok := v.ix.kvGet(strayKey + r.Path); !ok {
			continue
		}
		if _, ok, err := v.ix.getCommitted(r.Path); err != nil || !ok {
			continue
		}
		if err := v.st.Remove(ctx, r.Path); err != nil && !isNotFound(err) {
			v.logf("cfs: remove shadowed large file %s: %v (will be repeated at the next start)", r.Path, err)
			continue
		}
		v.ix.db.Exec("DELETE FROM kv WHERE k=?", strayKey+r.Path)
		v.dc.cacheDel(r.Path)
	}
}

// recoverDeferred writes the log of packs a crash left deferred: the packs are
// checked on the drive (they were verified before the crash, so a missing or
// changed one is damage), and the segment is written without uploading them
// again.
func (v *Volume) recoverDeferred(ctx context.Context) error {
	if v.ReadOnly() {
		return nil
	}
	rows, err := v.ix.db.Query("SELECT name, pack, bytes, sha1 FROM deferred ORDER BY seq")
	if err != nil {
		return err
	}
	type chk struct {
		name, pack, sha1 string
		bytes            int64
	}
	var all []chk
	for rows.Next() {
		var c chk
		if err := rows.Scan(&c.name, &c.pack, &c.bytes, &c.sha1); err != nil {
			rows.Close()
			return err
		}
		all = append(all, c)
	}
	rows.Close()
	if len(all) == 0 {
		return nil
	}
	for _, c := range all {
		e, err := v.st.Stat(ctx, CtlDir+"/packs/"+c.pack)
		switch {
		case err == nil && e.Size == c.bytes && (e.SHA1 == "" || c.sha1 == "" || e.SHA1 == c.sha1):
		case err != nil && !isNotFound(err):
			return fmt.Errorf("cfs: check pack %s of a commit that was not logged: %w", c.pack, err)
		default:
			v.logf("cfs: ERROR: pack %s of a commit that was not logged is not on the drive as it was uploaded (%v): its files cannot be read", c.pack, err)
			if _, derr := v.ix.db.Exec("DELETE FROM deferred WHERE name=?", c.name); derr != nil {
				return derr
			}
		}
	}
	v.logf("cfs: writing the log of %d packs that were uploaded before the crash", len(all))
	return v.logDeferred(ctx)
}

// segLogged counts a segment written to the drive, and starts a checkpoint when
// enough of them, or enough bytes of them, have piled up since the last one.
func (v *Volume) segLogged(size int64) {
	n := v.segsNew.Add(1)
	b := v.segBytes.Add(size)
	if (v.cfg.CheckpointEvery > 0 && n >= int64(v.cfg.CheckpointEvery)) ||
		(v.cfg.CheckpointEvery > 0 && v.cfg.CheckpointBytes > 0 && b >= v.cfg.CheckpointBytes) {
		v.maybeCheckpoint()
	}
}
