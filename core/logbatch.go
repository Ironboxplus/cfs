package core

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
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

// unsettledFloor is the lowest sequence number of a segment that is not on the
// drive yet and is not a deferred pack either: a commit that is under way, or
// the sealed metadata rows of a commit that gave up. ok is false when there is
// none.
func unsettledFloor(q queryer) (low uint64, ok bool, err error) {
	rows, err := q.Query("SELECT name FROM inflight UNION SELECT seg FROM pending WHERE seg IS NOT NULL AND seg NOT IN (SELECT name FROM deferred)")
	if err != nil {
		return 0, false, err
	}
	defer rows.Close()
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			return 0, false, err
		}
		if _, seq, valid := parseSegKey(k); valid && (!ok || seq < low) {
			low, ok = seq, true
		}
	}
	return low, ok, rows.Err()
}

// logDeferred writes one segment for the packs waiting for one. The segment is
// named after the highest of its commits and replays after every segment with
// a lower number, so it must not reach over a commit whose own segment is still
// to come (that commit's records would then replay before the older ones of the
// packs): only the packs below the lowest such commit go in. The others wait;
// whoever settles that commit writes them (writeSealedInOrder).
func (v *Volume) logDeferred(ctx context.Context) error {
	v.drainMu.Lock()
	defer v.drainMu.Unlock()
	v.defMu.Lock()
	if v.defTimer != nil {
		v.defTimer.Stop()
		v.defTimer = nil
	}
	v.defMu.Unlock()
	// The commits under way and the deferred packs are read in one transaction:
	// a commit that finishes between two reads would be in neither, and a pack
	// sealed after it could go in without it.
	var all []deferredRow
	var floor uint64
	var bounded bool
	err := v.ix.tx(func(tx *sql.Tx) error {
		var err error
		if floor, bounded, err = unsettledFloor(tx); err != nil {
			return err
		}
		rows, err := tx.Query("SELECT name, epoch, seq, pack, bytes, sha1, members, recs FROM deferred ORDER BY seq")
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var r deferredRow
			if err := rows.Scan(&r.name, &r.epoch, &r.seq, &r.pack, &r.bytes, &r.sha1, &r.members, &r.recs); err != nil {
				return err
			}
			all = append(all, r)
		}
		return rows.Err()
	})
	if err != nil {
		return err
	}
	if len(all) == 0 {
		v.defMu.Lock()
		v.defMembers = 0
		v.defMu.Unlock()
		return nil
	}
	if bounded {
		n := 0
		for n < len(all) && all[n].seq < floor {
			n++
		}
		all = all[:n]
		if n == 0 {
			v.armDrain()
			return nil
		}
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
	if f := v.afterBatchUpload; f != nil {
		v.afterBatchUpload = nil
		f()
	}
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
	v.defMu.Lock()
	v.defMembers = max(0, v.defMembers-members)
	again := v.defMembers > 0 && v.defTimer == nil
	if again {
		v.defTimer = time.AfterFunc(v.cfg.LogBatchAge, v.fireDrain)
	}
	v.defMu.Unlock()
	return nil
}

// armDrain makes sure the log of the deferred packs is written later.
func (v *Volume) armDrain() {
	v.defMu.Lock()
	if v.defTimer == nil {
		v.defTimer = time.AfterFunc(v.cfg.LogBatchAge, v.fireDrain)
	}
	v.defMu.Unlock()
}

// writeSealedInOrder writes what waits for the log in the order of the
// sequence numbers: the deferred packs below each sealed metadata segment, then
// that segment, and in the end the packs above the last of them. It stops at a
// commit that is under way: nothing above that commit's number is written
// before its own segment, because a segment written now and the segment of a
// batch named above it later (with the packs below it inside) would replay in
// the wrong order. blocked is true when something is still waiting then.
func (v *Volume) writeSealedInOrder(ctx context.Context) (blocked bool, err error) {
	for {
		if err := v.logDeferred(ctx); err != nil {
			return false, err
		}
		left, err := readPending(v.ix.db, "WHERE seg IS NOT NULL AND seg NOT IN (SELECT name FROM inflight) AND seg NOT IN (SELECT name FROM deferred)")
		if err != nil {
			return false, err
		}
		var inflightLow, inflightN, deferredN uint64
		if err := v.ix.db.QueryRow("SELECT count(*) FROM inflight").Scan(&inflightN); err != nil {
			return false, err
		}
		if err := v.ix.db.QueryRow("SELECT count(*) FROM deferred").Scan(&deferredN); err != nil {
			return false, err
		}
		if inflightN > 0 {
			low, _, err := inflightFloor(v.ix.db)
			if err != nil {
				return false, err
			}
			inflightLow = low
		}
		if len(left) == 0 {
			return inflightN > 0 || deferredN > 0, nil
		}
		var group []pendingRow
		for _, p := range left {
			if p.Seg != left[0].Seg {
				break
			}
			group = append(group, p)
		}
		if _, seq, ok := parseSegKey(left[0].Seg); ok && inflightN > 0 && inflightLow < seq {
			return true, nil
		}
		if err := v.uploadMetaSeg(ctx, left[0].Seg, group); err != nil {
			return false, err
		}
	}
}

// inflightFloor is the lowest sequence number of a commit that is under way.
func inflightFloor(q queryer) (low uint64, ok bool, err error) {
	rows, err := q.Query("SELECT name FROM inflight")
	if err != nil {
		return 0, false, err
	}
	defer rows.Close()
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			return 0, false, err
		}
		if _, seq, valid := parseSegKey(k); valid && (!ok || seq < low) {
			low, ok = seq, true
		}
	}
	return low, ok, rows.Err()
}

// dropLoggedDeferred forgets the deferred packs a segment on the drive already
// carries: a crash between the upload of a batch and the transaction that
// recorded it leaves them deferred. Written again, they would replay a second
// time after the segments that came in between.
func (v *Volume) dropLoggedDeferred(ctx context.Context) error {
	rows, err := v.ix.db.Query("SELECT name, pack FROM deferred ORDER BY seq")
	if err != nil {
		return err
	}
	type def struct{ name, pack string }
	var all []def
	for rows.Next() {
		var d def
		if err := rows.Scan(&d.name, &d.pack); err != nil {
			rows.Close()
			return err
		}
		all = append(all, d)
	}
	rows.Close()
	if len(all) == 0 {
		return nil
	}
	refs, err := v.segRefs(ctx)
	if err != nil {
		return err
	}
	onDrive := map[string]metalog.Ref{}
	for _, r := range refs {
		onDrive[segKeyOf(r.Epoch, r.Seq)] = r
	}
	covered := map[string]bool{}
	for _, d := range all {
		ref, ok := onDrive[d.name]
		if !ok {
			continue
		}
		seg, err := v.log.ReadSegment(ctx, ref)
		if err != nil {
			if !errors.Is(err, metalog.ErrCorrupt) && !isNotFound(err) {
				return fmt.Errorf("cfs: read log segment %s of deferred packs: %w", ref.Name, err)
			}
			// Cut short by the crash: it is written again.
			if !isNotFound(err) {
				v.logf("cfs: log segment %s is unreadable, writing it again: %v", ref.Name, err)
				if derr := v.log.DeleteSegment(ctx, ref); derr != nil {
					v.logf("cfs: could not remove unreadable log segment %s: %v", ref.Name, derr)
				}
			}
			continue
		}
		covered[seg.Pack] = true
		for _, x := range seg.Extra {
			covered[x.Name] = true
		}
	}
	return v.ix.tx(func(tx *sql.Tx) error {
		for _, d := range all {
			if !covered[d.pack] {
				continue
			}
			if _, err := tx.Exec("DELETE FROM pending WHERE seg=?", d.name); err != nil {
				return err
			}
			if _, err := tx.Exec("DELETE FROM deferred WHERE name=?", d.name); err != nil {
				return err
			}
		}
		return nil
	})
}

// recoverDeferred writes the log of packs a crash left deferred: the packs are
// checked on the drive (they were verified before the crash, so a missing or
// changed one is damage), and the segment is written without uploading them
// again.
func (v *Volume) recoverDeferred(ctx context.Context) error {
	if v.ReadOnly() {
		return nil
	}
	if err := v.dropLoggedDeferred(ctx); err != nil {
		return err
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
	_, err = v.writeSealedInOrder(ctx)
	return err
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
