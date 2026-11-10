package core

import (
	"context"
	"errors"
	"time"

	"github.com/Ironboxplus/cfs/store"
)

// A volume is a reader until its first change. Reading writes nothing to the
// drive, the lease included, so a volume that is only listed and downloaded
// from never stands in the way of the writer of the same drive. The first
// change takes the lease (becomeWriter) and starts the heartbeat; once the
// writer has been idle for Config.LeaseIdle with nothing left to commit, it
// writes a released lease and is a reader again (handBack).

// hasLocalWork reports whether the local index holds work of an earlier run that
// only a writer can finish: staged files, metadata not yet on the drive, a pack
// in commit, an unfinished move or removal.
func (v *Volume) hasLocalWork() bool {
	var n int
	err := v.ix.db.QueryRow(`SELECT (SELECT count(*) FROM staged) + (SELECT count(*) FROM pending) +
		(SELECT count(*) FROM inflight) + (SELECT count(*) FROM deferred) +
		(SELECT count(*) FROM kv WHERE k LIKE 'stray:%' OR k LIKE 'mv:%' OR k LIKE 'putbig:%' OR k = 'dmv_intent')`).Scan(&n)
	return err != nil || n > 0
}

func (v *Volume) touch() { v.changedAt.Store(time.Now().UnixNano()) }

// beginWrite is what a change calls first: it returns once this volume holds
// the lease, and the function to call when the change is over. Until then a
// hand-back waits, so that nothing acknowledged is written after the lease has
// gone. ErrReadOnly: another writer holds the lease.
func (v *Volume) beginWrite(ctx context.Context) (end func(), err error) {
	for {
		v.holdMu.RLock()
		werr := v.writable()
		if werr == nil && v.starting.Load() {
			// Another change is taking the lease and bringing the index up to
			// date: this one waits for it.
			v.holdMu.RUnlock()
			v.modeMu.Lock()
			v.modeMu.Unlock()
			continue
		}
		if werr == nil {
			v.touch()
			return func() { v.touch(); v.holdMu.RUnlock() }, nil
		}
		v.holdMu.RUnlock()
		if !errors.Is(werr, ErrReadOnly) {
			return nil, werr
		}
		if err := v.becomeWriter(ctx); err != nil {
			return nil, err
		}
	}
}

// becomeWriter takes the lease for a reader, one caller at a time (the others
// find it taken), and brings the index up to date with what the writer before
// left on the drive, as Open does.
func (v *Volume) becomeWriter(ctx context.Context) error {
	v.modeMu.Lock()
	defer v.modeMu.Unlock()
	if v.closing.Load() || v.closed.Load() {
		return ErrClosed
	}
	if v.fenced.Load() {
		return ErrFenced
	}
	if !v.readOnly.Load() {
		return nil
	}
	l, ok, err := v.readLeaseOrAbsent(ctx)
	if err != nil {
		return err
	}
	held, err := v.acquire(ctx, l, ok)
	if err != nil {
		return err
	}
	if !held {
		return ErrReadOnly
	}
	v.catchMu.Lock()
	defer v.catchMu.Unlock()
	v.starting.Store(true)
	defer v.starting.Store(false)
	v.readOnly.Store(false)
	fail := func(err error) error {
		v.readOnly.Store(true)
		if werr := v.writeLease(ctx, true); werr != nil {
			v.logf("cfs: giving the lease back after a failed start: %v", werr)
		}
		return err
	}
	if err := v.loadDirs(ctx); err != nil {
		return fail(err)
	}
	if err := v.recoverLocal(ctx); err != nil {
		return fail(err)
	}
	if err := v.catchUp(ctx); err != nil {
		return fail(err)
	}
	if err := v.st.Mkdir(ctx, ""); err != nil && !errors.Is(err, store.ErrExists) {
		return fail(err)
	}
	v.dc.reset()
	if v.stg.started.CompareAndSwap(false, true) {
		v.wg.Add(1)
		go v.packerLoop()
	}
	v.stg.poke()
	v.startHold()
	v.touch()
	v.kickMeta()
	v.logf("cfs: took the writer lease (epoch %d)", v.epoch)
	return nil
}

// startHold starts the heartbeat, and the watch for idleness, of the lease
// this volume has just taken. modeMu is held, or the volume is being opened.
func (v *Volume) startHold() {
	hold, cancel := context.WithCancel(v.ctx)
	v.leaseMu.Lock()
	v.leaseDone = false
	v.hold = cancel
	v.leaseMu.Unlock()
	v.touch()
	v.wg.Add(1)
	go v.heartbeatLoop(hold)
	if v.cfg.LeaseIdle > 0 {
		v.wg.Add(1)
		go v.idleLoop(hold)
	}
}

func (v *Volume) idleLoop(hold context.Context) {
	defer v.wg.Done()
	tick := min(max(v.cfg.LeaseIdle/4, 5*time.Millisecond), 30*time.Second)
	t := time.NewTicker(tick)
	defer t.Stop()
	for {
		select {
		case <-v.ctx.Done():
			return
		case <-hold.Done():
			return
		case <-t.C:
		}
		if time.Since(time.Unix(0, v.changedAt.Load())) < v.cfg.LeaseIdle {
			continue
		}
		if v.handBack(false) {
			return
		}
	}
}

// handBack returns the lease: when no change has been made for LeaseIdle and
// nothing is left to commit, it writes a released lease and makes this volume a
// reader. It reports whether it did. A change in progress, or one that arrives
// meanwhile, keeps the lease (the hand-back never waits for a change: it only
// tries for the lock that changes hold).
func (v *Volume) handBack(force bool) bool {
	v.modeMu.Lock()
	defer v.modeMu.Unlock()
	if v.closing.Load() || v.closed.Load() || v.fenced.Load() || v.readOnly.Load() {
		return false
	}
	idle := func() bool { return force || time.Since(time.Unix(0, v.changedAt.Load())) >= v.cfg.LeaseIdle }
	// New segments get a checkpoint before the lease goes, so that the next
	// volume to open reads that and next to nothing of the log. It is made
	// before the lock that changes wait for is taken: a change that comes
	// meanwhile keeps the lease.
	if v.segsNew.Load() > 0 && v.cfg.CheckpointEvery > 0 && idle() {
		if err := v.Flush(v.ctx); err == nil {
			if err := v.Checkpoint(v.ctx); err != nil {
				v.logf("cfs: checkpoint before handing the lease back: %v", err)
			}
		}
	}
	if !v.holdMu.TryLock() {
		return false
	}
	defer v.holdMu.Unlock()
	if !idle() {
		return false
	}
	// Everything acknowledged is on the drive before the lease goes.
	if err := v.Flush(v.ctx); err != nil {
		v.logf("cfs: not handing the lease back: %v", err)
		return false
	}
	if files, _ := v.Staged(); files > 0 || v.hasLocalWork() {
		return false
	}
	v.leaseMu.Lock()
	err := v.writeLease(v.ctx, true)
	if err == nil {
		v.leaseDone = true
		if v.hold != nil {
			v.hold()
		}
	}
	v.leaseMu.Unlock()
	if err != nil {
		v.logf("cfs: not handing the lease back: %v", err)
		return false
	}
	v.readOnly.Store(true)
	v.followAt.Store(time.Now().UnixNano())
	v.logf("cfs: idle: lease handed back (epoch %d)", v.epoch)
	return true
}

// follow lets a reader see what the writer has committed since: once the
// directory cache's time has passed it applies the log segments that are new on
// the drive. A failure is logged and tried again after the same time.
func (v *Volume) follow(ctx context.Context) {
	if !v.ReadOnly() || v.closed.Load() {
		return
	}
	ttl := v.cfg.DirCacheTTL
	due := func() bool { return time.Since(time.Unix(0, v.followAt.Load())) >= ttl }
	if !due() || !v.followMu.TryLock() {
		return
	}
	defer v.followMu.Unlock()
	if !due() {
		return
	}
	v.followAt.Store(time.Now().UnixNano())
	if err := v.Refresh(ctx); err != nil && ctx.Err() == nil {
		v.logf("cfs: following the writer: %v", err)
	}
}
