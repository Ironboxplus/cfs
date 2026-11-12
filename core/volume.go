// Package core is the cfs volume: a filesystem namespace over a store.Store
// (a rate-limited cloud drive) in which files below a size threshold live as
// members of zip packs, tracked by a local SQLite index and a drive-side log,
// and all larger files are stored 1:1 under their real path.
//
// core knows nothing about in-flight limits, retries, metering or logging of
// the store: those are decorators from package aspect that the caller puts
// around the store it hands to Open.
package core

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Ironboxplus/cfs/metalog"
	"github.com/Ironboxplus/cfs/store"
	"golang.org/x/sync/semaphore"
	"golang.org/x/sync/singleflight"
)

// Entry is a file or directory of the volume.
type Entry struct {
	Path    string
	IsDir   bool
	Size    int64
	ModTime time.Time
	// Packed marks a small file kept in a pack.
	Packed bool
	// SHA1 is known for packed files, and for large files only if the drive's
	// listing carried it; use Volume.Hash otherwise.
	SHA1 string
}

// Name is the last path element.
func (e Entry) Name() string { _, n := splitPath(e.Path); return n }

// Volume is one cfs volume. It is safe for concurrent use. A volume is
// single-writer, and it is a reader until its first change: opening, listing
// and reading write nothing to the drive, the lease included. The first change
// takes the lease, and a writer that has been idle for Config.LeaseIdle hands it
// back. If another live writer holds the lease, a change returns ErrReadOnly and
// the volume stays a reader.
type Volume struct {
	cfg Config
	st  store.Store
	log metalog.MetaLog
	ix  *index

	writerID  string
	epoch     uint64
	readOnly  atomic.Bool
	fenced    atomic.Bool
	closed    atomic.Bool
	closing   atomic.Bool
	closeOnce sync.Once
	closeErr  error

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// structMu: file operations and Put-adds hold it shared; operations on
	// directories, and snapshots, hold it exclusively.
	structMu sync.RWMutex
	// ckptMu: a batch commit holds it shared from allocating its sequence
	// number to marking it applied locally; a checkpoint holds it exclusively.
	ckptMu sync.RWMutex

	batchMu  sync.Mutex
	cur      *batch
	inflight map[*batch]struct{}
	slots    chan struct{}

	stg *stager // acknowledged small files that aren't on the drive yet (AckStaged)

	metaMu    sync.Mutex
	metaQuiet *time.Timer
	metaMax   *time.Timer
	metaFlush sync.Mutex

	dc      *dirCache
	mc      *memCache
	wc      *memCache           // read windows of packs
	packSz  sync.Map            // pack name -> bytes
	sf      singleflight.Group  // one load per member (pack@off) or read window (pack#n) at a time
	rb      *semaphore.Weighted // Config.ReadBuffer, in bytes
	rbUsed  atomic.Int64
	rbPeak  atomic.Int64
	segsNew atomic.Int64 // segments committed since the last checkpoint
	ckptRun atomic.Bool
	ckptOne sync.Mutex // one Checkpoint at a time

	modeMu    sync.Mutex         // becoming a writer and handing the lease back, one at a time
	holdMu    sync.RWMutex       // a change holds it shared from start to end; handing back, exclusively
	catchMu   sync.Mutex         // one catch-up of the index with the drive's log at a time
	hold      context.CancelFunc // ends the heartbeat and idle loops of the lease held now; leaseMu
	changedAt atomic.Int64       // unix nanos of the last change, or of taking the lease
	followAt  atomic.Int64       // unix nanos of the last time a reader looked for the writer's news
	followMu  sync.Mutex

	drainMu sync.Mutex // one write of the log of deferred packs at a time

	// afterBatchUpload is called by the tests between the upload of a batch of
	// deferred packs and the transaction that records it.
	afterBatchUpload func()

	dk dirKnow // the folders of the drive this tenure of the lease has listed (ddirs.go)

	defMu      sync.Mutex
	defMembers int         // members of the deferred packs
	defTimer   *time.Timer // writes their log when the first has waited LogBatchAge
	segBytes   atomic.Int64
	starting   atomic.Bool  // becomeWriter is bringing the index up to date
	leaseMu    sync.Mutex   // one lease write at a time: the heartbeat and the release
	leaseDone  bool         // the lease has been released: no heartbeat writes it again; leaseMu
	leaseAt    atomic.Int64 // unix nanos of the last successful read or write of the lease

	// The lease files (see readLease). leaseWriteMu is held for a whole write of
	// the lease, uploads included, and guards leaseOld and leaseLastHB;
	// leaseSeenMu is only held in memory and guards leaseSeen and leaseLegacy, so
	// that reading the lease (every commit's check) never waits for a write.
	leaseWriteMu sync.Mutex
	leaseOld     []string // names that the next write of the lease supersedes and removes
	leaseLastHB  int64    // heartbeat of the last lease file written, in unix millis
	leaseSeenMu  sync.Mutex
	leaseSeen    []string // names the last listing of the lease directory found
	leaseLegacy  bool     // a listing fell back to lease.json, which a write removes
}

func (v *Volume) logf(format string, args ...any) { v.cfg.Logf(format, args...) }

// Epoch returns this writer's epoch.
func (v *Volume) Epoch() uint64 { return v.epoch }

// ReadOnly reports whether this volume is a reader, which holds no lease: it
// has made no change yet, has handed the lease back, or another writer holds it.
func (v *Volume) ReadOnly() bool { return v.readOnly.Load() }

// Config returns the effective configuration.
func (v *Volume) Config() Config { return v.cfg }

// Features returns the capabilities of the store below.
func (v *Volume) Features() store.Features { return v.st.Features() }

// Open opens the volume stored in st, using cfg.StateDir for the local index.
// If the state directory is empty and the drive already holds a volume, the
// index is rebuilt from the drive first.
func Open(ctx context.Context, st store.Store, cfg Config) (*Volume, error) {
	if err := cfg.fill(); err != nil {
		return nil, err
	}
	v := &Volume{
		cfg: cfg, st: st,
		log:      &metalog.Jsonl{St: st, Ctl: CtlDir},
		inflight: map[*batch]struct{}{},
		slots:    make(chan struct{}, cfg.Pipeline+1),
		dc:       newDirCache(cfg.DirCacheTTL),
		mc:       newMemCache(cfg.MemberCache),
		wc:       newMemCache(32 << 20),
		rb:       newReadBuffer(cfg.ReadBuffer),
		stg:      newStager(cfg.StateDir),
	}
	v.ctx, v.cancel = context.WithCancel(context.Background())
	if err := os.MkdirAll(filepath.Join(cfg.StateDir, "spool"), 0o755); err != nil {
		return nil, err
	}
	// Spool files of a previous run are garbage: nothing in them was acknowledged.
	if ents, err := os.ReadDir(filepath.Join(cfg.StateDir, "spool")); err == nil {
		for _, e := range ents {
			os.Remove(filepath.Join(cfg.StateDir, "spool", e.Name()))
		}
	}

	dbPath := filepath.Join(cfg.StateDir, "index.db")
	_, statErr := os.Stat(dbPath)
	fresh := os.IsNotExist(statErr)
	if fresh {
		if err := v.fetchBaseCheckpoint(ctx, dbPath); err != nil {
			return nil, err
		}
	}
	ix, err := openIndex(cfg.StateDir)
	if err != nil {
		return nil, err
	}
	v.ix = ix
	fail := func(err error) (*Volume, error) {
		v.cancel()
		ix.close()
		return nil, err
	}
	if id, ok := ix.kvGet("writer_id"); ok {
		v.writerID = id
	} else {
		v.writerID = newID()
		if err := ix.kvSet("writer_id", v.writerID); err != nil {
			return fail(err)
		}
	}
	// A volume with work left from an earlier run (staged files, an unfinished
	// commit or move) is a writer at once, to finish it; any other starts as a
	// reader.
	if err := v.takeLease(ctx, v.hasLocalWork()); err != nil {
		return fail(err)
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
	if err := ix.kvSet("inited", "1"); err != nil {
		return fail(err)
	}
	if err := v.stageInit(); err != nil {
		return fail(err)
	}
	if !v.ReadOnly() {
		if err := v.st.Mkdir(ctx, ""); err != nil && !errors.Is(err, store.ErrExists) {
			return fail(fmt.Errorf("cfs: create root: %w", err))
		}
		v.startHold()
		v.kickMeta()
	}
	v.followAt.Store(time.Now().UnixNano())
	return v, nil
}

func newID() string {
	h, _ := os.Hostname()
	return fmt.Sprintf("%s-%d-%x", h, os.Getpid(), time.Now().UnixNano())
}

// Flush makes everything accepted so far durable on the drive: open packs are
// sealed and committed, and the pending metadata log is uploaded.
func (v *Volume) Flush(ctx context.Context) error {
	if v.ReadOnly() {
		return nil
	}
	if err := v.waitStaged(ctx, func(string) bool { return true }); err != nil {
		return err
	}
	if err := v.flushBatches(ctx); err != nil {
		return err
	}
	if _, err := v.writeSealedInOrder(ctx); err != nil {
		return err
	}
	return v.flushMeta(ctx)
}

// Close flushes, checkpoints when there is something new, releases the lease
// and closes the local index. It runs once; a call that comes while another is
// at work waits for it and returns its result (as sync.Once.Do does), so a
// process that closes from two places at exit does not leave before the lease
// is released.
func (v *Volume) Close(ctx context.Context) error {
	v.closeOnce.Do(func() { v.closeErr = v.close(ctx) })
	return v.closeErr
}

func (v *Volume) close(ctx context.Context) error {
	v.closing.Store(true)
	v.modeMu.Lock() // a change in the middle of taking the lease, or a hand-back, finishes first
	v.modeMu.Unlock()
	var first error
	keep := func(err error) {
		if err != nil && first == nil {
			first = err
		}
	}
	v.logf("cfs: closing (read-only %v, fenced %v)", v.ReadOnly(), v.fenced.Load())
	defer func() { v.logf("cfs: closed: %v", first) }()
	if !v.ReadOnly() && !v.fenced.Load() {
		keep(v.Flush(ctx))
		if v.segsNew.Load() > 0 && v.cfg.CheckpointEvery > 0 {
			keep(v.Checkpoint(ctx))
		}
		keep(v.releaseLease(ctx))
	}
	v.closed.Store(true)
	v.metaMu.Lock()
	if v.metaQuiet != nil {
		v.metaQuiet.Stop()
		v.metaMax.Stop()
		v.metaQuiet, v.metaMax = nil, nil
	}
	v.metaMu.Unlock()
	v.cancel()
	v.wg.Wait()
	v.abandonBatch()
	keep(v.ix.close())
	return first
}

func (v *Volume) writable() error {
	if v.closed.Load() {
		return ErrClosed
	}
	if v.fenced.Load() {
		return ErrFenced
	}
	if v.ReadOnly() {
		return ErrReadOnly
	}
	return nil
}

// ---- paths ----

// clean normalises a volume path: slash separated, no leading or trailing
// slash, no "." or ".." elements.
func clean(p string) (string, error) {
	p = strings.Trim(p, "/")
	if p == "" {
		return "", nil
	}
	for _, seg := range strings.Split(p, "/") {
		if seg == "" || seg == "." || seg == ".." || strings.ContainsRune(seg, 0) {
			return "", fmt.Errorf("cfs: invalid path %q", p)
		}
	}
	return p, nil
}

func reserved(p string) bool {
	first, _, _ := strings.Cut(p, "/")
	return strings.HasPrefix(first, ".cfs")
}

func parentOf(p string) string { d, _ := splitPath(p); return d }

// ---- lease ----

// The lease is a directory of small files, each written once under a name of
// its own and never rewritten. The drive has no atomic replace: a file
// rewritten in place can be read half written, or as the version it is
// replacing, and on 115 it was (JOURNAL 2026-11-12, cfs 1). restic's locks
// work this way (internal/repository/lock_file.go): a refresh writes a new
// file and then removes the old one. The name says all that the lease says,
// so who holds it is known from a listing, without reading a file: there is no
// unreadable lease to mistake for no lease.
const leaseDir = CtlDir + "/lease"

// legacyLeasePath is where versions before the lease directory kept the lease,
// rewritten in place. It is looked at while the directory is empty, and the
// first write of the lease removes it.
const legacyLeasePath = CtlDir + "/lease.json"

type lease struct {
	Writer    string `json:"writer"`
	Epoch     uint64 `json:"epoch"`
	Heartbeat int64  `json:"heartbeat_ms"`
	Released  bool   `json:"released,omitempty"`
	Host      string `json:"host,omitempty"`
}

// fileName is l's name in leaseDir: <epoch>-<heartbeat>-<held|released>-<writer>.json,
// the numbers zero-padded so that names sort as leases do.
func (l lease) fileName() string {
	state := "held"
	if l.Released {
		state = "released"
	}
	return fmt.Sprintf("%020d-%020d-%s-%s.json", l.Epoch, l.Heartbeat, state, l.Writer)
}

// parseLeaseName is the lease a name in leaseDir stands for.
func parseLeaseName(name string) (lease, bool) {
	base, ok := strings.CutSuffix(name, ".json")
	parts := strings.SplitN(base, "-", 4)
	if !ok || len(parts) != 4 || parts[3] == "" || (parts[2] != "held" && parts[2] != "released") {
		return lease{}, false
	}
	epoch, err1 := strconv.ParseUint(parts[0], 10, 64)
	hb, err2 := strconv.ParseInt(parts[1], 10, 64)
	if err1 != nil || err2 != nil {
		return lease{}, false
	}
	return lease{Writer: parts[3], Epoch: epoch, Heartbeat: hb, Released: parts[2] == "released"}, true
}

// after reports whether l comes after o: a higher epoch, or in the same epoch a
// later heartbeat.
func (l lease) after(o lease) bool {
	if l.Epoch != o.Epoch {
		return l.Epoch > o.Epoch
	}
	if l.Heartbeat != o.Heartbeat {
		return l.Heartbeat > o.Heartbeat
	}
	return l.Released && !o.Released
}

// readLease returns the lease: the last of the files in leaseDir, or with none
// there, the legacy file.
func (v *Volume) readLease(ctx context.Context) (lease, bool, error) {
	ents, err := v.st.List(ctx, leaseDir)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return lease{}, false, err
	}
	var l lease
	var ok bool
	var seen []string
	for _, e := range ents {
		c, good := parseLeaseName(e.Name())
		if e.IsDir || !good {
			continue
		}
		seen = append(seen, e.Name())
		if !ok || c.after(l) {
			l, ok = c, true
		}
	}
	v.leaseSeenMu.Lock()
	v.leaseSeen = seen
	v.leaseSeenMu.Unlock()
	if ok {
		return l, true, nil
	}
	return v.readLegacyLease(ctx)
}

// readLegacyLease reads lease.json. One that can't be read is not taken for no
// lease (restic, lock_file.go: "if we cannot load a lock then it is unclear
// whether it can be ignored"): it is held, by a writer nobody knows, since it
// was last modified.
func (v *Volume) readLegacyLease(ctx context.Context) (lease, bool, error) {
	e, err := v.st.Stat(ctx, legacyLeasePath)
	if errors.Is(err, store.ErrNotFound) {
		return lease{}, false, nil
	}
	if err != nil {
		return lease{}, false, err
	}
	v.leaseSeenMu.Lock()
	v.leaseLegacy = true
	v.leaseSeenMu.Unlock()
	rc, err := v.st.Open(ctx, legacyLeasePath, 0, -1)
	if errors.Is(err, store.ErrNotFound) {
		return lease{}, false, nil
	}
	if err != nil {
		return lease{}, false, err
	}
	var l lease
	err = json.NewDecoder(rc).Decode(&l)
	rc.Close()
	if err != nil {
		v.logf("cfs: %s can't be read (%v): held since %s", legacyLeasePath, err, e.ModTime.Format(time.RFC3339))
		return lease{Heartbeat: e.ModTime.UnixMilli()}, true, nil
	}
	return l, true, nil
}

// writeLease writes this writer's lease as a new file, and then removes what it
// supersedes: the file this writer wrote before, the files the last listing
// found (none of them comes after it: the writer would have been fenced, or
// would not have taken the lease) and lease.json. A removal that fails is
// tried again after the next write. Only writes of the lease wait for each
// other: a read of the lease never waits for an upload that hangs.
func (v *Volume) writeLease(ctx context.Context, released bool) error {
	v.leaseWriteMu.Lock()
	defer v.leaseWriteMu.Unlock()
	l := lease{Writer: v.writerID, Epoch: v.epoch, Heartbeat: max(time.Now().UnixMilli(), v.leaseLastHB+1), Released: released}
	l.Host, _ = os.Hostname()
	b, _ := json.Marshal(l)
	name := l.fileName()
	if err := v.st.Put(ctx, leaseDir+"/"+name, store.Bytes(b), store.PutOpts{Size: int64(len(b)), ModTime: time.Now(), Unique: true}); err != nil {
		return err
	}
	v.leaseAt.Store(time.Now().UnixNano())
	v.leaseLastHB = l.Heartbeat
	v.leaseSeenMu.Lock()
	seen, legacy := v.leaseSeen, v.leaseLegacy
	v.leaseSeen, v.leaseLegacy = nil, false
	v.leaseSeenMu.Unlock()
	old := v.leaseOld
	for _, n := range seen {
		if c, ok := parseLeaseName(n); ok && l.after(c) {
			old = append(old, n)
		}
	}
	v.leaseOld = []string{name}
	done := map[string]bool{name: true}
	for _, n := range old {
		if done[n] {
			continue
		}
		done[n] = true
		if err := v.st.Remove(ctx, leaseDir+"/"+n); err != nil && !errors.Is(err, store.ErrNotFound) {
			v.logf("cfs: removing the old lease file %s: %v", n, err)
			v.leaseOld = append(v.leaseOld, n)
		}
	}
	if legacy {
		if err := v.st.Remove(ctx, legacyLeasePath); err != nil && !errors.Is(err, store.ErrNotFound) {
			v.logf("cfs: removing %s: %v", legacyLeasePath, err)
			v.leaseSeenMu.Lock()
			v.leaseLegacy = true
			v.leaseSeenMu.Unlock()
		}
	}
	return nil
}

// maxDriveEpoch is the highest epoch visible on the drive.
func (v *Volume) maxDriveEpoch(ctx context.Context, l lease, haveLease bool) (uint64, error) {
	var m uint64
	if haveLease {
		m = l.Epoch
	}
	segs, err := v.log.ListSegments(ctx)
	if err != nil {
		return 0, err
	}
	for _, s := range segs {
		m = max(m, s.Epoch)
	}
	cks, err := v.log.ListCheckpoints(ctx)
	if err != nil {
		return 0, err
	}
	for _, c := range cks {
		m = max(m, c.Epoch)
	}
	return m, nil
}

// takeLease is what Open does about the lease. A volume with no work of its own
// left from an earlier run only looks at it and starts as a reader, writing
// nothing; one with such work takes the lease (acquire).
func (v *Volume) takeLease(ctx context.Context, asWriter bool) error {
	l, ok, err := v.readLease(ctx)
	if err != nil {
		return err
	}
	if asWriter {
		held, err := v.acquire(ctx, l, ok)
		if err != nil || held {
			return err
		}
		v.logf("cfs: volume held by writer %s (epoch %d), opening as a reader", l.Writer, l.Epoch)
	}
	v.readOnly.Store(true)
	v.epoch = l.Epoch
	return nil
}

// acquire takes the lease: a live lease of another writer means no (the volume
// stays a reader); a stale, released or foreign-but-dead one is taken over with
// epoch+1. The drive has no compare-and-swap, so this is a best effort: the
// commit path re-reads the lease before every commit and fences itself when it
// sees a newer epoch.
func (v *Volume) acquire(ctx context.Context, l lease, ok bool) (held bool, err error) {
	local := v.ix.kvUint("epoch")
	if ok && l.Writer != v.writerID && !l.Released &&
		time.Since(time.UnixMilli(l.Heartbeat)) < v.cfg.LeaseTTL {
		return false, nil
	}
	if ok && l.Writer == v.writerID && l.Epoch == local && local != 0 {
		v.epoch = local
	} else {
		drive, err := v.maxDriveEpoch(ctx, l, ok)
		if err != nil {
			return false, err
		}
		v.epoch = max(drive, local) + 1
		if err := v.ix.kvSet("epoch", strconv.FormatUint(v.epoch, 10)); err != nil {
			return false, err
		}
	}
	if err := v.writeLease(ctx, false); err != nil {
		return false, err
	}
	return true, nil
}

// releaseLease writes the released lease. It waits for a heartbeat that is
// writing the lease and keeps later ones from writing it: the last write of the
// lease must be the release, or the next writer finds a live lease and opens
// read-only.
func (v *Volume) releaseLease(ctx context.Context) error {
	if v.ReadOnly() || v.fenced.Load() {
		return nil
	}
	v.leaseMu.Lock()
	defer v.leaseMu.Unlock()
	v.leaseDone = true
	err := v.writeLease(ctx, true)
	v.logf("cfs: lease released (epoch %d): %v", v.epoch, err)
	return err
}

// checkLease re-reads the lease and fences this writer if a newer epoch or a
// different writer holds it. Called before every commit.
//
// A fenced writer always fails. Otherwise a read less than LeaseTTL/6 after the
// last write of the lease, or the last read that found it still ours, is skipped. Another writer can only take over after our heartbeat has
// failed for longer than LeaseTTL, so by then the last success is older than
// LeaseTTL/6 and the check before the next commit reads for real and fences.
func (v *Volume) checkLease(ctx context.Context) error {
	if v.fenced.Load() {
		return ErrFenced
	}
	if at := v.leaseAt.Load(); at != 0 && time.Since(time.Unix(0, at)) < v.cfg.LeaseTTL/6 {
		return nil
	}
	return v.readCheckLease(ctx)
}

func (v *Volume) readCheckLease(ctx context.Context) error {
	l, ok, err := v.readLease(ctx)
	if err != nil {
		return err
	}
	if ok && (l.Epoch > v.epoch || (l.Epoch == v.epoch && l.Writer != v.writerID)) {
		v.fenced.Store(true)
		return ErrFenced
	}
	if ok {
		v.leaseAt.Store(time.Now().UnixNano())
	}
	return nil
}

func (v *Volume) heartbeatLoop(hold context.Context) {
	defer v.wg.Done()
	t := time.NewTicker(v.cfg.LeaseTTL / 3)
	defer t.Stop()
	for {
		select {
		case <-v.ctx.Done():
			return
		case <-hold.Done():
			return
		case <-t.C:
		}
		v.leaseMu.Lock()
		if v.leaseDone {
			v.leaseMu.Unlock()
			return
		}
		err := v.readCheckLease(v.ctx)
		if err == nil {
			err = v.writeLease(v.ctx, false)
		}
		v.leaseMu.Unlock()
		if err != nil {
			v.logf("cfs: lease heartbeat: %v", err)
			if errors.Is(err, ErrFenced) {
				return
			}
		}
	}
}

// ---- directory cache ----

type dirEnt struct {
	at   time.Time
	ents map[string]store.Entry
	// absent: the drive said the directory doesn't exist. Directories that only
	// staged files live in don't exist there until their pack is committed, and
	// every Put under one would otherwise list it again.
	absent bool
}

type dirCache struct {
	mu  sync.Mutex
	ttl time.Duration
	m   map[string]*dirEnt
	sf  map[string]*dirFlight
}

type dirFlight struct {
	done chan struct{}
	ents map[string]store.Entry
	err  error
}

func newDirCache(ttl time.Duration) *dirCache {
	return &dirCache{ttl: ttl, m: map[string]*dirEnt{}, sf: map[string]*dirFlight{}}
}

// listStore lists dir on the drive through the cache. For the writer a folder
// that is known (ddirs.go) to be absent is not asked about. The returned map
// must not be modified.
func (v *Volume) listStore(ctx context.Context, dir string) (map[string]store.Entry, error) {
	if !v.ReadOnly() {
		ok, err := v.dirOnDrive(ctx, dir)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, store.ErrNotFound
		}
	}
	c := v.dc
	c.mu.Lock()
	if e, ok := c.m[dir]; ok && time.Since(e.at) < c.ttl {
		c.mu.Unlock()
		if e.absent {
			return nil, store.ErrNotFound
		}
		if !v.ReadOnly() && !v.dk.isListed(dir) {
			ents := make([]store.Entry, 0, len(e.ents))
			for _, x := range e.ents {
				ents = append(ents, x)
			}
			v.learnFolders(dir, ents)
		}
		return e.ents, nil
	}
	if f, ok := c.sf[dir]; ok {
		c.mu.Unlock()
		select {
		case <-f.done:
			return f.ents, f.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	f := &dirFlight{done: make(chan struct{})}
	c.sf[dir] = f
	c.mu.Unlock()

	ents, err := v.st.List(ctx, dir)
	m := make(map[string]store.Entry, len(ents))
	if err == nil {
		for _, e := range ents {
			m[e.Name()] = e
		}
	}
	c.mu.Lock()
	delete(c.sf, dir)
	if err == nil {
		c.m[dir] = &dirEnt{at: time.Now(), ents: m}
	} else if errors.Is(err, store.ErrNotFound) {
		c.m[dir] = &dirEnt{at: time.Now(), absent: true}
	}
	c.mu.Unlock()
	if !v.ReadOnly() {
		switch {
		case err == nil:
			// What the drive lists is there: remember its folders.
			v.learnFolders(dir, ents)
		case errors.Is(err, store.ErrNotFound):
			// The record said the folder exists and it doesn't (a crash between the
			// removal on the drive and the record): correct the record.
			v.dk.dropListed(dir)
			if err := v.ix.tx(func(tx *sql.Tx) error { return dropDDirs(tx, dir) }); err != nil {
				v.logf("cfs: forget folder %q: %v", dir, err)
			}
		}
	}
	f.ents, f.err = m, err
	close(f.done)
	return m, err
}

// cacheAdd records a new or changed entry in its parent's cached listing.
// The cached map is replaced, never modified, as readers hold it unlocked.
func (c *dirCache) cacheAdd(e store.Entry) {
	dir, name := splitPath(e.Path)
	c.mu.Lock()
	defer c.mu.Unlock()
	if d, ok := c.m[dir]; ok {
		n := make(map[string]store.Entry, len(d.ents)+1)
		for k, v := range d.ents {
			n[k] = v
		}
		n[name] = e
		c.m[dir] = &dirEnt{at: d.at, ents: n}
	}
}

func (c *dirCache) cacheDel(p string) {
	dir, name := splitPath(p)
	c.mu.Lock()
	defer c.mu.Unlock()
	if d, ok := c.m[dir]; ok {
		if _, has := d.ents[name]; has {
			n := make(map[string]store.Entry, len(d.ents))
			for k, v := range d.ents {
				if k != name {
					n[k] = v
				}
			}
			c.m[dir] = &dirEnt{at: d.at, ents: n}
		}
	}
}

func (c *dirCache) reset() {
	c.mu.Lock()
	c.m = map[string]*dirEnt{}
	c.mu.Unlock()
}

// dropTree forgets everything cached at or below p.
func (c *dirCache) dropTree(p string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for k := range c.m {
		if k == p || strings.HasPrefix(k, p+"/") {
			delete(c.m, k)
		}
	}
}

// mkdirAll makes sure dir and its ancestors exist as folders on the drive, and
// records them. It is for large files only: the other directories of the
// namespace live in the index.
func (v *Volume) mkdirAll(ctx context.Context, dir string) error {
	if dir == "" || v.ix.hasDDir(dir) {
		return nil
	}
	if p := parentOf(dir); p != "" {
		if err := v.mkdirAll(ctx, p); err != nil {
			return err
		}
	}
	err := v.st.Mkdir(ctx, dir)
	if err != nil && !errors.Is(err, store.ErrExists) {
		return err
	}
	if err := v.ix.addDDirs(dir); err != nil {
		return err
	}
	v.dc.cacheAdd(store.Entry{Path: dir, IsDir: true, ModTime: time.Now()})
	if err == nil {
		v.dk.markListed(dir) // made just now: nothing in it
		v.dc.nowExists(dir)
	} else {
		v.dc.forget(dir) // it was there already, with whatever it holds
	}
	return nil
}

// ---- read-side namespace ----

func (v *Volume) rowEntry(r fileRow) Entry {
	return Entry{Path: r.Path, Size: r.Size, ModTime: time.Unix(0, r.MTime), Packed: true, SHA1: r.SHA1}
}

func bigEntry(e store.Entry) Entry {
	return Entry{Path: e.Path, IsDir: e.IsDir, Size: e.Size, ModTime: e.ModTime, SHA1: e.SHA1}
}

// dirEntry is the entry of a directory kept in the index; mtime 0: not known.
func dirEntry(p string, mtime int64) Entry {
	mt := time.Now()
	if mtime != 0 {
		mt = time.Unix(0, mtime)
	}
	return Entry{Path: p, IsDir: true, ModTime: mt}
}

// Stat returns the file or directory at p.
func (v *Volume) Stat(ctx context.Context, p string) (Entry, error) {
	v.follow(ctx)
	p, err := clean(p)
	if err != nil {
		return Entry{}, err
	}
	if p == "" {
		return Entry{IsDir: true}, nil
	}
	if reserved(p) {
		return Entry{}, store.ErrNotFound
	}
	if r, ok, err := v.ix.get(p); err != nil {
		return Entry{}, err
	} else if ok {
		return v.rowEntry(r), nil
	}
	if mt, ok, err := v.ix.dirMTime(p); err != nil {
		return Entry{}, err
	} else if ok {
		return dirEntry(p, mt), nil
	}
	if v.ix.hasDDir(p) {
		return dirEntry(p, 0), nil
	}
	dir, name := splitPath(p)
	ents, err := v.listStore(ctx, dir)
	if err == nil {
		if e, ok := ents[name]; ok {
			return bigEntry(e), nil
		}
	} else if !errors.Is(err, store.ErrNotFound) {
		return Entry{}, err
	}
	// A directory that only exists because staged files are going to live in it.
	if any, err := v.ix.anyStagedUnder(p); err != nil {
		return Entry{}, err
	} else if any {
		return dirEntry(p, 0), nil
	}
	return Entry{}, store.ErrNotFound
}

// List returns the children of dir: large files from the drive's listing
// (only for folders the drive has), small files and directories from the index.
func (v *Volume) List(ctx context.Context, dir string) ([]Entry, error) {
	v.follow(ctx)
	dir, err := clean(dir)
	if err != nil {
		return nil, err
	}
	if reserved(dir) {
		return nil, store.ErrNotFound
	}
	ents, lerr := v.listStore(ctx, dir)
	if lerr != nil && !errors.Is(lerr, store.ErrNotFound) {
		return nil, lerr
	}
	rows, err := v.ix.listDir(dir)
	if err != nil {
		return nil, err
	}
	sdirs, err := v.ix.stagedDirs(dir)
	if err != nil {
		return nil, err
	}
	cdirs, err := v.ix.childDirs(dir)
	if err != nil {
		return nil, err
	}
	if lerr != nil && len(rows) == 0 && len(sdirs) == 0 && len(cdirs) == 0 && dir != "" {
		// Nothing in it: it still exists if it was made (Mkdir) or is a folder.
		if isDir, err := v.ix.isDir(dir); err != nil {
			return nil, err
		} else if !isDir {
			return nil, store.ErrNotFound
		}
	}
	out := make([]Entry, 0, len(ents)+len(rows)+len(sdirs)+len(cdirs))
	small := make(map[string]bool, len(rows))
	for _, r := range rows {
		small[r.Name] = true
		out = append(out, v.rowEntry(r))
	}
	for _, d := range cdirs {
		_, name := splitPath(d.Path)
		if !small[name] {
			small[name] = true
			out = append(out, dirEntry(d.Path, d.MTime))
		}
	}
	for _, name := range sdirs {
		if _, ok := ents[name]; !ok && !small[name] {
			small[name] = true
			out = append(out, dirEntry(path.Join(dir, name), 0))
		}
	}
	for name, e := range ents {
		if small[name] || (dir == "" && strings.HasPrefix(name, ".cfs")) {
			continue
		}
		out = append(out, bigEntry(e))
	}
	return out, nil
}

// Hash returns the SHA-1 of a file: from the index for packed files, from the
// drive for large ones (empty if the drive has none).
func (v *Volume) Hash(ctx context.Context, p string) (string, error) {
	p, err := clean(p)
	if err != nil {
		return "", err
	}
	if r, ok, err := v.ix.get(p); err != nil {
		return "", err
	} else if ok {
		return r.SHA1, nil
	}
	e, err := v.Stat(ctx, p)
	if err != nil {
		return "", err
	}
	if e.IsDir {
		return "", store.ErrIsDir
	}
	if e.SHA1 != "" {
		return e.SHA1, nil
	}
	return v.st.Hash(ctx, p)
}

// Mkdir creates a directory (and missing parents). It is recorded in the index
// and the log; the drive gets a folder only when a large file needs one.
func (v *Volume) Mkdir(ctx context.Context, dir string) error {
	dir, err := clean(dir)
	if err != nil {
		return err
	}
	if dir == "" {
		return nil
	}
	if reserved(dir) {
		return ErrReserved
	}
	end, werr := v.beginWrite(ctx)
	if werr != nil {
		return werr
	}
	defer end()
	v.structMu.RLock()
	defer v.structMu.RUnlock()
	if _, ok, err := v.ix.get(dir); err != nil {
		return err
	} else if ok {
		return store.ErrExists
	}
	if err := v.checkAncestors(ctx, dir); err != nil {
		return err
	}
	if big, ok, err := v.bigAt(ctx, dir); err != nil {
		return err
	} else if ok && !big.IsDir {
		return store.ErrExists
	}
	if isDir, err := v.ix.isDir(dir); err != nil || isDir {
		return err
	}
	return v.localOp(metalog.Record{Op: metalog.OpMkdir, Path: dir, MTime: time.Now().UnixNano()})
}

// Rmdir removes an empty directory.
func (v *Volume) Rmdir(ctx context.Context, dir string) error {
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
	ents, err := v.List(ctx, dir)
	if err != nil {
		return err
	}
	if len(ents) > 0 {
		return store.ErrNotEmpty
	}
	if v.ix.hasDDir(dir) {
		if err := v.st.Rmdir(ctx, dir); err != nil && !isNotFound(err) {
			return err
		}
		v.dk.dropListed(dir)
		if err := v.ix.tx(func(tx *sql.Tx) error { return dropDDirs(tx, dir) }); err != nil {
			return err
		}
	}
	if _, ok, err := v.ix.dirMTime(dir); err != nil {
		return err
	} else if ok {
		if err := v.localOp(metalog.Record{Op: metalog.OpRmdir, Path: dir}); err != nil {
			return err
		}
	}
	v.dc.dropTree(dir)
	v.dc.cacheDel(dir)
	return nil
}

// ---- small helpers shared by the files of this package ----

func isNotFound(err error) bool { return errors.Is(err, store.ErrNotFound) }

var _ = path.Join
var _ = sql.ErrNoRows

// nowExists makes the cached listing of a folder this volume has just created
// an empty one: the folder wasn't recorded, so nothing was in it.
func (c *dirCache) nowExists(dir string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if d, ok := c.m[dir]; !ok || d.absent {
		c.m[dir] = &dirEnt{at: time.Now(), ents: map[string]store.Entry{}}
	}
}

// forget drops the cached listing of one folder.
func (c *dirCache) forget(dir string) {
	c.mu.Lock()
	delete(c.m, dir)
	c.mu.Unlock()
}
