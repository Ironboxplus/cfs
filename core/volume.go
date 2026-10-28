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
// single-writer: if another live writer holds the lease, this one is read-only
// and every mutation returns ErrReadOnly.
type Volume struct {
	cfg Config
	st  store.Store
	log metalog.MetaLog
	ix  *index

	writerID string
	epoch    uint64
	readOnly atomic.Bool
	fenced   atomic.Bool
	closed   atomic.Bool
	closing  atomic.Bool

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
	wc      *memCache    // read windows of packs
	packSz  sync.Map     // pack name -> bytes
	segsNew atomic.Int64 // segments committed since the last checkpoint
	ckptRun atomic.Bool
	ckptOne sync.Mutex // one Checkpoint at a time

	leaseMu sync.Mutex
}

func (v *Volume) logf(format string, args ...any) { v.cfg.Logf(format, args...) }

// Epoch returns this writer's epoch.
func (v *Volume) Epoch() uint64 { return v.epoch }

// ReadOnly reports whether another writer holds the volume.
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
	if err := v.takeLease(ctx); err != nil {
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
		v.wg.Add(1)
		go v.heartbeatLoop()
		v.kickMeta()
	}
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
	return v.flushMeta(ctx)
}

// Close flushes, checkpoints when there is something new, releases the lease
// and closes the local index.
func (v *Volume) Close(ctx context.Context) error {
	if !v.closing.CompareAndSwap(false, true) {
		return nil
	}
	var first error
	keep := func(err error) {
		if err != nil && first == nil {
			first = err
		}
	}
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

type lease struct {
	Writer    string `json:"writer"`
	Epoch     uint64 `json:"epoch"`
	Heartbeat int64  `json:"heartbeat_ms"`
	Released  bool   `json:"released,omitempty"`
	Host      string `json:"host,omitempty"`
}

const leasePath = CtlDir + "/lease.json"

// readLease reads the lease file. The drive may replace the file in place, so a
// read that catches it half written is retried before it counts as an error.
func (v *Volume) readLease(ctx context.Context) (lease, bool, error) {
	var lastErr error
	for i := 0; i < 8; i++ {
		if i > 0 {
			select {
			case <-time.After(time.Duration(i) * 20 * time.Millisecond):
			case <-ctx.Done():
				return lease{}, false, ctx.Err()
			}
		}
		rc, err := v.st.Open(ctx, leasePath, 0, -1)
		if errors.Is(err, store.ErrNotFound) {
			return lease{}, false, nil
		}
		if err != nil {
			return lease{}, false, err
		}
		var l lease
		err = json.NewDecoder(rc).Decode(&l)
		rc.Close()
		if err == nil {
			return l, true, nil
		}
		lastErr = err
	}
	return lease{}, false, fmt.Errorf("%w: %v", errBadLease, lastErr)
}

// errBadLease is a lease file that stays unreadable: a writer that died while
// rewriting it (the drive has no atomic replace) leaves it truncated for good.
var errBadLease = errors.New("cfs: unreadable lease")

func (v *Volume) writeLease(ctx context.Context, released bool) error {
	l := lease{Writer: v.writerID, Epoch: v.epoch, Heartbeat: time.Now().UnixMilli(), Released: released}
	l.Host, _ = os.Hostname()
	b, _ := json.Marshal(l)
	return v.st.Put(ctx, leasePath, store.Bytes(b), store.PutOpts{Size: int64(len(b)), ModTime: time.Now()})
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

// takeLease decides between writer and read-only. A live lease of another
// writer means read-only; a stale, released or foreign-but-dead one is taken
// over with epoch+1. The drive has no compare-and-swap, so this is a best
// effort: the commit path re-reads the lease before every commit and fences
// itself when it sees a newer epoch.
func (v *Volume) takeLease(ctx context.Context) error {
	l, ok, err := v.readLease(ctx)
	if errors.Is(err, errBadLease) {
		// Nobody can be heartbeating into a file that has stayed unreadable
		// through all the retries: treat it as absent. The epoch still comes
		// from what the drive holds.
		v.logf("cfs: %v: taking the volume over", err)
		l, ok, err = lease{}, false, nil
	}
	if err != nil {
		return err
	}
	local := v.ix.kvUint("epoch")
	if ok && l.Writer != v.writerID && !l.Released &&
		time.Since(time.UnixMilli(l.Heartbeat)) < v.cfg.LeaseTTL {
		v.readOnly.Store(true)
		v.epoch = l.Epoch
		v.logf("cfs: volume held by writer %s (epoch %d), opening read-only", l.Writer, l.Epoch)
		return nil
	}
	if ok && l.Writer == v.writerID && l.Epoch == local && local != 0 {
		v.epoch = local
	} else {
		drive, err := v.maxDriveEpoch(ctx, l, ok)
		if err != nil {
			return err
		}
		v.epoch = max(drive, local) + 1
		if err := v.ix.kvSet("epoch", strconv.FormatUint(v.epoch, 10)); err != nil {
			return err
		}
	}
	return v.writeLease(ctx, false)
}

func (v *Volume) releaseLease(ctx context.Context) error {
	if v.ReadOnly() || v.fenced.Load() {
		return nil
	}
	return v.writeLease(ctx, true)
}

// checkLease re-reads the lease and fences this writer if a newer epoch or a
// different writer holds it. Called before every commit.
func (v *Volume) checkLease(ctx context.Context) error {
	l, ok, err := v.readLease(ctx)
	if err != nil {
		return err
	}
	if ok && (l.Epoch > v.epoch || (l.Epoch == v.epoch && l.Writer != v.writerID)) {
		v.fenced.Store(true)
		return ErrFenced
	}
	return nil
}

func (v *Volume) heartbeatLoop() {
	defer v.wg.Done()
	t := time.NewTicker(v.cfg.LeaseTTL / 3)
	defer t.Stop()
	for {
		select {
		case <-v.ctx.Done():
			return
		case <-t.C:
		}
		v.leaseMu.Lock()
		err := v.checkLease(v.ctx)
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
	mu    sync.Mutex
	ttl   time.Duration
	m     map[string]*dirEnt
	known map[string]time.Time // directories seen to exist on the drive
	sf    map[string]*dirFlight
}

type dirFlight struct {
	done chan struct{}
	ents map[string]store.Entry
	err  error
}

func newDirCache(ttl time.Duration) *dirCache {
	return &dirCache{ttl: ttl, m: map[string]*dirEnt{}, known: map[string]time.Time{}, sf: map[string]*dirFlight{}}
}

func (c *dirCache) isKnown(dir string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	t, ok := c.known[dir]
	return ok && time.Since(t) < c.ttl
}

func (c *dirCache) markKnown(dir string) {
	c.mu.Lock()
	c.known[dir] = time.Now()
	c.mu.Unlock()
}

// listStore lists dir on the drive through the cache. The returned map must
// not be modified.
func (v *Volume) listStore(ctx context.Context, dir string) (map[string]store.Entry, error) {
	c := v.dc
	c.mu.Lock()
	if e, ok := c.m[dir]; ok && time.Since(e.at) < c.ttl {
		c.mu.Unlock()
		if e.absent {
			return nil, store.ErrNotFound
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
		c.known[dir] = time.Now()
		for _, e := range ents {
			if e.IsDir {
				c.known[e.Path] = time.Now()
			}
		}
	} else if errors.Is(err, store.ErrNotFound) {
		c.m[dir] = &dirEnt{at: time.Now(), absent: true}
	}
	c.mu.Unlock()
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
	if e.IsDir {
		c.known[e.Path] = time.Now()
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
	c.known = map[string]time.Time{}
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
	for k := range c.known {
		if k == p || strings.HasPrefix(k, p+"/") {
			delete(c.known, k)
		}
	}
}

// mkdirAll makes sure dir and its ancestors exist on the drive.
func (v *Volume) mkdirAll(ctx context.Context, dir string) error {
	if dir == "" || v.dc.isKnown(dir) {
		return nil
	}
	if p := parentOf(dir); p != "" {
		if err := v.mkdirAll(ctx, p); err != nil {
			return err
		}
	}
	if err := v.st.Mkdir(ctx, dir); err != nil && !errors.Is(err, store.ErrExists) {
		return err
	}
	v.dc.cacheAdd(store.Entry{Path: dir, IsDir: true, ModTime: time.Now()})
	v.dc.markKnown(dir)
	v.dc.nowExists(dir)
	return nil
}

// ---- read-side namespace ----

func (v *Volume) rowEntry(r fileRow) Entry {
	return Entry{Path: r.Path, Size: r.Size, ModTime: time.Unix(0, r.MTime), Packed: true, SHA1: r.SHA1}
}

func bigEntry(e store.Entry) Entry {
	return Entry{Path: e.Path, IsDir: e.IsDir, Size: e.Size, ModTime: e.ModTime, SHA1: e.SHA1}
}

// Stat returns the file or directory at p.
func (v *Volume) Stat(ctx context.Context, p string) (Entry, error) {
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
		return Entry{Path: p, IsDir: true, ModTime: time.Now()}, nil
	}
	return Entry{}, store.ErrNotFound
}

// List returns the children of dir: directories and large files from the
// drive's listing, small files from the index.
func (v *Volume) List(ctx context.Context, dir string) ([]Entry, error) {
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
	if lerr != nil && len(rows) == 0 && len(sdirs) == 0 && dir != "" {
		return nil, store.ErrNotFound
	}
	out := make([]Entry, 0, len(ents)+len(rows)+len(sdirs))
	small := make(map[string]bool, len(rows))
	for _, r := range rows {
		small[r.Name] = true
		out = append(out, v.rowEntry(r))
	}
	for _, name := range sdirs {
		if _, ok := ents[name]; !ok && !small[name] {
			small[name] = true
			out = append(out, Entry{Path: path.Join(dir, name), IsDir: true, ModTime: time.Now()})
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

// Mkdir creates a directory (and missing parents) on the drive.
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
	if err := v.writable(); err != nil {
		return err
	}
	if _, ok, _ := v.ix.get(dir); ok {
		return store.ErrExists
	}
	return v.mkdirAll(ctx, dir)
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
	if err := v.writable(); err != nil {
		return err
	}
	v.structMu.Lock()
	defer v.structMu.Unlock()
	ents, err := v.List(ctx, dir)
	if err != nil {
		return err
	}
	if len(ents) > 0 {
		return store.ErrNotEmpty
	}
	if err := v.st.Rmdir(ctx, dir); err != nil {
		return err
	}
	v.dc.dropTree(dir)
	v.dc.cacheDel(dir)
	return nil
}

// ---- small helpers shared by the files of this package ----

func isNotFound(err error) bool { return errors.Is(err, store.ErrNotFound) }

var _ = path.Join
var _ = sql.ErrNoRows

// nowExists turns a cached "no such directory" into an empty directory, after
// it has been created.
func (c *dirCache) nowExists(dir string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if d, ok := c.m[dir]; ok && d.absent {
		c.m[dir] = &dirEnt{at: time.Now(), ents: map[string]store.Entry{}}
	}
}
