package core

import (
	"context"
	"crypto/sha1"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Ironboxplus/cfs/internal/crash"
)

// Staging (AckStaged).
//
// A small file is acknowledged once its bytes are fsynced into
// <state>/stage/<id>.dat and a row in the local index's "staged" table says
// which path it belongs to. Readers see the committed rows overlaid with the
// staged ones (index.get/listDir), so the file is readable and listed from that
// moment. A single packer goroutine takes staged files, in the order they were
// acknowledged, and feeds them to the pack pipeline; when a pack is committed
// (pack and log segment on the drive) the same local transaction that applies
// its records deletes the staged rows. A restart replays whatever is still
// staged, so an acknowledged file always ends up on the drive.
//
// The staged state per path is one entry: a newer Put of the same path replaces
// the older one, which is then never committed unless it was already in a
// sealed pack. A pack that fails to commit puts its staged files back in the
// queue after a back-off (they are still on local disk); the metadata rows that
// rode in the failed segment stay under its sequence number (releaseInflight).
//
// Metadata operations that touch a staged name (remove, move, directory move,
// set mtime, a large Put of the same name) first force the files concerned out
// to the drive (waitConflicts), so the log order is the order of the calls and
// the log never has to mention a file it hasn't got yet.

type stagedEnt struct {
	id   int64
	size int64
}

type stager struct {
	dir string

	wmu sync.Mutex // serialises staging rows (tx and registry update are one step)

	mu      sync.Mutex
	byPath  map[string]*stagedEnt
	bytes   int64
	changed chan struct{} // closed and replaced whenever the registry changes
	intake  []*member
	fails   int
	failGen uint64
	lastErr error

	wake    chan struct{}
	sealReq atomic.Bool
	nextID  atomic.Int64
	started atomic.Bool
}

func newStager(stateDir string) *stager {
	return &stager{
		dir:     filepath.Join(stateDir, "stage"),
		byPath:  map[string]*stagedEnt{},
		changed: make(chan struct{}),
		wake:    make(chan struct{}, 1),
	}
}

func (s *stager) path(id int64) string { return filepath.Join(s.dir, fmt.Sprintf("%012d.dat", id)) }

// broadcast wakes everything waiting for a change. s.mu held.
func (s *stager) broadcast() {
	close(s.changed)
	s.changed = make(chan struct{})
}

func (s *stager) poke() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// Staged reports how many small files, and how many bytes, are acknowledged
// but not on the drive yet.
func (v *Volume) Staged() (files int, bytes int64) {
	v.stg.mu.Lock()
	defer v.stg.mu.Unlock()
	return len(v.stg.byPath), v.stg.bytes
}

func writeFileSync(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(path)
		return err
	}
	if runtime.GOOS != "windows" { // make the directory entry durable too
		if d, derr := os.Open(filepath.Dir(path)); derr == nil {
			d.Sync()
			d.Close()
		}
	}
	return nil
}

// stageInit loads the staged rows left by an earlier run, throws away stage
// files nothing refers to, and starts the packer.
func (v *Volume) stageInit() error {
	s := v.stg
	if err := os.MkdirAll(s.dir, 0o755); err != nil {
		return err
	}
	rows, err := v.ix.db.Query("SELECT " + stagedCols + " FROM staged ORDER BY id")
	if err != nil {
		return err
	}
	type srow struct {
		r     fileRow
		stray bool
	}
	var all []srow
	for rows.Next() {
		var r fileRow
		var stray int
		if err := rows.Scan(&r.Path, &r.Dir, &r.Name, &r.Off, &r.Size, &r.MTime, &r.SHA1, &stray); err != nil {
			rows.Close()
			return err
		}
		all = append(all, srow{r, stray != 0})
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	keep := map[int64]bool{}
	var maxID int64
	for _, x := range all {
		maxID = max(maxID, x.r.Off)
		st, err := os.Stat(s.path(x.r.Off))
		if err != nil || st.Size() != x.r.Size {
			v.logf("cfs: staged file %s (stage id %d) is missing or damaged on local disk (%v): dropping it", x.r.Path, x.r.Off, err)
			if _, derr := v.ix.db.Exec("DELETE FROM staged WHERE path=? AND id=?", x.r.Path, x.r.Off); derr != nil {
				return derr
			}
			continue
		}
		keep[x.r.Off] = true
		s.byPath[x.r.Path] = &stagedEnt{id: x.r.Off, size: x.r.Size}
		s.bytes += x.r.Size
		s.intake = append(s.intake, &member{path: x.r.Path, size: x.r.Size, mtime: x.r.MTime, sha1: x.r.SHA1, strayBig: x.stray, id: x.r.Off})
	}
	if ents, err := os.ReadDir(s.dir); err == nil {
		for _, e := range ents {
			id, perr := strconv.ParseInt(strings.TrimSuffix(e.Name(), ".dat"), 10, 64)
			if perr != nil {
				continue
			}
			maxID = max(maxID, id)
			if !keep[id] {
				os.Remove(filepath.Join(s.dir, e.Name()))
			}
		}
	}
	s.nextID.Store(maxID)
	if len(s.intake) > 0 {
		v.logf("cfs: %d staged files (%d bytes) from an earlier run will be committed", len(s.intake), s.bytes)
	}
	if !v.ReadOnly() {
		s.started.Store(true)
		v.wg.Add(1)
		go v.packerLoop()
		s.poke()
	}
	return nil
}

// stageReserve waits until n more bytes fit into the staging area.
func (v *Volume) stageReserve(ctx context.Context, n int64) error {
	s := v.stg
	for {
		s.mu.Lock()
		if s.bytes == 0 || s.bytes+n <= v.cfg.StageMaxBytes {
			s.bytes += n
			s.mu.Unlock()
			return nil
		}
		ch := s.changed
		s.mu.Unlock()
		v.requestSeal() // a full area must drain: don't wait for the seal timers
		select {
		case <-ch:
		case <-time.After(100 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		case <-v.ctx.Done():
			return ErrClosed
		}
	}
}

func (v *Volume) stageUnreserve(n int64) {
	v.stg.mu.Lock()
	v.stg.bytes -= n
	v.stg.broadcast()
	v.stg.mu.Unlock()
}

// putStaged is putSmall for AckStaged.
func (v *Volume) putStaged(ctx context.Context, p string, data []byte, mtime time.Time, stray bool) (Entry, error) {
	if !v.stg.started.Load() {
		return Entry{}, ErrReadOnly
	}
	sum := sha1.Sum(data)
	m := &member{path: p, size: int64(len(data)), mtime: mtime.UnixNano(), sha1: hex.EncodeToString(sum[:]), strayBig: stray}
	n := m.size
	s := v.stg
	if err := v.stageReserve(ctx, n); err != nil {
		return Entry{}, err
	}
	m.id = s.nextID.Add(1)
	if err := writeFileSync(s.path(m.id), data); err != nil {
		v.stageUnreserve(n)
		return Entry{}, err
	}
	fail := func(err error) (Entry, error) {
		os.Remove(s.path(m.id))
		v.stageUnreserve(n)
		return Entry{}, err
	}
	if err := ctx.Err(); err != nil {
		return fail(err)
	}
	v.structMu.RLock()
	defer v.structMu.RUnlock()
	if v.closed.Load() {
		return fail(ErrClosed)
	}
	dir, name := splitPath(p)
	strayN := 0
	if stray {
		strayN = 1
	}
	s.wmu.Lock()
	err := v.ix.tx(func(tx *sql.Tx) error {
		_, err := tx.Exec("INSERT OR REPLACE INTO staged("+stagedCols+") VALUES(?,?,?,?,?,?,?,?)",
			p, dir, name, m.id, m.size, m.mtime, m.sha1, strayN)
		return err
	})
	if err != nil {
		s.wmu.Unlock()
		return fail(err)
	}
	crash.Point("staged")
	s.mu.Lock()
	old := s.byPath[p]
	s.byPath[p] = &stagedEnt{id: m.id, size: m.size}
	if old != nil {
		s.bytes -= old.size
	}
	s.intake = append(s.intake, m)
	s.broadcast()
	s.mu.Unlock()
	s.wmu.Unlock()
	if old != nil {
		os.Remove(s.path(old.id))
	}
	s.poke()
	return Entry{Path: p, Size: m.size, ModTime: time.Unix(0, m.mtime), Packed: true, SHA1: m.sha1}, nil
}

// readStaged returns the content of a staged file, verified.
func (v *Volume) readStaged(r fileRow) ([]byte, error) {
	if r.Size == 0 {
		return nil, nil
	}
	data, err := os.ReadFile(v.stg.path(r.Off))
	if err != nil {
		return nil, err
	}
	sum := sha1.Sum(data)
	if int64(len(data)) != r.Size || hex.EncodeToString(sum[:]) != r.SHA1 {
		return nil, fmt.Errorf("cfs: staged file %s: %w", r.Path, ErrCorrupt)
	}
	return data, nil
}

// stageDone forgets the staged files a commit has just put on the drive.
func (v *Volume) stageDone(live []*member) {
	s := v.stg
	s.mu.Lock()
	for _, m := range live {
		if m.id == 0 {
			continue
		}
		if e := s.byPath[m.path]; e != nil && e.id == m.id {
			delete(s.byPath, m.path)
			s.bytes -= e.size
		}
	}
	s.fails = 0
	s.broadcast()
	s.mu.Unlock()
	for _, m := range live {
		if m.id != 0 {
			os.Remove(s.path(m.id))
		}
	}
}

// stageFailed puts the staged files of a pack that failed to commit back in
// the queue, after a back-off. Files that were replaced meanwhile are dropped.
func (v *Volume) stageFailed(b *batch, err error) {
	if errors.Is(err, ErrClosed) || v.closed.Load() {
		return
	}
	s := v.stg
	s.mu.Lock()
	var again []*member
	for _, m := range b.mem {
		if m.id == 0 {
			continue
		}
		if e := s.byPath[m.path]; e != nil && e.id == m.id {
			again = append(again, &member{path: m.path, size: m.size, mtime: m.mtime, sha1: m.sha1, strayBig: m.strayBig, id: m.id})
		}
	}
	s.fails++
	s.failGen++
	s.lastErr = err
	delay := min(250*time.Millisecond<<min(s.fails-1, 6), 10*time.Second)
	s.broadcast()
	s.mu.Unlock()
	if len(again) == 0 {
		return
	}
	v.logf("cfs: %d staged files go back to the queue after a failed commit (retry in %v): %v", len(again), delay, err)
	time.AfterFunc(delay, func() {
		if v.closed.Load() {
			return
		}
		s.mu.Lock()
		s.intake = append(s.intake, again...)
		s.mu.Unlock()
		s.poke()
	})
}

func (v *Volume) requestSeal() {
	v.stg.sealReq.Store(true)
	v.stg.poke()
}

func (s *stager) pop() *member {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.intake) == 0 {
		return nil
	}
	m := s.intake[0]
	s.intake[0] = nil
	s.intake = s.intake[1:]
	return m
}

func (s *stager) current(m *member) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.byPath[m.path]
	return e != nil && e.id == m.id
}

// packerLoop moves staged files, in order, into the pack pipeline. It is the
// only producer of staged members, which is what keeps two versions of one
// name in the order they were written.
func (v *Volume) packerLoop() {
	defer v.wg.Done()
	s := v.stg
	for {
		if v.ctx.Err() != nil {
			return
		}
		m := s.pop()
		if m == nil {
			if s.sealReq.Swap(false) {
				v.batchMu.Lock()
				if v.cur != nil {
					v.sealLocked(v.cur)
				}
				v.batchMu.Unlock()
			}
			select {
			case <-s.wake:
			case <-v.ctx.Done():
				return
			}
			continue
		}
		v.packStaged(m)
	}
}

func (v *Volume) packStaged(m *member) {
	s := v.stg
	if !s.current(m) {
		return // replaced by a newer version, or gone
	}
	// An older version of this name in a pack that is already on its way must
	// be committed first, or the two could reach the log in the wrong order.
	if err := v.waitSealedPath(v.ctx, m.path); err != nil {
		return
	}
	if !s.current(m) {
		return
	}
	data, err := os.ReadFile(s.path(m.id))
	if err == nil {
		sum := sha1.Sum(data)
		if int64(len(data)) != m.size || hex.EncodeToString(sum[:]) != m.sha1 {
			err = ErrCorrupt
		}
	}
	if err != nil {
		if !s.current(m) {
			return // replaced while we looked
		}
		v.logf("cfs: staged file %s (stage id %d) can't be read back (%v): dropping it", m.path, m.id, err)
		v.dropStaged(m)
		return
	}
	stored, meth, crc := v.encodeMember(m.path, data)
	m.meth = meth
	if _, err := v.packAdd(v.ctx, m, stored, crc); err != nil {
		if v.ctx.Err() == nil {
			v.logf("cfs: pack %s: %v", m.path, err)
		}
		// Not in a pack: go back to the queue.
		s.mu.Lock()
		s.intake = append([]*member{{path: m.path, size: m.size, mtime: m.mtime, sha1: m.sha1, strayBig: m.strayBig, id: m.id}}, s.intake...)
		s.mu.Unlock()
		select {
		case <-time.After(time.Second):
		case <-v.ctx.Done():
		}
	}
}

func (v *Volume) dropStaged(m *member) {
	s := v.stg
	s.wmu.Lock()
	defer s.wmu.Unlock()
	if _, err := v.ix.db.Exec("DELETE FROM staged WHERE path=? AND id=?", m.path, m.id); err != nil {
		v.logf("cfs: drop staged %s: %v", m.path, err)
		return
	}
	s.mu.Lock()
	if e := s.byPath[m.path]; e != nil && e.id == m.id {
		delete(s.byPath, m.path)
		s.bytes -= e.size
	}
	s.broadcast()
	s.mu.Unlock()
	os.Remove(s.path(m.id))
}

// waitSealedPath waits for every sealed pack holding a live version of p.
func (v *Volume) waitSealedPath(ctx context.Context, p string) error {
	for {
		var wait *batch
		v.batchMu.Lock()
		for b := range v.inflight {
			if b == v.cur {
				continue
			}
			for _, m := range b.mem {
				if m.path == p && !m.superseded {
					wait = b
					break
				}
			}
			if wait != nil {
				break
			}
		}
		v.batchMu.Unlock()
		if wait == nil {
			return nil
		}
		select {
		case <-wait.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// waitStaged forces the staged files whose path matches out to the drive and
// waits until none is left. It fails when a commit fails while it waits: the
// drive is not taking writes, and the caller is better off knowing than hanging.
func (v *Volume) waitStaged(ctx context.Context, match func(string) bool) error {
	s := v.stg
	s.mu.Lock()
	gen := s.failGen
	s.mu.Unlock()
	for {
		s.mu.Lock()
		hit := false
		for p := range s.byPath {
			if match(p) {
				hit = true
				break
			}
		}
		ch := s.changed
		failed := s.failGen != gen
		lastErr := s.lastErr
		s.mu.Unlock()
		if !hit {
			return nil
		}
		if failed {
			return fmt.Errorf("cfs: staged files could not be committed: %w", lastErr)
		}
		if !s.started.Load() {
			return ErrReadOnly
		}
		v.requestSeal()
		select {
		case <-ch:
		case <-time.After(100 * time.Millisecond):
			// Files that come back to the queue after a failed commit arrive in a
			// pack nobody has asked to seal: ask again.
		case <-ctx.Done():
			return ctx.Err()
		case <-v.ctx.Done():
			return ErrClosed
		}
	}
}
