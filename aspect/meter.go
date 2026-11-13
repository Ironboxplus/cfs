package aspect

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Ironboxplus/cfs/store"
)

// Stats is a snapshot of what reached the store below the meter.
type Stats struct {
	Calls        map[string]int64
	Total        int64
	BytesRead    int64
	BytesWritten int64
	// Peak is the highest number of simultaneously running calls per class
	// since the meter was created or reset.
	PeakMeta     int
	PeakTransfer int
	PeakRead     int
}

func (s Stats) String() string {
	keys := make([]string, 0, len(s.Calls))
	for k := range s.Calls {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&b, "%s=%d ", k, s.Calls[k])
	}
	return fmt.Sprintf("calls=%d [%s] read=%dB written=%dB peak_inflight meta=%d transfer=%d read=%d",
		s.Total, strings.TrimSpace(b.String()), s.BytesRead, s.BytesWritten, s.PeakMeta, s.PeakTransfer, s.PeakRead)
}

// Meter counts the calls, bytes and peak concurrency per class reaching the
// store below it. Put it directly above the real store (below the gate) so the
// peak it records is what the drive actually sees.
type Meter struct {
	store.Store
	mu    sync.Mutex
	calls map[string]int64
	cur   [3]int
	peak  [3]int
	rd    atomic.Int64
	wr    atomic.Int64
}

func NewMeter(s store.Store) *Meter { return &Meter{Store: s, calls: map[string]int64{}} }

func (m *Meter) enter(op string, c Class) {
	m.mu.Lock()
	m.calls[op]++
	m.cur[c]++
	if m.cur[c] > m.peak[c] {
		m.peak[c] = m.cur[c]
	}
	m.mu.Unlock()
}

func (m *Meter) leave(c Class) {
	m.mu.Lock()
	m.cur[c]--
	m.mu.Unlock()
}

func (m *Meter) Snapshot() Stats {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := Stats{Calls: map[string]int64{}, PeakMeta: m.peak[Meta], PeakTransfer: m.peak[Transfer], PeakRead: m.peak[Read],
		BytesRead: m.rd.Load(), BytesWritten: m.wr.Load()}
	for k, v := range m.calls {
		s.Calls[k] = v
		s.Total += v
	}
	return s
}

// Reset zeroes the counters and restarts peak tracking.
func (m *Meter) Reset() {
	m.mu.Lock()
	m.calls = map[string]int64{}
	m.peak = m.cur
	m.mu.Unlock()
	m.rd.Store(0)
	m.wr.Store(0)
}

func (m *Meter) List(ctx context.Context, dir string) ([]store.Entry, error) {
	m.enter("list", Meta)
	defer m.leave(Meta)
	return m.Store.List(ctx, dir)
}

func (m *Meter) Stat(ctx context.Context, p string) (store.Entry, error) {
	m.enter("stat", Meta)
	defer m.leave(Meta)
	return m.Store.Stat(ctx, p)
}

func (m *Meter) Remove(ctx context.Context, p string) error {
	m.enter("remove", Meta)
	defer m.leave(Meta)
	return m.Store.Remove(ctx, p)
}

func (m *Meter) Move(ctx context.Context, src, dst string) error {
	m.enter("move", Meta)
	defer m.leave(Meta)
	return m.Store.Move(ctx, src, dst)
}

func (m *Meter) DirMove(ctx context.Context, src, dst string) error {
	m.enter("dirmove", Meta)
	defer m.leave(Meta)
	return m.Store.DirMove(ctx, src, dst)
}

func (m *Meter) Purge(ctx context.Context, dir string) error {
	m.enter("purge", Meta)
	defer m.leave(Meta)
	return m.Store.Purge(ctx, dir)
}

func (m *Meter) Mkdir(ctx context.Context, dir string) error {
	m.enter("mkdir", Meta)
	defer m.leave(Meta)
	return m.Store.Mkdir(ctx, dir)
}

func (m *Meter) Rmdir(ctx context.Context, dir string) error {
	m.enter("rmdir", Meta)
	defer m.leave(Meta)
	return m.Store.Rmdir(ctx, dir)
}

func (m *Meter) SetModTime(ctx context.Context, p string, t time.Time) error {
	m.enter("setmodtime", Meta)
	defer m.leave(Meta)
	return m.Store.SetModTime(ctx, p, t)
}

func (m *Meter) Hash(ctx context.Context, p string) (string, error) {
	m.enter("hash", Meta)
	defer m.leave(Meta)
	return m.Store.Hash(ctx, p)
}

// OpenStream counts the call and the bytes. The body is not part of the in-flight
// peaks: a stream is paced by its consumer and holds no slot.
func (m *Meter) OpenStream(ctx context.Context, p string, off, n int64, want store.Entry) (io.ReadCloser, error) {
	m.mu.Lock()
	m.calls["openstream"]++
	m.mu.Unlock()
	rc, err := m.Store.OpenStream(ctx, p, off, n, want)
	if err != nil {
		return nil, err
	}
	return &countReader{rc: rc, n: &m.rd}, nil
}

type countSource struct {
	store.Source
	n *atomic.Int64
}

func (c countSource) Open() (io.ReadCloser, error) {
	rc, err := c.Source.Open()
	if err != nil {
		return nil, err
	}
	return &countReader{rc: rc, n: c.n}, nil
}

type countRange struct {
	store.RangeOpener
	n *atomic.Int64
}

func (c countRange) OpenRange(off, n int64) (io.ReadCloser, error) {
	rc, err := c.RangeOpener.OpenRange(off, n)
	if err != nil {
		return nil, err
	}
	return &countReader{rc: rc, n: c.n}, nil
}

type countReader struct {
	rc   io.ReadCloser
	n    *atomic.Int64
	once sync.Once
	done func()
}

func (r *countReader) Read(p []byte) (int, error) {
	n, err := r.rc.Read(p)
	r.n.Add(int64(n))
	if err != nil && r.done != nil {
		r.once.Do(r.done)
	}
	return n, err
}

func (r *countReader) Close() error {
	err := r.rc.Close()
	if r.done != nil {
		r.once.Do(r.done)
	}
	return err
}

func (m *Meter) Put(ctx context.Context, p string, src store.Source, o store.PutOpts) error {
	m.enter("put", Transfer)
	defer m.leave(Transfer)
	if o.Ranged != nil {
		o.Ranged = countRange{o.Ranged, &m.wr}
	}
	return m.Store.Put(ctx, p, countSource{src, &m.wr}, o)
}

func (m *Meter) Open(ctx context.Context, p string, off, n int64) (io.ReadCloser, error) {
	m.enter("open", Read)
	rc, err := m.Store.Open(ctx, p, off, n)
	if err != nil {
		m.leave(Read)
		return nil, err
	}
	return &countReader{rc: rc, n: &m.rd, done: func() { m.leave(Read) }}, nil
}
