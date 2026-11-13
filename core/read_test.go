package core_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Ironboxplus/cfs/aspect"
	"github.com/Ironboxplus/cfs/core"
	"github.com/Ironboxplus/cfs/internal/testutil"
	"github.com/Ironboxplus/cfs/store"
	"github.com/Ironboxplus/cfs/store/rclonestore"
	"github.com/rclone/rclone/fs"
)

// probe sits between the aspect stack and a real store and counts what the
// volume asks of it: every Open (path, offset, length) and every byte the
// bodies hand out. It can hand the bytes out slowly and cut a body short with an
// error, so the tests can see what a reader does while data is still on its way.
type probe struct {
	store.Store
	mu    sync.Mutex
	opens []probeOpen
	read  atomic.Int64
	// delay is how long every Open takes before it answers.
	delay time.Duration
	// slow, when > 0, is the most a single Read of a body returns.
	slow int
	// putHook, when set, is called with every Put; it returns how long the Put
	// stays on its way to the drive. The write then lands whatever happens to the
	// caller's context: a request that has reached the server is not taken back by
	// the client giving up.
	putHook func(path string, src store.Source) time.Duration
	// cut, when set, says after how many bytes the body of that Open fails
	// (seq counts the Opens of the path so far, from 0); < 0: never.
	cut func(path string, off int64, seq int) int64
}

type probeOpen struct {
	path   string
	off, n int64
}

var errInjected = errors.New("injected read error")

// OpenStream counts like Open: the probe sees every body.
func (p *probe) OpenStream(ctx context.Context, path string, off, n int64, want store.Entry) (io.ReadCloser, error) {
	return p.opened(path, off, n, func() (io.ReadCloser, error) { return p.Store.OpenStream(ctx, path, off, n, want) })
}

func (p *probe) Open(ctx context.Context, path string, off, n int64) (io.ReadCloser, error) {
	return p.opened(path, off, n, func() (io.ReadCloser, error) { return p.Store.Open(ctx, path, off, n) })
}

func (p *probe) opened(path string, off, n int64, open func() (io.ReadCloser, error)) (io.ReadCloser, error) {
	if p.delay > 0 {
		time.Sleep(p.delay)
	}
	rc, err := open()
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	seq := 0
	for _, o := range p.opens {
		if o.path == path {
			seq++
		}
	}
	p.opens = append(p.opens, probeOpen{path, off, n})
	p.mu.Unlock()
	cut := int64(-1)
	if p.cut != nil {
		cut = p.cut(path, off, seq)
	}
	return &probeBody{rc: rc, p: p, cut: cut}, nil
}

type probeBody struct {
	rc        io.ReadCloser
	p         *probe
	cut       int64
	delivered int64
}

func (b *probeBody) Read(buf []byte) (int, error) {
	if b.p.slow > 0 && len(buf) > b.p.slow {
		buf = buf[:b.p.slow]
	}
	if b.cut >= 0 {
		if b.delivered >= b.cut {
			return 0, errInjected
		}
		if int64(len(buf)) > b.cut-b.delivered {
			buf = buf[:b.cut-b.delivered]
		}
	}
	n, err := b.rc.Read(buf)
	b.delivered += int64(n)
	b.p.read.Add(int64(n))
	return n, err
}

func (b *probeBody) Close() error { return b.rc.Close() }

func (p *probe) Put(ctx context.Context, path string, src store.Source, o store.PutOpts) error {
	if p.putHook == nil {
		return p.Store.Put(ctx, path, src, o)
	}
	d := p.putHook(path, src)
	time.Sleep(d)
	return p.Store.Put(context.WithoutCancel(ctx), path, src, o)
}

func (p *probe) reset() {
	p.mu.Lock()
	p.opens = nil
	p.mu.Unlock()
	p.read.Store(0)
}

func (p *probe) opensOf(substr string) []probeOpen {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []probeOpen
	for _, o := range p.opens {
		if strings.Contains(o.path, substr) {
			out = append(out, o)
		}
	}
	return out
}

type probeEnv struct {
	v     *core.Volume
	pr    *probe
	meter *aspect.Meter
	drive string
}

// newProbeEnv opens a volume on a local directory through a probe.
func newProbeEnv(t *testing.T, gate aspect.GateOpts, tweak func(*core.Config)) *probeEnv {
	t.Helper()
	drive := t.TempDir()
	pr := &probe{Store: testutil.LocalStore(t, drive)}
	st, m := testutil.Stacked(pr, aspect.Options{Gate: gate})
	cfg := testutil.FastConfig(t.TempDir())
	cfg.Threshold = 64 << 10
	if tweak != nil {
		tweak(&cfg)
	}
	v, err := core.Open(context.Background(), st, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { v.Close(context.Background()) })
	return &probeEnv{v: v, pr: pr, meter: m, drive: drive}
}

func (e *probeEnv) put(t *testing.T, p string, data []byte) {
	t.Helper()
	if _, err := e.v.Put(context.Background(), p, bytes.NewReader(data), int64(len(data)), time.Now()); err != nil {
		t.Fatalf("put %s: %v", p, err)
	}
}

// allocDelta is the bytes allocated while f runs.
func allocDelta(f func()) uint64 {
	runtime.GC()
	var a, b runtime.MemStats
	runtime.ReadMemStats(&a)
	f()
	runtime.ReadMemStats(&b)
	return b.TotalAlloc - a.TotalAlloc
}

// drainFixed reads rc to its end through one fixed buffer and returns the byte
// count and the SHA-1.
func drainFixed(rc io.Reader) (int64, string, error) {
	buf := make([]byte, 32<<10)
	h := sha256.New()
	var n int64
	for {
		k, err := rc.Read(buf)
		h.Write(buf[:k])
		n += int64(k)
		if err == io.EOF {
			return n, hex.EncodeToString(h.Sum(nil)), nil
		}
		if err != nil {
			return n, "", err
		}
	}
}

// A large file's reader is the body of one ranged request for exactly the bytes
// asked for, and its first Read does not wait for more than it needs.
func TestReadBigFileIsOneRangeAndStartsAtOnce(t *testing.T) {
	ctx := context.Background()
	e := newProbeEnv(t, aspect.GateOpts{}, nil)
	data := randBytes(4 << 20)
	e.put(t, "big/f.bin", data)
	e.pr.slow = 4096

	e.pr.reset()
	rc, err := e.v.Open(ctx, "big/f.bin", 0, 2<<20)
	if err != nil {
		t.Fatal(err)
	}
	first := make([]byte, 100)
	n, err := rc.Read(first)
	if err != nil || n == 0 {
		t.Fatalf("first read: %d %v", n, err)
	}
	got := e.pr.read.Load()
	t.Logf("first Read returned %d bytes after the store handed out %d of the 2 MiB asked for", n, got)
	if got > 8192 {
		t.Errorf("the store handed out %d bytes before the first Read returned", got)
	}
	rest, err := io.ReadAll(rc)
	rc.Close()
	if err != nil || !bytes.Equal(append(first[:n:n], rest...), data[:2<<20]) {
		t.Fatalf("content: %v", err)
	}

	for _, c := range []struct{ off, n int64 }{{1000003, 777}, {5, 1 << 20}, {4<<20 - 10, 100}, {123, -1}} {
		e.pr.reset()
		rc, err := e.v.Open(ctx, "big/f.bin", c.off, c.n)
		if err != nil {
			t.Fatal(err)
		}
		b, err := io.ReadAll(rc)
		rc.Close()
		end := int64(len(data))
		if c.n >= 0 && c.off+c.n < end {
			end = c.off + c.n
		}
		if err != nil || !bytes.Equal(b, data[c.off:end]) {
			t.Fatalf("range %+v: %v", c, err)
		}
		ops := e.pr.opensOf("big/f.bin")
		if len(ops) != 1 || ops[0].off != c.off || ops[0].n != end-c.off {
			t.Errorf("range %+v made the store opens %+v, want one of [%d,%d)", c, ops, c.off, end)
		}
	}
}

// Reading a large file allocates nothing that grows with its size.
func TestReadBigFileMemoryIsFlat(t *testing.T) {
	ctx := context.Background()
	e := newProbeEnv(t, aspect.GateOpts{}, nil)
	const size = 64 << 20
	src := &pattern{seed: 99, size: size}
	want := (&pattern{seed: 99, size: size}).sum()
	if _, err := e.v.Put(ctx, "huge.bin", src, size, time.Now()); err != nil {
		t.Fatal(err)
	}
	// Warm up everything that is allocated once.
	rc, _ := e.v.Open(ctx, "huge.bin", 0, 1<<20)
	drainFixed(rc)
	rc.Close()
	var got string
	var n int64
	delta := allocDelta(func() {
		rc, err := e.v.Open(ctx, "huge.bin", 0, -1)
		if err != nil {
			t.Error(err)
			return
		}
		defer rc.Close()
		n, got, err = drainFixed(rc)
		if err != nil {
			t.Error(err)
		}
	})
	t.Logf("reading %d MiB allocated %d bytes", size>>20, delta)
	if n != size || got != want {
		t.Fatalf("read %d bytes, hash ok=%v", n, got == want)
	}
	if delta > 1<<20 {
		t.Errorf("reading 64 MiB allocated %d bytes, want under 1 MiB", delta)
	}
}

// cutProxy is a real HTTP proxy in front of a WebDAV server that, once, ends the
// body of a GET early: the status line and headers announce the whole range, the
// connection closes after cutAt bytes.
type cutProxy struct {
	srv   *httptest.Server
	cutAt int64
	done  atomic.Bool
	cuts  atomic.Int64
}

func newCutProxy(t *testing.T, target string, cutAt int64, match func(*http.Request) bool) *cutProxy {
	u, err := url.Parse(target)
	if err != nil {
		t.Fatal(err)
	}
	rp := httputil.NewSingleHostReverseProxy(u)
	rp.FlushInterval = -1
	c := &cutProxy{cutAt: cutAt}
	c.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || !match(r) || !c.done.CompareAndSwap(false, true) {
			rp.ServeHTTP(w, r)
			return
		}
		out := r.Clone(context.Background())
		out.URL.Scheme, out.URL.Host, out.Host, out.RequestURI = u.Scheme, u.Host, u.Host, ""
		resp, err := http.DefaultTransport.RoundTrip(out)
		if err != nil {
			http.Error(w, err.Error(), 502)
			return
		}
		defer resp.Body.Close()
		for k, vs := range resp.Header {
			w.Header()[k] = vs
		}
		w.WriteHeader(resp.StatusCode)
		io.CopyN(w, resp.Body, c.cutAt)
		w.(http.Flusher).Flush()
		c.cuts.Add(1)
		if conn, _, err := w.(http.Hijacker).Hijack(); err == nil {
			conn.Close()
		}
	}))
	t.Cleanup(c.srv.Close)
	return c
}

// A read that breaks off in the middle is carried on by rclone's operations.Open
// (ReOpen) in the store, and the caller gets every byte, once.
func TestReadBigFileSurvivesABrokenBody(t *testing.T) {
	ctx := context.Background()
	drive := t.TempDir()
	davURL, _ := testutil.WebDAVServer(t, drive)
	proxy := newCutProxy(t, davURL, 300<<10, func(r *http.Request) bool { return strings.HasSuffix(r.URL.Path, "/cut.bin") })
	cfg := fs.GetConfig(ctx)
	old := cfg.LowLevelRetries
	cfg.LowLevelRetries = 4
	t.Cleanup(func() { cfg.LowLevelRetries = old })
	f, err := fs.NewFs(ctx, ":webdav,url='"+proxy.srv.URL+"',vendor=other:")
	if err != nil {
		t.Fatal(err)
	}
	st, _ := testutil.Stacked(rclonestore.New(f), aspect.Options{})
	vcfg := testutil.FastConfig(t.TempDir())
	vcfg.Threshold = 64 << 10
	v, err := core.Open(ctx, st, vcfg)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close(ctx)
	data := randBytes(2 << 20)
	if _, err := v.Put(ctx, "cut.bin", bytes.NewReader(data), int64(len(data)), time.Now()); err != nil {
		t.Fatal(err)
	}
	rc, err := v.Open(ctx, "cut.bin", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(rc)
	rc.Close()
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("read after a broken body: %d bytes, err %v", len(got), err)
	}
	if proxy.cuts.Load() != 1 {
		t.Fatalf("the proxy cut %d bodies, want 1", proxy.cuts.Load())
	}
}

// Large files are streams: a client that opens one and does not read holds
// nothing the other reads need. With more such streams open than there are read
// slots, a packed read, a commit (which reads the lease) and an upload all still
// complete; the bounded reads themselves never exceed ReadTransfers.
func TestReadStalledStreamsDoNotHoldSlots(t *testing.T) {
	ctx := context.Background()
	const slots = 2
	e := newProbeEnv(t, aspect.GateOpts{Transfers: 1, ReadTransfers: slots}, func(c *core.Config) {
		c.Ack = core.AckStaged
		c.LeaseTTL = 600 * time.Millisecond // so that a commit reads the lease again
	})
	e.put(t, "big.bin", randBytes(1<<20))
	small := randBytes(5000)
	e.put(t, "small.bin", small)
	if err := e.v.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	var streams []io.ReadCloser
	for i := 0; i < slots+3; i++ {
		rc, err := e.v.Open(ctx, "big.bin", 0, -1)
		if err != nil {
			t.Fatalf("stream %d: %v", i, err)
		}
		rc.Read(make([]byte, 10)) // started, then left alone
		streams = append(streams, rc)
	}
	defer func() {
		for _, rc := range streams {
			rc.Close()
		}
	}()
	time.Sleep(300 * time.Millisecond) // past a sixth of the lease TTL
	tctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	rc, err := e.v.Open(tctx, "small.bin", 0, -1)
	if err != nil {
		t.Fatalf("a packed read waited for stalled streams: %v", err)
	}
	got, err := io.ReadAll(rc)
	rc.Close()
	if err != nil || !bytes.Equal(got, small) {
		t.Fatalf("packed read: %v", err)
	}
	before := e.pr.opensOf("lease")
	if _, err := e.v.Put(tctx, "c.bin", bytes.NewReader(randBytes(1<<20)), 1<<20, time.Now()); err != nil {
		t.Fatalf("an upload waited for stalled streams: %v", err)
	}
	if _, err := e.v.Put(tctx, "d.txt", strings.NewReader("tiny"), 4, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := e.v.Flush(tctx); err != nil {
		t.Fatalf("a commit waited for stalled streams: %v", err)
	}
	t.Logf("lease opens before %d, after %d; %d streams open", len(before), len(e.pr.opensOf("lease")), len(streams))
	if s := e.meter.Snapshot(); s.PeakRead > slots {
		t.Errorf("bounded reads peaked at %d, limit %d: %s", s.PeakRead, slots, s)
	}
}

// Bounded reads (members) are limited to ReadTransfers at a time.
func TestReadBoundedReadsRespectReadTransfers(t *testing.T) {
	ctx := context.Background()
	e := newProbeEnv(t, aspect.GateOpts{ReadTransfers: 2}, func(c *core.Config) {
		c.Threshold = 1 << 20
		c.MemberCache = -1
		c.ReadWindow = -1
	})
	want := map[string][]byte{}
	for i := 0; i < 12; i++ {
		p := fmt.Sprintf("b/f%02d", i)
		want[p] = randBytes(10000 + i)
		e.put(t, p, want[p])
	}
	if err := e.v.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	e.pr.delay = 30 * time.Millisecond
	e.meter.Reset()
	var wg sync.WaitGroup
	for p, w := range want {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rc, err := e.v.Open(ctx, p, 0, -1)
			if err != nil {
				t.Error(err)
				return
			}
			got, err := io.ReadAll(rc)
			rc.Close()
			if err != nil || !bytes.Equal(got, w) {
				t.Errorf("%s: %v", p, err)
			}
		}()
	}
	wg.Wait()
	s := e.meter.Snapshot()
	t.Logf("peaks: %s", s)
	if s.PeakRead < 2 || s.PeakRead > 2 {
		t.Errorf("bounded reads peaked at %d, want 2", s.PeakRead)
	}
}

// The memory that reads of small files hold stays within ReadBuffer, a member
// larger than the whole budget can still be read, and everything read is right.
func TestReadBufferBoundsMemory(t *testing.T) {
	ctx := context.Background()
	const limit = 1 << 20
	e := newProbeEnv(t, aspect.GateOpts{ReadTransfers: 16}, func(c *core.Config) {
		c.Threshold = 4 << 20
		c.ReadBuffer = limit
		c.MemberCache = -1
		c.ReadWindow = -1
	})
	want := map[string][]byte{}
	for i := 0; i < 16; i++ {
		p := fmt.Sprintf("m/f%02d", i)
		want[p] = randBytes(512 << 10) // stored: 512 KiB packed + 512 KiB decoded each
		e.put(t, p, want[p])
	}
	if err := e.v.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for p, w := range want {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rc, err := e.v.Open(ctx, p, 0, -1)
			if err != nil {
				t.Error(err)
				return
			}
			time.Sleep(20 * time.Millisecond) // a slow consumer keeps its share
			got, err := io.ReadAll(rc)
			rc.Close()
			if err != nil || !bytes.Equal(got, w) {
				t.Errorf("%s: read %d bytes, err %v", p, len(got), err)
			}
		}()
	}
	wg.Wait()
	inUse, peak := e.v.ReadBufferStats()
	t.Logf("budget %d, peak in flight %d, in use at the end %d", limit, peak, inUse)
	if peak > limit || peak == 0 || inUse != 0 {
		t.Errorf("peak %d (limit %d), %d still in use", peak, limit, inUse)
	}

	// A member bigger than the budget.
	small := newProbeEnv(t, aspect.GateOpts{}, func(c *core.Config) {
		c.Threshold = 4 << 20
		c.ReadBuffer = 128 << 10
		c.MemberCache = -1
	})
	big := randBytes(600 << 10)
	small.put(t, "over.bin", big)
	if err := small.v.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	rc, err := small.v.Open(ctx, "over.bin", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(rc)
	rc.Close()
	if err != nil || !bytes.Equal(got, big) {
		t.Fatalf("member larger than the budget: %d bytes, err %v", len(got), err)
	}
}

// Concurrent reads of one member (or of one window) are one download.
func TestReadConcurrentReadsShareOneDownload(t *testing.T) {
	ctx := context.Background()
	for _, window := range []int64{-1, 512 << 10} {
		t.Run(fmt.Sprintf("window=%d", window), func(t *testing.T) {
			e := newProbeEnv(t, aspect.GateOpts{ReadTransfers: 16}, func(c *core.Config) {
				c.Threshold = 1 << 20
				c.MemberCache = -1 // so only the sharing can save a download
				c.ReadWindow = window
			})
			data := randBytes(20000)
			e.put(t, "one/member", data)
			if err := e.v.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			e.pr.reset()
			e.pr.delay = 300 * time.Millisecond // every reader arrives before the first answer
			var wg sync.WaitGroup
			for i := 0; i < 8; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					rc, err := e.v.Open(ctx, "one/member", 0, -1)
					if err != nil {
						t.Error(err)
						return
					}
					got, err := io.ReadAll(rc)
					rc.Close()
					if err != nil || !bytes.Equal(got, data) {
						t.Errorf("read: %d bytes, %v", len(got), err)
					}
				}()
			}
			wg.Wait()
			ops := e.pr.opensOf("packs/")
			t.Logf("8 readers, %d store opens: %+v", len(ops), ops)
			if len(ops) != 1 {
				t.Errorf("8 concurrent reads of one member made %d store opens, want 1", len(ops))
			}
		})
	}
}

// packFile returns the path of the only pack on the drive.
func packFile(t *testing.T, drive string) string {
	t.Helper()
	packs, _ := filepath.Glob(filepath.Join(drive, ".cfs", "packs", "*.zip"))
	if len(packs) != 1 {
		t.Fatalf("want one pack on the drive, found %d", len(packs))
	}
	return packs[0]
}

// A pack of 32 MiB is checked with one streaming read and memory that does not
// depend on the pack's size; damage is found and a transient error is retried.
func TestReadFsckStreamsThePack(t *testing.T) {
	ctx := context.Background()
	const member = 4 << 20
	e := newProbeEnv(t, aspect.GateOpts{}, func(c *core.Config) {
		c.Threshold = 8 << 20
		c.Ack = core.AckStaged
		c.SealQuiet, c.SealMax = time.Hour, time.Hour
		// The allocations are counted process-wide: no heartbeat (a listing, a
		// write and a removal of lease files) runs while they are.
		c.LeaseTTL = 30 * time.Minute
	})
	for i := 0; i < 8; i++ {
		e.put(t, fmt.Sprintf("pk/f%d", i), randBytes(member))
	}
	if err := e.v.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	pack := packFile(t, e.drive)
	fi, _ := os.Stat(pack)
	t.Logf("pack: %d bytes", fi.Size())

	fsck := func() core.FsckReport {
		rep, err := e.v.Fsck(ctx, core.FsckOpts{Deep: true})
		if err != nil {
			t.Fatal(err)
		}
		return rep
	}
	fsck() // warm up
	// What a fsck allocates apart from reading packs, to be taken off the deep one.
	base := allocDelta(func() {
		if _, err := e.v.Fsck(ctx, core.FsckOpts{}); err != nil {
			t.Fatal(err)
		}
	})
	e.pr.reset()
	var rep core.FsckReport
	deepDelta := allocDelta(func() { rep = fsck() })
	delta := deepDelta - min(deepDelta, base)
	ops := e.pr.opensOf("packs/")
	t.Logf("deep fsck of a %d MiB pack: %d bytes allocated (%d without the pack check), %d opens of the pack, problems %v", fi.Size()>>20, deepDelta, base, len(ops), rep.Problems)
	if !rep.OK() {
		t.Fatalf("clean pack reported: %v", rep.Problems)
	}
	if len(ops) != 1 {
		t.Errorf("%d store opens for one pack, want 1", len(ops))
	}
	if limit := uint64(4<<20 + member + 256<<10); delta > limit {
		t.Errorf("the pack check allocated %d bytes, want under the 4 MiB read-ahead plus the largest member (%d, with 256 KiB to spare)", delta, limit)
	}

	kinds := func(r core.FsckReport) map[string]bool {
		m := map[string]bool{}
		for _, p := range r.Problems {
			m[p.Kind] = true
		}
		return m
	}
	orig, _ := os.ReadFile(pack)
	flip := func(at int) {
		b := bytes.Clone(orig)
		b[at] ^= 0xff
		if err := os.WriteFile(pack, b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// A byte inside a member: that member and the pack hash.
	flip(member + member/2)
	if k := kinds(fsck()); !k["member_checksum"] || !k["pack_checksum"] {
		t.Errorf("a damaged member: problems %v", k)
	}
	// A byte in the central directory at the end: only the pack hash.
	flip(len(orig) - 10)
	if k := kinds(fsck()); !k["pack_checksum"] || k["member_checksum"] {
		t.Errorf("a damaged directory: problems %v", k)
	}
	os.WriteFile(pack, orig, 0o644)

	// One transient failure in the middle of the stream: the second run passes.
	e.pr.cut = func(path string, off int64, seq int) int64 {
		if strings.Contains(path, "packs/") && seq == 0 {
			return 3 << 20
		}
		return -1
	}
	e.pr.reset()
	rep = fsck()
	ops = e.pr.opensOf("packs/")
	t.Logf("with one transient error: %d opens of the pack, problems %v", len(ops), rep.Problems)
	if !rep.OK() {
		t.Errorf("a transient error was reported: %v", rep.Problems)
	}
	if len(ops) != 2 {
		t.Errorf("%d opens of the pack, want 2 (the failed run and the retry)", len(ops))
	}
}
