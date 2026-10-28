package core_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Ironboxplus/cfs/aspect"
	"github.com/Ironboxplus/cfs/core"
	"github.com/Ironboxplus/cfs/internal/testutil"
	"github.com/Ironboxplus/cfs/store"
)

// slowSeal keeps the packs from sealing on their own: a test that wants to see
// the state "acknowledged, not on the drive yet" needs nothing to commit
// behind its back.
func slowSeal(c *core.Config) {
	c.Ack = core.AckStaged
	c.SealQuiet = time.Hour
	c.SealMax = time.Hour
}

func countDir(dir string) int {
	ents, _ := os.ReadDir(dir)
	return len(ents)
}

// A staged Put is acknowledged, readable, listed and visible to Stat before
// anything is on the drive, directories included; Flush puts it all there.
func TestStagedIsVisibleBeforeItIsOnTheDrive(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, aspect.Options{}, slowSeal)
	want := map[string][]byte{}
	for i := 0; i < 20; i++ {
		p := fmt.Sprintf("new/dir%d/sub/f%d.txt", i%3, i)
		want[p] = randBytes(100 + i*7)
		e.put(p, want[p])
	}
	if n, b := e.v.Staged(); n != 20 || b == 0 {
		t.Fatalf("staged files %d bytes %d, want 20 files", n, b)
	}
	if got := countDir(filepath.Join(e.drive, ".cfs", "packs")); got != 0 {
		t.Fatalf("%d packs on the drive before any seal", got)
	}
	for p, d := range want {
		if got := e.read(p); !bytes.Equal(got, d) {
			t.Fatalf("%s: wrong content from the staging area", p)
		}
		if st, err := e.v.Stat(ctx, p); err != nil || st.Size != int64(len(d)) || st.IsDir {
			t.Fatalf("stat %s: %+v %v", p, st, err)
		}
	}
	if st, err := e.v.Stat(ctx, "new/dir1/sub"); err != nil || !st.IsDir {
		t.Fatalf("a directory that only staged files live in: %+v %v", st, err)
	}
	if n := e.listNames("new"); fmt.Sprint(n) != "[dir0/ dir1/ dir2/]" {
		t.Fatalf("list new = %v", n)
	}
	if n := e.listNames("new/dir0/sub"); len(n) != 7 {
		t.Fatalf("list new/dir0/sub = %v", n)
	}
	if err := e.v.Rmdir(ctx, "new/dir0/sub"); !errors.Is(err, store.ErrNotEmpty) {
		t.Fatalf("rmdir of a directory with staged files: %v", err)
	}
	// An overwrite replaces the staged version: only the newest ever goes out.
	e.put("new/dir0/sub/f0.txt", []byte("second version"))
	want["new/dir0/sub/f0.txt"] = []byte("second version")
	if got := e.read("new/dir0/sub/f0.txt"); string(got) != "second version" {
		t.Fatalf("overwrite of a staged file: %q", got)
	}

	if err := e.v.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if n, b := e.v.Staged(); n != 0 || b != 0 {
		t.Fatalf("after Flush: %d staged files, %d bytes", n, b)
	}
	if got := countDir(filepath.Join(e.drive, ".cfs", "packs")); got != 1 {
		t.Fatalf("%d packs after Flush, want 1", got)
	}
	if got := countDir(filepath.Join(e.state, "stage")); got != 0 {
		t.Fatalf("%d stage files left after Flush", got)
	}
	for p, d := range want {
		if got := e.read(p); !bytes.Equal(got, d) {
			t.Fatalf("%s: wrong content after commit", p)
		}
	}
	rep, err := e.v.Fsck(ctx, core.FsckOpts{Deep: true})
	if err != nil || !rep.OK() {
		t.Fatalf("fsck: %v %v", err, rep.Problems)
	}
}

// Metadata operations on staged names see them and keep the log in the order
// of the calls.
func TestStagedMetadataOps(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, aspect.Options{}, func(c *core.Config) {
		slowSeal(c)
		c.Threshold = 1 << 20
	})
	e.put("a/one.txt", []byte("one"))
	e.put("a/two.txt", []byte("two"))
	e.put("a/sub/three.txt", []byte("three"))
	e.put("b/other.txt", []byte("other"))
	if err := e.v.Move(ctx, "a/one.txt", "a/uno.txt"); err != nil {
		t.Fatal(err)
	}
	if err := e.v.Remove(ctx, "a/two.txt"); err != nil {
		t.Fatal(err)
	}
	e.put("a/two.txt", []byte("two again"))
	if err := e.v.DirMove(ctx, "a/sub", "a/moved"); err != nil {
		t.Fatal(err)
	}
	// A large file replaces a staged small one of the same name.
	e.put("b/other.txt", []byte("small first"))
	e.put("b/other.txt", randBytes(2<<20))
	// And a small one a large one.
	big := randBytes(2 << 20)
	e.put("c/grow.bin", big)
	e.put("c/grow.bin", []byte("tiny now"))
	if err := e.v.SetModTime(ctx, "a/uno.txt", time.Unix(1700000000, 0)); err != nil {
		t.Fatal(err)
	}
	check := func(when string) {
		t.Helper()
		for p, want := range map[string]string{"a/uno.txt": "one", "a/two.txt": "two again", "a/moved/three.txt": "three", "c/grow.bin": "tiny now"} {
			if got := e.read(p); string(got) != want {
				t.Fatalf("%s: %s = %q, want %q", when, p, got, want)
			}
		}
		for _, p := range []string{"a/one.txt", "a/sub/three.txt"} {
			if _, err := e.v.Stat(ctx, p); !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("%s: %s should be gone: %v", when, p, err)
			}
		}
		if st, _ := e.v.Stat(ctx, "b/other.txt"); st.Packed || st.Size != 2<<20 {
			t.Fatalf("%s: b/other.txt: %+v", when, st)
		}
		if st, _ := e.v.Stat(ctx, "a/uno.txt"); st.ModTime.Unix() != 1700000000 {
			t.Fatalf("%s: mtime %v", when, st.ModTime)
		}
	}
	check("live")
	if err := e.v.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	check("after flush")
	// What the drive's log says must be what the volume says.
	rep, err := e.v.Fsck(ctx, core.FsckOpts{Deep: true})
	if err != nil || !rep.OK() {
		t.Fatalf("fsck: %v %v", err, rep.Problems)
	}
	e.reopen()
	check("after reopen")
	e.newState()
	e.reopen() // from the drive alone
	check("rebuilt from the drive")
}

// With the staging area full, Put waits (it doesn't fail) until packs have
// gone out, and the area never holds more than its limit.
func TestStagedBackpressure(t *testing.T) {
	ctx := context.Background()
	const limit = 200 << 10
	e := newEnv(t, aspect.Options{}, func(c *core.Config) {
		slowSeal(c)
		c.StageMaxBytes = limit
	})
	var peak atomic.Int64
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			if _, b := e.v.Staged(); b > peak.Load() {
				peak.Store(b)
			}
			time.Sleep(time.Millisecond)
		}
	}()
	want := map[string][]byte{}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 30; i++ {
				p := fmt.Sprintf("bp/w%d/f%d", w, i)
				d := randBytes(20 << 10)
				if _, err := e.v.Put(ctx, p, bytes.NewReader(d), int64(len(d)), time.Now()); err != nil {
					t.Errorf("put %s: %v", p, err)
					return
				}
				mu.Lock()
				want[p] = d
				mu.Unlock()
			}
		}()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("puts did not finish: backpressure deadlocked")
	}
	close(stop)
	if peak.Load() > limit {
		t.Errorf("staging area peaked at %d bytes, limit %d", peak.Load(), limit)
	}
	if err := e.v.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	for p, d := range want {
		if got := e.read(p); !bytes.Equal(got, d) {
			t.Fatalf("%s wrong", p)
		}
	}
	packs := countDir(filepath.Join(e.drive, ".cfs", "packs"))
	t.Logf("90 files of 20 KiB through a %d KiB staging area: %d packs, peak staged %d bytes", limit>>10, packs, peak.Load())
	if packs < 2 {
		t.Errorf("a full staging area must force packs out, got %d", packs)
	}
}

// The drive refuses writes: staged Puts are acknowledged and readable, Flush
// reports the failure, nothing is lost, and when the drive is back the same
// volume commits everything. Closing with the drive down and opening again
// replays the staging area.
func TestStagedDriveDownThenUp(t *testing.T) {
	ctx := context.Background()
	e := newFaultEnv(t, 2, slowSeal)
	e.proxy.Add(testutil.Fault{Match: isPut, Kind: testutil.Status, Code: 503, Count: -1})
	want := map[string][]byte{}
	for i := 0; i < 12; i++ {
		p := fmt.Sprintf("down/f%d", i)
		want[p] = randBytes(300 + i)
		if _, err := e.v.Put(ctx, p, bytes.NewReader(want[p]), int64(len(want[p])), time.Now()); err != nil {
			t.Fatalf("a staged put must not need the drive: %v", err)
		}
	}
	if err := e.v.Flush(ctx); err == nil {
		t.Fatal("Flush must report that nothing can be committed")
	} else {
		t.Logf("expected: %v", err)
	}
	if n, _ := e.v.Staged(); n != 12 {
		t.Fatalf("%d files staged after the failed commit, want 12", n)
	}
	for p, d := range want {
		if got, err := readAllVol(e.v, p); err != nil || !bytes.Equal(got, d) {
			t.Fatalf("%s unreadable while the commit fails: %v", p, err)
		}
	}
	e.proxy.Clear()
	if err := e.v.Flush(ctx); err != nil {
		t.Fatalf("after the drive is back: %v", err)
	}
	if n, _ := e.v.Staged(); n != 0 {
		t.Fatalf("%d staged after the drive is back", n)
	}
	e.verify(want)

	// Close with the drive down, open again: the staging area is replayed.
	e.proxy.Add(testutil.Fault{Match: isPut, Kind: testutil.Status, Code: 503, Count: -1})
	more := map[string][]byte{}
	for i := 0; i < 5; i++ {
		p := fmt.Sprintf("replay/f%d", i)
		more[p] = randBytes(500 + i)
		if _, err := e.v.Put(ctx, p, bytes.NewReader(more[p]), int64(len(more[p])), time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.v.Close(ctx); err == nil {
		t.Log("close with the drive down reported no error")
	}
	e.proxy.Clear()
	base := testutil.WebDAVStore(t, e.proxy.URL)
	st, _ := aspect.Stack(base, aspect.Options{Retry: aspect.RetryOpts{Attempts: 3, Base: 20 * time.Millisecond, Max: 100 * time.Millisecond}})
	cfg := testutil.FastConfig(e.stateDir)
	cfg.Threshold = 64 << 10
	v2, err := core.Open(ctx, st, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer v2.Close(ctx)
	for p, d := range more {
		if got, err := readAllVol(v2, p); err != nil || !bytes.Equal(got, d) {
			t.Fatalf("%s after the restart: %v", p, err)
		}
	}
	if err := v2.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if n, _ := v2.Staged(); n != 0 {
		t.Fatalf("%d staged after the replay", n)
	}
	// On the drive: a rebuild without local state must have all of it.
	st3, _ := aspect.Stack(testutil.WebDAVStore(t, e.proxy.URL), aspect.Options{})
	cfg3 := testutil.FastConfig(t.TempDir())
	cfg3.Threshold = 64 << 10
	cfg3.LeaseTTL = time.Second
	v2.Close(ctx)
	v3, err := core.Open(ctx, st3, cfg3)
	if err != nil {
		t.Fatal(err)
	}
	defer v3.Close(ctx)
	for _, m := range []map[string][]byte{want, more} {
		for p, d := range m {
			if got, err := readAllVol(v3, p); err != nil || !bytes.Equal(got, d) {
				t.Fatalf("%s missing from the drive: %v", p, err)
			}
		}
	}
}

// A Put whose context has ended leaves nothing staged.
func TestStagedCancelledPutLeavesNoTrace(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, aspect.Options{}, slowSeal)
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := e.v.Put(cctx, "x/never", strings.NewReader("data"), 4, time.Now()); err == nil {
		t.Fatal("put with a dead context succeeded")
	}
	if n, b := e.v.Staged(); n != 0 || b != 0 {
		t.Fatalf("staged %d files / %d bytes after a cancelled put", n, b)
	}
	if _, err := e.v.Stat(ctx, "x/never"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("cancelled put is visible: %v", err)
	}
	if got := countDir(filepath.Join(e.state, "stage")); got != 0 {
		t.Fatalf("%d stage files left", got)
	}
}

// The pack-efficiency requirement: three callers, 2000 small files, then Flush:
// the number of packs follows the bytes, not the files or the callers.
func TestPackEfficiency(t *testing.T) {
	const files, callers = 2000, 3
	const packMax = 256 << 10
	rng := rand.New(rand.NewPCG(5, 6))
	sizes := make([]int, files)
	var total int64
	for i := range sizes {
		sizes[i] = 200 + rng.IntN(1800)
		total += int64(sizes[i])
	}
	expect := int((total + packMax - 1) / packMax)
	results := map[core.AckMode][2]int{}
	for _, mode := range []core.AckMode{core.AckStaged, core.AckDurable} {
		t.Run(string(mode), func(t *testing.T) {
			ctx := context.Background()
			e := newEnv(t, aspect.Options{Gate: aspect.GateOpts{MetaInflight: 1, Transfers: 8}}, func(c *core.Config) {
				c.Ack = mode
				c.PackMaxBytes = packMax
				c.SealQuiet = time.Hour
				c.SealMax = time.Hour
				if mode == core.AckDurable {
					c.SealQuiet = 40 * time.Millisecond // a durable caller waits for the seal
					c.SealMax = 400 * time.Millisecond
				}
			})
			var wg sync.WaitGroup
			var next atomic.Int64
			sums := make([]string, files)
			for c := 0; c < callers; c++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for {
						i := int(next.Add(1)) - 1
						if i >= files {
							return
						}
						d := randBytes(sizes[i])
						sums[i] = sum(d)
						if _, err := e.v.Put(ctx, fmt.Sprintf("eff/d%d/f%04d", i%10, i), bytes.NewReader(d), int64(len(d)), time.Now()); err != nil {
							t.Errorf("put %d: %v", i, err)
							return
						}
					}
				}()
			}
			wg.Wait()
			if err := e.v.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			packs := countDir(filepath.Join(e.drive, ".cfs", "packs"))
			segs := countDir(filepath.Join(e.drive, ".cfs", "log"))
			results[mode] = [2]int{packs, segs}
			t.Logf("%s: %d files, %d bytes, callers %d: %d packs, %d log segments (bytes / pack size = %d)", mode, files, total, callers, packs, segs, expect)
			for i := 0; i < files; i += 37 {
				got := e.read(fmt.Sprintf("eff/d%d/f%04d", i%10, i))
				if sum(got) != sums[i] {
					t.Fatalf("file %d wrong", i)
				}
			}
			if mode == core.AckStaged {
				if packs > expect+2 || segs > packs+1 {
					t.Errorf("staged: %d packs and %d segments for %d bytes, want about %d packs", packs, segs, total, expect)
				}
			}
			e.report()
		})
	}
	if s, d := results[core.AckStaged], results[core.AckDurable]; s[0] > 0 && d[0] > 0 {
		t.Logf("packs / segments: staged %d / %d, durable %d / %d", s[0], s[1], d[0], d[1])
		if d[0] < 3*s[0] {
			t.Logf("note: durable made %d packs against %d staged", d[0], s[0])
		}
	}
}

// Many writers and readers on staged data under -race: every acknowledged
// write is readable at once and survives the commit.
func TestStagedConcurrentReadWrite(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, aspect.Options{}, func(c *core.Config) {
		c.Ack = core.AckStaged
		c.SealQuiet = 30 * time.Millisecond
		c.SealMax = 200 * time.Millisecond
		c.PackMaxFiles = 25
	})
	var wg sync.WaitGroup
	final := make([]map[string][]byte, 4)
	for w := range final {
		final[w] = map[string][]byte{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(w), 9))
			for i := 0; i < 150; i++ {
				p := fmt.Sprintf("cc/w%d/f%d", w, rng.IntN(20))
				switch rng.IntN(10) {
				case 0:
					if err := e.v.Remove(ctx, p); err == nil {
						delete(final[w], p)
					}
				default:
					d := randBytes(rng.IntN(3000))
					if _, err := e.v.Put(ctx, p, bytes.NewReader(d), int64(len(d)), time.Now()); err != nil {
						t.Errorf("put %s: %v", p, err)
						return
					}
					final[w][p] = d
					if got, err := readAllVol(e.v, p); err != nil || !bytes.Equal(got, d) {
						t.Errorf("%s not readable right after its put: %v", p, err)
						return
					}
				}
			}
		}()
	}
	wg.Wait()
	check := func(when string) {
		t.Helper()
		for w := range final {
			for p, d := range final[w] {
				if got, err := readAllVol(e.v, p); err != nil || !bytes.Equal(got, d) {
					t.Fatalf("%s: %s wrong: %v", when, p, err)
				}
			}
			ents, err := e.v.List(ctx, fmt.Sprintf("cc/w%d", w))
			if err != nil && !errors.Is(err, store.ErrNotFound) {
				t.Fatal(err)
			}
			if len(ents) != len(final[w]) {
				t.Fatalf("%s: w%d lists %d files, want %d", when, w, len(ents), len(final[w]))
			}
		}
	}
	check("live")
	if err := e.v.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	check("flushed")
	e.newState() // rebuild from the drive alone
	e.reopen()
	check("rebuilt")
	rep, err := e.v.Fsck(ctx, core.FsckOpts{Deep: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range rep.Problems {
		if p.Kind != "orphan_pack" {
			t.Fatalf("fsck: %s", p)
		}
	}
}

var _ = http.MethodPut

// A staged file whose bytes changed on local disk without changing its size
// (bit rot, a bad sector, a stray writer) must not be committed to the drive as
// if it were the acknowledged content. It is dropped loudly: an error line
// naming the path, a counter that stats can export, and the damaged file kept
// aside. Its neighbours replay normally.
func TestStagedSameSizeCorruptionIsLoud(t *testing.T) {
	ctx := context.Background()
	e := newFaultEnv(t, 2, slowSeal)
	e.proxy.Add(testutil.Fault{Match: isPut, Kind: testutil.Status, Code: 503, Count: -1})
	want := map[string][]byte{}
	for i := 0; i < 4; i++ {
		p := fmt.Sprintf("rot/f%d", i)
		want[p] = randBytes(400 + i)
		if _, err := e.v.Put(ctx, p, bytes.NewReader(want[p]), int64(len(want[p])), time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.v.Close(ctx); err == nil {
		t.Log("close with the drive down reported no error")
	}
	// Flip one byte in the middle of one staged file; the size stays the same.
	stage := filepath.Join(e.stateDir, "stage")
	ents, err := os.ReadDir(stage)
	if err != nil || len(ents) != 4 {
		t.Fatalf("want 4 staged files on disk, got %d (%v)", len(ents), err)
	}
	victim := filepath.Join(stage, ents[1].Name())
	b, err := os.ReadFile(victim)
	if err != nil {
		t.Fatal(err)
	}
	b[len(b)/2] ^= 0xff
	if err := os.WriteFile(victim, b, 0o644); err != nil {
		t.Fatal(err)
	}
	var lmu sync.Mutex
	var logs []string

	e.proxy.Clear()
	st, _ := aspect.Stack(testutil.WebDAVStore(t, e.proxy.URL), aspect.Options{Retry: aspect.RetryOpts{Attempts: 3, Base: 20 * time.Millisecond, Max: 100 * time.Millisecond}})
	cfg := testutil.FastConfig(e.stateDir)
	cfg.Threshold = 64 << 10
	cfg.Logf = func(format string, args ...any) {
		lmu.Lock()
		logs = append(logs, fmt.Sprintf(format, args...))
		lmu.Unlock()
	}
	v2, err := core.Open(ctx, st, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer v2.Close(ctx)
	if got := v2.StagedDamaged(); got != 1 {
		t.Fatalf("StagedDamaged = %d, want 1", got)
	}
	lmu.Lock()
	var loud string
	for _, l := range logs {
		if strings.Contains(l, "DATA LOSS") {
			loud = l
		}
	}
	lmu.Unlock()
	if loud == "" || !strings.Contains(loud, "sha1") {
		t.Fatalf("no loud sha1 line in the log: %q", logs)
	}
	if _, err := os.Stat(victim + ".damaged"); err != nil {
		t.Fatalf("the damaged file was not kept aside: %v", err)
	}
	if err := v2.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	lost := 0
	for p, d := range want {
		got, err := readAllVol(v2, p)
		switch {
		case err != nil:
			lost++
			if !strings.Contains(loud, p) {
				t.Fatalf("%s is gone but the log line does not name it: %s", p, loud)
			}
		case !bytes.Equal(got, d):
			t.Fatalf("%s: content differs from what was acknowledged and nobody noticed", p)
		}
	}
	if lost != 1 {
		t.Fatalf("%d files lost, want exactly the 1 damaged", lost)
	}
}
