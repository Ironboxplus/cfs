package core_test

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Ironboxplus/cfs/aspect"
	"github.com/Ironboxplus/cfs/core"
	"github.com/Ironboxplus/cfs/internal/testutil"
)

// faultEnv is a volume on a real WebDAV server reached through a real TCP
// fault proxy.
type faultEnv struct {
	t     *testing.T
	drive string
	proxy *testutil.FaultProxy
	v     *core.Volume
	meter *aspect.Meter
}

func newFaultEnv(t *testing.T, retries int) *faultEnv {
	drive := t.TempDir()
	url, _ := testutil.WebDAVServer(t, drive)
	proxy := testutil.NewFaultProxy(t, url)
	base := testutil.WebDAVStore(t, proxy.URL)
	st, m := aspect.Stack(base, aspect.Options{
		Gate:  aspect.GateOpts{MetaInflight: 1, Transfers: 4},
		Retry: aspect.RetryOpts{Attempts: retries, Base: 20 * time.Millisecond, Max: 100 * time.Millisecond},
	})
	cfg := testutil.FastConfig(t.TempDir())
	cfg.Threshold = 64 << 10
	cfg.Pipeline = 4
	v, err := core.Open(context.Background(), st, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { v.Close(context.Background()) })
	return &faultEnv{t: t, drive: drive, proxy: proxy, v: v, meter: m}
}

func isPut(r *http.Request) bool      { return r.Method == "PUT" }
func isPropfind(r *http.Request) bool { return r.Method == "PROPFIND" }

// writeMany writes n small and a few large files concurrently and returns their
// content by path, or the first error.
func (e *faultEnv) writeMany(prefix string, n int) (map[string][]byte, error) {
	return e.writeMix(prefix, n, true)
}

func (e *faultEnv) writeMix(prefix string, n int, withBig bool) (map[string][]byte, error) {
	files := map[string][]byte{}
	var mu sync.Mutex
	var wg sync.WaitGroup
	var firstErr error
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sz := 500 + i*13
			if withBig && i%9 == 0 {
				sz = 100 << 10
			}
			p := fmt.Sprintf("%s/d%d/f%d.dat", prefix, i%4, i)
			data := randBytes(sz)
			_, err := e.v.Put(context.Background(), p, bytes.NewReader(data), int64(sz), time.Now())
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if firstErr == nil {
					firstErr = fmt.Errorf("put %s: %w", p, err)
				}
				return
			}
			files[p] = data
		}()
	}
	wg.Wait()
	return files, firstErr
}

func (e *faultEnv) verify(files map[string][]byte) {
	e.t.Helper()
	for p, d := range files {
		got, err := readAllVol(e.v, p)
		if err != nil || !bytes.Equal(got, d) {
			e.t.Fatalf("%s wrong after faults: %v", p, err)
		}
	}
	rep, err := e.v.Fsck(context.Background(), core.FsckOpts{Deep: true})
	if err != nil {
		e.t.Fatal(err)
	}
	for _, p := range rep.Problems {
		if p.Kind != "orphan_pack" {
			e.t.Fatalf("fsck: %s", p)
		}
	}
}

func (e *faultEnv) packCount() int {
	ents, _ := os.ReadDir(filepath.Join(e.drive, ".cfs", "packs"))
	return len(ents)
}

func TestFaultStatusCodes(t *testing.T) {
	for _, tc := range []struct {
		name string
		code int
	}{{"429", 429}, {"503", 503}, {"500", 500}} {
		t.Run(tc.name, func(t *testing.T) {
			e := newFaultEnv(t, 6)
			e.proxy.Add(testutil.Fault{Match: isPut, Kind: testutil.Status, Code: tc.code, Count: 4})
			e.proxy.Add(testutil.Fault{Match: isPropfind, Kind: testutil.Status, Code: tc.code, Count: 3})
			files, err := e.writeMany("a", 60)
			if err != nil {
				t.Fatalf("writes should survive transient %d answers: %v", tc.code, err)
			}
			e.verify(files)
			t.Logf("underlying: %s", e.meter.Snapshot())
			e.assertPeaks(1, 4)
		})
	}
}

func (e *faultEnv) assertPeaks(meta, transfer int) {
	s := e.meter.Snapshot()
	if s.PeakMeta > meta || s.PeakTransfer > transfer {
		e.t.Errorf("in-flight peaks meta=%d transfer=%d exceed %d/%d", s.PeakMeta, s.PeakTransfer, meta, transfer)
	}
}

func TestFaultConnectionDrops(t *testing.T) {
	e := newFaultEnv(t, 6)
	e.proxy.Add(testutil.Fault{Match: isPut, Kind: testutil.DropBefore, Count: 3})
	e.proxy.Add(testutil.Fault{Match: isPropfind, Kind: testutil.DropBefore, Count: 2})
	files, err := e.writeMany("b", 50)
	if err != nil {
		t.Fatal(err)
	}
	e.verify(files)
	t.Logf("underlying: %s", e.meter.Snapshot())
}

// The upload lands on the server but the client sees a dropped connection: the
// retry must recognise the pack that is already there, not upload a copy.
func TestFaultUploadSucceededButErrored(t *testing.T) {
	e := newFaultEnv(t, 6)
	e.proxy.Add(testutil.Fault{Match: isPut, Kind: testutil.DropAfter, Count: 3})
	files, err := e.writeMix("c", 50, false)
	if err != nil {
		t.Fatal(err)
	}
	e.verify(files)
	// One pack and one log segment per committed batch, however many dropped answers.
	packs := e.packCount()
	segs, _ := os.ReadDir(filepath.Join(e.drive, ".cfs", "log"))
	if packs < 1 || len(segs) < packs {
		t.Errorf("packs=%d segments=%d", packs, len(segs))
	}
	var loose []string
	filepath.WalkDir(e.drive, func(p string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() && !strings.Contains(p, ".cfs") && !strings.HasSuffix(p, ".dat") {
			loose = append(loose, p)
		}
		return nil
	})
	if len(loose) > 0 {
		t.Errorf("stray files on the drive: %v", loose)
	}
	t.Logf("packs=%d segments=%d underlying: %s", packs, len(segs), e.meter.Snapshot())
}

func TestFaultSlowListing(t *testing.T) {
	e := newFaultEnv(t, 4)
	e.proxy.Add(testutil.Fault{Match: isPropfind, Kind: testutil.Delay, Wait: 150 * time.Millisecond, Count: -1})
	files, err := e.writeMany("d", 40)
	if err != nil {
		t.Fatal(err)
	}
	e.proxy.Clear()
	e.verify(files)
	e.assertPeaks(1, 4)
}

// With the drive down for good, writes fail cleanly and nothing half-written
// becomes visible; once the drive is back, the same volume works again.
func TestFaultDriveDownThenUp(t *testing.T) {
	e := newFaultEnv(t, 3)
	e.proxy.Add(testutil.Fault{Match: isPut, Kind: testutil.Status, Code: 503, Count: -1})
	_, err := e.v.Put(context.Background(), "down/x.txt", strings.NewReader("hello"), 5, time.Now())
	if err == nil {
		t.Fatal("put must fail while the drive refuses uploads")
	}
	t.Logf("expected failure: %v", err)
	if _, serr := e.v.Stat(context.Background(), "down/x.txt"); serr == nil {
		t.Fatal("a failed put is visible")
	}
	e.proxy.Clear()
	files, err := e.writeMany("up", 20)
	if err != nil {
		t.Fatalf("after recovery: %v", err)
	}
	e.verify(files)
	if _, serr := e.v.Stat(context.Background(), "down/x.txt"); serr == nil {
		t.Fatal("the failed put appeared later")
	}
}

// A large file whose upload fails after the bytes landed returns an error (the
// stream can't be replayed); the caller retries and the final state is right.
func TestFaultLargeUploadDropped(t *testing.T) {
	e := newFaultEnv(t, 4)
	e.proxy.Add(testutil.Fault{Match: isPut, Kind: testutil.DropAfter, Count: 1})
	data := randBytes(1 << 20)
	_, err := e.v.Put(context.Background(), "big/file.bin", bytes.NewReader(data), int64(len(data)), time.Now())
	if err == nil {
		t.Log("put went through without error")
	}
	if err != nil {
		if _, err = e.v.Put(context.Background(), "big/file.bin", bytes.NewReader(data), int64(len(data)), time.Now()); err != nil {
			t.Fatalf("second try: %v", err)
		}
	}
	got, err := readAllVol(e.v, "big/file.bin")
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("large file wrong: %v", err)
	}
	ents, _ := os.ReadDir(filepath.Join(e.drive, "big"))
	if len(ents) != 1 {
		t.Errorf("expected exactly one object in the directory, found %d", len(ents))
	}
}
