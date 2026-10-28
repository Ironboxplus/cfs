package aspect_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Ironboxplus/cfs/aspect"
	"github.com/Ironboxplus/cfs/internal/testutil"
	"github.com/Ironboxplus/cfs/store"
)

func TestGateBoundsEachClassSeparately(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	m := aspect.NewMeter(testutil.LocalStore(t, dir))
	g := aspect.Gate(m, aspect.GateOpts{MetaInflight: 1, Transfers: 3, ReadTransfers: 3})
	for i := 0; i < 30; i++ {
		if err := g.Put(ctx, fmt.Sprintf("f%d", i), store.Bytes(make([]byte, 1<<16)), store.PutOpts{Size: 1 << 16}); err != nil {
			t.Fatal(err)
		}
	}
	m.Reset()
	var wg sync.WaitGroup
	for i := 0; i < 60; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			switch i % 3 {
			case 0:
				if _, err := g.List(ctx, ""); err != nil {
					t.Error(err)
				}
			case 1:
				rc, err := g.Open(ctx, fmt.Sprintf("f%d", i%30), 0, -1)
				if err != nil {
					t.Error(err)
					return
				}
				time.Sleep(5 * time.Millisecond) // hold the slot a while
				io.Copy(io.Discard, rc)
				rc.Close()
			case 2:
				if err := g.Put(ctx, fmt.Sprintf("g%d", i), store.Bytes(make([]byte, 1000)), store.PutOpts{Size: 1000}); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	wg.Wait()
	s := m.Snapshot()
	t.Logf("%s", s)
	if s.PeakMeta != 1 {
		t.Errorf("metadata peak %d, want 1", s.PeakMeta)
	}
	if s.PeakRead < 2 || s.PeakRead > 3 {
		t.Errorf("read peak %d, want 2..3", s.PeakRead)
	}
	if s.PeakTransfer > 3 {
		t.Errorf("transfer peak %d, want at most 3", s.PeakTransfer)
	}
}

func TestGateSlotIsReleasedAtEOFAndClose(t *testing.T) {
	ctx := context.Background()
	st := testutil.LocalStore(t, t.TempDir())
	st.Put(ctx, "a", store.Bytes([]byte("hello")), store.PutOpts{Size: 5})
	g := aspect.Gate(st, aspect.GateOpts{ReadTransfers: 1})
	// Reading to EOF frees the slot even if the caller forgets Close.
	rc, err := g.Open(ctx, "a", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, rc)
	ctx2, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	rc2, err := g.Open(ctx2, "a", 0, 2)
	if err != nil {
		t.Fatalf("slot not released at EOF: %v", err)
	}
	// An unread reader holds the slot until Close; a waiter gives up with its context.
	ctx3, cancel3 := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel3()
	if _, err := g.Open(ctx3, "a", 0, 2); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second open should wait for the slot and time out, got %v", err)
	}
	rc2.Close()
	rc3, err := g.Open(ctx, "a", 0, 2)
	if err != nil {
		t.Fatalf("slot not released at Close: %v", err)
	}
	rc3.Close()
	rc.Close()
}

func TestGateMetadataIsNotBlockedByTransfers(t *testing.T) {
	ctx := context.Background()
	st := testutil.LocalStore(t, t.TempDir())
	st.Put(ctx, "a", store.Bytes([]byte("hello")), store.PutOpts{Size: 5})
	g := aspect.Gate(st, aspect.GateOpts{MetaInflight: 1, ReadTransfers: 1})
	rc, err := g.Open(ctx, "a", 0, 1) // holds the only read slot
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	ctx2, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if _, err := g.List(ctx2, ""); err != nil {
		t.Fatalf("a listing must not wait for a running transfer: %v", err)
	}
}

// A reader that stops reading keeps its read slot, and uploads still go on: the
// two classes are counted apart.
func TestGateStalledReaderDoesNotBlockUploads(t *testing.T) {
	ctx := context.Background()
	st := testutil.LocalStore(t, t.TempDir())
	st.Put(ctx, "a", store.Bytes(make([]byte, 1<<16)), store.PutOpts{Size: 1 << 16})
	g := aspect.Gate(st, aspect.GateOpts{Transfers: 1, ReadTransfers: 1})
	rc, err := g.Open(ctx, "a", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	rc.Read(make([]byte, 10)) // started, then stalled
	ctx2, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := g.Put(ctx2, "b", store.Bytes([]byte("x")), store.PutOpts{Size: 1}); err != nil {
		t.Fatalf("an upload waited for a stalled reader: %v", err)
	}
	ctx3, cancel3 := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel3()
	if _, err := g.Open(ctx3, "a", 0, 1); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a second reader should wait for the read slot, got %v", err)
	}
}

// A stream holds a read slot during the call only, however long its consumer takes.
func TestGateStreamHoldsNoSlotAfterOpen(t *testing.T) {
	ctx := context.Background()
	st := testutil.LocalStore(t, t.TempDir())
	st.Put(ctx, "a", store.Bytes(make([]byte, 1<<16)), store.PutOpts{Size: 1 << 16})
	g := aspect.Gate(st, aspect.GateOpts{ReadTransfers: 1})
	var open []io.ReadCloser
	for i := 0; i < 5; i++ {
		rc, err := g.OpenStream(ctx, "a", 0, -1)
		if err != nil {
			t.Fatal(err)
		}
		open = append(open, rc)
	}
	ctx2, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	rc, err := g.Open(ctx2, "a", 0, 1)
	if err != nil {
		t.Fatalf("a bounded read waited for open streams: %v", err)
	}
	rc.Close()
	for _, rc := range open {
		rc.Close()
	}
}

func davStack(t *testing.T, attempts int) (store.Store, *aspect.Meter, *testutil.FaultProxy, string) {
	drive := t.TempDir()
	url, _ := testutil.WebDAVServer(t, drive)
	proxy := testutil.NewFaultProxy(t, url)
	st, m := aspect.Stack(testutil.WebDAVStore(t, proxy.URL), aspect.Options{
		Retry: aspect.RetryOpts{Attempts: attempts, Base: 10 * time.Millisecond, Max: 50 * time.Millisecond},
	})
	return st, m, proxy, drive
}

func isMethod(m string) func(*http.Request) bool {
	return func(r *http.Request) bool { return r.Method == m }
}

func TestRetryTransientAndGiveUp(t *testing.T) {
	ctx := context.Background()
	st, m, proxy, _ := davStack(t, 4)
	if err := st.Put(ctx, "a.txt", store.Bytes([]byte("one")), store.PutOpts{Size: 3}); err != nil {
		t.Fatal(err)
	}
	// Three failures then success: within 4 attempts.
	proxy.Add(testutil.Fault{Match: isMethod("PROPFIND"), Kind: testutil.Status, Code: 503, Count: 3})
	m.Reset()
	if _, err := st.List(ctx, ""); err != nil {
		t.Fatalf("list should survive 3 failures: %v", err)
	}
	if n := m.Snapshot().Calls["list"]; n != 4 {
		t.Errorf("expected 4 list attempts, saw %d", n)
	}
	// A definite answer is not retried.
	m.Reset()
	if _, err := st.Stat(ctx, "missing"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("stat missing: %v", err)
	}
	if n := m.Snapshot().Calls["stat"]; n != 1 {
		t.Errorf("not-found must not be retried, saw %d attempts", n)
	}
	// Persistent failure gives up after the configured attempts.
	proxy.Add(testutil.Fault{Match: isMethod("PROPFIND"), Kind: testutil.Status, Code: 500, Count: -1})
	m.Reset()
	if _, err := st.List(ctx, ""); err == nil {
		t.Fatal("list should fail while the drive answers 500")
	}
	if n := m.Snapshot().Calls["list"]; n != 4 {
		t.Errorf("expected exactly 4 attempts, saw %d", n)
	}
}

func TestRetryRecognisesUploadThatLanded(t *testing.T) {
	ctx := context.Background()
	st, m, proxy, drive := davStack(t, 4)
	data := []byte(strings.Repeat("x", 5000))
	// rclone's WebDAV client deletes an upload that errored; the network that
	// dropped the answer is down for that clean-up too, so the object stays.
	proxy.Add(testutil.Fault{Match: isMethod("PUT"), Kind: testutil.DropAfter, Count: 1})
	proxy.Add(testutil.Fault{Match: isMethod("DELETE"), Kind: testutil.DropBefore, Count: 1})
	m.Reset()
	// Unique: the name never holds other content, so the object that is there is ours.
	if err := st.Put(ctx, "pack-1", store.Bytes(data), store.PutOpts{Size: int64(len(data)), Unique: true}); err != nil {
		t.Fatalf("put with a dropped answer: %v", err)
	}
	if n := m.Snapshot().Calls["put"]; n != 1 {
		t.Errorf("the landed upload must not be sent again, saw %d puts", n)
	}
	if b, err := os.ReadFile(filepath.Join(drive, "pack-1")); err != nil || string(b) != string(data) {
		t.Fatalf("object on the drive: %v", err)
	}
	// Not Unique and no hash: same size could be older content, so it is uploaded again.
	proxy.Add(testutil.Fault{Match: isMethod("PUT"), Kind: testutil.DropAfter, Count: 1})
	proxy.Add(testutil.Fault{Match: isMethod("DELETE"), Kind: testutil.DropBefore, Count: 1})
	m.Reset()
	if err := st.Put(ctx, "plain", store.Bytes(data), store.PutOpts{Size: int64(len(data))}); err != nil {
		t.Fatal(err)
	}
	if n := m.Snapshot().Calls["put"]; n != 2 {
		t.Errorf("without Unique or a hash the put must be repeated, saw %d puts", n)
	}
}

func TestRetryMutationsAreIdempotent(t *testing.T) {
	ctx := context.Background()
	st, m, proxy, drive := davStack(t, 4)
	st.Put(ctx, "a", store.Bytes([]byte("A")), store.PutOpts{Size: 1})
	st.Put(ctx, "b", store.Bytes([]byte("B")), store.PutOpts{Size: 1})
	proxy.Add(testutil.Fault{Match: isMethod("MOVE"), Kind: testutil.DropAfter, Count: 1})
	if err := st.Move(ctx, "a", "a2"); err != nil {
		t.Fatalf("move whose answer was lost: %v", err)
	}
	if _, err := os.Stat(filepath.Join(drive, "a2")); err != nil {
		t.Fatal("moved file missing")
	}
	proxy.Add(testutil.Fault{Match: isMethod("DELETE"), Kind: testutil.DropAfter, Count: 1})
	m.Reset()
	if err := st.Remove(ctx, "b"); err != nil {
		t.Fatalf("remove whose answer was lost: %v", err)
	}
	if _, err := os.Stat(filepath.Join(drive, "b")); err == nil {
		t.Fatal("file still there")
	}
}

func TestRedactScrubsCredentials(t *testing.T) {
	for _, c := range []struct{ in, notWant string }{
		{"Get https://user:hunter2@host/x failed", "hunter2"},
		{"GET /d/a?sign=abcdef123456&x=1: 403", "abcdef123456"},
		{"request failed: access_token=ya29.SECRET.value more", "SECRET"},
		{"Authorization: Bearer eyJhbGciOi.payload.sig", "eyJhbGciOi"},
		{`{"password":"p4ss w0rd","user":"a"}`, "p4ss"},
	} {
		got := aspect.Scrub(c.in)
		if strings.Contains(got, c.notWant) {
			t.Errorf("Scrub(%q) = %q still contains the secret", c.in, got)
		}
	}
	if got := aspect.Scrub("plain error: file not found"); got != "plain error: file not found" {
		t.Errorf("harmless text was changed: %q", got)
	}
}
