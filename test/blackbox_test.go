package test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Ironboxplus/cfs/internal/testutil"
)

func sha(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:]) }

// ---- threshold edges through the WebDAV server ----

func TestEdgeFilesWebDAV(t *testing.T) {
	e := newLocalEnv(t)
	s := e.serve(t, nil)
	rng := rand.New(rand.NewPCG(3, 4))
	const T = 8 << 20
	type f struct {
		p    string
		size int
		data []byte
	}
	var files []f
	for _, sz := range []int{0, 1, T - 1, T, T + 1, 3 * T / 2} {
		files = append(files, f{p: fmt.Sprintf("edge/size-%d", sz), size: sz, data: randData(rng, sz)})
	}
	for _, x := range files {
		if err := s.DAV.MkcolAll(path.Dir(x.p)); err != nil {
			t.Fatal(err)
		}
		if err := s.DAV.PutBytes(x.p, x.data); err != nil {
			t.Fatal(err)
		}
	}
	// A file growing across the threshold in steps, and shrinking back.
	steps := []int{100, T - 1, T, T + 1, T - 1, T/2 + 1, T/2 - 1, 5, T + 7}
	var last []byte
	for _, sz := range steps {
		last = randData(rng, sz)
		if err := s.DAV.PutBytes("edge/grow", last); err != nil {
			t.Fatal(err)
		}
		s.WaitDrained(t, time.Minute)
		got, err := s.DAV.Get("edge/grow")
		if err != nil || !bytes.Equal(got, last) {
			t.Fatalf("grow step %d: wrong content (%v)", sz, err)
		}
	}
	s.WaitDrained(t, time.Minute)
	for _, x := range files {
		got, err := s.DAV.Get(x.p)
		if err != nil || !bytes.Equal(got, x.data) {
			t.Fatalf("%s: wrong content (%v)", x.p, err)
		}
		// Layout on the drive: stored 1:1 exactly from the threshold up.
		_, err = os.Stat(filepath.Join(e.drive, filepath.FromSlash(x.p)))
		if loose := err == nil; loose != (x.size >= T) {
			t.Errorf("%s (%d bytes): loose on the drive = %v", x.p, x.size, loose)
		}
	}
	if _, err := os.Stat(filepath.Join(e.drive, "edge", "grow")); err != nil {
		t.Errorf("final size T+7 should be a file of its own: %v", err)
	}
	fsckOK(t, s, true)
	logStats(t, "edges", s)
	checkPeaks(t, s, 1, 8)
	s.Quit(t)
}

// ---- big files through the WebDAV server ----

type streamer struct {
	seed uint64
	size int64
	pos  int64
}

func (s *streamer) fill(b []byte, off int64) {
	for i := 0; i < len(b); {
		pos := off + int64(i)
		x := s.seed ^ uint64(pos>>3)
		x += 0x9e3779b97f4a7c15
		x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
		x = (x ^ (x >> 27)) * 0x94d049bb133111eb
		x ^= x >> 31
		var tmp [8]byte
		for k := range tmp {
			tmp[k] = byte(x >> (8 * k))
		}
		i += copy(b[i:], tmp[pos&7:])
	}
}

func (s *streamer) Read(b []byte) (int, error) {
	if s.pos >= s.size {
		return 0, io.EOF
	}
	if int64(len(b)) > s.size-s.pos {
		b = b[:s.size-s.pos]
	}
	s.fill(b, s.pos)
	s.pos += int64(len(b))
	return len(b), nil
}

func (s *streamer) sum() string {
	h := sha256.New()
	buf := make([]byte, 1<<20)
	for off := int64(0); off < s.size; off += int64(len(buf)) {
		n := min(int64(len(buf)), s.size-off)
		s.fill(buf[:n], off)
		h.Write(buf[:n])
	}
	return hex.EncodeToString(h.Sum(nil))
}

func bigViaServe(t *testing.T, size int64) {
	if testing.Short() {
		t.Skip("big files skipped in -short mode")
	}
	e := newLocalEnv(t)
	s := e.serve(t, nil)
	src := &streamer{seed: uint64(size) * 31, size: size}
	want := src.sum()
	t0 := time.Now()
	if err := s.DAV.MkcolAll("big"); err != nil {
		t.Fatal(err)
	}
	if err := s.DAV.Put("big/huge.bin", src, size); err != nil {
		t.Fatal(err)
	}
	put := time.Since(t0)
	s.WaitDrained(t, 20*time.Minute)
	drained := time.Since(t0)
	fi, err := os.Stat(filepath.Join(e.drive, "big", "huge.bin"))
	if err != nil || fi.Size() != size {
		t.Fatalf("object on the drive: %v", err)
	}
	t0 = time.Now()
	got, n, err := s.DAV.Sum("big/huge.bin")
	if err != nil || n != size || got != want {
		t.Fatalf("download: n=%d err=%v ok=%v", n, err, got == want)
	}
	rng := rand.New(rand.NewPCG(8, 9))
	ref := &streamer{seed: src.seed, size: size}
	for i := 0; i < 10; i++ {
		off := rng.Int64N(size - 1)
		ln := int64(1 + rng.IntN(5<<20))
		if off+ln > size {
			ln = size - off
		}
		b, err := s.DAV.GetRange("big/huge.bin", off, ln)
		w := make([]byte, ln)
		ref.fill(w, off)
		if err != nil || !bytes.Equal(b, w) {
			t.Fatalf("range %d+%d: %v", off, ln, err)
		}
	}
	t.Logf("%d MiB: PUT returned after %v, on the drive after %v, download+verify %v", size>>20, put, drained, time.Since(t0))
	logStats(t, "big file", s)
	checkPeaks(t, s, 1, 8)
	s.Quit(t)
}

func TestBigFile1GiBWebDAVServe(t *testing.T) { bigViaServe(t, 1<<30) }
func TestBigFile4GiBWebDAVServe(t *testing.T) { bigViaServe(t, 4<<30) }

// ---- crashes of the whole server ----

type upload struct {
	p    string
	data []byte
}

func makeUploads(n int, seed uint64) []upload {
	rng := rand.New(rand.NewPCG(seed, 11))
	out := make([]upload, n)
	for i := range out {
		sz := []int{0, 20, 900, 5000, 60000, 400000}[rng.IntN(6)]
		if i%97 == 5 {
			sz = 8<<20 + rng.IntN(1000) // a few large ones
		}
		out[i] = upload{fmt.Sprintf("crash/d%d/file%03d.dat", i%7, i), randData(rng, sz)}
	}
	return out
}

// pushAll uploads everything with a few workers and returns what the server
// acknowledged (HTTP 2xx). Failures after the server died are expected.
func pushAll(d *testutil.DAV, ups []upload, workers int) map[string]string {
	acked := map[string]string{}
	seen := map[string]bool{}
	for _, u := range ups {
		if dir := path.Dir(u.p); !seen[dir] {
			seen[dir] = true
			d.MkcolAll(dir)
		}
	}
	var mu sync.Mutex
	var wg sync.WaitGroup
	ch := make(chan upload)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for u := range ch {
				if err := d.PutBytes(u.p, u.data); err == nil {
					mu.Lock()
					acked[u.p] = sha(u.data)
					mu.Unlock()
				}
			}
		}()
	}
	for _, u := range ups {
		ch <- u
	}
	close(ch)
	wg.Wait()
	return acked
}

func TestServerCrashKeepsAcknowledgedUploads(t *testing.T) {
	cases := []string{"pack_uploaded:2", "pack_verified:3", "log_uploaded:2", "local_marked:2", "random"}
	for _, cp := range cases {
		t.Run(strings.ReplaceAll(cp, ":", "_"), func(t *testing.T) {
			e := newLocalEnv(t)
			ups := makeUploads(400, 21)
			mod := func(o *testutil.ServeOpts) {
				if cp != "random" {
					o.Crashpoint = cp
				}
			}
			s := e.serve(t, mod)
			if _, err := os.Stat(e.state); err != nil {
				t.Fatalf("state dir: %v", err)
			}
			if cp == "random" {
				go func() {
					time.Sleep(350 * time.Millisecond)
					s.Kill()
				}()
			}
			acked := pushAll(s.DAV, ups, 6)
			// Let the crash happen even if the client finished first.
			deadline := time.Now().Add(20 * time.Second)
			for cp != "random" && !s.Exited() && time.Now().Before(deadline) {
				time.Sleep(100 * time.Millisecond)
			}
			if cp != "random" {
				if !s.Exited() {
					t.Fatalf("the server never reached crash point %s", cp)
				}
				if code := s.Wait(); code != 137 {
					t.Fatalf("exit code %d, want 137\n%s", code, s.LogTail())
				}
			} else {
				s.Kill()
			}
			t.Logf("%s: server died; %d of %d uploads were acknowledged", cp, len(acked), len(ups))

			// Restart on the same state and VFS cache: the VFS re-uploads what it still held.
			s2 := e.serve(t, nil)
			s2.WaitDrained(t, 5*time.Minute)
			bad := 0
			byPath := map[string][]byte{}
			for _, u := range ups {
				byPath[u.p] = u.data
			}
			for p, want := range acked {
				got, err := s2.DAV.Get(p)
				if err != nil || sha(got) != want {
					bad++
					if bad < 10 {
						t.Errorf("acknowledged upload %s lost or wrong after the crash (%v)", p, err)
					}
				}
			}
			// Unacknowledged uploads are absent, complete, or (rclone VFS) a written prefix.
			present := 0
			for _, u := range ups {
				if _, ok := acked[u.p]; ok {
					continue
				}
				got, err := s2.DAV.Get(u.p)
				if err == nil {
					present++
					// rclone's VFS re-uploads a file that was half written when the
					// process died, as a local disk would keep the written prefix.
					if !bytes.HasPrefix(u.data, got) {
						t.Errorf("unacknowledged upload %s present with content that is not a prefix of what was sent", u.p)
					}
				}
			}
			t.Logf("%d acknowledged files verified; %d unacknowledged ones had made it too", len(acked), present)
			// A crash between uploading a pack and logging it leaves an unreferenced
			// pack; nothing else may be wrong, and fsck --repair removes the orphan.
			res := s2.Cmd(t, "fsck", map[string]string{"deep": "true"})
			for _, p := range res["problems"].([]any) {
				if !strings.HasPrefix(p.(string), "orphan_pack") {
					t.Errorf("fsck: %v", p)
				}
			}
			s2.Cmd(t, "fsck", map[string]string{"repair": "true"})
			fsckOK(t, s2, true)
			logStats(t, "after restart", s2)
			checkPeaks(t, s2, 1, 8)
			s2.Quit(t)
		})
	}
}

// ---- faults between cfs and the drive, seen through the server ----

func TestServerOverFaultyDrive(t *testing.T) {
	var proxy *testutil.FaultProxy
	e, _ := newDavEnv(t, func(url string) string {
		proxy = testutil.NewFaultProxy(t, url)
		return proxy.URL
	})
	ups := makeUploads(150, 33)
	s := e.serve(t, func(o *testutil.ServeOpts) { o.Meta = 1 })
	proxy.Add(testutil.Fault{Match: func(r *http.Request) bool { return r.Method == "PUT" }, Kind: testutil.Status, Code: 429, Count: 4})
	proxy.Add(testutil.Fault{Match: func(r *http.Request) bool { return r.Method == "PUT" }, Kind: testutil.DropAfter, Count: 2})
	proxy.Add(testutil.Fault{Match: func(r *http.Request) bool { return r.Method == "PUT" }, Kind: testutil.DropBefore, Count: 2})
	proxy.Add(testutil.Fault{Match: func(r *http.Request) bool { return r.Method == "PROPFIND" }, Kind: testutil.Status, Code: 503, Count: 2})
	proxy.Add(testutil.Fault{Match: func(r *http.Request) bool { return r.Method == "PROPFIND" }, Kind: testutil.Delay, Wait: 100 * time.Millisecond, Count: 30})
	acked := pushAll(s.DAV, ups, 6)
	if len(acked) != len(ups) {
		t.Fatalf("the server accepted %d of %d uploads\n%s", len(acked), len(ups), s.LogTail())
	}
	s.WaitDrained(t, 5*time.Minute)
	proxy.Clear()
	for _, u := range ups {
		got, err := s.DAV.Get(u.p)
		if err != nil || !bytes.Equal(got, u.data) {
			t.Fatalf("%s wrong after faults: %v", u.p, err)
		}
	}
	fsckOK(t, s, true)
	logStats(t, "faulty drive", s)
	checkPeaks(t, s, 1, 8)
	t.Logf("proxy saw PUT=%d PROPFIND=%d GET=%d", proxy.Count("PUT"), proxy.Count("PROPFIND"), proxy.Count("GET"))
	s.Quit(t)
}
