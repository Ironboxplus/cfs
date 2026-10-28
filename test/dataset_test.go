// Package test holds the black-box tests: they only talk to a running
// "cfs serve webdav" through WebDAV, and compare what comes back with a local
// reference tree by content hash.
package test

import (
	"fmt"
	"math/rand/v2"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Ironboxplus/cfs/internal/testutil"
)

// Scratch is where the large generated data lives.
func scratch(t testing.TB) string {
	if d := os.Getenv("CFS_SCRATCH"); d != "" {
		return d
	}
	return `E:\Go\Openlist\tmp\cfs-bb`
}

// dsThreshold is the size from which the generated dataset's files count as
// large (the servers of these tests run with testutil's 64 KiB threshold).
const dsThreshold = 64 << 10

// buildDataset writes a deterministic source tree below dst: 1500 files of a
// source-tree-like mix of sizes (mostly small, some 10-100 KiB, a few above the
// threshold), about 40 MB in all, in directories four levels deep.
func buildDataset(t testing.TB, dst string, files int) {
	t.Helper()
	rng := rand.New(rand.NewPCG(42, 7))
	exts := []string{".go", ".js", ".json", ".txt", ".md", ".css"}
	for i := 0; i < files; i++ {
		dir := fmt.Sprintf("p%d/q%d/r%d/s%d", i%6, (i/6)%5, (i/30)%4, (i/120)%3)
		ext := exts[i%len(exts)]
		var size int
		switch r := rng.IntN(100); {
		case i%250 == 3: // the large ones: .js, 1-3 MiB
			size, ext = 1<<20+rng.IntN(3<<19), ".js"
		case r < 70:
			size = rng.IntN(8 << 10)
		case r < 95:
			size = 8<<10 + rng.IntN(60<<10)
		default:
			size = 100<<10 + rng.IntN(200<<10)
		}
		p := filepath.Join(dst, filepath.FromSlash(dir), fmt.Sprintf("f%04d%s", i, ext))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, randData(rng, size), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// uploadTree PUTs every file of ref under root with the given parallelism.
func uploadTree(t testing.TB, d *testutil.DAV, ref testutil.RefTree, root string, workers int) (files int, bytes int64) {
	t.Helper()
	rf, rd, err := ref.Scan()
	if err != nil {
		t.Fatal(err)
	}
	if root != "" {
		if err := d.MkcolAll(root); err != nil {
			t.Fatal(err)
		}
	}
	dirs := make([]string, 0, len(rd))
	for p := range rd {
		dirs = append(dirs, p)
	}
	sort.Strings(dirs) // parents sort before children
	for _, p := range dirs {
		if err := d.Mkcol(path.Join(root, p)); err != nil {
			t.Fatal(err)
		}
	}
	names := make([]string, 0, len(rf))
	for p := range rf {
		names = append(names, p)
	}
	sort.Strings(names)
	var wg sync.WaitGroup
	var mu sync.Mutex
	ch := make(chan string)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for p := range ch {
				f, err := os.Open(filepath.Join(ref.Root, filepath.FromSlash(p)))
				if err != nil {
					t.Errorf("open %s: %v", p, err)
					continue
				}
				err = d.Put(path.Join(root, p), f, rf[p])
				f.Close()
				if err != nil {
					t.Errorf("upload %s: %v", p, err)
				}
				mu.Lock()
				files++
				bytes += rf[p]
				mu.Unlock()
			}
		}()
	}
	for _, p := range names {
		ch <- p
	}
	close(ch)
	wg.Wait()
	if t.Failed() {
		t.FailNow()
	}
	return
}

var dsWords = []string{"func ", "return ", "const ", "var ", "if ", "else ", "for ", "err ", "nil\n", "{\n", "}\n", "\t", "import ", "package ", "the ", "of ", "and ", "data ", "value ", "name ", "index ", "style ", "color: ", "margin: ", "0;\n", "1;\n"}

// randData returns n bytes of deterministic, text-like content (low entropy,
// like real source files).
func randData(rng *rand.Rand, n int) []byte {
	b := make([]byte, 0, n+16)
	for len(b) < n {
		b = append(b, dsWords[rng.IntN(len(dsWords))]...)
	}
	return b[:n]
}

type env struct {
	drive, state, cache string
	underlying          string // rclone remote string
	cleanup             func()
}

func newLocalEnv(t *testing.T) *env {
	base := t.TempDir()
	e := &env{drive: filepath.Join(base, "drive"), state: filepath.Join(base, "state"), cache: filepath.Join(base, "cache")}
	os.MkdirAll(e.drive, 0o755)
	e.underlying = e.drive
	return e
}

// newDavEnv puts the volume on a real WebDAV server (rclone serving a local
// directory in another process), reached through the given URL transformer.
func newDavEnv(t *testing.T, viaProxy func(url string) string) (*env, *testutil.Proc) {
	base := t.TempDir()
	e := &env{drive: filepath.Join(base, "drive"), state: filepath.Join(base, "state"), cache: filepath.Join(base, "cache")}
	os.MkdirAll(e.drive, 0o755)
	url, p := testutil.WebDAVServer(t, e.drive)
	if viaProxy != nil {
		url = viaProxy(url)
	}
	e.underlying = fmt.Sprintf(":webdav,url='%s',vendor=other:", url)
	return e, p
}

func (e *env) serve(t *testing.T, mod func(*testutil.ServeOpts)) *testutil.Server {
	o := testutil.ServeOpts{Underlying: e.underlying, StateDir: e.state, CacheDir: e.cache}
	if mod != nil {
		mod(&o)
	}
	return testutil.StartServe(t, o)
}

func logStats(t *testing.T, label string, s *testutil.Server) {
	t.Logf("[%s] underlying calls: %s", label, s.StatsString())
}

func checkPeaks(t *testing.T, s *testutil.Server, meta, transfers int) {
	t.Helper()
	st := s.Stats()
	if st.PeakMeta > meta {
		t.Errorf("metadata in-flight peak %d exceeds %d", st.PeakMeta, meta)
	}
	if st.PeakTransfer > transfers {
		t.Errorf("transfer in-flight peak %d exceeds %d", st.PeakTransfer, transfers)
	}
}

func fsckOK(t *testing.T, s *testutil.Server, deep bool) {
	t.Helper()
	res := s.Cmd(t, "fsck", map[string]string{"deep": fmt.Sprint(deep)})
	if ok, _ := res["ok"].(bool); !ok {
		t.Fatalf("fsck found problems: %v", res["problems"])
	}
	t.Logf("fsck ok: packs=%v members=%v segments=%v", res["packs"], res["members"], res["segments"])
}

func runDataset(t *testing.T, e *env, files int) {
	refDir := filepath.Join(t.TempDir(), "ref")
	os.MkdirAll(refDir, 0o755)
	t0 := time.Now()
	buildDataset(t, refDir, files)
	ref := testutil.RefTree{Root: refDir}

	serve := func() *testutil.Server { return e.serve(t, nil) }
	s := serve()
	defer func() {
		if t.Failed() {
			var keep []string
			for _, l := range strings.Split(s.LogAll(), "\n") {
				if !strings.Contains(l, "Copied") {
					keep = append(keep, l)
				}
			}
			t.Logf("server exited=%v code=%d; its log without the copy lines:\n%s", s.Exited(), s.Wait(), strings.Join(keep, "\n"))
		}
	}()
	files, bytes := uploadTree(t, s.DAV, ref, "", 12)
	upload := time.Since(t0)
	s.WaitDrained(t, 5*time.Minute)
	t.Logf("uploaded %d files, %d bytes in %v (drained after %v)", files, bytes, upload, time.Since(t0))
	logStats(t, "after upload", s)
	if st := s.Stats(); st.PeakTransfer < 2 {
		t.Errorf("expected concurrent transfers during the bulk upload, peak = %d", st.PeakTransfer)
	}
	n, b := ref.Compare(t, s.DAV, "", true)
	t.Logf("verified %d files, %d bytes", n, b)
	fsckOK(t, s, true)

	// Modify a tenth of the files, grow some across the threshold, shrink some big ones.
	rng := rand.New(rand.NewPCG(1, 2))
	rf, _, _ := ref.Scan()
	names := make([]string, 0, len(rf))
	for p := range rf {
		names = append(names, p)
	}
	sort.Strings(names)
	write := func(p string, data []byte) {
		if err := s.DAV.PutBytes(p, data); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(refDir, filepath.FromSlash(p)), data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	changed := 0
	for i, p := range names {
		switch {
		case i%10 == 0:
			write(p, randData(rng, []int{0, 1, 700, 5000, 200 << 10}[rng.IntN(5)]))
			changed++
		case i%400 == 7: // small file grows past the threshold
			write(p, randData(rng, dsThreshold+1))
			changed++
		}
	}
	shrunk := 0
	for _, p := range names {
		if rf[p] > dsThreshold && shrunk < 2 && strings.HasSuffix(p, ".js") {
			write(p, randData(rng, 100))
			shrunk++
		}
	}
	s.WaitDrained(t, 5*time.Minute)
	ref.Compare(t, s.DAV, "", true)
	t.Logf("modified %d files (+%d shrunk) and verified", changed, shrunk)
	logStats(t, "after modify", s)

	// Rename and move directories.
	_, rd, _ := ref.Scan()
	dirs := make([]string, 0, len(rd))
	for p := range rd {
		dirs = append(dirs, p)
	}
	sort.Strings(dirs)
	moved := 0
	for _, dname := range dirs {
		if moved >= 3 || strings.Count(dname, "/") != 1 || strings.Contains(dname, ".git") {
			continue
		}
		if _, err := os.Stat(filepath.Join(refDir, filepath.FromSlash(dname))); err != nil {
			continue
		}
		dst := dname + "-renamed"
		if moved == 2 {
			dst = "moved-here/" + path.Base(dname)
			os.MkdirAll(filepath.Join(refDir, "moved-here"), 0o755)
			if err := s.DAV.Mkcol("moved-here"); err != nil {
				t.Fatal(err)
			}
		}
		if err := s.DAV.Move(dname, dst); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(filepath.Join(refDir, filepath.FromSlash(dname)), filepath.Join(refDir, filepath.FromSlash(dst))); err != nil {
			t.Fatal(err)
		}
		moved++
	}
	s.WaitDrained(t, 5*time.Minute)
	ref.Compare(t, s.DAV, "", true)
	t.Logf("renamed/moved %d directories and verified", moved)
	logStats(t, "after dirmove", s)

	// Delete files and a directory tree.
	rf, rd, _ = ref.Scan()
	names = names[:0]
	for p := range rf {
		names = append(names, p)
	}
	sort.Strings(names)
	deleted := 0
	for i, p := range names {
		if i%13 == 0 {
			if err := s.DAV.Delete(p); err != nil {
				t.Fatal(err)
			}
			os.Remove(filepath.Join(refDir, filepath.FromSlash(p)))
			deleted++
		}
	}
	for _, dname := range dirs {
		if strings.HasSuffix(dname, "-renamed") {
			if err := s.DAV.Delete(dname); err != nil {
				t.Fatal(err)
			}
			os.RemoveAll(filepath.Join(refDir, filepath.FromSlash(dname)))
			break
		}
	}
	s.WaitDrained(t, 5*time.Minute)
	ref.Compare(t, s.DAV, "", true)
	t.Logf("deleted %d files and one directory tree and verified", deleted)
	logStats(t, "after delete", s)
	fsckOK(t, s, true)
	checkPeaks(t, s, 1, 8)

	// Lose all local state, rebuild from the drive alone.
	s.Cmd(t, "checkpoint", nil)
	s.Quit(t)
	os.RemoveAll(e.state)
	os.RemoveAll(e.cache)
	t1 := time.Now()
	s = serve()
	n, b = ref.Compare(t, s.DAV, "", true)
	t.Logf("rebuilt from the drive and verified %d files, %d bytes in %v", n, b, time.Since(t1))
	logStats(t, "after rebuild+verify", s)
	fsckOK(t, s, true)
	checkPeaks(t, s, 1, 8)

	// And once more with a stale checkpoint: more changes, hard kill, no checkpoint of them.
	if err := s.DAV.PutBytes("after-ckpt.txt", []byte("written after the last checkpoint")); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(refDir, "after-ckpt.txt"), []byte("written after the last checkpoint"), 0o644)
	s.WaitDrained(t, time.Minute)
	s.Cmd(t, "flush", nil)
	s.Kill()
	os.RemoveAll(e.state)
	os.RemoveAll(e.cache)
	s = serve()
	ref.Compare(t, s.DAV, "", true)
	t.Logf("rebuilt again after a hard kill (checkpoint + log replay): ok")
	logStats(t, "after second rebuild", s)
	s.Quit(t)
}

func TestDatasetLocal(t *testing.T) {
	runDataset(t, newLocalEnv(t), 1500)
}

func TestDatasetWebDAV(t *testing.T) {
	e, _ := newDavEnv(t, nil)
	// A third of the tree: the point here is the drive behind WebDAV, the full tree runs in TestDatasetLocal
	// (CFS_LONG=1 for all 1500 files).
	files := 500
	if os.Getenv("CFS_LONG") != "" {
		files = 1500
	}
	runDataset(t, e, files)
}
