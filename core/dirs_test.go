package core_test

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Ironboxplus/cfs/core"
	"github.com/Ironboxplus/cfs/store"
)

// treeDump lists the whole volume with List: one line per entry.
func treeDump(t *testing.T, v *core.Volume) []string {
	t.Helper()
	var out []string
	var rec func(dir string)
	rec = func(dir string) {
		ents, err := v.List(context.Background(), dir)
		if err != nil {
			t.Fatalf("list %q: %v", dir, err)
		}
		for _, e := range ents {
			if e.IsDir {
				out = append(out, e.Path+"/")
				rec(e.Path)
			} else {
				out = append(out, fmt.Sprintf("%s %d", e.Path, e.Size))
			}
		}
	}
	rec("")
	sort.Strings(out)
	return out
}

// driveFolders lists the folders on the drive outside the control directory.
func driveFolders(t *testing.T, drive string) []string {
	t.Helper()
	var out []string
	filepath.WalkDir(drive, func(p string, d fs.DirEntry, err error) error {
		rel, _ := filepath.Rel(drive, p)
		rel = filepath.ToSlash(rel)
		if rel == "." {
			return nil
		}
		if strings.HasPrefix(rel, ".cfs") {
			return fs.SkipDir
		}
		if d.IsDir() {
			out = append(out, rel)
		}
		return nil
	})
	sort.Strings(out)
	return out
}

func calls(e *env, names ...string) string {
	s := e.meter.Snapshot()
	var b strings.Builder
	for _, n := range names {
		fmt.Fprintf(&b, "%s=%d ", n, s.Calls[n])
	}
	return strings.TrimSpace(b.String())
}

// Directories of small files cost the drive nothing: no folder is made, no
// folder is listed. A large file makes exactly the folders it needs. Everything
// survives a reopen and a rebuild from the drive alone, empty directories too.
func TestLazyDirsNoDriveCalls(t *testing.T) {
	for _, ack := range []core.AckMode{core.AckStaged, core.AckDurable} {
		t.Run(string(ack), func(t *testing.T) {
			ctx := context.Background()
			e := newEnv(t, aspectOpts(), func(c *core.Config) { c.Ack = ack; c.Threshold = 1 << 20 })
			e.meter.Reset()

			var dirs []string
			nDirs, nFiles := 100, 500
			if os.Getenv("CFS_LONG") != "" {
				nDirs, nFiles = 300, 2000
			}
			for i := 0; i < nDirs; i++ {
				dirs = append(dirs, fmt.Sprintf("top%d/mid%d/leaf%d", i%10, i%30, i))
			}
			var wg sync.WaitGroup
			files := make(chan int)
			for w := 0; w < 16; w++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := range files {
						p := fmt.Sprintf("%s/f%d.txt", dirs[i%len(dirs)], i)
						if _, err := e.v.Put(ctx, p, strings.NewReader(fmt.Sprintf("content %d", i)), -1, time.Now()); err != nil {
							t.Errorf("put %s: %v", p, err)
							return
						}
					}
				}()
			}
			for i := 0; i < nFiles; i++ {
				files <- i
			}
			close(files)
			wg.Wait()
			empties := []string{"empty1", "empty2/inner", "top1/mid1/emptyleaf", "top3/deep/er/est"}
			for _, d := range empties {
				if err := e.v.Mkdir(ctx, d); err != nil {
					t.Fatalf("mkdir %s: %v", d, err)
				}
			}
			if err := e.v.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			s := e.meter.Snapshot()
			t.Logf("%s, %d small files in %d directories + %d Mkdir: underlying %s", ack, nFiles, nDirs, len(empties), s)
			if s.Calls["mkdir"] != 0 || s.Calls["list"] > 5 {
				t.Fatalf("drive calls: mkdir=%d list=%d, want 0 and <= 5", s.Calls["mkdir"], s.Calls["list"])
			}
			if f := driveFolders(t, e.drive); len(f) != 0 {
				t.Fatalf("folders on the drive: %v", f)
			}
			for _, d := range append(append([]string{}, empties...), "top0", "top9/mid9", dirs[7]) {
				if en, err := e.v.Stat(ctx, d); err != nil || !en.IsDir {
					t.Fatalf("stat %s: %+v %v", d, en, err)
				}
			}

			// One large file in a deep directory: only its ancestors are made.
			e.meter.Reset()
			big := randBytes(2 << 20)
			e.put("big1/big2/big3/huge.bin", big)
			got := e.meter.Snapshot().Calls["mkdir"]
			t.Logf("one large file three levels down: %s", calls(e, "mkdir", "list", "put"))
			if got != 3 {
				t.Fatalf("mkdir calls %d, want 3", got)
			}
			if f := driveFolders(t, e.drive); fmt.Sprint(f) != "[big1 big1/big2 big1/big2/big3]" {
				t.Fatalf("folders on the drive: %v", f)
			}
			if err := e.v.Flush(ctx); err != nil {
				t.Fatal(err)
			}

			want := treeDump(t, e.v)
			e.reopen()
			if got := treeDump(t, e.v); fmt.Sprint(got) != fmt.Sprint(want) {
				t.Fatalf("after reopen the tree differs: %d vs %d entries", len(got), len(want))
			}
			// Lose the local index: the log and the drive's folders give it all back.
			if err := e.v.Close(ctx); err != nil {
				t.Fatal(err)
			}
			e.v = nil
			removeAll(t, e.state)
			e.newState()
			e.open()
			if got := treeDump(t, e.v); fmt.Sprint(got) != fmt.Sprint(want) {
				t.Fatalf("after rebuild the tree differs: %d vs %d entries\n%v", len(got), len(want), diffLines(got, want))
			}
			if got := e.read("big1/big2/big3/huge.bin"); !bytes.Equal(got, big) {
				t.Fatal("large file differs after rebuild")
			}
			if en, err := e.v.Stat(ctx, "top3/deep/er/est"); err != nil || !en.IsDir {
				t.Fatalf("empty directory after rebuild: %+v %v", en, err)
			}
			fsckClean(t, e.v, false)
		})
	}
}

func diffLines(a, b []string) []string {
	in := map[string]bool{}
	for _, x := range b {
		in[x] = true
	}
	var out []string
	for _, x := range a {
		if !in[x] {
			out = append(out, "+"+x)
		}
	}
	in = map[string]bool{}
	for _, x := range a {
		in[x] = true
	}
	for _, x := range b {
		if !in[x] {
			out = append(out, "-"+x)
		}
	}
	return out
}

// Rmdir, DirMove and RemoveAll on trees that are only in the index make no
// call that changes the drive; where a folder exists they make exactly one.
func TestLazyDirsRmdirDirMoveRemoveAll(t *testing.T) {
	for _, ack := range []core.AckMode{core.AckStaged, core.AckDurable} {
		t.Run(string(ack), func(t *testing.T) {
			ctx := context.Background()
			e := newEnv(t, aspectOpts(), func(c *core.Config) { c.Ack = ack; c.Threshold = 1 << 20 })
			mutating := func() int64 {
				s := e.meter.Snapshot()
				return s.Calls["mkdir"] + s.Calls["rmdir"] + s.Calls["dirmove"] + s.Calls["purge"] + s.Calls["move"] + s.Calls["remove"]
			}

			// Rmdir.
			if err := e.v.Mkdir(ctx, "r/a/b"); err != nil {
				t.Fatal(err)
			}
			e.put("r/a/b/file.txt", []byte("x"))
			e.meter.Reset()
			if err := e.v.Rmdir(ctx, "r/a/b"); !errors.Is(err, store.ErrNotEmpty) {
				t.Fatalf("rmdir of a directory with a file: %v", err)
			}
			if err := e.v.Rmdir(ctx, "r/nothing"); !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("rmdir of a missing directory: %v", err)
			}
			if err := e.v.Remove(ctx, "r/a/b/file.txt"); err != nil {
				t.Fatal(err)
			}
			if err := e.v.Rmdir(ctx, "r/a/b"); err != nil {
				t.Fatal(err)
			}
			if _, err := e.v.Stat(ctx, "r/a/b"); !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("stat after rmdir: %v", err)
			}
			if en, err := e.v.Stat(ctx, "r/a"); err != nil || !en.IsDir {
				t.Fatalf("parent after rmdir: %+v %v", en, err)
			}
			if err := e.v.Rmdir(ctx, "r/a"); err != nil {
				t.Fatal(err)
			}
			t.Logf("rmdir of index-only directories: %s", calls(e, "rmdir", "mkdir", "list", "remove"))
			if n := calls(e, "rmdir"); n != "rmdir=0" {
				t.Fatalf("drive rmdir calls: %s", n)
			}
			// A folder of the drive is removed on the drive.
			e.put("r/folder/big.bin", randBytes(2<<20))
			if err := e.v.Remove(ctx, "r/folder/big.bin"); err != nil {
				t.Fatal(err)
			}
			e.meter.Reset()
			if err := e.v.Rmdir(ctx, "r/folder"); err != nil {
				t.Fatal(err)
			}
			if got := calls(e, "rmdir"); got != "rmdir=1" {
				t.Fatalf("rmdir of a folder: %s", got)
			}
			if _, err := os.Stat(filepath.Join(e.drive, "r", "folder")); !os.IsNotExist(err) {
				t.Fatalf("folder still on the drive: %v", err)
			}

			// DirMove of a tree that is only in the index.
			for i := 0; i < 20; i++ {
				e.put(fmt.Sprintf("m/src/x%d/y/f%d.txt", i%4, i), []byte("data"))
			}
			if err := e.v.Mkdir(ctx, "m/src/empty/er"); err != nil {
				t.Fatal(err)
			}
			if err := e.v.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			before := treeDump(t, e.v)
			e.meter.Reset()
			if err := e.v.DirMove(ctx, "m/src", "m/other/dst"); err != nil {
				t.Fatal(err)
			}
			t.Logf("dirmove of an index-only tree: %s", calls(e, "dirmove", "mkdir", "list", "move", "remove"))
			if mutating() != 0 {
				t.Fatalf("drive calls that change the drive: %d", mutating())
			}
			after := treeDump(t, e.v)
			var wantMoved []string
			for _, l := range before {
				if strings.HasPrefix(l, "m/src/") {
					wantMoved = append(wantMoved, strings.Replace(l, "m/src", "m/other/dst", 1))
				}
			}
			gotMoved := withPrefix(after, "m/other/dst/")
			if len(wantMoved) == 0 || fmt.Sprint(gotMoved) != fmt.Sprint(wantMoved) {
				t.Fatalf("tree after dirmove:\n%v", diffLines(gotMoved, wantMoved))
			}
			if _, err := e.v.Stat(ctx, "m/src"); !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("source still there: %v", err)
			}
			if en, err := e.v.Stat(ctx, "m/other/dst/empty/er"); err != nil || !en.IsDir {
				t.Fatalf("empty directory moved: %+v %v", en, err)
			}
			// ... and of a tree with a large file: one DirMove on the drive.
			e.put("m/withbig/sub/big.bin", randBytes(2<<20))
			e.put("m/withbig/sub/small.txt", []byte("s"))
			e.put("m/withbig/only/small2.txt", []byte("s"))
			e.meter.Reset()
			if err := e.v.DirMove(ctx, "m/withbig", "m/moved/withbig"); err != nil {
				t.Fatal(err)
			}
			t.Logf("dirmove of a tree with a large file: %s", calls(e, "dirmove", "mkdir", "list"))
			if got := calls(e, "dirmove"); got != "dirmove=1" {
				t.Fatalf("dirmove calls: %s", got)
			}
			if got := e.read("m/moved/withbig/sub/big.bin"); len(got) != 2<<20 {
				t.Fatalf("large file after dirmove: %d bytes", len(got))
			}
			if got := e.read("m/moved/withbig/only/small2.txt"); string(got) != "s" {
				t.Fatalf("small file after dirmove: %q", got)
			}
			if _, err := os.Stat(filepath.Join(e.drive, "m", "withbig")); !os.IsNotExist(err) {
				t.Fatalf("old folder still on the drive: %v", err)
			}

			// RemoveAll.
			e.meter.Reset()
			if err := e.v.RemoveAll(ctx, "m/other"); err != nil {
				t.Fatal(err)
			}
			t.Logf("removeall of an index-only tree: %s", calls(e, "purge", "list", "remove", "rmdir"))
			if got := calls(e, "purge"); got != "purge=0" {
				t.Fatalf("purge calls: %s", got)
			}
			if _, err := e.v.Stat(ctx, "m/other"); !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("tree still there: %v", err)
			}
			e.meter.Reset()
			if err := e.v.RemoveAll(ctx, "m/moved"); err != nil {
				t.Fatal(err)
			}
			t.Logf("removeall of a tree with a large file: %s", calls(e, "purge", "list", "remove", "rmdir"))
			if got := calls(e, "purge"); got != "purge=1" {
				t.Fatalf("purge calls: %s", got)
			}
			if _, err := os.Stat(filepath.Join(e.drive, "m", "moved")); !os.IsNotExist(err) {
				t.Fatalf("folder still on the drive: %v", err)
			}

			// What is left is the same after a reopen and after a rebuild.
			want := treeDump(t, e.v)
			e.reopen()
			if got := treeDump(t, e.v); fmt.Sprint(got) != fmt.Sprint(want) {
				t.Fatalf("after reopen: %v vs %v", got, want)
			}
			if err := e.v.Close(ctx); err != nil {
				t.Fatal(err)
			}
			e.v = nil
			removeAll(t, e.state)
			e.newState()
			e.open()
			if got := treeDump(t, e.v); fmt.Sprint(got) != fmt.Sprint(want) {
				t.Fatalf("after rebuild: %v vs %v", got, want)
			}
			fsckClean(t, e.v, false)
		})
	}
}

// withPrefix keeps the entries that start with prefix.
func withPrefix(lines []string, prefix string) []string {
	var out []string
	for _, l := range lines {
		if strings.HasPrefix(l, prefix) {
			out = append(out, l)
		}
	}
	return out
}

// A volume made before directories lived in the index has real folders for all
// of them and no record of any. Opening it records the folders (by walking the
// drive once) and the directories of its small files.
func TestLazyDirsMigration(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, aspectOpts(), func(c *core.Config) { c.Threshold = 1 << 20 })
	e.put("old/dir/a.txt", []byte("small a"))
	e.put("old/dir/b.txt", []byte("small b"))
	e.put("kept/big.bin", randBytes(2<<20))
	if err := e.v.Close(ctx); err != nil {
		t.Fatal(err)
	}
	e.v = nil

	// What the old code left on the drive: a folder per directory, empty ones
	// and large files in places the index never heard of.
	bigData := randBytes(3 << 20)
	for _, d := range []string{"old/dir", "legacy/deep", "legacy/emptydir", "legacy/deep/er"} {
		if err := os.MkdirAll(filepath.Join(e.drive, filepath.FromSlash(d)), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(e.drive, "legacy", "deep", "er", "legacy.bin"), bigData, 0o644); err != nil {
		t.Fatal(err)
	}
	// What the old index lacked: the two tables and the markers.
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(filepath.Join(e.state, "index.db")))
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		"DELETE FROM ddirs", "DELETE FROM dirs",
		"DELETE FROM kv WHERE k IN ('ddirs_complete','dirs_backfilled')",
	} {
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	db.Close()

	e.open()
	for _, c := range []struct {
		dir  string
		want string
	}{
		{"", "[kept/ legacy/ old/]"},
		{"old", "[dir/]"},
		{"old/dir", "[a.txt b.txt]"},
		{"legacy", "[deep/ emptydir/]"},
		{"legacy/deep", "[er/]"},
		{"legacy/deep/er", "[legacy.bin]"},
		{"legacy/emptydir", "[]"},
		{"kept", "[big.bin]"},
	} {
		if got := e.listNames(c.dir); fmt.Sprint(got) != c.want {
			t.Errorf("list %q = %v, want %s", c.dir, got, c.want)
		}
	}
	if got := e.read("legacy/deep/er/legacy.bin"); !bytes.Equal(got, bigData) {
		t.Fatal("legacy large file differs")
	}
	if en, err := e.v.Stat(ctx, "legacy/emptydir"); err != nil || !en.IsDir {
		t.Fatalf("legacy empty folder: %+v %v", en, err)
	}
	// And it behaves like a folder from now on: a small file can't take its name.
	if _, err := e.v.Put(ctx, "legacy/emptydir", strings.NewReader("x"), 1, time.Now()); !errors.Is(err, store.ErrIsDir) {
		t.Fatalf("put over a folder: %v", err)
	}
	// A large file under it uses the folder instead of making it again.
	e.meter.Reset()
	e.put("legacy/emptydir/new.bin", randBytes(2<<20))
	if got := calls(e, "mkdir"); got != "mkdir=0" {
		t.Fatalf("a known folder was made again: %s", got)
	}
	fsckClean(t, e.v, false)
	// The walk is not repeated at the next start.
	e.reopen()
	e.meter.Reset()
	if got := e.listNames("legacy"); fmt.Sprint(got) != "[deep/ emptydir/]" {
		t.Fatalf("after reopen: %v", got)
	}
}

// A name taken by a large file can't be a directory, and the other way round,
// though the index never made a folder for the small files' directories.
func TestLazyDirsShapeChecks(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, aspectOpts(), func(c *core.Config) { c.Threshold = 1 << 20 })
	e.put("big.bin", randBytes(2<<20))
	e.put("d/big2.bin", randBytes(2<<20))
	e.put("d/small.txt", []byte("s"))
	for _, p := range []string{"big.bin/x.txt", "d/big2.bin/y/z.txt"} {
		if _, err := e.v.Put(ctx, p, strings.NewReader("no"), 2, time.Now()); !errors.Is(err, store.ErrNotDir) {
			t.Errorf("put %s: %v, want ErrNotDir", p, err)
		}
	}
	if err := e.v.Mkdir(ctx, "big.bin/sub"); !errors.Is(err, store.ErrNotDir) {
		t.Errorf("mkdir below a large file: %v", err)
	}
	if err := e.v.Mkdir(ctx, "d/big2.bin"); !errors.Is(err, store.ErrExists) {
		t.Errorf("mkdir over a large file: %v", err)
	}
	if err := e.v.Mkdir(ctx, "d/small.txt"); !errors.Is(err, store.ErrExists) {
		t.Errorf("mkdir over a small file: %v", err)
	}
	if err := e.v.Mkdir(ctx, "emptydir"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.v.Put(ctx, "emptydir", strings.NewReader("x"), 1, time.Now()); !errors.Is(err, store.ErrIsDir) {
		t.Errorf("put over an empty directory: %v", err)
	}
	if _, err := e.v.Put(ctx, "emptydir", bytes.NewReader(randBytes(2<<20)), 2<<20, time.Now()); !errors.Is(err, store.ErrIsDir) {
		t.Errorf("large put over an empty directory: %v", err)
	}
	if err := e.v.Remove(ctx, "emptydir"); !errors.Is(err, store.ErrIsDir) {
		t.Errorf("remove of a directory: %v", err)
	}
	if err := e.v.Move(ctx, "d/small.txt", "emptydir"); !errors.Is(err, store.ErrIsDir) {
		t.Errorf("move onto a directory: %v", err)
	}
	if err := e.v.Mkdir(ctx, "emptydir"); err != nil {
		t.Errorf("mkdir of an existing directory: %v", err)
	}
}

// Writes into a directory right after it was renamed replace the files that
// moved with it, before and after the commit, a reopen and a rebuild.
func TestOverwriteAfterDirMove(t *testing.T) {
	for _, ack := range []core.AckMode{core.AckStaged, core.AckDurable} {
		t.Run(string(ack), func(t *testing.T) {
			ctx := context.Background()
			e := newEnv(t, aspectOpts(), func(c *core.Config) { c.Ack = ack; c.Threshold = 1 << 20 })
			for i := 0; i < 6; i++ {
				e.put(fmt.Sprintf("a/src/sub/f%d", i), []byte(fmt.Sprintf("old %d", i)))
			}
			if err := e.v.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			if err := e.v.DirMove(ctx, "a/src", "a/dst"); err != nil {
				t.Fatal(err)
			}
			check := func(when string) {
				t.Helper()
				for i := 0; i < 6; i++ {
					want := fmt.Sprintf("old %d", i)
					if i%2 == 0 {
						want = fmt.Sprintf("new content %d", i)
					}
					p := fmt.Sprintf("a/dst/sub/f%d", i)
					if got := string(e.read(p)); got != want {
						t.Fatalf("%s: %s = %q, want %q", when, p, got, want)
					}
					en, err := e.v.Stat(ctx, p)
					if err != nil || en.Size != int64(len(want)) {
						t.Fatalf("%s: stat %s = %+v %v, want size %d", when, p, en, err, len(want))
					}
				}
				for _, en := range func() []core.Entry { l, _ := e.v.List(ctx, "a/dst/sub"); return l }() {
					if en.Size < 5 || en.Size > 14 {
						t.Fatalf("%s: listing says %s is %d bytes", when, en.Path, en.Size)
					}
				}
			}
			for i := 0; i < 6; i += 2 {
				e.put(fmt.Sprintf("a/dst/sub/f%d", i), []byte(fmt.Sprintf("new content %d", i)))
			}
			check("right after the writes")
			if err := e.v.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			check("after the flush")
			e.reopen()
			check("after a reopen")
			if err := e.v.Close(ctx); err != nil {
				t.Fatal(err)
			}
			e.v = nil
			removeAll(t, e.state)
			e.newState()
			e.open()
			check("after a rebuild")
		})
	}
}

// Once a write is acknowledged, no read sees the previous version again, not
// even while the write is being committed.
func TestOverwriteNeverRevertsDuringCommit(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, aspectOpts(), func(c *core.Config) {
		c.Threshold = 1 << 20
		c.SealQuiet = time.Millisecond
		c.SealMax = 5 * time.Millisecond
	})
	for round := 0; round < 60; round++ {
		dir := fmt.Sprintf("r%d", round)
		old := bytes.Repeat([]byte("o"), 3000+round)
		for i := 0; i < 4; i++ {
			e.put(fmt.Sprintf("%s/f%d", dir, i), old)
		}
		if err := e.v.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		nw := bytes.Repeat([]byte("n"), 100+round)
		p := dir + "/f1"
		e.put(p, nw)
		stop := make(chan struct{})
		var wg sync.WaitGroup
		bad := make(chan string, 8)
		for w := 0; w < 3; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				for {
					select {
					case <-stop:
						return
					default:
					}
					switch w {
					case 0:
						ents, err := e.v.List(ctx, dir)
						if err != nil {
							bad <- err.Error()
							return
						}
						for _, en := range ents {
							if en.Name() == "f1" && en.Size != int64(len(nw)) {
								bad <- fmt.Sprintf("list: f1 is %d bytes", en.Size)
								return
							}
						}
					case 1:
						if en, err := e.v.Stat(ctx, p); err != nil || en.Size != int64(len(nw)) {
							bad <- fmt.Sprintf("stat: %+v %v", en, err)
							return
						}
					default:
						rc, err := e.v.Open(ctx, p, 0, -1)
						if err != nil {
							bad <- err.Error()
							return
						}
						b, _ := io.ReadAll(rc)
						rc.Close()
						if !bytes.Equal(b, nw) {
							bad <- fmt.Sprintf("read: %d bytes", len(b))
							return
						}
					}
				}
			}(w)
		}
		time.Sleep(30 * time.Millisecond)
		e.v.Flush(ctx)
		close(stop)
		wg.Wait()
		select {
		case m := <-bad:
			t.Fatalf("round %d: %s", round, m)
		default:
		}
	}
}
