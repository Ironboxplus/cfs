package testutil

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
)

// RefTree is a local reference directory the tests compare a server with.
type RefTree struct{ Root string }

// HashFile returns the SHA-256 of a local file.
func HashFile(p string) (string, int64, error) {
	f, err := os.Open(p)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	return hex.EncodeToString(h.Sum(nil)), n, err
}

// Scan lists the reference: file -> size, and directories.
func (r RefTree) Scan() (files map[string]int64, dirs map[string]bool, err error) {
	files, dirs = map[string]int64{}, map[string]bool{}
	err = filepath.WalkDir(r.Root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(r.Root, p)
		rel = filepath.ToSlash(rel)
		if rel == "." {
			return nil
		}
		if d.IsDir() {
			dirs[rel] = true
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		files[rel] = info.Size()
		return nil
	})
	return
}

// Compare checks that the tree under dav root equals the reference: same
// directories, same files, same sizes and, when deep, the same content hash for
// every file. It returns the number of files and bytes compared.
func (r RefTree) Compare(t testing.TB, d *DAV, root string, deep bool) (int, int64) {
	t.Helper()
	rf, rd, err := r.Scan()
	if err != nil {
		t.Fatal(err)
	}
	gf, gd, err := d.Walk(root)
	if err != nil {
		t.Fatalf("walk server: %v", err)
	}
	bad := 0
	report := func(format string, args ...any) {
		bad++
		if bad <= 20 {
			t.Errorf(format, args...)
		}
	}
	for p := range rd {
		if !gd[p] {
			report("directory %s missing on the server", p)
		}
	}
	for p := range gd {
		if !rd[p] {
			report("unexpected directory %s on the server", p)
		}
	}
	for p, sz := range rf {
		g, ok := gf[p]
		if !ok {
			report("file %s missing on the server", p)
		} else if g != sz {
			report("file %s: server size %d, reference %d", p, g, sz)
		}
	}
	for p := range gf {
		if _, ok := rf[p]; !ok {
			report("unexpected file %s on the server", p)
		}
	}
	var bytes int64
	if deep {
		names := make([]string, 0, len(rf))
		for p := range rf {
			names = append(names, p)
		}
		sort.Strings(names)
		var mu sync.Mutex
		var wg sync.WaitGroup
		ch := make(chan string)
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for p := range ch {
					want, n, err := HashFile(filepath.Join(r.Root, filepath.FromSlash(p)))
					if err != nil {
						mu.Lock()
						report("reference %s: %v", p, err)
						mu.Unlock()
						continue
					}
					got, gn, err := d.Sum(joinURL(root, p))
					mu.Lock()
					switch {
					case err != nil:
						report("download %s: %v", p, err)
					case gn != n || got != want:
						report("content of %s differs (server %d bytes, reference %d)", p, gn, n)
					}
					bytes += n
					mu.Unlock()
				}
			}()
		}
		for _, p := range names {
			ch <- p
		}
		close(ch)
		wg.Wait()
	}
	if bad > 0 {
		t.Fatalf("%d differences between the server and the reference", bad)
	}
	return len(rf), bytes
}

func joinURL(a, b string) string {
	if a == "" {
		return b
	}
	return a + "/" + b
}

var _ = fmt.Sprint
