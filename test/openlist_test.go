package test

import (
	"fmt"
	"math/rand/v2"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/Ironboxplus/cfs/internal/testutil"
)

// These tests run a real OpenList process (CFS_OP_BIN: the OP-sync build with
// the cfs driver) and use it only from outside: the admin API to create the
// storages, WebDAV to read and write. The cfs storage lives on a second
// OpenList storage (a Local one here; point it at a WebDAV storage to put it on
// a real remote, see STATUS.md).

type opEnv struct {
	o     *testutil.OpenList
	under string // directory behind the underlying OpenList storage
	data  string
}

func newOpEnv(t *testing.T, cfsAddition map[string]any) *opEnv {
	bin := testutil.OpenListBinary(t)
	base := t.TempDir()
	e := &opEnv{under: filepath.Join(base, "under"), data: filepath.Join(base, "op-data")}
	os.MkdirAll(e.under, 0o755)
	e.o = testutil.NewOpenList(t, bin, e.data, testutil.FreePort(t))
	e.o.Start(t, nil)
	e.addStorages(t, cfsAddition)
	return e
}

func (e *opEnv) addStorages(t *testing.T, cfsAddition map[string]any) {
	e.o.AddStorage(t, "/under", "Local", map[string]any{"root_folder_path": e.under})
	add := map[string]any{
		"remote_path": "/under/vol", "root_folder_path": "/",
		"seal_quiet": "100ms", "seal_max": "1s",
	}
	for k, v := range cfsAddition {
		add[k] = v
	}
	e.o.AddStorage(t, "/cfs", "Cfs", add)
}

// drive lists what the volume put on the underlying storage.
func (e *opEnv) drive(t *testing.T) (packs, segs, bigFiles int, bytes int64) {
	root := filepath.Join(e.under, "vol")
	filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		rel, _ := filepath.Rel(root, p)
		rel = filepath.ToSlash(rel)
		info, _ := d.Info()
		bytes += info.Size()
		switch {
		case strings.HasPrefix(rel, ".cfs/packs/"):
			packs++
		case strings.HasPrefix(rel, ".cfs/log/"), strings.HasPrefix(rel, ".cfs/ckpt/"):
			segs++
		case strings.HasPrefix(rel, ".cfs/"):
		default:
			bigFiles++
		}
		return nil
	})
	return
}

func (e *opEnv) logStats(t *testing.T, label string) {
	time.Sleep(700 * time.Millisecond) // the meter is written every 300 ms
	t.Logf("[%s] cfs -> underlying: %s", label, e.o.StatsString())
}

func (e *opEnv) checkPeaks(t *testing.T, meta, transfers int) {
	t.Helper()
	time.Sleep(700 * time.Millisecond)
	st := e.o.Stats()
	if st.PeakMeta > meta {
		t.Errorf("metadata in-flight peak %d exceeds %d", st.PeakMeta, meta)
	}
	if st.PeakTransfer > transfers {
		t.Errorf("transfer in-flight peak %d exceeds %d", st.PeakTransfer, transfers)
	}
}

func TestOpenListCfsDataset(t *testing.T) {
	src := datasetSource(t)
	e := newOpEnv(t, nil)
	refDir := filepath.Join(t.TempDir(), "ref")
	os.MkdirAll(refDir, 0o755)
	copyTree(t, src, refDir)
	ref := testutil.RefTree{Root: refDir}
	dav := e.cfs()

	t0 := time.Now()
	files, nbytes := uploadTree(t, dav, ref, "", 12)
	t.Logf("uploaded %d files, %d bytes through OpenList WebDAV in %v", files, nbytes, time.Since(t0))
	e.logStats(t, "after upload")
	packs, segs, big, dbytes := e.drive(t)
	t.Logf("underlying storage now holds %d packs, %d log/checkpoint files, %d large files, %d bytes (from %d files)", packs, segs, big, dbytes, files)
	if packs+segs+big >= files/4 {
		t.Errorf("packing did not reduce the file count: %d objects for %d files", packs+segs+big, files)
	}
	n, b := ref.Compare(t, dav, "", true)
	t.Logf("verified %d files, %d bytes", n, b)

	// Modify, grow across the threshold, shrink, rename, delete.
	rng := rand.New(rand.NewPCG(7, 8))
	rf, _, _ := ref.Scan()
	names := make([]string, 0, len(rf))
	for p := range rf {
		names = append(names, p)
	}
	sort.Strings(names)
	write := func(p string, data []byte) {
		if err := dav.PutBytes(p, data); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(refDir, filepath.FromSlash(p)), data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for i, p := range names {
		switch {
		case i%10 == 0:
			write(p, randData(rng, []int{0, 1, 700, 5000, 200 << 10}[rng.IntN(5)]))
		case i%400 == 7:
			write(p, randData(rng, 8<<20+1))
		}
	}
	for i, p := range names {
		if i%13 == 3 {
			if _, err := os.Stat(filepath.Join(refDir, filepath.FromSlash(p))); err != nil {
				continue
			}
			if err := dav.Delete(p); err != nil {
				t.Fatal(err)
			}
			os.Remove(filepath.Join(refDir, filepath.FromSlash(p)))
		}
	}
	_, rd, _ := ref.Scan()
	dirs := make([]string, 0)
	for p := range rd {
		if strings.Count(p, "/") == 1 && !strings.Contains(p, ".git") {
			dirs = append(dirs, p)
		}
	}
	sort.Strings(dirs)
	if len(dirs) > 0 {
		d := dirs[0]
		if err := dav.Move(d, d+"-renamed"); err != nil {
			t.Fatal(err)
		}
		os.Rename(filepath.Join(refDir, filepath.FromSlash(d)), filepath.Join(refDir, filepath.FromSlash(d+"-renamed")))
	}
	n, b = ref.Compare(t, dav, "", true)
	t.Logf("after modify/delete/rename: verified %d files, %d bytes", n, b)
	e.logStats(t, "after changes")
	e.checkPeaks(t, 1, 8)
	if st := e.o.Stats(); st.PeakTransfer < 2 {
		t.Errorf("expected overlapping transfers during the bulk upload, peak = %d", st.PeakTransfer)
	}

	// Kill the process the hard way: everything acknowledged must come back.
	e.o.Kill()
	e.o.Start(t, nil)
	n, b = ref.Compare(t, dav, "", true)
	t.Logf("after kill -9 and restart: verified %d files, %d bytes", n, b)

	// Lose the local index too: it is rebuilt from the drive.
	e.o.Kill()
	state := filepath.Join(e.data, "cfs")
	if err := os.RemoveAll(state); err != nil {
		t.Fatal(err)
	}
	e.o.Start(t, nil)
	n, b = ref.Compare(t, dav, "", true)
	t.Logf("after losing the index: rebuilt and verified %d files, %d bytes", n, b)
	e.logStats(t, "after rebuild")
}

// Both ways into one volume at once: OpenList's own WebDAV and the VFS layer
// that "openlist cfs serve webdav" (and a mount) puts in front of it. If they
// were two instances, one would not see the other's files.
func TestOpenListCfsOneInstanceForTwoFrontEnds(t *testing.T) {
	bin := testutil.OpenListBinary(t)
	base := t.TempDir()
	e := &opEnv{under: filepath.Join(base, "under"), data: filepath.Join(base, "op-data")}
	os.MkdirAll(e.under, 0o755)
	e.o = testutil.NewOpenList(t, bin, e.data, testutil.FreePort(t))
	// First start: only the server, to create the storages.
	e.o.Start(t, nil)
	e.addStorages(t, nil)
	e.o.Kill()
	// Second start: the server plus the VFS WebDAV of the same cfs storage.
	vfsAddr := fmt.Sprintf("127.0.0.1:%d", testutil.FreePort(t))
	e.o.Start(t, nil, "cfs", "serve", "webdav", "/cfs", "--addr", vfsAddr, "--vfs-cache-mode", "writes")
	testutil.WaitPort(t, vfsAddr, 60*time.Second)
	vfs := testutil.NewDAV("http://" + vfsAddr)

	rng := rand.New(rand.NewPCG(3, 4))
	small, big := randData(rng, 1500), randData(rng, 9<<20)
	if err := e.cfs().PutBytes("via-openlist/small.bin", small); err != nil {
		// parent directories first
		e.cfs().MkcolAll("via-openlist")
		if err := e.cfs().PutBytes("via-openlist/small.bin", small); err != nil {
			t.Fatal(err)
		}
	}
	vfs.MkcolAll("via-vfs")
	if err := vfs.PutBytes("via-vfs/small.bin", small[:700]); err != nil {
		t.Fatal(err)
	}
	if err := vfs.PutBytes("via-vfs/big.bin", big); err != nil {
		t.Fatal(err)
	}
	// Written by one front end, read by the other.
	waitFor(t, 30*time.Second, func() error {
		got, err := vfs.Get("via-openlist/small.bin")
		if err != nil {
			return err
		}
		if string(got) != string(small) {
			return fmt.Errorf("content differs")
		}
		return nil
	})
	waitFor(t, 60*time.Second, func() error {
		got, err := e.cfs().Get("via-vfs/big.bin")
		if err != nil {
			return err
		}
		if string(got) != string(big) {
			return fmt.Errorf("big content differs (%d bytes)", len(got))
		}
		return nil
	})
	waitFor(t, 30*time.Second, func() error {
		got, err := e.cfs().Get("via-vfs/small.bin")
		if err != nil {
			return err
		}
		if string(got) != string(small[:700]) {
			return fmt.Errorf("content differs")
		}
		return nil
	})
	e.logStats(t, "two front ends")
	e.checkPeaks(t, 1, 8)
	packs, _, bigFiles, _ := e.drive(t)
	t.Logf("underlying: %d packs, %d large files", packs, bigFiles)
}

func waitFor(t *testing.T, d time.Duration, f func() error) {
	t.Helper()
	deadline := time.Now().Add(d)
	var err error
	for time.Now().Before(deadline) {
		if err = f(); err == nil {
			return
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatalf("not true after %v: %v", d, err)
}

// cfs is OpenList's WebDAV rooted at the cfs storage.
func (e *opEnv) cfs() *testutil.DAV {
	d := testutil.NewDAV(fmt.Sprintf("http://127.0.0.1:%d/dav/cfs", e.o.Port))
	d.User, d.Pass = e.o.DAV.User, e.o.DAV.Pass
	return d
}

// The volume on a WebDAV storage of OpenList (its "WebDav" driver) instead of
// a Local one, through a real TCP fault proxy that drops answers and returns
// 503s while files are written. Point it at a real remote with
// CFS_OP_UNDER_URL / CFS_OP_UNDER_USER / CFS_OP_UNDER_PASS (then no proxy and
// no faults are injected, and the volume lives in the folder CFS_OP_UNDER_DIR,
// default /cfs-test).
func TestOpenListCfsOnWebDavStorage(t *testing.T) {
	bin := testutil.OpenListBinary(t)
	base := t.TempDir()
	data := filepath.Join(base, "op-data")
	o := testutil.NewOpenList(t, bin, data, testutil.FreePort(t))
	o.Start(t, nil)

	var proxy *testutil.FaultProxy
	url, user, pass, dir := os.Getenv("CFS_OP_UNDER_URL"), os.Getenv("CFS_OP_UNDER_USER"), os.Getenv("CFS_OP_UNDER_PASS"), os.Getenv("CFS_OP_UNDER_DIR")
	drive := filepath.Join(base, "drive")
	if url == "" {
		os.MkdirAll(drive, 0o755)
		u, _ := testutil.WebDAVServer(t, drive)
		proxy = testutil.NewFaultProxy(t, u)
		url, user, pass, dir = proxy.URL, "u", "p", "/"
	} else if dir == "" {
		dir = "/cfs-test"
	}
	o.AddStorage(t, "/under", "WebDav", map[string]any{
		"vendor": "other", "address": url, "username": user, "password": pass, "root_folder_path": dir,
	})
	o.AddStorage(t, "/cfs", "Cfs", map[string]any{
		"remote_path": "/under/vol", "root_folder_path": "/", "seal_quiet": "100ms", "seal_max": "1s",
		"threshold": "64KiB",
	})
	dav := testutil.NewDAV(fmt.Sprintf("http://127.0.0.1:%d/dav/cfs", o.Port))
	dav.User, dav.Pass = o.DAV.User, o.DAV.Pass

	if proxy != nil {
		isPut := func(r *http.Request) bool { return r.Method == "PUT" }
		isPropfind := func(r *http.Request) bool { return r.Method == "PROPFIND" }
		proxy.Add(testutil.Fault{Match: isPut, Kind: testutil.Status, Code: 503, Count: 2})
		proxy.Add(testutil.Fault{Match: isPut, Kind: testutil.DropBefore, Count: 1})
		// Never more consecutive failures than cfs tries (4).
		proxy.Add(testutil.Fault{Match: isPropfind, Kind: testutil.Status, Code: 503, Count: 3})
	}
	rng := rand.New(rand.NewPCG(11, 12))
	want := map[string][]byte{}
	for i := 0; i < 120; i++ {
		size := []int{0, 10, 700, 5000, 30000, 70000, 300000}[rng.IntN(7)]
		p := fmt.Sprintf("d%d/f%03d.bin", i%6, i)
		if i%6 == i && i < 6 {
			if err := dav.Mkcol(fmt.Sprintf("d%d", i)); err != nil {
				t.Fatal(err)
			}
		}
		want[p] = randData(rng, size)
		if err := dav.PutBytes(p, want[p]); err != nil {
			t.Fatalf("put %s: %v", p, err)
		}
	}
	for p, b := range want {
		got, err := dav.Get(p)
		if err != nil || string(got) != string(b) {
			t.Fatalf("%s differs (%v)", p, err)
		}
	}
	time.Sleep(700 * time.Millisecond)
	t.Logf("cfs -> underlying OpenList storage: %s", o.StatsString())
	if proxy != nil {
		t.Logf("requests that reached the WebDAV server: PUT=%d PROPFIND=%d MKCOL=%d GET=%d", proxy.Count("PUT"), proxy.Count("PROPFIND"), proxy.Count("MKCOL"), proxy.Count("GET"))
	}
	if st := o.Stats(); st.PeakMeta > 1 || st.PeakTransfer > 8 {
		t.Errorf("in-flight peaks meta=%d transfer=%d exceed the limits", st.PeakMeta, st.PeakTransfer)
	}
	// The same after a hard kill.
	o.Kill()
	o.Start(t, nil)
	for p, b := range want {
		got, err := dav.Get(p)
		if err != nil || string(got) != string(b) {
			t.Fatalf("after restart %s differs (%v)", p, err)
		}
	}
}

// The mount front end ("openlist cfs mount"): rclone's VFS through WinFsp (or
// FUSE) in the same process as the storage. It needs a binary built with
// -tags cmount (CFS_OP_MOUNT_BIN) and WinFsp installed; everywhere else it is
// skipped with the reason. TestOpenListCfsOneInstanceForTwoFrontEnds covers
// the same VFS layer over WebDAV, which needs neither.
func TestOpenListCfsMount(t *testing.T) {
	bin := os.Getenv("CFS_OP_MOUNT_BIN")
	if bin == "" {
		t.Skip("CFS_OP_MOUNT_BIN (an OpenList built with -tags cmount) is not set")
	}
	if runtime.GOOS == "windows" {
		found := false
		for _, p := range []string{`C:\Program Files (x86)\WinFsp\bin\winfsp-x64.dll`, `C:\Program Files\WinFsp\bin\winfsp-x64.dll`} {
			if _, err := os.Stat(p); err == nil {
				found = true
			}
		}
		if !found {
			t.Skip("WinFsp is not installed (https://winfsp.dev)")
		}
	}
	mountPoint := os.Getenv("CFS_OP_MOUNT_POINT")
	if mountPoint == "" {
		mountPoint = `Z:`
		if runtime.GOOS != "windows" {
			mountPoint = filepath.Join(t.TempDir(), "mnt")
			os.MkdirAll(mountPoint, 0o755)
		}
	}
	base := t.TempDir()
	e := &opEnv{under: filepath.Join(base, "under"), data: filepath.Join(base, "op-data")}
	os.MkdirAll(e.under, 0o755)
	e.o = testutil.NewOpenList(t, bin, e.data, testutil.FreePort(t))
	e.o.Start(t, nil)
	e.addStorages(t, nil)
	e.o.Kill()
	e.o.Start(t, nil, "cfs", "mount", "/cfs", mountPoint)
	root := mountPoint + string(os.PathSeparator)
	waitFor(t, 60*time.Second, func() error { _, err := os.ReadDir(root); return err })

	rng := rand.New(rand.NewPCG(5, 6))
	small, big := randData(rng, 1500), randData(rng, 9<<20)
	if err := os.WriteFile(filepath.Join(root, "small.bin"), small, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "big.bin"), big, 0o644); err != nil {
		t.Fatal(err)
	}
	dav := e.cfs()
	waitFor(t, 60*time.Second, func() error {
		for p, want := range map[string][]byte{"small.bin": small, "big.bin": big} {
			got, err := dav.Get(p)
			if err != nil {
				return err
			}
			if string(got) != string(want) {
				return fmt.Errorf("%s differs", p)
			}
		}
		return nil
	})
	e.logStats(t, "mount")
	e.checkPeaks(t, 1, 8)
}
