package core_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Ironboxplus/cfs/core"
)

// bigDrive makes a drive that has some log and n folders of its own (folders of
// large files an earlier writer made), and returns it.
func bigDrive(t *testing.T, n int) string {
	t.Helper()
	drive := t.TempDir()
	w, _ := openCounting(t, drive, t.TempDir(), nil)
	putMany(t, w, "seed", 20)
	if err := w.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		if err := os.MkdirAll(filepath.Join(drive, fmt.Sprintf("f%04d", i), "sub"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	return drive
}

// Taking the lease and the administrator's maintenance do not walk the drive's
// folders: 2000 folders (4000 with their subfolders), no listing of any.
func TestWriterDoesNotWalkTheDrive(t *testing.T) {
	ctx := context.Background()
	drive := bigDrive(t, 2000)
	m, oc := openCounting(t, drive, t.TempDir(), nil)
	defer m.Close(ctx)
	rep, err := m.MaintainLog(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("take the lease and maintain on a drive with 4000 folders: %d folder listings, %d other listings, %d folders made; %+v",
		oc.folderLists.Load(), oc.lists.Load()-oc.folderLists.Load(), oc.mkdirs.Load(), rep)
	if oc.folderLists.Load() != 0 {
		t.Errorf("%d folder listings, want 0", oc.folderLists.Load())
	}
}

// A large file in a new deep folder makes that path's folders and nothing
// else; the next one in it makes none; neither lists the folders of the drive.
func TestBigPutIntoANewDeepFolderTouchesOnlyItsPath(t *testing.T) {
	ctx := context.Background()
	drive := bigDrive(t, 2000)
	w, oc := openCounting(t, drive, t.TempDir(), func(c *core.Config) { c.Threshold = 64 << 10 })
	defer w.Close(ctx)
	put := func(p string) {
		data := randBytes(100 << 10)
		if _, err := w.Put(ctx, p, bytes.NewReader(data), int64(len(data)), time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.TakeLease(ctx); err != nil { // its root folder is not what is measured
		t.Fatal(err)
	}
	l0, m0 := oc.folderLists.Load(), oc.mkdirs.Load()
	put("new/deep/er/path/big.bin")
	l1, m1 := oc.folderLists.Load()-l0, oc.mkdirs.Load()-m0
	t.Logf("first large file in new/deep/er/path: %d folder listings, %d folders made", l1, m1)
	if l1 > 1 || m1 != 4 {
		t.Errorf("%d listings and %d folders made, want at most 1 and 4", l1, m1)
	}
	l0, m0 = oc.folderLists.Load(), oc.mkdirs.Load()
	put("new/deep/er/path/big2.bin")
	l2, m2 := oc.folderLists.Load()-l0, oc.mkdirs.Load()-m0
	t.Logf("second one: %d folder listings, %d folders made", l2, m2)
	if l2 != 0 || m2 != 0 {
		t.Errorf("%d listings and %d folders made for the second file, want none", l2, m2)
	}
	for _, p := range []string{"new/deep/er/path/big.bin", "new/deep/er/path/big2.bin"} {
		if st, err := os.Stat(filepath.Join(drive, filepath.FromSlash(p))); err != nil || st.Size() != 100<<10 {
			t.Errorf("%s on the drive: %v", p, err)
		}
	}
}

// Folders the writer was never told about are found when a change needs them:
// a folder moved with what is in it, an empty one removed, a tree removed.
func TestFoldersOfTheDriveAreFoundWhenAChangeNeedsThem(t *testing.T) {
	ctx := context.Background()
	drive := t.TempDir()
	w, _ := openCounting(t, drive, t.TempDir(), nil)
	putMany(t, w, "seed", 5)
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}
	data := randBytes(100 << 10)
	for _, d := range []string{"mv/src/inner", "empty/e1", "gone/a/b"} {
		if err := os.MkdirAll(filepath.Join(drive, filepath.FromSlash(d)), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for _, f := range []string{"mv/src/inner/big.bin", "gone/a/b/big.bin"} {
		if err := os.WriteFile(filepath.Join(drive, filepath.FromSlash(f)), data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	v, _ := openCounting(t, drive, t.TempDir(), nil)
	defer v.Close(ctx)
	exists := func(p string) bool {
		_, err := os.Stat(filepath.Join(drive, filepath.FromSlash(p)))
		return err == nil
	}
	if err := v.DirMove(ctx, "mv/src", "mv/dst"); err != nil {
		t.Fatal(err)
	}
	if exists("mv/src") || !exists("mv/dst/inner/big.bin") {
		t.Errorf("after the move: src %v, dst file %v", exists("mv/src"), exists("mv/dst/inner/big.bin"))
	}
	if got, err := readAllVol(v, "mv/dst/inner/big.bin"); err != nil || !bytes.Equal(got, data) {
		t.Errorf("read after the move: %v", err)
	}
	if err := v.Rmdir(ctx, "empty/e1"); err != nil {
		t.Fatal(err)
	}
	if exists("empty/e1") {
		t.Error("the empty folder is still on the drive")
	}
	if err := v.RemoveAll(ctx, "gone"); err != nil {
		t.Fatal(err)
	}
	if exists("gone") {
		t.Error("the removed tree is still on the drive")
	}
	rep, err := v.Fsck(ctx, core.FsckOpts{Deep: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range rep.Problems {
		t.Errorf("fsck: %s", p)
	}
}
