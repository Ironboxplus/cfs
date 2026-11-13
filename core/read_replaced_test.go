package core_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Ironboxplus/cfs/core"
	"github.com/Ironboxplus/cfs/internal/testutil"
)

// A reader that has listed a large file and then finds it replaced by another
// node (a different length, the same name) reads the whole new file, not as many
// bytes of it as the old listing said. The store has looked the file up again
// since (as any Hash or Move of it would), so it holds the new object while the
// reader's directory cache still holds the old entry.
func TestReadBigFileReplacedByAnotherNodeIsReadWhole(t *testing.T) {
	ctx := context.Background()
	drive := t.TempDir()
	w, err := openAt(t, drive, t.TempDir(), bigTweak)
	if err != nil {
		t.Fatal(err)
	}
	old := randBytes(300 << 10)
	if _, err := w.Put(ctx, "lib/big.bin", bytes.NewReader(old), int64(len(old)), time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}

	base := testutil.LocalStore(t, drive)
	st, _ := testutil.Stacked(base, aspectOpts())
	cfg := testutil.FastConfig(t.TempDir())
	cfg.Threshold = 64 << 10
	r, err := core.Open(ctx, st, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close(ctx)
	if !r.ReadOnly() {
		t.Fatal("the reader holds the lease")
	}
	if got, err := readAllVol(r, "lib/big.bin"); err != nil || !bytes.Equal(got, old) {
		t.Fatalf("first read: %d bytes, err %v", len(got), err)
	}

	// Another node replaces the file on the drive with a longer one.
	replaced := randBytes(400 << 10)
	file := filepath.Join(drive, "lib", "big.bin")
	if _, err := os.Stat(file); err != nil {
		t.Fatalf("the large file is not stored under its name: %v", err)
	}
	if err := os.WriteFile(file, replaced, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := base.Stat(ctx, "lib/big.bin"); err != nil {
		t.Fatal(err)
	}

	got, err := readAllVol(r, "lib/big.bin")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, replaced) {
		t.Fatalf("read %d bytes after the file was replaced, want the new file of %d bytes (a prefix of it: %v)", len(got), len(replaced), bytes.HasPrefix(replaced, got))
	}
}
