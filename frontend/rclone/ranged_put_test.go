package cfsrclone_test

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"io"
	"math/rand/v2"
	"os"
	"sync"
	"testing"
	"time"

	cfsrclone "github.com/Ironboxplus/cfs/frontend/rclone"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/fs/object"
	"github.com/rclone/rclone/vfs"
	"github.com/rclone/rclone/vfs/vfscommon"
)

// The drive of these tests is rclone's local backend behind a thin Fs that
// records every Put and reads the source the way OpenList's rclonefs does
// (internal/rclonefs/put.go, newPutStream): when fs.UnWrapObjectInfo(src) is an
// object it is read by range and the stream `in` is left unread, and the SHA-1
// comes from src. The recording is all it adds to the local backend.

type putCall struct {
	remote    string
	size      int64
	ranged    bool   // src unwraps to an object that was read by range
	inRead    int64  // bytes read from the stream
	rangeRead int64  // bytes read from the source object by range
	sha1      string // src.Hash(SHA1)
}

type probeStats struct {
	mu    sync.Mutex
	calls []putCall
}

func (s *probeStats) add(c putCall) {
	s.mu.Lock()
	s.calls = append(s.calls, c)
	s.mu.Unlock()
}

// of returns the calls for remote
func (s *probeStats) of(remote string) (out []putCall) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.calls {
		if c.remote == remote {
			out = append(out, c)
		}
	}
	return out
}

var probes sync.Map // root -> *probeStats

func init() {
	fs.Register(&fs.RegInfo{
		Name:        "rangeprobe",
		Description: "local backend that records how files are put",
		NewFs: func(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
			f, err := fs.NewFs(ctx, root)
			if err != nil {
				return nil, err
			}
			st, _ := probes.LoadOrStore(root, &probeStats{})
			return &probeFs{Fs: f, st: st.(*probeStats)}, nil
		},
	})
}

type probeFs struct {
	fs.Fs
	st *probeStats
}

type probeObject struct {
	fs.Object
	f *probeFs
}

func (f *probeFs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	o, err := f.Fs.NewObject(ctx, remote)
	if err != nil {
		return nil, err
	}
	return &probeObject{o, f}, nil
}

func (f *probeFs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	var o fs.Object
	err := f.record(ctx, in, src, func(r io.Reader) (err error) {
		o, err = f.Fs.Put(ctx, r, src, options...)
		return err
	})
	if err != nil {
		return nil, err
	}
	return &probeObject{o, f}, nil
}

func (o *probeObject) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	return o.f.record(ctx, in, src, func(r io.Reader) error { return o.Object.Update(ctx, r, src, options...) })
}

func (f *probeFs) record(ctx context.Context, in io.Reader, src fs.ObjectInfo, put func(io.Reader) error) error {
	c := putCall{remote: src.Remote(), size: src.Size()}
	cin := &countingReader{r: in}
	var r io.Reader = cin
	if so := fs.UnWrapObjectInfo(src); so != nil {
		c.ranged = true
		c.sha1, _ = src.Hash(ctx, hash.SHA1)
		r = &rangeReader{ctx: ctx, o: so, size: src.Size(), n: &c.rangeRead}
	}
	err := put(r)
	c.inRead = cin.n
	f.st.add(c)
	return err
}

type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// rangeReader reads o by range, 256 KiB at a time
type rangeReader struct {
	ctx  context.Context
	o    fs.Object
	size int64
	pos  int64
	n    *int64
	cur  io.ReadCloser
}

func (r *rangeReader) Read(p []byte) (int, error) {
	for {
		if r.cur == nil {
			if r.pos >= r.size {
				return 0, io.EOF
			}
			end := min(r.pos+256<<10, r.size)
			rc, err := r.o.Open(r.ctx, &fs.RangeOption{Start: r.pos, End: end - 1})
			if err != nil {
				return 0, err
			}
			r.cur = rc
		}
		n, err := r.cur.Read(p)
		r.pos += int64(n)
		*r.n += int64(n)
		if err == io.EOF {
			r.cur.Close()
			r.cur = nil
			err = nil
		}
		if n > 0 || err != nil {
			return n, err
		}
	}
}

func setupProbeVFS(t *testing.T) (*vfs.VFS, fs.Fs, *probeStats) {
	t.Helper()
	ctx := context.Background()
	drive, state, cache := t.TempDir(), t.TempDir(), t.TempDir()
	config.SetCacheDir(cache)
	f, err := cfsrclone.NewFs(ctx, "probecfs", "", configmap.Simple{
		"remote": "rangeprobe:" + drive, "state_dir": state, "threshold": "64Ki",
		"seal_quiet": "10ms", "seal_max": "100ms", "dir_cache": "1h", "lease_ttl": "30s",
	})
	if err != nil {
		t.Fatal(err)
	}
	opt := vfscommon.Opt
	opt.CacheMode = vfscommon.CacheModeFull
	opt.WriteBack = fs.Duration(30 * time.Millisecond)
	opt.HandleCaching = 0
	opt.CachePollInterval = fs.Duration(time.Hour)
	v := vfs.New(ctx, f, &opt)
	t.Cleanup(func() {
		v.Shutdown()
		cfsrclone.CloseAll(ctx)
	})
	st, _ := probes.Load(drive)
	return v, f, st.(*probeStats)
}

func vfsWrite(t *testing.T, v *vfs.VFS, name string, data []byte) {
	t.Helper()
	fh, err := v.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o666)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fh.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := fh.Close(); err != nil {
		t.Fatal(err)
	}
	settle(v)
}

func fsContent(t *testing.T, f fs.Fs, name string) []byte {
	t.Helper()
	ctx := context.Background()
	o, err := f.NewObject(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	rc, err := o.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// A large file written through the VFS reaches the drive as the VFS's cache file,
// read by range, with its SHA-1, and the stream is not read: no second copy. The
// same when the file is replaced. A small file never goes there at its own name,
// and a Put of a plain stream (no source object) is read as a stream as before.
func TestVFSRangedPut(t *testing.T) {
	v, f, st := setupProbeVFS(t)
	rng := rand.New(rand.NewPCG(11, 12))
	rnd := func(n int) []byte {
		b := make([]byte, n)
		for i := range b {
			b[i] = byte(rng.Uint32())
		}
		return b
	}
	sum := func(b []byte) string { h := sha1.Sum(b); return hex.EncodeToString(h[:]) }

	for i, size := range []int{3 << 20, 2<<20 + 17} { // the second one replaces the first
		data := rnd(size)
		vfsWrite(t, v, "big.bin", data)
		calls := st.of("big.bin")
		if len(calls) != i+1 {
			t.Fatalf("write %d: %d puts of big.bin: %+v", i, len(calls), calls)
		}
		c := calls[i]
		if !c.ranged || c.inRead != 0 || c.rangeRead != int64(size) || c.sha1 != sum(data) {
			t.Fatalf("write %d: %+v, want ranged, stream unread, %d bytes by range, sha1 %s", i, c, size, sum(data))
		}
		if got := fsContent(t, f, "big.bin"); !bytes.Equal(got, data) {
			t.Fatalf("write %d: content differs (%d bytes)", i, len(got))
		}
	}

	small := rnd(4 << 10)
	vfsWrite(t, v, "small.bin", small)
	if calls := st.of("small.bin"); len(calls) != 0 {
		t.Fatalf("a small file was put at its own name: %+v", calls)
	}
	if got := fsContent(t, f, "small.bin"); !bytes.Equal(got, small) {
		t.Fatal("small file differs")
	}

	plain := rnd(1 << 20)
	info := object.NewStaticObjectInfo("plain.bin", time.Now(), int64(len(plain)), true, nil, f)
	if _, err := f.Put(context.Background(), bytes.NewReader(plain), info); err != nil {
		t.Fatal(err)
	}
	calls := st.of("plain.bin")
	if len(calls) != 1 || calls[0].ranged || calls[0].inRead != int64(len(plain)) {
		t.Fatalf("plain put: %+v", calls)
	}
	if got := fsContent(t, f, "plain.bin"); !bytes.Equal(got, plain) {
		t.Fatal("plain file differs")
	}
}
