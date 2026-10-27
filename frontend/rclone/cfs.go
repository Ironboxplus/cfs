// Package cfsrclone exposes a cfs volume as an rclone backend ("cfs"), so
// rclone's mount, serve webdav/nfs/..., sync and copy run on it unchanged.
// rclone's VFS turns POSIX operations into whole-object Put/Move/DirMove/Remove
// calls, which is what the volume implements.
package cfsrclone

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Ironboxplus/cfs/aspect"
	"github.com/Ironboxplus/cfs/core"
	"github.com/Ironboxplus/cfs/store"
	"github.com/Ironboxplus/cfs/store/rclonestore"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/cache"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/fspath"
	"github.com/rclone/rclone/fs/hash"
)

func init() {
	fs.Register(&fs.RegInfo{
		Name:        "cfs",
		Description: "Small-file packing filesystem over a rate-limited remote",
		NewFs:       NewFs,
		Options: []fs.Option{{
			Name:     "remote",
			Help:     "Remote (the drive) to keep the volume on, e.g. \"mydrive:cfs-volume\".",
			Required: true,
		}, {
			Name:    "state_dir",
			Help:    "Directory for the local index. Default: <user cache dir>/cfs/<hash of remote>. Losing it is safe: the index is rebuilt from the drive.",
			Default: "",
		}, {
			Name:     "threshold",
			Help:     "Files below this size are packed; others are stored 1:1.",
			Default:  fs.SizeSuffix(8 << 20),
			Advanced: true,
		}, {
			Name:     "pack_max_bytes",
			Help:     "Seal a pack when it reaches this size.",
			Default:  fs.SizeSuffix(128 << 20),
			Advanced: true,
		}, {
			Name:     "pack_max_files",
			Help:     "Seal a pack when it holds this many files.",
			Default:  10000,
			Advanced: true,
		}, {
			Name:     "seal_quiet",
			Help:     "Seal the open pack when no small file arrived for this long.",
			Default:  fs.Duration(2 * time.Second),
			Advanced: true,
		}, {
			Name:     "seal_max",
			Help:     "Seal the open pack this long after its first file.",
			Default:  fs.Duration(15 * time.Second),
			Advanced: true,
		}, {
			Name:     "meta_inflight",
			Help:     "Maximum simultaneous metadata calls (list, stat, remove, move, mkdir) to the remote.",
			Default:  1,
			Advanced: true,
		}, {
			Name:     "transfers",
			Help:     "Maximum simultaneous data transfers (uploads and ranged reads) to the remote. Each large file is still transferred sequentially.",
			Default:  8,
			Advanced: true,
		}, {
			Name:     "read_chunk",
			Help:     "Size of one ranged read of a large file.",
			Default:  fs.SizeSuffix(8 << 20),
			Advanced: true,
		}, {
			Name:     "dir_cache",
			Help:     "How long directory listings of the remote are trusted.",
			Default:  fs.Duration(5 * time.Minute),
			Advanced: true,
		}, {
			Name:     "lease_ttl",
			Help:     "A writer whose heartbeat is older than this is considered dead.",
			Default:  fs.Duration(60 * time.Second),
			Advanced: true,
		}, {
			Name:     "gc_grace",
			Help:     "Grace period before unreferenced packs, old log segments and checkpoints are deleted.",
			Default:  fs.Duration(24 * time.Hour),
			Advanced: true,
		}, {
			Name:     "checkpoint_every",
			Help:     "Upload a checkpoint after this many log segments (negative: never).",
			Default:  200,
			Advanced: true,
		}, {
			Name:     "retries",
			Help:     "Tries per call to the remote before giving up.",
			Default:  4,
			Advanced: true,
		}},
	})
}

// Options are the backend options.
type Options struct {
	Remote          string        `config:"remote"`
	StateDir        string        `config:"state_dir"`
	Threshold       fs.SizeSuffix `config:"threshold"`
	PackMaxBytes    fs.SizeSuffix `config:"pack_max_bytes"`
	PackMaxFiles    int           `config:"pack_max_files"`
	SealQuiet       fs.Duration   `config:"seal_quiet"`
	SealMax         fs.Duration   `config:"seal_max"`
	MetaInflight    int           `config:"meta_inflight"`
	Transfers       int           `config:"transfers"`
	ReadChunk       fs.SizeSuffix `config:"read_chunk"`
	DirCache        fs.Duration   `config:"dir_cache"`
	LeaseTTL        fs.Duration   `config:"lease_ttl"`
	GCGrace         fs.Duration   `config:"gc_grace"`
	CheckpointEvery int           `config:"checkpoint_every"`
	Retries         int           `config:"retries"`
}

// A volume must have exactly one instance per process, however often rclone
// asks for an Fs of it (every root and every command builds a new Fs).
var (
	regMu   sync.Mutex
	volumes = map[string]*volRef{}
)

type volRef struct {
	v     *core.Volume
	meter *aspect.Meter
	refs  int
	key   string
}

// Meter returns the meter of the volume behind f, if f is a cfs Fs.
func Meter(f fs.Fs) *aspect.Meter {
	if x, ok := f.(*Fs); ok {
		return x.ref.meter
	}
	return nil
}

// Volume returns the volume behind f, if f is a cfs Fs.
func Volume(f fs.Fs) *core.Volume {
	if x, ok := f.(*Fs); ok {
		return x.vol
	}
	return nil
}

func defaultStateDir(remote string) string {
	base, err := os.UserCacheDir()
	if err != nil {
		base = os.TempDir()
	}
	h := sha1.Sum([]byte(remote))
	return filepath.Join(base, "cfs", hex.EncodeToString(h[:6]))
}

// NewFs makes an Fs of the cfs backend.
func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	opt := new(Options)
	if err := configstruct.Set(m, opt); err != nil {
		return nil, err
	}
	if opt.Remote == "" {
		return nil, errors.New("cfs: remote is required")
	}
	if strings.HasPrefix(opt.Remote, name+":") {
		return nil, errors.New("cfs: can't point the remote at itself")
	}
	if opt.StateDir == "" {
		opt.StateDir = defaultStateDir(opt.Remote)
	}
	key := opt.Remote + "\x00" + opt.StateDir
	regMu.Lock()
	ref := volumes[key]
	if ref == nil {
		under, err := cache.Get(ctx, opt.Remote)
		if err != nil && !errors.Is(err, fs.ErrorIsFile) {
			regMu.Unlock()
			return nil, fmt.Errorf("cfs: open remote %q: %w", opt.Remote, err)
		}
		st, meter := aspect.Stack(rclonestore.New(under), aspect.Options{
			Gate:  aspect.GateOpts{MetaInflight: opt.MetaInflight, Transfers: opt.Transfers},
			Retry: aspect.RetryOpts{Attempts: opt.Retries},
		})
		v, err := core.Open(ctx, st, core.Config{
			StateDir:        opt.StateDir,
			Threshold:       int64(opt.Threshold),
			PackMaxBytes:    int64(opt.PackMaxBytes),
			PackMaxFiles:    opt.PackMaxFiles,
			SealQuiet:       time.Duration(opt.SealQuiet),
			SealMax:         time.Duration(opt.SealMax),
			ReadChunk:       int64(opt.ReadChunk),
			Pipeline:        opt.Transfers,
			DirCacheTTL:     time.Duration(opt.DirCache),
			LeaseTTL:        time.Duration(opt.LeaseTTL),
			GCGrace:         time.Duration(opt.GCGrace),
			CheckpointEvery: opt.CheckpointEvery,
			Logf:            func(f string, a ...any) { fs.Logf(nil, "cfs: "+f, a...) },
		})
		if err != nil {
			regMu.Unlock()
			return nil, err
		}
		ref = &volRef{v: v, meter: meter, key: key}
		volumes[key] = ref
	}
	ref.refs++
	regMu.Unlock()
	return newFs(ctx, ref, name, root, opt.Remote)
}

// FromVolume wraps an already open volume (shared with another front end, e.g.
// a storage driver) as an rclone Fs. The volume is not closed by Shutdown.
func FromVolume(ctx context.Context, v *core.Volume, name, root string) (fs.Fs, error) {
	return newFs(ctx, &volRef{v: v, refs: 1 << 30}, name, root, "volume")
}

func newFs(ctx context.Context, ref *volRef, name, root, desc string) (fs.Fs, error) {
	root = strings.Trim(path.Clean(root), "/")
	if root == "." {
		root = ""
	}
	f := &Fs{name: name, root: root, vol: ref.v, ref: ref, desc: desc}
	prec := ref.v.Features().ModTimePrecision
	if prec < 0 {
		prec = fs.ModTimeNotSupported
	}
	f.prec = prec
	f.features = (&fs.Features{
		CanHaveEmptyDirectories: true,
		PutStream:               f.PutStream,
		Move:                    f.Move,
		DirMove:                 f.DirMove,
	}).Fill(ctx, f)
	if root != "" {
		e, err := ref.v.Stat(ctx, root)
		if err == nil && !e.IsDir {
			f.root = path.Dir(root)
			if f.root == "." {
				f.root = ""
			}
			return f, fs.ErrorIsFile
		}
	}
	return f, nil
}

// Fs is a cfs volume (or a directory of it) as an rclone Fs.
type Fs struct {
	name, root, desc string
	vol              *core.Volume
	ref              *volRef
	prec             time.Duration
	features         *fs.Features
	once             sync.Once
}

var (
	_ fs.Fs          = (*Fs)(nil)
	_ fs.PutStreamer = (*Fs)(nil)
	_ fs.Mover       = (*Fs)(nil)
	_ fs.DirMover    = (*Fs)(nil)
	_ fs.Shutdowner  = (*Fs)(nil)
)

func (f *Fs) Name() string           { return f.name }
func (f *Fs) Root() string           { return f.root }
func (f *Fs) String() string         { return "cfs:" + f.desc + ":" + f.root }
func (f *Fs) Features() *fs.Features { return f.features }
func (f *Fs) Precision() time.Duration {
	return f.prec
}
func (f *Fs) Hashes() hash.Set {
	if f.vol.Features().SHA1 {
		return hash.NewHashSet(hash.SHA1)
	}
	return hash.Set(hash.None)
}

func (f *Fs) full(remote string) string { return path.Join(f.root, remote) }

func (f *Fs) rel(full string) string {
	if f.root == "" {
		return full
	}
	return strings.TrimPrefix(strings.TrimPrefix(full, f.root), "/")
}

func mapErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, store.ErrNotFound):
		return fs.ErrorObjectNotFound
	case errors.Is(err, store.ErrNotEmpty):
		return fs.ErrorDirectoryNotEmpty
	case errors.Is(err, store.ErrExists):
		return fs.ErrorDirExists
	case errors.Is(err, store.ErrIsDir):
		return fs.ErrorIsDir
	case errors.Is(err, store.ErrNotDir):
		return fs.ErrorIsFile
	case errors.Is(err, store.ErrNotSupported):
		return fs.ErrorCantSetModTime
	}
	return err
}

func (f *Fs) List(ctx context.Context, dir string) (fs.DirEntries, error) {
	ents, err := f.vol.List(ctx, f.full(dir))
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, fs.ErrorDirNotFound
		}
		return nil, mapErr(err)
	}
	out := make(fs.DirEntries, 0, len(ents))
	for _, e := range ents {
		remote := f.rel(e.Path)
		if e.IsDir {
			out = append(out, fs.NewDir(remote, e.ModTime))
		} else {
			out = append(out, f.newObject(e))
		}
	}
	return out, nil
}

func (f *Fs) newObject(e core.Entry) *Object {
	return &Object{fs: f, remote: f.rel(e.Path), size: e.Size, modTime: e.ModTime, sha1: e.SHA1, packed: e.Packed}
}

func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	e, err := f.vol.Stat(ctx, f.full(remote))
	if err != nil {
		return nil, mapErr(err)
	}
	if e.IsDir {
		return nil, fs.ErrorIsDir
	}
	return f.newObject(e), nil
}

func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	e, err := f.vol.Put(ctx, f.full(src.Remote()), in, src.Size(), src.ModTime(ctx))
	if err != nil {
		return nil, mapErr(err)
	}
	return f.newObject(e), nil
}

func (f *Fs) PutStream(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	e, err := f.vol.Put(ctx, f.full(src.Remote()), in, -1, src.ModTime(ctx))
	if err != nil {
		return nil, mapErr(err)
	}
	return f.newObject(e), nil
}

func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	return mapErr(f.vol.Mkdir(ctx, f.full(dir)))
}

func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	full := f.full(dir)
	if full == "" {
		ents, err := f.vol.List(ctx, "")
		if err != nil {
			return mapErr(err)
		}
		if len(ents) > 0 {
			return fs.ErrorDirectoryNotEmpty
		}
		return nil
	}
	err := f.vol.Rmdir(ctx, full)
	if errors.Is(err, store.ErrNotFound) {
		return fs.ErrorDirNotFound
	}
	return mapErr(err)
}

func (f *Fs) Move(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	so, ok := src.(*Object)
	if !ok || so.fs.vol != f.vol {
		return nil, fs.ErrorCantMove
	}
	if err := f.vol.Move(ctx, so.fs.full(so.remote), f.full(remote)); err != nil {
		return nil, mapErr(err)
	}
	return f.NewObject(ctx, remote)
}

func (f *Fs) DirMove(ctx context.Context, src fs.Fs, srcRemote, dstRemote string) error {
	sf, ok := src.(*Fs)
	if !ok || sf.vol != f.vol {
		return fs.ErrorCantDirMove
	}
	err := f.vol.DirMove(ctx, sf.full(srcRemote), f.full(dstRemote))
	switch {
	case errors.Is(err, store.ErrNotFound):
		return fs.ErrorDirNotFound
	case errors.Is(err, store.ErrExists):
		return fs.ErrorDirExists
	}
	return mapErr(err)
}

// Shutdown flushes the volume and closes it when the last Fs of it goes.
func (f *Fs) Shutdown(ctx context.Context) error {
	var err error
	f.once.Do(func() {
		regMu.Lock()
		f.ref.refs--
		last := f.ref.refs <= 0
		if last && f.ref.key != "" {
			delete(volumes, f.ref.key)
		}
		regMu.Unlock()
		if last && f.ref.key != "" {
			err = f.vol.Close(ctx)
		} else {
			err = f.vol.Flush(ctx)
		}
	})
	return err
}

// Object is a file of a cfs volume.
type Object struct {
	fs      *Fs
	remote  string
	size    int64
	modTime time.Time
	sha1    string
	packed  bool
}

var _ fs.Object = (*Object)(nil)

func (o *Object) Fs() fs.Info { return o.fs }
func (o *Object) String() string {
	if o == nil {
		return "<nil>"
	}
	return o.remote
}
func (o *Object) Remote() string                        { return o.remote }
func (o *Object) Size() int64                           { return o.size }
func (o *Object) ModTime(ctx context.Context) time.Time { return o.modTime }
func (o *Object) Storable() bool                        { return true }

func (o *Object) Hash(ctx context.Context, ty hash.Type) (string, error) {
	if ty != hash.SHA1 {
		return "", hash.ErrUnsupported
	}
	if o.sha1 != "" {
		return o.sha1, nil
	}
	if !o.fs.vol.Features().SHA1 {
		return "", hash.ErrUnsupported
	}
	h, err := o.fs.vol.Hash(ctx, o.fs.full(o.remote))
	if err != nil {
		return "", mapErr(err)
	}
	o.sha1 = h
	return h, nil
}

func (o *Object) SetModTime(ctx context.Context, t time.Time) error {
	if err := o.fs.vol.SetModTime(ctx, o.fs.full(o.remote), t); err != nil {
		return mapErr(err)
	}
	o.modTime = t
	return nil
}

func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	var off, n int64 = 0, -1
	for _, opt := range options {
		switch x := opt.(type) {
		case *fs.RangeOption:
			off, n = x.Decode(o.size)
		case *fs.SeekOption:
			off = x.Offset
		default:
			if opt.Mandatory() {
				fs.Logf(o, "Unsupported mandatory option: %v", opt)
			}
		}
	}
	rc, err := o.fs.vol.Open(ctx, o.fs.full(o.remote), off, n)
	return rc, mapErr(err)
}

func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	e, err := o.fs.vol.Put(ctx, o.fs.full(o.remote), in, src.Size(), src.ModTime(ctx))
	if err != nil {
		return mapErr(err)
	}
	o.size, o.modTime, o.sha1, o.packed = e.Size, e.ModTime, e.SHA1, e.Packed
	return nil
}

func (o *Object) Remove(ctx context.Context) error {
	return mapErr(o.fs.vol.Remove(ctx, o.fs.full(o.remote)))
}

var _ = fspath.JoinRootPath

// CloseAll flushes and closes every volume this process opened through NewFs.
// A process normally ends through Fs.Shutdown; tests and embedders use this.
func CloseAll(ctx context.Context) error {
	regMu.Lock()
	refs := make([]*volRef, 0, len(volumes))
	for k, r := range volumes {
		refs = append(refs, r)
		delete(volumes, k)
	}
	regMu.Unlock()
	var first error
	for _, r := range refs {
		if err := r.v.Close(ctx); err != nil && first == nil {
			first = err
		}
	}
	return first
}
