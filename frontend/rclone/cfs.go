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
	"github.com/rclone/rclone/fs/accounting"
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
		CommandHelp: commandHelp,
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
			Name:     "ack",
			Help:     "When a write of a small file returns. \"staged\": the data is on the local disk (state_dir) and is packed and uploaded in the background, so a pack holds everything that arrived in the seal window however few files are written at once; only losing the local disk before the upload loses acknowledged data. \"durable\": the write returns when its pack is on the remote (one pack per few concurrent writes).",
			Default:  "staged",
			Advanced: true,
			Examples: []fs.OptionExample{{Value: "staged", Help: "Acknowledge from the local staging area."}, {Value: "durable", Help: "Acknowledge when the data is on the remote."}},
		}, {
			Name:     "stage_max_bytes",
			Help:     "Most data waiting in the staging area (ack=staged). Writes wait while it is full.",
			Default:  fs.SizeSuffix(1 << 30),
			Advanced: true,
		}, {
			Name:     "seal_quiet",
			Help:     "Seal the open pack when no small file arrived for this long. 0: 30s with ack=staged (a pause that ends a burst), 2s with ack=durable.",
			Default:  fs.Duration(0),
			Advanced: true,
		}, {
			Name:     "seal_max",
			Help:     "Seal the open pack this long after its first file. 0: 10 minutes with ack=staged, 15s with ack=durable.",
			Default:  fs.Duration(0),
			Advanced: true,
		}, {
			Name:     "meta_inflight",
			Help:     "Maximum simultaneous metadata calls (list, stat, remove, move, mkdir) to the remote.",
			Default:  1,
			Advanced: true,
		}, {
			Name:     "transfers",
			Help:     "Maximum simultaneous uploads to the remote (packs, log segments, large files). Reads have their own limit, read_transfers.",
			Default:  8,
			Advanced: true,
		}, {
			Name:     "read_transfers",
			Help:     "Maximum simultaneous read streams from the remote (ranged reads of large files and of packs). Each one holds its slot until it is read to the end or closed. 0: the value of transfers.",
			Default:  0,
			Advanced: true,
		}, {
			Name:     "read_buffer",
			Help:     "Most memory the reads of small (packed or staged) files hold at once; a read waits while it is spent.",
			Default:  fs.SizeSuffix(300 << 20),
			Advanced: true,
		}, {
			Name:     "read_window",
			Help:     "Reading a packed file fetches the aligned window of this size around it from the pack, so that reading neighbours costs one request per window. Negative disables.",
			Default:  fs.SizeSuffix(512 << 10),
			Advanced: true,
		}, {
			Name:     "dir_cache",
			Help:     "How long directory listings of the remote are trusted.",
			Default:  fs.Duration(5 * time.Minute),
			Advanced: true,
		}, {
			Name:     "lease_ttl",
			Help:     "A writer whose heartbeat is older than this is considered dead. The heartbeat (a read and a write of the lease on the remote) runs every third of it.",
			Default:  fs.Duration(30 * time.Minute),
			Advanced: true,
		}, {
			Name:     "lease_idle",
			Help:     "A volume only takes the writer lease for its first change, and gives it back (a released lease) once it has made no change for this long and has nothing left to upload, so that another writer can take over; listing and reading never take it. Negative keeps it until the volume is closed.",
			Default:  fs.Duration(10 * time.Minute),
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
	Ack             string        `config:"ack"`
	StageMaxBytes   fs.SizeSuffix `config:"stage_max_bytes"`
	SealQuiet       fs.Duration   `config:"seal_quiet"`
	SealMax         fs.Duration   `config:"seal_max"`
	MetaInflight    int           `config:"meta_inflight"`
	Transfers       int           `config:"transfers"`
	ReadTransfers   int           `config:"read_transfers"`
	ReadBuffer      fs.SizeSuffix `config:"read_buffer"`
	ReadWindow      fs.SizeSuffix `config:"read_window"`
	DirCache        fs.Duration   `config:"dir_cache"`
	LeaseTTL        fs.Duration   `config:"lease_ttl"`
	LeaseIdle       fs.Duration   `config:"lease_idle"`
	GCGrace         fs.Duration   `config:"gc_grace"`
	CheckpointEvery int           `config:"checkpoint_every"`
	Retries         int           `config:"retries"`
}

// A volume must have exactly one instance per process, however often rclone
// asks for an Fs of it (every root and every command builds a new Fs).
var (
	regMu   sync.Mutex
	volumes = map[string]*volRef{}
	// closing counts the volumes whose last reference is being given up right
	// now: they are out of volumes, so CloseAll waits for them here. Two
	// goroutines end a process (rclone clears its Fs cache on one, the exit
	// handler runs on another) and the process must not exit before the close.
	closing sync.WaitGroup
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

// OptionsFrom reads the backend options from m, with the documented defaults
// for everything m leaves out.
func OptionsFrom(m configmap.Mapper) (*Options, error) {
	ri, err := fs.Find("cfs")
	if err != nil {
		return nil, err
	}
	def := configmap.Simple{}
	for i := range ri.Options {
		def[ri.Options[i].Name] = ri.Options[i].String()
	}
	cm := configmap.New()
	cm.AddGetter(m, configmap.PriorityNormal)
	cm.AddGetter(def, configmap.PriorityDefault)
	opt := new(Options)
	if err := configstruct.Set(cm, opt); err != nil {
		return nil, err
	}
	return opt, nil
}

// openRef opens the volume for key over under, or returns the one already
// open, with its reference count raised. Caller holds regMu.
func openRef(ctx context.Context, key string, under func() (fs.Fs, error), opt *Options) (*volRef, error) {
	if ref := volumes[key]; ref != nil {
		ref.refs++
		return ref, nil
	}
	u, err := under()
	if err != nil {
		return nil, err
	}
	st, meter := aspect.Stack(rclonestore.New(u), aspect.Options{
		Gate:  aspect.GateOpts{MetaInflight: opt.MetaInflight, Transfers: opt.Transfers, ReadTransfers: opt.ReadTransfers},
		Retry: aspect.RetryOpts{Attempts: opt.Retries},
	})
	v, err := core.Open(ctx, st, core.Config{
		StateDir:        opt.StateDir,
		Threshold:       int64(opt.Threshold),
		PackMaxBytes:    int64(opt.PackMaxBytes),
		PackMaxFiles:    opt.PackMaxFiles,
		Ack:             core.AckMode(opt.Ack),
		StageMaxBytes:   int64(opt.StageMaxBytes),
		SealQuiet:       time.Duration(opt.SealQuiet),
		SealMax:         time.Duration(opt.SealMax),
		ReadBuffer:      int64(opt.ReadBuffer),
		ReadWindow:      int64(opt.ReadWindow),
		Pipeline:        opt.Transfers,
		DirCacheTTL:     time.Duration(opt.DirCache),
		LeaseTTL:        time.Duration(opt.LeaseTTL),
		LeaseIdle:       time.Duration(opt.LeaseIdle),
		GCGrace:         time.Duration(opt.GCGrace),
		CheckpointEvery: opt.CheckpointEvery,
		Logf:            func(f string, a ...any) { fs.Logf(nil, "cfs: "+f, a...) },
	})
	if err != nil {
		return nil, err
	}
	ref := &volRef{v: v, meter: meter, key: key, refs: 1}
	startStats(ref)
	registerExit()
	volumes[key] = ref
	return ref, nil
}

// NewFs makes an Fs of the cfs backend.
func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	opt, err := OptionsFrom(m)
	if err != nil {
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
	regMu.Lock()
	ref, err := openRef(ctx, opt.Remote+"\x00"+opt.StateDir, func() (fs.Fs, error) {
		u, err := cache.Get(ctx, opt.Remote)
		if err != nil && !errors.Is(err, fs.ErrorIsFile) {
			return nil, fmt.Errorf("cfs: open remote %q: %w", opt.Remote, err)
		}
		return u, nil
	}, opt)
	regMu.Unlock()
	if err != nil {
		return nil, err
	}
	return newFs(ctx, ref, name, root, opt.Remote)
}

// Shared is a volume kept open for the whole process under a key, so that
// every front end (a storage driver, a mount, a WebDAV server) works on the
// same instance: a volume must have exactly one writer.
type Shared struct {
	ref  *volRef
	once sync.Once
}

// Acquire returns a reference to the volume registered under key, opening it
// over under (and with opt) when it is not open yet. under is called only then.
func Acquire(ctx context.Context, key string, under func() (fs.Fs, error), opt *Options) (*Shared, error) {
	regMu.Lock()
	defer regMu.Unlock()
	ref, err := openRef(ctx, key, under, opt)
	if err != nil {
		return nil, err
	}
	return &Shared{ref: ref}, nil
}

// Lookup returns another reference to the volume registered under key, or nil.
func Lookup(key string) *Shared {
	regMu.Lock()
	defer regMu.Unlock()
	ref := volumes[key]
	if ref == nil {
		return nil
	}
	ref.refs++
	return &Shared{ref: ref}
}

func (s *Shared) Volume() *core.Volume { return s.ref.v }
func (s *Shared) Meter() *aspect.Meter { return s.ref.meter }

// NewFs returns an rclone Fs on the shared volume. Its Shutdown gives up its
// own reference.
func (s *Shared) NewFs(ctx context.Context, name, root string) (fs.Fs, error) {
	regMu.Lock()
	s.ref.refs++
	regMu.Unlock()
	return newFs(ctx, s.ref, name, root, "shared")
}

// Release gives up this reference; the last one closes the volume.
func (s *Shared) Release(ctx context.Context) error {
	var err error
	s.once.Do(func() { err = releaseRef(ctx, s.ref) })
	return err
}

func releaseRef(ctx context.Context, ref *volRef) error {
	regMu.Lock()
	ref.refs--
	last := ref.refs <= 0
	closeNow := last && ref.key != ""
	if closeNow {
		closing.Add(1)
		if volumes[ref.key] == ref {
			delete(volumes, ref.key)
		}
	}
	regMu.Unlock()
	if closeNow {
		defer closing.Done()
		return ref.v.Close(ctx)
	}
	return ref.v.Flush(ctx)
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
	e, err := f.vol.PutWith(ctx, f.full(src.Remote()), in, f.putOpts(ctx, in, src))
	if err != nil {
		return nil, mapErr(err)
	}
	return f.newObject(e), nil
}

// putOpts is what Put and Update tell the volume about src. For a large file
// that rclone has in a file of its own (the VFS write-back's cache file, any
// object that can be opened by range) it also hands down that object, so a drive
// that hashes and uploads by range reads the source itself instead of making a
// copy of the stream, and the SHA-1 the source knows, so the drive may take the
// file without a transfer. Small files go into packs and need neither.
func (f *Fs) putOpts(ctx context.Context, in io.Reader, src fs.ObjectInfo) core.PutOpts {
	o := core.PutOpts{Size: src.Size(), MTime: src.ModTime(ctx)}
	srcObj := fs.UnWrapObjectInfo(src)
	if srcObj == nil || o.Size < f.vol.Config().Threshold {
		return o
	}
	if sum, err := src.Hash(ctx, hash.SHA1); err == nil {
		o.SHA1 = sum
	}
	acc, _ := in.(*accounting.Account)
	o.Ranged = objectRange{ctx, srcObj, acc}
	return o
}

// objectRange reads an rclone object by range. The bytes are accounted to the
// transfer's stream (acc, if rclone gave one), so that rclone's statistics and
// bandwidth limit apply to what is read, as they did when the stream was read.
type objectRange struct {
	ctx context.Context
	o   fs.Object
	acc *accounting.Account
}

func (r objectRange) OpenRange(off, n int64) (io.ReadCloser, error) {
	if n == 0 {
		return io.NopCloser(strings.NewReader("")), nil
	}
	end := int64(-1)
	if n > 0 {
		end = off + n - 1
	}
	rc, err := r.o.Open(r.ctx, &fs.RangeOption{Start: off, End: end})
	if err != nil || r.acc == nil {
		return rc, err
	}
	return &accountedRead{rc, r.acc}, nil
}

type accountedRead struct {
	io.ReadCloser
	acc *accounting.Account
}

func (a *accountedRead) Read(p []byte) (int, error) {
	n, err := a.ReadCloser.Read(p)
	if n > 0 {
		if accErr := a.acc.AccountRead(n); accErr != nil && err == nil {
			err = accErr
		}
	}
	return n, err
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
		err = releaseRef(ctx, f.ref)
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
	e, err := o.fs.vol.PutWith(ctx, o.fs.full(o.remote), in, o.fs.putOpts(ctx, in, src))
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
	closing.Wait()
	return first
}

var commandHelp = []fs.CommandHelp{{
	Name:  "fsck",
	Short: "Check the index, packs and large files against each other and the drive's log.",
	Long: `Reports problems, and fixes the safe ones only with -o repair=true.
Use -o deep=true to read every pack and verify every packed file's SHA-1.

    rclone backend fsck cfs:
`,
}, {
	Name:  "checkpoint",
	Short: "Upload a checkpoint of the index now.",
}, {
	Name:  "maintain",
	Short: "Compact packs with dead space, checkpoint, and delete what nothing refers to any more.",
}, {
	Name:  "flush",
	Short: "Make everything accepted so far durable on the drive.",
}, {
	Name:  "stats",
	Short: "Calls, bytes and peak in-flight counts seen by the drive since the volume was opened.",
}}

// Command runs the backend commands.
func (f *Fs) Command(ctx context.Context, name string, arg []string, opt map[string]string) (any, error) {
	switch name {
	case "fsck":
		rep, err := f.vol.Fsck(ctx, core.FsckOpts{Deep: opt["deep"] == "true", Repair: opt["repair"] == "true"})
		if err != nil {
			return nil, err
		}
		probs := make([]string, 0, len(rep.Problems))
		for _, p := range rep.Problems {
			probs = append(probs, p.String())
		}
		return map[string]any{"ok": rep.OK(), "packs": rep.Packs, "members": rep.Members, "segments": rep.Segments,
			"repaired": rep.Repaired, "problems": probs}, nil
	case "checkpoint":
		return map[string]any{}, f.vol.Checkpoint(ctx)
	case "flush":
		return map[string]any{}, f.vol.Flush(ctx)
	case "maintain":
		c, g, err := f.vol.Maintain(ctx)
		return map[string]any{"compacted_packs": c.PacksCompacted, "members_moved": c.MembersMoved,
			"packs_deleted": g.PacksDeleted, "orphans_deleted": g.OrphansDeleted,
			"segments_deleted": g.SegmentsDeleted, "checkpoints_deleted": g.CheckpointsDeleted, "dead_packs": g.DeadPacks}, err
	case "stats":
		if f.ref.meter == nil {
			return map[string]any{}, nil
		}
		s := f.ref.meter.Snapshot()
		return map[string]any{"calls": s.Calls, "total": s.Total, "bytes_read": s.BytesRead, "bytes_written": s.BytesWritten,
			"peak_meta": s.PeakMeta, "peak_transfer": s.PeakTransfer, "peak_read": s.PeakRead}, nil
	}
	return nil, fs.ErrorCommandNotFound
}

var _ fs.Commander = (*Fs)(nil)
