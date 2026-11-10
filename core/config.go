package core

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// Errors returned by Volume. The store sentinels (store.ErrNotFound, ...) are
// reused where they fit.
var (
	ErrReadOnly = errors.New("cfs: another writer is writing to the volume (it holds the lease)")
	ErrFenced   = errors.New("cfs: this writer was fenced off by a newer epoch")
	ErrReserved = errors.New("cfs: name is reserved")
	ErrClosed   = errors.New("cfs: volume closed")
	ErrCorrupt  = errors.New("cfs: data does not match its recorded checksum")
)

// CtlDir is the control directory at the volume root: lease, packs, log and
// checkpoints live below it and nothing else may.
const CtlDir = ".cfs"

// DefaultStoreSuffixes are the extensions whose content is already compressed;
// their pack members are stored, everything else is deflated.
var DefaultStoreSuffixes = []string{
	".pt", ".pth", ".ckpt", ".gz", ".tgz", ".zip", ".npz", ".npy", ".arrow",
	".safetensors", ".bin", ".so", ".cubin", ".png", ".jpg", ".jpeg", ".webp",
	".mp4", ".mkv", ".zst", ".xz", ".bz2", ".7z", ".parquet", ".h5", ".pkl",
	".pickle", ".whl",
}

// AckMode says when Put of a small file returns.
type AckMode string

const (
	// AckStaged (the default): the data is fsynced to a local staging area and
	// recorded in the local index, then Put returns. Packing and commit to the
	// drive happen in the background, so a pack holds whatever arrived in the
	// seal window however few callers there are. The data is readable and
	// listed from the staging area meanwhile, and replayed after a restart until
	// it is on the drive. Only losing the local disk before the upload loses
	// acknowledged data.
	AckStaged AckMode = "staged"
	// AckDurable: Put returns when the pack holding the file and the log
	// segment describing it are on the drive. Files per pack are then about the
	// number of concurrent callers.
	AckDurable AckMode = "durable"
)

// Config configures a Volume. Zero values mean the defaults noted.
type Config struct {
	// Ack: when Put of a small file returns. Default AckStaged.
	Ack AckMode
	// StageMaxBytes caps the data waiting in the staging area (AckStaged): Put
	// blocks, it does not fail, while the area is full. Default 1 GiB.
	StageMaxBytes int64
	// LeaseIdle: a writer that has made no change for this long, with nothing
	// left to commit, hands the lease back (a released lease on the drive) and
	// goes back to being a reader, so that another writer can take over without
	// waiting for the TTL; the next change takes the lease again. Default 10
	// minutes; negative keeps the lease until Close.
	LeaseIdle time.Duration
	// StateDir holds the local SQLite index and spool. Required. Losing it is
	// safe: it is rebuilt from the drive.
	StateDir string
	// Threshold: files smaller than this go into packs, others are stored 1:1.
	// Default 8 MiB.
	Threshold int64
	// PackMaxBytes / PackMaxFiles seal a pack when it is full. Default 128 MiB
	// and 10000 members.
	PackMaxBytes int64
	PackMaxFiles int
	// SealQuiet seals the open pack when no small file arrived for this long;
	// SealMax seals it this long after its first file. Defaults: 2s and 15s with
	// AckDurable, where callers wait for the commit. With AckStaged nobody
	// waits, so a short quiet period would only cut small packs out of a slow
	// stream of files: the defaults are 30s (a pause that ends a burst) and 10 minutes
	// (the longest a file waits for its pack; Flush, Close and any operation
	// that touches a staged name seal at once, and so does a full staging area).
	SealQuiet time.Duration
	SealMax   time.Duration
	// ReadBuffer bounds the memory the reads of packed and staged files hold at
	// once (compressed plus decoded bytes, from the start of a read until its
	// reader is closed); a read that would go over waits. Default 300 MiB, the
	// default of JuiceFS'"'"'s buffer-size. A member larger than the whole budget takes
	// all of it.
	ReadBuffer int64
	// Pipeline is how many sealed packs may be in commit at once, besides the
	// one filling. Default 8 (the transfers setting).
	Pipeline int
	// DirCacheTTL: how long a directory listing of the drive is trusted for
	// large files and directories. Default 5 minutes.
	DirCacheTTL time.Duration
	// LeaseTTL: a lease whose heartbeat is older than this is considered dead
	// and may be taken over. The heartbeat runs every LeaseTTL/3. Default 30
	// minutes: every heartbeat is a read and a write on the drive.
	LeaseTTL time.Duration
	// GCGrace: how long an unreferenced pack, old log segment or checkpoint is
	// kept before it may be deleted. Default 24h.
	GCGrace time.Duration
	// CheckpointEvery: upload a checkpoint after this many committed segments.
	// Default 200; negative disables.
	CheckpointEvery int
	// StoreSuffixes: members with these suffixes are not compressed.
	StoreSuffixes []string
	// Shrink keeps a large file stored 1:1 when it is rewritten smaller than
	// Threshold but not smaller than Threshold*Shrink (hysteresis, so a file
	// hovering near the threshold doesn't move back and forth). Default 0.5.
	Shrink float64
	// ReadWindow: reading a packed file fetches the aligned window of this size
	// around it from the pack, so reading neighbours (a directory listing, a
	// tree walk) costs one request per window instead of one per file. Default
	// 512 KiB; negative disables.
	ReadWindow int64
	// MemberCache is the size of the in-memory cache of recently read pack
	// members. Default 64 MiB; negative disables.
	MemberCache int64
	// Logf receives diagnostics (already free of credentials by construction:
	// they only contain volume paths). Optional.
	Logf func(format string, args ...any)
}

func (c *Config) fill() error {
	if c.StateDir == "" {
		return fmt.Errorf("cfs: Config.StateDir is required")
	}
	if c.Threshold <= 0 {
		c.Threshold = 8 << 20
	}
	if c.PackMaxBytes <= 0 {
		c.PackMaxBytes = 128 << 20
	}
	if c.PackMaxFiles <= 0 {
		c.PackMaxFiles = 10000
	}
	switch c.Ack {
	case "":
		c.Ack = AckStaged
	case AckStaged, AckDurable:
	default:
		return fmt.Errorf("cfs: unknown ack mode %q (want %q or %q)", c.Ack, AckStaged, AckDurable)
	}
	if c.StageMaxBytes <= 0 {
		c.StageMaxBytes = 1 << 30
	}
	staged := c.Ack == AckStaged
	if c.SealQuiet <= 0 {
		c.SealQuiet = 2 * time.Second
		if staged {
			c.SealQuiet = 30 * time.Second
		}
	}
	if c.SealMax <= 0 {
		c.SealMax = 15 * time.Second
		if staged {
			c.SealMax = 10 * time.Minute
		}
	}
	if c.ReadBuffer <= 0 {
		c.ReadBuffer = 300 << 20
	}
	if c.Pipeline <= 0 {
		c.Pipeline = 8
	}
	if c.DirCacheTTL <= 0 {
		c.DirCacheTTL = 5 * time.Minute
	}
	if c.LeaseTTL <= 0 {
		c.LeaseTTL = 30 * time.Minute
	}
	if c.LeaseIdle == 0 {
		c.LeaseIdle = 10 * time.Minute
	}
	if c.GCGrace <= 0 {
		c.GCGrace = 24 * time.Hour
	}
	if c.CheckpointEvery == 0 {
		c.CheckpointEvery = 200
	}
	if c.StoreSuffixes == nil {
		c.StoreSuffixes = DefaultStoreSuffixes
	}
	if c.Shrink <= 0 || c.Shrink > 1 {
		c.Shrink = 0.5
	}
	if c.ReadWindow == 0 {
		c.ReadWindow = 512 << 10
	}
	if c.MemberCache == 0 {
		c.MemberCache = 64 << 20
	}
	if c.Logf == nil {
		c.Logf = func(string, ...any) {}
	}
	return nil
}

func (c *Config) stores(name string) bool {
	name = strings.ToLower(name)
	for _, s := range c.StoreSuffixes {
		if strings.HasSuffix(name, s) {
			return true
		}
	}
	return false
}
