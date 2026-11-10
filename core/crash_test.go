package core_test

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Ironboxplus/cfs/core"
	"github.com/Ironboxplus/cfs/internal/testutil"
	"github.com/Ironboxplus/cfs/store"
)

var (
	cwOnce sync.Once
	cwBin  string
	cwErr  error
)

func crashwriterBin(t *testing.T) string {
	cwOnce.Do(func() {
		dir, err := os.MkdirTemp("", "cfs-cw-")
		if err != nil {
			cwErr = err
			return
		}
		cwBin = filepath.Join(dir, "crashwriter.exe")
		cmd := exec.Command("go", "build", "-o", cwBin, "../internal/cmd/crashwriter")
		if out, err := cmd.CombinedOutput(); err != nil {
			cwErr = fmt.Errorf("%v\n%s", err, out)
		}
	})
	if cwErr != nil {
		t.Fatal(cwErr)
	}
	return cwBin
}

type cwEvent struct {
	W    int    `json:"w"`
	Ev   string `json:"ev"`
	Op   string `json:"op"`
	Path string `json:"p"`
	To   string `json:"to"`
	SHA1 string `json:"sha1"`
	Size int    `json:"sz"`
}

func readEvents(t *testing.T, file string) []cwEvent {
	f, err := os.Open(file)
	if os.IsNotExist(err) {
		return nil // killed before the writer logged anything: nothing was acknowledged
	}
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var out []cwEvent
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		var e cwEvent
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			continue // a torn last line from a kill
		}
		out = append(out, e)
	}
	return out
}

// runWriter runs the workload process and returns its exit code.
func runWriter(t *testing.T, args []string, crashpoint string, killAfter time.Duration) int {
	cmd := exec.Command(crashwriterBin(t), args...)
	cmd.Env = os.Environ()
	if crashpoint != "" {
		cmd.Env = append(cmd.Env, "CFS_CRASHPOINT="+crashpoint)
	}
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	if killAfter > 0 {
		select {
		case <-done:
		case <-time.After(killAfter):
			cmd.Process.Kill()
			<-done
		}
	} else {
		select {
		case <-done:
		case <-time.After(3 * time.Minute):
			cmd.Process.Kill()
			t.Fatalf("writer hangs:\n%s", out.String())
		}
	}
	code := cmd.ProcessState.ExitCode()
	if code != 0 && code != 137 && code != 1 {
		t.Logf("writer output:\n%s", out.String())
	}
	return code
}

// expectation is what the events allow a path to hold.
type state struct {
	present bool
	sha1    string
	size    int
}

type model struct {
	events  []cwEvent
	acked   map[string]state // after the last acknowledged operation
	touched map[string]bool
	lastPut map[string]bool // the last acked operation on the path was a put
	// unresolved are operations whose outcome the process never reported (it
	// was killed): they may or may not have taken effect, until a later
	// acknowledged operation on the same path settles it.
	unresolved []unresolvedOp
	ackIdx     map[string]int
}

type unresolvedOp struct {
	idx   int
	ev    cwEvent
	srcSt state // for a move: what the source held when it was tried
}

func buildModel(events []cwEvent) *model {
	m := &model{events: events, acked: map[string]state{}, touched: map[string]bool{}, lastPut: map[string]bool{}, ackIdx: map[string]int{}}
	pending := map[int]*unresolvedOp{}
	for i := range events {
		e := events[i]
		switch e.Ev {
		case "try":
			if old := pending[e.W]; old != nil {
				m.unresolved = append(m.unresolved, *old) // its process died
			}
			pending[e.W] = &unresolvedOp{idx: i, ev: e, srcSt: m.acked[e.Path]}
			m.touched[e.Path] = true
			if e.To != "" {
				m.touched[e.To] = true
			}
		case "err":
			// The operation failed: it may have partly happened.
			if old := pending[e.W]; old != nil {
				m.unresolved = append(m.unresolved, *old)
				delete(pending, e.W)
			}
		case "ack":
			delete(pending, e.W)
			switch e.Op {
			case "put":
				m.acked[e.Path] = state{true, e.SHA1, e.Size}
				m.lastPut[e.Path] = true
				m.ackIdx[e.Path] = i
			case "rm":
				m.acked[e.Path] = state{}
				m.lastPut[e.Path] = false
				m.ackIdx[e.Path] = i
			case "mv":
				m.acked[e.To] = m.acked[e.Path]
				m.acked[e.Path] = state{}
				m.lastPut[e.To], m.lastPut[e.Path] = false, false
				m.ackIdx[e.To], m.ackIdx[e.Path] = i, i
			}
		}
	}
	for _, p := range pending {
		m.unresolved = append(m.unresolved, *p)
	}
	return m
}

// allowed lists the states a path may be in.
func (m *model) allowed(p string) []state {
	out := []state{m.acked[p]}
	for _, u := range m.unresolved {
		e := u.ev
		if u.idx < m.ackIdx[p] {
			continue
		}
		switch {
		case e.Op == "put" && e.Path == p:
			out = append(out, state{true, e.SHA1, e.Size})
		case e.Op == "rm" && e.Path == p:
			out = append(out, state{})
		case e.Op == "mv" && e.Path == p:
			out = append(out, state{})
		case e.Op == "mv" && e.To == p:
			out = append(out, u.srcSt)
		}
	}
	return out
}

func readAllVol(v *core.Volume, p string) ([]byte, error) {
	rc, err := v.Open(context.Background(), p, 0, -1)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(rc)
}

func walkVolume(t *testing.T, v *core.Volume, dir string, out map[string]core.Entry) {
	ents, err := v.List(context.Background(), dir)
	if err != nil {
		t.Fatalf("list %q: %v", dir, err)
	}
	for _, e := range ents {
		if e.IsDir {
			walkVolume(t, v, e.Path, out)
		} else {
			out[e.Path] = e
		}
	}
}

// verify checks the volume against the model. strict: every path must be in an
// allowed state; otherwise (local state lost) only acknowledged puts count.
func (m *model) verify(t *testing.T, v *core.Volume, strict bool) {
	t.Helper()
	have := map[string]core.Entry{}
	walkVolume(t, v, "", have)
	bad := 0
	fail := func(format string, args ...any) {
		bad++
		if bad < 15 {
			t.Errorf(format, args...)
		}
	}
	for p := range have {
		if !m.touched[p] {
			fail("unexpected file %s", p)
		}
	}
	for p := range m.touched {
		e, present := have[p]
		var got state
		if present {
			b, err := readAllVol(v, p)
			if err != nil {
				fail("read %s: %v", p, err)
				continue
			}
			got = state{true, sum(b), len(b)}
			if int64(len(b)) != e.Size {
				fail("%s: listing says %d bytes, content has %d", p, e.Size, len(b))
			}
		}
		if !strict {
			if m.lastPut[p] {
				want := m.acked[p]
				ok := got == want
				for _, a := range m.allowed(p) {
					if got == a {
						ok = true
					}
				}
				if !ok {
					fail("%s: acknowledged put lost or wrong (have %+v, want %+v)", p, got, want)
				}
			}
			continue
		}
		ok := false
		for _, a := range m.allowed(p) {
			if got == a {
				ok = true
				break
			}
		}
		if !ok && m.partialUpload(p, got) {
			t.Logf("%s: an unacknowledged large upload left a prefix of %d bytes (rclone's local backend writes the final name in place)", p, got.size)
			ok = true
		}
		if !ok {
			fail("%s: state %+v is not any of the allowed %+v", p, got, m.allowed(p))
			for i, e := range m.events {
				if e.Path == p || e.To == p {
					t.Logf("  event %d: %+v", i, e)
				}
			}
		}
	}
	if bad > 0 {
		t.Fatalf("%d violations", bad)
	}
}

func openVol(t *testing.T, drive, state string, tweak func(*core.Config)) (*core.Volume, func()) {
	t.Helper()
	base := testutil.LocalStore(t, drive)
	st, _ := testutil.Stacked(base, aspectOpts())
	cfg := testutil.FastConfig(state)
	cfg.Threshold = 256 << 10
	cfg.LeaseTTL = 2 * time.Second
	// The volume's own diagnostics, for as long as the test runs (a background
	// goroutine may still log after it ended).
	var ended atomic.Bool
	t.Cleanup(func() { ended.Store(true) })
	cfg.Logf = func(format string, args ...any) {
		if !ended.Load() {
			t.Logf(format, args...)
		}
	}
	if tweak != nil {
		tweak(&cfg)
	}
	v, err := core.Open(context.Background(), st, cfg)
	if err != nil {
		t.Fatalf("reopen after crash: %v", err)
	}
	return v, func() { v.Close(context.Background()) }
}

// dumpLog prints the records of the drive's log segments that mention path, in
// replay order: what a failed check needs to be understood.
func dumpLog(t *testing.T, drive, path string) {
	segs, _ := filepath.Glob(filepath.Join(drive, ".cfs", "log", "*.jsonl.gz"))
	sort.Strings(segs)
	for _, s := range segs {
		f, err := os.Open(s)
		if err != nil {
			continue
		}
		zr, err := gzip.NewReader(f)
		if err != nil {
			t.Logf("  log %s: %v", filepath.Base(s), err)
			f.Close()
			continue
		}
		b, _ := io.ReadAll(zr)
		f.Close()
		for _, line := range strings.Split(string(b), "\n") {
			if strings.Contains(line, `"`+path+`"`) {
				t.Logf("  log %s: %s", filepath.Base(s), line)
			}
		}
	}
}

var crashDrive string // the drive of the crash test being run, for dumpLog

func fsckClean(t *testing.T, v *core.Volume, allowOrphans bool, events ...cwEvent) {
	t.Helper()
	rep, err := v.Fsck(context.Background(), core.FsckOpts{Deep: true})
	if err != nil {
		t.Fatal(err)
	}
	var bad []string
	for _, p := range rep.Problems {
		if allowOrphans && p.Kind == "orphan_pack" {
			continue
		}
		bad = append(bad, p.String())
		if crashDrive != "" && p.Path != "" {
			dumpLog(t, crashDrive, p.Path)
		}
		for i, e := range events { // what the writer did to the path
			if p.Path != "" && (e.Path == p.Path || e.To == p.Path) {
				t.Logf("  %s: event %d: %+v", p.Path, i, e)
			}
		}
	}
	if len(bad) > 0 {
		t.Fatalf("fsck: %s", strings.Join(bad, "; "))
	}
}

// modes are the two acknowledgement semantics every crash test runs under:
// durable (acknowledged = on the drive) and staged (acknowledged = fsynced in
// the local staging area, and from there it must get to the drive after a
// restart).
var ackModes = []core.AckMode{core.AckStaged, core.AckDurable}

func ackTweak(mode core.AckMode) func(*core.Config) {
	return func(c *core.Config) { c.Ack = mode }
}

// TestCrashPoints kills the writer at every named step of the commit protocol
// and checks what was acknowledged is intact.
func TestCrashPoints(t *testing.T) {
	for _, mode := range ackModes {
		t.Run(string(mode), func(t *testing.T) {
			// Every step of a commit once; CFS_LONG=1 adds the early and late hits.
			points := []string{"pack_uploaded:4", "pack_verified:2", "log_uploaded:1", "local_marked:6", "meta_fsynced:20", "meta_log_uploaded:1"}
			if os.Getenv("CFS_LONG") != "" {
				points = append(points, "pack_uploaded:1", "log_uploaded:5", "local_marked:1", "meta_fsynced:1")
			}
			if mode == core.AckStaged {
				points = append(points, "staged:90")
				if os.Getenv("CFS_LONG") != "" {
					points = append(points, "staged:1")
				}
			}
			for _, cp := range points {
				t.Run(cp, func(t *testing.T) { crashPoint(t, mode, cp) })
			}
		})
	}
}

func crashPoint(t *testing.T, mode core.AckMode, cp string) {
	base := t.TempDir()
	drive, state, events := filepath.Join(base, "drive"), filepath.Join(base, "state"), filepath.Join(base, "events")
	crashDrive = drive
	os.MkdirAll(drive, 0o755)
	common := []string{"-remote", drive, "-state", state, "-events", events, "-workers", "4", "-ack", string(mode)}
	if mode == core.AckStaged {
		// Staged puts return at once: small packs make the writer reach the
		// commit steps often enough.
		common = append(common, "-packfiles", "6")
	}
	// Whether the first metadata-only segment is ever uploaded depends on timing
	// (a pack's segment may carry the queued records first): another seed gets
	// there if this one doesn't.
	code := 0
	for attempt := 0; attempt < 4; attempt++ {
		if attempt > 0 {
			os.RemoveAll(drive)
			os.RemoveAll(state)
			os.RemoveAll(events)
			os.MkdirAll(drive, 0o755)
		}
		args := append(append([]string{}, common...), "-ops", "60", "-seed", strconv.Itoa(7+10*attempt), "-pause", "2ms")
		if code = runWriter(t, args, cp, 0); code == 137 || !strings.HasPrefix(cp, "meta_log_uploaded") {
			break
		}
	}
	if code != 137 {
		t.Fatalf("expected the writer to die at %s (exit 137), got exit %d", cp, code)
	}
	ev := readEvents(t, events)
	acks := 0
	for _, e := range ev {
		if e.Ev == "ack" {
			acks++
		}
	}
	t.Logf("writer died at %s after %d events (%d acked)", cp, len(ev), acks)
	m := buildModel(ev)

	// 1. Restart with the local state intact.
	v, closeV := openVol(t, drive, state, ackTweak(mode))
	m.verify(t, v, true)
	fsckClean(t, v, true, ev...)
	// A second run continues on top of the recovered volume without crashing.
	closeV()
	args2 := append(append([]string{}, common...), "-ops", "40", "-seed", "8")
	if code := runWriter(t, args2, "", 0); code != 0 {
		t.Fatalf("second run exit %d", code)
	}
	m = buildModel(readEvents(t, events))
	v, closeV = openVol(t, drive, state, ackTweak(mode))
	m.verify(t, v, true)
	fsckClean(t, v, true)
	closeV()

	// 2. Lose all local state: acknowledged puts must survive a rebuild. (In
	// staged mode the restart above has replayed the staging area and the
	// close flushed it: acknowledged = staged means it gets here eventually.)
	removeAll(t, state)
	v, closeV = openVol(t, drive, filepath.Join(base, "state2"), ackTweak(mode))
	defer closeV()
	m.verify(t, v, false)
}

// TestStagedReplayReachesTheDrive is the staged guarantee on its own: kill the
// writer, open the volume again and do nothing but wait; every acknowledged
// write ends up on the drive with the right content, and a rebuild from the
// drive alone has it.
func TestStagedReplayReachesTheDrive(t *testing.T) {
	for _, cp := range []string{"pack_uploaded:2", "log_uploaded:2", "local_marked:2", "staged:70"} {
		t.Run(cp, func(t *testing.T) {
			base := t.TempDir()
			drive, state, events := filepath.Join(base, "drive"), filepath.Join(base, "state"), filepath.Join(base, "events")
			os.MkdirAll(drive, 0o755)
			args := []string{"-remote", drive, "-state", state, "-events", events, "-workers", "4", "-ack", "staged",
				"-packfiles", "6", "-ops", "60", "-seed", "21", "-pause", "2ms"}
			if code := runWriter(t, args, cp, 0); code != 137 {
				t.Fatalf("expected the writer to die at %s, got exit %d", cp, code)
			}
			m := buildModel(readEvents(t, events))
			v, closeV := openVol(t, drive, state, ackTweak(core.AckStaged))
			// No Flush here: the replay alone must get there.
			deadline := time.Now().Add(30 * time.Second)
			for {
				if n, _ := v.Staged(); n == 0 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("%d files still staged after 30s", func() int { n, _ := v.Staged(); return n }())
				}
				time.Sleep(50 * time.Millisecond)
			}
			closeV()
			removeAll(t, state)
			v, closeV = openVol(t, drive, filepath.Join(base, "state2"), ackTweak(core.AckStaged))
			defer closeV()
			m.verify(t, v, false)
		})
	}
}

// TestRandomKills kills the writer at random moments, restarts it on the same
// volume, and repeats, checking after every round.
func TestRandomKills(t *testing.T) {
	for _, mode := range ackModes {
		t.Run(string(mode), func(t *testing.T) { randomKills(t, mode) })
	}
}

func randomKills(t *testing.T, mode core.AckMode) {
	rounds := 3
	if os.Getenv("CFS_LONG") != "" && !testing.Short() {
		rounds = 8
	}
	base := t.TempDir()
	drive, state, events := filepath.Join(base, "drive"), filepath.Join(base, "state"), filepath.Join(base, "events")
	os.MkdirAll(drive, 0o755)
	crashDrive = drive
	rng := rand.New(rand.NewPCG(99, 1))
	for r := 0; r < rounds; r++ {
		args := []string{"-remote", drive, "-state", state, "-events", events, "-workers", "4", "-ops", "200",
			"-seed", fmt.Sprint(100 + r), "-pause", "1ms", "-ckpt", "5", "-ack", string(mode)}
		kill := time.Duration(600+rng.IntN(2500)) * time.Millisecond
		code := runWriter(t, args, "", kill)
		ev := readEvents(t, events)
		t.Logf("round %d: killed after %v (exit %d), %d events", r, kill, code, len(ev))
		m := buildModel(ev)
		logSegments(t, drive)
		v, closeV := openVol(t, drive, state, ackTweak(mode))
		m.verify(t, v, true)
		fsckClean(t, v, true, ev...)
		closeV()
	}
	// Final: rebuild from the drive alone.
	m := buildModel(readEvents(t, events))
	removeAll(t, state)
	v, closeV := openVol(t, drive, filepath.Join(base, "state2"), ackTweak(mode))
	defer closeV()
	m.verify(t, v, false)
}

var _ = sort.Strings
var _ store.Entry

// logSegments lists the drive's log segments with their sizes: a failed check
// after a kill needs to know what the kill left behind.
func logSegments(t *testing.T, drive string) {
	segs, _ := filepath.Glob(filepath.Join(drive, ".cfs", "log", "*"))
	sort.Strings(segs)
	var b strings.Builder
	for _, s := range segs {
		if fi, err := os.Stat(s); err == nil {
			fmt.Fprintf(&b, " %s=%d", filepath.Base(s), fi.Size())
		}
	}
	t.Logf("log segments after the kill:%s", b.String())
}

// partialUpload: the drive below is a local directory, and rclone's local
// backend writes a file at its final name while it uploads it. A large file
// whose upload was cut short by the kill (no answer was ever given) can
// therefore be seen as a prefix of the new content. Real drives (115, WebDAV
// servers) show a file only when it is complete; this is a property of the test
// drive, not of the volume. Only an unresolved put of a file over the threshold,
// onto a name that holds no small file, qualifies, and the prefix must be
// shorter than the new content.
func (m *model) partialUpload(p string, got state) bool {
	if !got.present {
		return false
	}
	for _, u := range m.unresolved {
		if u.idx >= m.ackIdx[p] && u.ev.Op == "put" && u.ev.Path == p && u.ev.Size >= 256<<10 && got.size < u.ev.Size {
			return true
		}
	}
	return false
}
