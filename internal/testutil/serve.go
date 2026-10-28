package testutil

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Ironboxplus/cfs/aspect"
)

// ServeOpts describe a "cfs serve webdav" process over a cfs volume.
type ServeOpts struct {
	// Underlying is the rclone remote string the volume lives on: a directory,
	// or a connection string for WebDAV.
	Underlying string
	StateDir   string
	CacheDir   string // rclone VFS cache; keep it across restarts to test replay
	Threshold  string // default 64Ki: a few hundred KiB are "big", as rclone's chunked-upload tests shrink their chunks
	SealQuiet  string // default 100ms
	SealMax    string // default 1s
	Transfers  int    // cfs transfers; default 8
	Meta       int    // cfs meta_inflight; default 1
	WriteBack  string // VFS write-back delay; default 200ms
	Crashpoint string
	Env        []string
	Args       []string
}

// Server is a running "cfs serve webdav".
type Server struct {
	*Proc
	DAV       *DAV
	RC        string
	StatsFile string
	Opts      ServeOpts
}

// StartServe starts the server and waits for it to listen.
func StartServe(t testing.TB, o ServeOpts) *Server {
	t.Helper()
	if o.Threshold == "" {
		o.Threshold = "64Ki"
	}
	if o.SealQuiet == "" {
		o.SealQuiet = "100ms"
	}
	if o.SealMax == "" {
		o.SealMax = "1s"
	}
	if o.WriteBack == "" {
		o.WriteBack = "200ms"
	}
	if o.Transfers == 0 {
		o.Transfers = 8
	}
	if o.Meta == 0 {
		o.Meta = 1
	}
	stats := fmt.Sprintf("%s/stats-%d.json", t.TempDir(), time.Now().UnixNano())
	port, rcport := FreePort(t), FreePort(t)
	addr, rc := fmt.Sprintf("127.0.0.1:%d", port), fmt.Sprintf("127.0.0.1:%d", rcport)
	env := []string{
		"RCLONE_CONFIG_BBCFS_TYPE=cfs",
		"RCLONE_CONFIG_BBCFS_REMOTE=" + o.Underlying,
		"RCLONE_CONFIG_BBCFS_STATE_DIR=" + o.StateDir,
		"RCLONE_CONFIG_BBCFS_THRESHOLD=" + o.Threshold,
		"RCLONE_CONFIG_BBCFS_SEAL_QUIET=" + o.SealQuiet,
		"RCLONE_CONFIG_BBCFS_SEAL_MAX=" + o.SealMax,
		fmt.Sprintf("RCLONE_CONFIG_BBCFS_TRANSFERS=%d", o.Transfers),
		fmt.Sprintf("RCLONE_CONFIG_BBCFS_META_INFLIGHT=%d", o.Meta),
		"RCLONE_CONFIG_BBCFS_LEASE_TTL=20s",
		"RCLONE_CONFIG_BBCFS_DIR_CACHE=1h",
		"RCLONE_CONFIG_BBCFS_RETRIES=4",
		"RCLONE_LOW_LEVEL_RETRIES=1",
		"CFS_STATS_FILE=" + stats,
	}
	if o.Crashpoint != "" {
		env = append(env, "CFS_CRASHPOINT="+o.Crashpoint)
	}
	env = append(env, o.Env...)
	args := []string{"serve", "webdav", "BBCFS:", "--addr", addr,
		"--vfs-cache-mode", "full", "--vfs-write-back", o.WriteBack, "--cache-dir", o.CacheDir,
		"--transfers", "32", "--vfs-cache-max-age", "24h",
		"--rc", "--rc-addr", rc, "--rc-no-auth"}
	args = append(args, o.Args...)
	p := StartCfs(t, env, args...)
	p.Addr = addr
	s := &Server{Proc: p, DAV: NewDAV("http://" + addr), RC: "http://" + rc, StatsFile: stats, Opts: o}
	WaitPort(t, addr, 30*time.Second)
	return s
}

func (s *Server) rc(call string, in any) (map[string]any, error) {
	b, _ := json.Marshal(in)
	resp, err := http.Post(s.RC+"/"+call, "application/json", bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var out map[string]any
	json.NewDecoder(resp.Body).Decode(&out)
	if resp.StatusCode != 200 {
		return out, fmt.Errorf("rc %s: %d %v", call, resp.StatusCode, out["error"])
	}
	return out, nil
}

// WaitDrained waits until rclone's VFS has nothing left to upload.
func (s *Server) WaitDrained(t testing.TB, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	quiet := 0
	for time.Now().Before(deadline) {
		if s.Exited() {
			t.Fatalf("server exited while waiting for the upload queue:\n%s", s.LogTail())
		}
		out, err := s.rc("vfs/queue", map[string]any{})
		if err == nil {
			q, _ := out["queue"].([]any)
			if len(q) == 0 {
				quiet++
				if quiet >= 4 {
					return
				}
			} else {
				quiet = 0
			}
		}
		time.Sleep(150 * time.Millisecond)
	}
	t.Fatalf("upload queue not empty after %v", d)
}

// Quit stops the server gracefully (exit handlers run: volumes are flushed and
// closed, the lease is released).
func (s *Server) Quit(t testing.TB) {
	t.Helper()
	s.rc("core/quit", map[string]any{})
	select {
	case <-s.done:
	case <-time.After(2 * time.Minute):
		t.Fatalf("server did not quit:\n%s", s.LogTail())
	}
}

// Stats reads the meter of the volume behind the server.
func (s *Server) Stats() aspect.Stats {
	b, err := os.ReadFile(s.StatsFile)
	if err != nil {
		return aspect.Stats{}
	}
	var st aspect.Stats
	json.Unmarshal(b, &st)
	return st
}

// StatsString is Stats formatted for a test log.
func (s *Server) StatsString() string {
	st := s.Stats()
	return strings.TrimSpace(st.String())
}

// Cmd runs a cfs backend command (fsck, checkpoint, maintain, flush, stats) in
// the server process through rclone's rc, so it sees the one live volume.
func (s *Server) Cmd(t testing.TB, name string, opt map[string]string) map[string]any {
	t.Helper()
	in := map[string]any{"command": name, "fs": "BBCFS:", "opt": opt}
	out, err := s.rc("backend/command", in)
	if err != nil {
		t.Fatalf("backend command %s: %v", name, err)
	}
	res, _ := out["result"].(map[string]any)
	return res
}
