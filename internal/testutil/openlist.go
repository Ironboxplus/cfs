package testutil

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/Ironboxplus/cfs/aspect"
)

// OpenList is a real OpenList server process (the OP-sync build with the cfs
// driver) with its own data directory, listening on a private port. Tests talk
// to it only over HTTP: the admin API to create storages, WebDAV to use them.
type OpenList struct {
	*Proc
	Bin, Data string
	Port      int
	DAV       *DAV // WebDAV of OpenList itself, rooted at /dav
	StatsFile string
	pass      string
	token     string
	args      []string
}

// OpenListBinary is the OpenList executable the black-box tests run, from the
// environment (CFS_OP_BIN). Tests skip when it is not set.
func OpenListBinary(t testing.TB) string {
	t.Helper()
	b := os.Getenv("CFS_OP_BIN")
	if b == "" {
		t.Skip("CFS_OP_BIN (an OpenList build with the cfs driver) is not set")
	}
	if _, err := os.Stat(b); err != nil {
		t.Fatalf("CFS_OP_BIN: %v", err)
	}
	return b
}

var (
	reHTTPPort = regexp.MustCompile(`"http_port":\s*\d+`)
	reAddress  = regexp.MustCompile(`"address":\s*"[^"]*"`)
)

// NewOpenList prepares data (a fresh OpenList data directory) with a random
// admin password that never leaves the test, and listens only on 127.0.0.1:port.
// extraArgs replace the default "server" command (e.g. "cfs", "serve", ...).
func NewOpenList(t testing.TB, bin, data string, port int) *OpenList {
	t.Helper()
	if port == 5244 {
		t.Fatal("refusing to use port 5244")
	}
	var raw [8]byte
	rand.Read(raw[:])
	o := &OpenList{Bin: bin, Data: data, Port: port, pass: hex.EncodeToString(raw[:])}
	os.MkdirAll(data, 0o755)
	if out, err := exec.Command(bin, "admin", "set", o.pass, "--data", data).CombinedOutput(); err != nil {
		t.Fatalf("openlist admin set: %v\n%s", err, Scrub(string(out)))
	}
	cfg := filepath.Join(data, "config.json")
	b, err := os.ReadFile(cfg)
	if err != nil {
		t.Fatal(err)
	}
	b = reHTTPPort.ReplaceAll(b, []byte(fmt.Sprintf(`"http_port": %d`, port)))
	b = reAddress.ReplaceAll(b, []byte(`"address": "127.0.0.1"`))
	if err := os.WriteFile(cfg, b, 0o600); err != nil {
		t.Fatal(err)
	}
	o.args = []string{"server"}
	o.StatsFile = filepath.Join(t.TempDir(), "stats.json")
	o.DAV = NewDAV(fmt.Sprintf("http://127.0.0.1:%d/dav", port))
	o.DAV.User, o.DAV.Pass = "cfsdav", o.pass
	return o
}

// Scrub removes anything that looks like a secret from text for logs.
func Scrub(s string) string {
	return regexp.MustCompile(`(?i)(password|token)[^\n]*`).ReplaceAllString(s, "$1 ***")
}

// Start runs the server (or, with args, another command that also runs it) and
// waits until the API answers.
func (o *OpenList) Start(t testing.TB, env []string, args ...string) {
	t.Helper()
	if len(args) > 0 {
		o.args = args
	}
	log, err := os.CreateTemp("", "openlist-*.log")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(o.Bin, append(append([]string{}, o.args...), "--data", o.Data)...)
	cmd.Env = append(append(os.Environ(), "CFS_STATS_FILE="+o.StatsFile), env...)
	cmd.Stdout, cmd.Stderr = log, log
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p := &Proc{Cmd: cmd, log: log, done: make(chan struct{}), Addr: fmt.Sprintf("127.0.0.1:%d", o.Port)}
	go func() { cmd.Wait(); close(p.done) }()
	o.Proc = p
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("openlist log tail:\n%s", Scrub(p.LogTail()))
			if b, err := os.ReadFile(filepath.Join(o.Data, "log", "log.log")); err == nil {
				if len(b) > 6000 {
					b = b[len(b)-6000:]
				}
				t.Logf("openlist data/log tail:\n%s", Scrub(string(b)))
			}
		}
		if !p.Exited() {
			cmd.Process.Kill()
			<-p.done
		}
		log.Close()
		os.Remove(log.Name())
	})
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		if p.Exited() {
			t.Fatalf("openlist exited early:\n%s", Scrub(p.LogTail()))
		}
		if err := o.login(); err == nil {
			o.ensureDavUser(t)
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("openlist did not come up:\n%s", Scrub(p.LogTail()))
}

func (o *OpenList) login() error {
	out, err := o.apiRaw("POST", "/api/auth/login", map[string]any{"username": "admin", "password": o.pass}, "")
	if err != nil {
		return err
	}
	d, _ := out["data"].(map[string]any)
	tok, _ := d["token"].(string)
	if tok == "" {
		return fmt.Errorf("no token: %v", out["message"])
	}
	o.token = tok
	return nil
}

func (o *OpenList) apiRaw(method, path string, body any, token string) (map[string]any, error) {
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, fmt.Sprintf("http://127.0.0.1:%d%s", o.Port, path), rd)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", token)
	}
	resp, err := (&http.Client{Timeout: 2 * time.Minute}).Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("%s %s: HTTP %d, not JSON", method, path, resp.StatusCode)
	}
	return out, nil
}

// API calls an OpenList API endpoint as admin and returns its data.
func (o *OpenList) API(t testing.TB, method, path string, body any) map[string]any {
	t.Helper()
	out, err := o.apiRaw(method, path, body, o.token)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	if c, _ := out["code"].(float64); c != 200 {
		t.Fatalf("%s %s: code %v: %v", method, path, out["code"], out["message"])
	}
	d, _ := out["data"].(map[string]any)
	return d
}

// AddStorage creates a storage and waits until its driver initialised.
func (o *OpenList) AddStorage(t testing.TB, mount, driver string, addition map[string]any) {
	t.Helper()
	add, _ := json.Marshal(addition)
	d := o.API(t, "POST", "/api/admin/storage/create", map[string]any{
		"mount_path": mount, "driver": driver, "order": 0, "cache_expiration": 0,
		"addition": string(add), "webdav_policy": "native_proxy",
	})
	id, _ := d["id"].(float64)
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		g := o.API(t, "GET", fmt.Sprintf("/api/admin/storage/get?id=%d", int(id)), nil)
		if st, _ := g["status"].(string); st == "work" {
			return
		} else if st != "" && st != "init" {
			t.Fatalf("storage %s: status %q", mount, st)
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("storage %s did not become ready", mount)
}

// Stats reads the cfs meter of the OpenList process (the volume of its cfs
// storage; written by the process every 300 ms).
func (o *OpenList) Stats() aspect.Stats {
	var st aspect.Stats
	if b, err := os.ReadFile(o.StatsFile); err == nil {
		json.Unmarshal(b, &st)
	}
	return st
}

// StatsString is Stats formatted for a test log.
func (o *OpenList) StatsString() string { return strings.TrimSpace(o.Stats().String()) }

// ensureDavUser makes sure there is a user that may use WebDAV for writing: the
// built-in admin has no WebDAV-manage permission.
func (o *OpenList) ensureDavUser(t testing.TB) {
	t.Helper()
	out, err := o.apiRaw("POST", "/api/admin/user/create", map[string]any{
		"username": "cfsdav", "password": o.pass, "base_path": "/", "role": 0, "permission": 0x73FF,
	}, o.token)
	if err != nil {
		t.Fatalf("create webdav user: %v", err)
	}
	if c, _ := out["code"].(float64); c != 200 {
		// Exists already after a restart: fine.
		if msg, _ := out["message"].(string); !strings.Contains(strings.ToLower(msg), "exist") && !strings.Contains(strings.ToLower(msg), "unique") && !strings.Contains(strings.ToLower(msg), "duplicate") {
			t.Fatalf("create webdav user: %v", out["message"])
		}
	}
}
