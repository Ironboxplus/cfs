package testutil

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

var (
	buildOnce sync.Once
	buildPath string
	buildErr  error
)

func moduleRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		p := filepath.Dir(dir)
		if p == dir {
			return "", fmt.Errorf("go.mod not found")
		}
		dir = p
	}
}

// CfsBinary builds cmd/cfs once per test process and returns its path.
func CfsBinary(t testing.TB) string {
	t.Helper()
	buildOnce.Do(func() {
		root, err := moduleRoot()
		if err != nil {
			buildErr = err
			return
		}
		dir, err := os.MkdirTemp("", "cfs-bin-")
		if err != nil {
			buildErr = err
			return
		}
		buildPath = filepath.Join(dir, "cfs.exe")
		cmd := exec.Command("go", "build", "-o", buildPath, "./cmd/cfs")
		cmd.Dir = root
		if out, err := cmd.CombinedOutput(); err != nil {
			buildErr = fmt.Errorf("go build ./cmd/cfs: %v\n%s", err, out)
		}
	})
	if buildErr != nil {
		t.Fatal(buildErr)
	}
	return buildPath
}

// FreePort returns a TCP port nobody listens on right now.
func FreePort(t testing.TB) int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// WaitPort waits until addr accepts connections.
func WaitPort(t testing.TB, addr string, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			c.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("nothing listening on %s after %v", addr, d)
}

// Proc is a running subprocess.
type Proc struct {
	Cmd  *exec.Cmd
	Addr string
	log  *os.File
	done chan struct{}
}

// Kill terminates the process the hard way (as a crash would).
func (p *Proc) Kill() {
	if p.Cmd.Process != nil {
		p.Cmd.Process.Kill()
	}
	<-p.done
}

// Wait waits for the process to exit and returns its exit code.
func (p *Proc) Wait() int {
	<-p.done
	return p.Cmd.ProcessState.ExitCode()
}

// Exited reports whether the process has exited.
func (p *Proc) Exited() bool {
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}

// LogTail returns the last part of the process output, for failure messages.
func (p *Proc) LogTail() string {
	b, _ := os.ReadFile(p.log.Name())
	if len(b) > 4000 {
		b = b[len(b)-4000:]
	}
	return string(b)
}

// StartCfs starts the cfs binary with the given arguments and environment
// additions. The output goes to a log file.
func StartCfs(t testing.TB, env []string, args ...string) *Proc {
	t.Helper()
	bin := CfsBinary(t)
	cfgFile := filepath.Join(t.TempDir(), "rclone.conf")
	os.WriteFile(cfgFile, nil, 0o600)
	log, err := os.CreateTemp("", "cfs-proc-*.log")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(bin, append([]string{"--config", cfgFile}, args...)...)
	cmd.Env = append(os.Environ(), env...)
	cmd.Stdout, cmd.Stderr = log, log
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p := &Proc{Cmd: cmd, log: log, done: make(chan struct{})}
	go func() { cmd.Wait(); close(p.done) }()
	t.Cleanup(func() {
		if !p.Exited() {
			cmd.Process.Kill()
			<-p.done
		}
		log.Close()
		os.Remove(log.Name())
	})
	return p
}

// WebDAVServer serves dir over real HTTP with "cfs serve webdav" (rclone's
// WebDAV server) in a subprocess and returns its base URL.
func WebDAVServer(t testing.TB, dir string) (string, *Proc) {
	t.Helper()
	port := FreePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	p := StartCfs(t, nil, "serve", "webdav", dir, "--addr", addr, "--vfs-cache-mode", "off", "--dir-cache-time", "1ms", "--read-only=false")
	p.Addr = addr
	WaitPort(t, addr, 20*time.Second)
	return "http://" + addr, p
}

// WebDAVEnv returns environment variables defining an rclone remote called
// name of type webdav that points at url, with rclone's own low-level retries
// off so that failures reach cfs's retry logic.
func WebDAVEnv(name, url string) []string {
	n := strings.ToUpper(name)
	return []string{
		"RCLONE_CONFIG_" + n + "_TYPE=webdav",
		"RCLONE_CONFIG_" + n + "_URL=" + url,
		"RCLONE_CONFIG_" + n + "_VENDOR=other",
		"RCLONE_LOW_LEVEL_RETRIES=1",
	}
}

// ---- TCP fault proxy ----

// FaultKind is what a Fault does to a matching request.
type FaultKind int

const (
	// Status answers with an HTTP status without contacting the server.
	Status FaultKind = iota
	// DropBefore closes the connection without contacting the server.
	DropBefore
	// DropAfter lets the server process the request and then closes the
	// connection without delivering the answer: the operation happened but
	// the client sees an error.
	DropAfter
	// Delay holds the request before forwarding it.
	Delay
)

// Fault is one rule of the proxy.
type Fault struct {
	Match func(*http.Request) bool // nil matches everything
	Kind  FaultKind
	Code  int           // for Status
	Wait  time.Duration // for Delay
	Count int           // how many requests it applies to; <0 forever
	hits  int
}

// FaultProxy is a real TCP listener in front of a real HTTP server that
// injects failures into the requests passing through.
type FaultProxy struct {
	URL string

	mu     sync.Mutex
	faults []*Fault
	srv    *http.Server
	rp     *httputil.ReverseProxy
	client *http.Client
	target *url.URL
	// Requests counts the requests that arrived, by method.
	Requests map[string]int
}

// NewFaultProxy starts a proxy to target (e.g. "http://127.0.0.1:8080").
func NewFaultProxy(t testing.TB, target string) *FaultProxy {
	t.Helper()
	u, err := url.Parse(target)
	if err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &FaultProxy{target: u, URL: "http://" + l.Addr().String(), Requests: map[string]int{},
		rp: httputil.NewSingleHostReverseProxy(u), client: &http.Client{Transport: &http.Transport{DisableCompression: true}}}
	p.rp.FlushInterval = -1
	p.srv = &http.Server{Handler: http.HandlerFunc(p.handle)}
	go p.srv.Serve(l)
	t.Cleanup(func() { p.srv.Close() })
	return p
}

// Add appends a rule.
func (p *FaultProxy) Add(f Fault) {
	p.mu.Lock()
	p.faults = append(p.faults, &f)
	p.mu.Unlock()
}

// Clear removes all rules.
func (p *FaultProxy) Clear() {
	p.mu.Lock()
	p.faults = nil
	p.mu.Unlock()
}

// Count returns how many requests of the given method have arrived.
func (p *FaultProxy) Count(method string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.Requests[method]
}

func (p *FaultProxy) pick(r *http.Request) *Fault {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.Requests[r.Method]++
	for _, f := range p.faults {
		if f.Count == 0 || (f.Match != nil && !f.Match(r)) {
			continue
		}
		if f.Count > 0 {
			f.Count--
		}
		f.hits++
		return f
	}
	return nil
}

func drop(w http.ResponseWriter) {
	if h, ok := w.(http.Hijacker); ok {
		if c, _, err := h.Hijack(); err == nil {
			// RST rather than FIN, as a dying server would do.
			if tc, ok := c.(*net.TCPConn); ok {
				tc.SetLinger(0)
			}
			c.Close()
		}
	}
}

func (p *FaultProxy) handle(w http.ResponseWriter, r *http.Request) {
	f := p.pick(r)
	if f != nil {
		switch f.Kind {
		case Status:
			io.Copy(io.Discard, r.Body)
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(f.Code)
			return
		case DropBefore:
			drop(w)
			return
		case Delay:
			time.Sleep(f.Wait)
		case DropAfter:
			out := r.Clone(context.Background())
			out.URL.Scheme, out.URL.Host = p.target.Scheme, p.target.Host
			out.Host = p.target.Host
			out.RequestURI = ""
			if resp, derr := p.client.Do(out); derr == nil {
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}
			drop(w)
			return
		}
	}
	p.rp.ServeHTTP(w, r)
}

var _ = bufio.NewReader
