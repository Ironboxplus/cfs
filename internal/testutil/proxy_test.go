package testutil_test

import (
	"bytes"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Ironboxplus/cfs/internal/testutil"
)

// The proxy has to do what it says, or the fault tests prove nothing.
func TestFaultProxyDropAfterLetsTheRequestLand(t *testing.T) {
	dir := t.TempDir()
	url, _ := testutil.WebDAVServer(t, dir)
	p := testutil.NewFaultProxy(t, url)
	p.Add(testutil.Fault{Match: func(r *http.Request) bool { return r.Method == "PUT" }, Kind: testutil.DropAfter, Count: 1})
	body := bytes.Repeat([]byte("z"), 5000)
	req, _ := http.NewRequest("PUT", p.URL+"/landed", bytes.NewReader(body))
	if resp, err := http.DefaultClient.Do(req); err == nil {
		resp.Body.Close()
		t.Fatalf("the client must see a dropped connection, got %s", resp.Status)
	}
	var got []byte
	for i := 0; i < 20 && len(got) != len(body); i++ {
		time.Sleep(50 * time.Millisecond)
		got, _ = os.ReadFile(filepath.Join(dir, "landed"))
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("the request did not land on the server (%d bytes there)", len(got))
	}
	// And the next request is served normally.
	req, _ = http.NewRequest("PUT", p.URL+"/second", bytes.NewReader(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode >= 300 {
		t.Fatalf("second put: %s", resp.Status)
	}
}

func TestFaultProxyStatusAndDelay(t *testing.T) {
	dir := t.TempDir()
	url, _ := testutil.WebDAVServer(t, dir)
	p := testutil.NewFaultProxy(t, url)
	p.Add(testutil.Fault{Kind: testutil.Status, Code: 429, Count: 2})
	for i := 0; i < 2; i++ {
		resp, err := http.Get(p.URL + "/")
		if err != nil || resp.StatusCode != 429 {
			t.Fatalf("expected 429, got %v %v", resp, err)
		}
		resp.Body.Close()
	}
	p.Add(testutil.Fault{Kind: testutil.Delay, Wait: 300 * time.Millisecond, Count: 1})
	t0 := time.Now()
	resp, err := http.Get(p.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if d := time.Since(t0); d < 300*time.Millisecond {
		t.Fatalf("delay not applied: %v", d)
	}
}
