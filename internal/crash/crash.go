// Package crash implements named crash points for fault-injection tests. With
// CFS_CRASHPOINT=<name> (or <name>:<n> for the n-th time the point is reached)
// in the environment, the process dies with exit status 137 at that point,
// exactly as if it had been killed there. Without the variable every point is
// a single string comparison.
package crash

import (
	"os"
	"strconv"
	"strings"
	"sync/atomic"
)

var (
	target string
	want   int64 = 1
	hits   atomic.Int64
)

func init() {
	v := os.Getenv("CFS_CRASHPOINT")
	if v == "" {
		return
	}
	target = v
	if i := strings.LastIndexByte(v, ':'); i > 0 {
		if n, err := strconv.ParseInt(v[i+1:], 10, 64); err == nil && n > 0 {
			target, want = v[:i], n
		}
	}
}

// Point dies here if this is the configured crash point.
func Point(name string) {
	if target == "" || name != target {
		return
	}
	if hits.Add(1) == want {
		os.Stderr.WriteString("cfs: crashpoint " + name + "\n")
		os.Exit(137)
	}
}
