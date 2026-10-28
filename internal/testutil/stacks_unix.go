//go:build !windows

package testutil

import (
	"syscall"
	"time"
)

// DumpStacks makes a running process print its goroutines (SIGQUIT) and
// waits for it to die, so a hang can be diagnosed from the log.
func (p *Proc) DumpStacks() {
	if p.Exited() || p.Cmd.Process == nil {
		return
	}
	p.Cmd.Process.Signal(syscall.SIGQUIT)
	select {
	case <-p.done:
	case <-time.After(5 * time.Second):
	}
}
