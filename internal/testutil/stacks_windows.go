//go:build windows

package testutil

// DumpStacks is a no-op on Windows: there is no signal that prints the stacks.
func (p *Proc) DumpStacks() {}
