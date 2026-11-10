package core

import (
	"context"
	"strings"
	"sync"

	"github.com/Ironboxplus/cfs/store"
)

// What the writer knows of the drive's folders is learned when a change needs
// it, never by walking the drive. ddirs holds the folders known to exist; a
// folder is known to be absent when its parent's listing has been read in full
// (every folder it lists is in ddirs) and does not have it, or when its parent
// is absent. So the first question under the root costs one listing of the root
// and settles every path of the namespace that has no folder on the drive;
// each folder that exists is listed once, when a path goes through it. The
// knowledge is that of one tenure of the lease: another writer may have made
// or removed folders in between, so taking the lease starts it afresh.

// dirKnow is the set of folders whose listing has been read in full.
type dirKnow struct {
	mu     sync.Mutex
	listed map[string]struct{}
}

func (k *dirKnow) isListed(dir string) bool {
	k.mu.Lock()
	defer k.mu.Unlock()
	_, ok := k.listed[dir]
	return ok
}

func (k *dirKnow) markListed(dir string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.listed == nil {
		k.listed = map[string]struct{}{}
	}
	k.listed[dir] = struct{}{}
}

// dropListed forgets the folder p and everything below it.
func (k *dirKnow) dropListed(p string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	for d := range k.listed {
		if d == p || strings.HasPrefix(d, p+"/") {
			delete(k.listed, d)
		}
	}
}

func (k *dirKnow) reset() {
	k.mu.Lock()
	k.listed = nil
	k.mu.Unlock()
}

// dirOnDrive says whether the drive has a folder p, listing only what the
// answer needs and has not been listed yet in this tenure of the lease. A
// reader has no record and asks the drive about every folder.
func (v *Volume) dirOnDrive(ctx context.Context, p string) (bool, error) {
	if p == "" || v.ReadOnly() || v.ix.hasDDir(p) {
		return true, nil
	}
	parent := parentOf(p)
	ok, err := v.dirOnDrive(ctx, parent)
	if err != nil || !ok {
		return false, err
	}
	if v.dk.isListed(parent) {
		return false, nil
	}
	if _, err := v.listStore(ctx, parent); err != nil && !isNotFound(err) {
		return false, err
	}
	return v.ix.hasDDir(p), nil
}

// learnFolders records what a listing of dir showed: the folder itself, the
// folders in it, and that the list of them is complete.
func (v *Volume) learnFolders(dir string, ents []store.Entry) {
	var subs []string
	if dir != "" {
		subs = append(subs, dir)
	}
	for _, e := range ents {
		if e.IsDir && !(dir == "" && strings.HasPrefix(e.Name(), ".cfs")) {
			subs = append(subs, e.Path)
		}
	}
	if err := v.ix.addDDirs(subs...); err != nil {
		v.logf("cfs: record folders of %q: %v", dir, err)
		return
	}
	v.dk.markListed(dir)
}
