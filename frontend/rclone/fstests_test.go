package cfsrclone_test

import (
	"context"
	"testing"

	cfsrclone "github.com/Ironboxplus/cfs/frontend/rclone"
	"github.com/Ironboxplus/cfs/internal/testutil"
	_ "github.com/rclone/rclone/backend/local"
	"github.com/rclone/rclone/fstest"
	"github.com/rclone/rclone/fstest/fstests"
)

func runFstests(t *testing.T, name, threshold string) {
	if *fstest.RemoteName != "" {
		t.Skip("-remote set")
	}
	drive := t.TempDir()
	state := t.TempDir()
	t.Cleanup(func() { cfsrclone.CloseAll(context.Background()) })
	fstests.Run(t, &fstests.Opt{
		RemoteName: name + ":",
		NilObject:  (*cfsrclone.Object)(nil),
		ExtraConfig: []fstests.ExtraConfigItem{
			{Name: name, Key: "type", Value: "cfs"},
			{Name: name, Key: "remote", Value: drive},
			{Name: name, Key: "state_dir", Value: state},
			{Name: name, Key: "threshold", Value: threshold},
			{Name: name, Key: "seal_quiet", Value: "20ms"},
			{Name: name, Key: "seal_max", Value: "200ms"},
			{Name: name, Key: "dir_cache", Value: "1h"},
			{Name: name, Key: "lease_ttl", Value: "30s"},
		},
		UnimplementableFsMethods:     []string{"OpenWriterAt", "OpenChunkWriter", "UserInfo", "Disconnect", "About", "PublicLink", "Copy", "Purge", "CleanUp", "ListR", "ListP", "MergeDirs", "DirSetModTime", "MkdirMetadata", "Command", "PutUnchecked"},
		UnimplementableObjectMethods: []string{"MimeType", "Metadata", "SetMetadata", "SetTier", "GetTier", "ID", "ParentID"},
		SkipInvalidUTF8:              true,
		QuickTestOK:                  true,
	})
}

// All files in the rclone tests are tiny, so with the default threshold they
// all live in packs.
func TestFstestsPacked(t *testing.T) { runFstests(t, "TestCfsPacked", "8Mi") }

// A 64 byte threshold sends a mix of files to packs and to their own objects.
func TestFstestsMixed(t *testing.T) { runFstests(t, "TestCfsMixed", "64") }

// The same suite with the drive being a real WebDAV server (rclone serving a
// directory in another process) over real HTTP.
func TestFstestsWebDAV(t *testing.T) {
	if *fstest.RemoteName != "" {
		t.Skip("-remote set")
	}
	url, _ := testutil.WebDAVServer(t, t.TempDir())
	name := "TestCfsDav"
	state := t.TempDir()
	t.Cleanup(func() { cfsrclone.CloseAll(context.Background()) })
	fstests.Run(t, &fstests.Opt{
		RemoteName: name + ":",
		NilObject:  (*cfsrclone.Object)(nil),
		ExtraConfig: []fstests.ExtraConfigItem{
			{Name: name, Key: "type", Value: "cfs"},
			{Name: name, Key: "remote", Value: ":webdav,url='" + url + "',vendor=other:"},
			{Name: name, Key: "state_dir", Value: state},
			{Name: name, Key: "threshold", Value: "64"},
			{Name: name, Key: "seal_quiet", Value: "20ms"},
			{Name: name, Key: "seal_max", Value: "200ms"},
			{Name: name, Key: "dir_cache", Value: "1h"},
			{Name: name, Key: "lease_ttl", Value: "30s"},
		},
		UnimplementableFsMethods:     []string{"OpenWriterAt", "OpenChunkWriter", "UserInfo", "Disconnect", "About", "PublicLink", "Copy", "Purge", "CleanUp", "ListR", "ListP", "MergeDirs", "DirSetModTime", "MkdirMetadata", "Command", "PutUnchecked"},
		UnimplementableObjectMethods: []string{"MimeType", "Metadata", "SetMetadata", "SetTier", "GetTier", "ID", "ParentID"},
		SkipInvalidUTF8:              true,
		SkipBadWindowsCharacters:     true,
	})
}
