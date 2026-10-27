// Command cfs is rclone with the cfs backend: every rclone command (mount,
// serve webdav|nfs|..., copy, sync, ls, ...) works on cfs remotes.
package main

import (
	_ "github.com/Ironboxplus/cfs/frontend/rclone"
	_ "github.com/rclone/rclone/backend/local"
	_ "github.com/rclone/rclone/backend/webdav"
	"github.com/rclone/rclone/cmd"
	_ "github.com/rclone/rclone/cmd/all"
)

func main() { cmd.Main() }
