#!/usr/bin/env bash
# go.mod replaces github.com/rclone/rclone with ../rclone-fork: rclone v1.75.2 plus
# the patches in ci/rclone-patches. This builds that directory next to the checkout.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
dest="${1:-$root/../rclone-fork}"
if [ -e "$dest/.git" ]; then
	echo "$dest exists, leaving it alone"
	exit 0
fi
git clone -c core.autocrlf=false --depth 1 --branch v1.75.2 https://github.com/rclone/rclone.git "$dest"
git -C "$dest" -c user.name=ci -c user.email=ci@example.invalid -c core.autocrlf=false am "$root"/ci/rclone-patches/*.patch
