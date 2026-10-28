#!/usr/bin/env bash
# Prints the job matrix (JSON) built from groups.txt.
set -euo pipefail
cd "$(dirname "$0")"
grep -vE '^\s*(#|$)' groups.txt | while read -r pkgs name win run; do
	jq -cn --arg pkgs "${pkgs//,/ }" --arg name "$name" --arg run "$run" \
		'{os: "ubuntu-latest", name: $name, label: ($name + " (linux)"), pkgs: $pkgs, run: $run}'
	if [ "$win" = 1 ]; then
		jq -cn --arg pkgs "${pkgs//,/ }" --arg name "$name" --arg run "$run" \
			'{os: "windows-latest", name: $name, label: ($name + " (windows)"), pkgs: $pkgs, run: $run}'
	fi
done | jq -cs '.'
