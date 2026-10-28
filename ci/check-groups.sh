#!/usr/bin/env bash
# Fails when a test (or a package with tests) is not covered by a line of groups.txt.
# Run from the module root, after ci/setup-rclone.sh.
set -euo pipefail
cd "$(dirname "$0")/.."
bad=0
for pkg in $(go list -f '{{if or .TestGoFiles .XTestGoFiles}}{{.ImportPath}}{{end}}' ./...); do
	rel="./${pkg#github.com/Ironboxplus/cfs/}"
	regexes=$(grep -vE '^\s*(#|$)' ci/groups.txt | awk -v p="$rel" '{ n = split($1, a, ","); for (i = 1; i <= n; i++) if (a[i] == p) print $4 }')
	if [ -z "$regexes" ]; then
		echo "package $rel has tests but no group in ci/groups.txt"
		bad=1
		continue
	fi
	n=0
	for t in $(go test -list '.' "$rel" | grep -E '^(Test|Fuzz|Example)'); do
		n=$((n + 1))
		hit=0
		while read -r re; do
			if printf '%s\n' "$t" | grep -Eq "$re"; then hit=1; fi
		done <<<"$regexes"
		if [ "$hit" = 0 ]; then
			echo "$rel: $t matches no group"
			bad=1
		fi
	done
	echo "$rel: $n tests checked"
done
exit $bad
