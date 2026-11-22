#!/usr/bin/env bash
# Compares the public API with the recorded v2 baseline, or rewrites it.
#
#   ci/apidiff.sh check   fail on an incompatible change to a public package
#   ci/apidiff.sh write   record the baseline from the working tree
#
# The baseline is the export data golang.org/x/exp/cmd/apidiff writes for each
# public package (the packages testdata/api.txt lists), one file per package
# in APIDIFF_BASELINE_DIR, such as testdata/apidiff/v2.0.0. Only incompatible
# changes fail the check: a new symbol or package is allowed in a minor
# release. See RELEASING.md for when to record a baseline.
set -euo pipefail
shopt -s nullglob

APIDIFF_VERSION="${APIDIFF_VERSION:?set APIDIFF_VERSION, as the Makefile does}"
BASELINE_DIR="${APIDIFF_BASELINE_DIR:?set APIDIFF_BASELINE_DIR, as the Makefile does}"
# One directory per release, such as testdata/apidiff/v2.0.0; write removes it
# first, so anything else is refused.
if [[ ! "$BASELINE_DIR" =~ ^testdata/apidiff/[^/]+$ ]]; then
	echo "APIDIFF_BASELINE_DIR must be testdata/apidiff/<release>, not ${BASELINE_DIR}" >&2
	exit 2
fi

cd "$(dirname "$0")/.."
ROOT="$(pwd)"
MODULE="$(go list -m)"

# Export data records the source file of every object, in this module and in
# its dependencies. Writing it through fixed symlinks to the checkout and the
# module cache keeps both paths out of the baseline, so the bytes do not
# depend on where they are.
NEUTRAL_ROOT=/tmp/gocql-v2-apidiff
NEUTRAL_MODCACHE=/tmp/gocql-v2-apidiff-modcache

# neutral_link creates the symlink $2 to $1. It refuses an existing path,
# which is another checkout's write in progress or something that is not
# ours.
neutral_link() {
	if [ -e "$2" ] || [ -L "$2" ]; then
		echo "$2 already exists; another write may be running, or remove it" >&2
		exit 1
	fi
	ln -s "$1" "$2"
}

# apidiff loads the module with the go/types of the toolchain that built it,
# so it is built with the module's toolchain: go run would otherwise pick the
# oldest one golang.org/x/exp allows, which cannot type-check this module.
apidiff() {
	GOTOOLCHAIN="$(go env GOVERSION)" go run "golang.org/x/exp/cmd/apidiff@${APIDIFF_VERSION}" "$@"
}

# public_packages prints "<name> <import path>" for each package
# testdata/api.txt lists; the root package is listed as gocql.
public_packages() {
	sed -n 's/^package //p' testdata/api.txt | while read -r name; do
		if [ "$name" = gocql ]; then
			echo "gocql ${MODULE}"
		else
			echo "${name} ${MODULE}/${name}"
		fi
	done
}

case "${1:-}" in
write)
	neutral_link "$ROOT" "$NEUTRAL_ROOT"
	trap 'rm -f "$NEUTRAL_ROOT"' EXIT
	neutral_link "$(go env GOMODCACHE)" "$NEUTRAL_MODCACHE"
	trap 'rm -f "$NEUTRAL_ROOT" "$NEUTRAL_MODCACHE"' EXIT
	rm -rf "$BASELINE_DIR"
	mkdir -p "$BASELINE_DIR"
	public_packages | while read -r name path; do
		(cd "$NEUTRAL_ROOT" && GOMODCACHE="$NEUTRAL_MODCACHE" \
			apidiff -w "${ROOT}/${BASELINE_DIR}/${name}.api" "$path")
	done
	echo "wrote $(find "$BASELINE_DIR" -name '*.api' | wc -l) baselines to ${BASELINE_DIR}"
	;;
check)
	if [ ! -d "$BASELINE_DIR" ]; then
		echo "no baseline in ${BASELINE_DIR}" >&2
		exit 1
	fi
	failed=0
	listed=" "
	while read -r name path; do
		listed+="${name} "
		baseline="${BASELINE_DIR}/${name}.api"
		if [ ! -f "$baseline" ]; then
			echo "${path}: new package, not in ${BASELINE_DIR}"
			continue
		fi
		report="$(apidiff -incompatible "$baseline" "$path")"
		if [ -n "$report" ]; then
			echo "${path}: incompatible changes since ${BASELINE_DIR}:"
			echo "${report}" | sed 's/^/  /'
			failed=1
		fi
	done < <(public_packages)
	for baseline in "$BASELINE_DIR"/*.api; do
		name="$(basename "$baseline" .api)"
		if [[ "$listed" != *" ${name} "* ]]; then
			echo "${name}: package removed from the public API"
			failed=1
		fi
	done
	if [ "$failed" -ne 0 ]; then
		exit 1
	fi
	echo "public API compatible with ${BASELINE_DIR}"
	;;
*)
	echo "usage: $0 check|write" >&2
	exit 2
	;;
esac
