#!/bin/sh
# usage: three/build.sh
#
# Builds the three clients of the three-driver comparison into
# .codex-cache/driver-comparison/three: old (gocql master, the commit in
# MASTER, checked out as a worktree), v2 (this checkout) and rust
# (scylla-rust-driver, the release pinned in rust/Cargo.toml).
set -eu
here=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
repo=$(CDPATH= cd -- "$here/../.." && pwd)
cache="$repo/.codex-cache/driver-comparison"
MASTER=${GOCQL_THREE_MASTER:-a61c57f756811346a18545746ff269f27dc8142c}
src="$cache/master-$(echo "$MASTER" | cut -c1-7)"
if ! git -C "$repo" cat-file -e "$MASTER^{commit}" 2>/dev/null; then git -C "$repo" fetch origin master; fi
test -d "$src" || git -C "$repo" worktree add --detach "$src" "$MASTER"
test "$(git -C "$src" rev-parse HEAD)" = "$MASTER"
grep -q "=> ../../.codex-cache/driver-comparison/$(basename "$src")\$" "$here/go.mod" || {
	echo "go.mod must replace github.com/gocql/gocql with $src" >&2
	exit 1
}
mkdir -p "$cache/three"
GOWORK=off go -C "$here" build -trimpath -tags old -o "$cache/three/old" .
GOWORK=off go -C "$here" build -trimpath -o "$cache/three/v2" .
CARGO_TARGET_DIR="$cache/rust-target" cargo build --release --manifest-path "$here/rust/Cargo.toml"
cp "$cache/rust-target/release/driver-comparison-rust" "$cache/three/rust"
{
	go version
	echo "master $MASTER"
	echo "v2 $(git -C "$repo" rev-parse HEAD)$(git -C "$repo" diff --quiet HEAD -- . ':!tests' || echo ' (dirty)')"
	grep -A1 '^name = "scylla"$' "$here/rust/Cargo.lock" | tail -1
	rustc --version
} >"$cache/three/build.txt"
cat "$cache/three/build.txt"
