#!/usr/bin/env bash
# Runs every fuzz target of the module for a fixed budget each.
#
#   ci/fuzz.sh [budget]    budget is a go test -fuzztime value, default 30s
#
# go test fuzzes one target per run, so each target gets its own run. A
# failing target does not stop the others; the script exits non-zero if any
# failed. A failing input is written under the package's testdata/fuzz/ by go
# test; commit it to keep it as a regression seed.
#
# FUZZ_PARALLEL sets the fuzzing workers per target (go test -parallel);
# default 2, so a target's workers fit a CI runner beside the compile.
set -euo pipefail

budget=${1:-30s}
parallel=${FUZZ_PARALLEL:-2}
if ! [[ "$budget" =~ ^[0-9]+(ns|us|ms|s|m|h|x)$ && "$parallel" =~ ^[0-9]+$ ]]; then
	echo "invalid budget ${budget@Q} or FUZZ_PARALLEL ${parallel@Q}" >&2
	exit 2
fi
cd "$(dirname "$0")/.."

# Targets are the functions func FuzzX(name *testing.F) of this module's test
# files; files of nested modules (a directory with its own go.mod) are
# skipped. Each run below must report that it fuzzed, so a target the default
# build leaves out fails instead of passing silently.
decl='^func Fuzz[A-Za-z0-9_]*\([A-Za-z_][A-Za-z0-9_]* \*testing\.F\)'
mapfile -t nested < <(find . -mindepth 2 -name go.mod -not -path '*/testdata/*' -printf '%h/\n')
files=$(grep -rlE --include='*_test.go' --exclude-dir=testdata "$decl" . || true)
targets=()
for file in $files; do
	for dir in "${nested[@]}"; do
		[[ "$file" == "$dir"* ]] && continue 2
	done
	dir=$(dirname "$file")
	if ! go list "$dir" >/dev/null; then
		echo "cannot load package $dir" >&2
		exit 1
	fi
	for name in $(grep -oE "$decl" "$file" | sed -E 's/^func (Fuzz[A-Za-z0-9_]*)\(.*/\1/'); do
		targets+=("$dir $name")
	done
done
if ((${#targets[@]} == 0)); then
	echo "no fuzz targets found" >&2
	exit 1
fi
mapfile -t targets < <(printf '%s\n' "${targets[@]}" | sort)
log=$(mktemp)
trap 'rm -f "$log"' EXIT

failed=()
summary=()
for target in "${targets[@]}"; do
	read -r dir name <<<"$target"
	echo "::group::$dir $name ($budget)"
	start=$SECONDS
	if go test -run='^$' -fuzz="^${name}\$" -fuzztime="$budget" -parallel="$parallel" "$dir" 2>&1 | tee "$log" &&
		grep -q '^fuzz: elapsed' "$log"; then
		result=pass
	else
		result=FAIL
		failed+=("$dir $name")
	fi
	echo "::endgroup::"
	summary+=("| \`$dir\` | \`$name\` | $result | $((SECONDS - start))s |")
done

if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
	{
		echo "### Fuzz targets ($budget each)"
		echo
		echo "| package | target | result | wall time |"
		echo "|---|---|---|---|"
		printf '%s\n' "${summary[@]}"
	} >>"$GITHUB_STEP_SUMMARY"
fi
printf '%s\n' "${summary[@]}"

if ((${#failed[@]} > 0)); then
	printf 'fuzz target failed: %s\n' "${failed[@]}" >&2
	exit 1
fi
