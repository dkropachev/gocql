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
cd "$(dirname "$0")/.."

# Targets are found in the default build, which every fuzz file uses, in
# the packages of this module (nested modules are skipped).
module=$(go list -m)
mapfile -t targets < <(
	grep -rl --include='*_test.go' --exclude-dir=testdata '^func Fuzz' . |
		while read -r file; do
			dir=$(dirname "$file")
			[[ "$(go list -f '{{.Module.Path}}' "$dir" 2>/dev/null)" == "$module" ]] || continue
			sed -n 's/^func \(Fuzz[A-Za-z0-9_]*\)(.*/\1/p' "$file" | while read -r name; do
				echo "$dir $name"
			done
		done | sort
)
if ((${#targets[@]} == 0)); then
	echo "no fuzz targets found" >&2
	exit 1
fi

failed=()
summary=()
for target in "${targets[@]}"; do
	read -r dir name <<<"$target"
	echo "::group::$dir $name ($budget)"
	start=$SECONDS
	if go test -run='^$' -fuzz="^${name}\$" -fuzztime="$budget" -parallel="$parallel" "$dir"; then
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
