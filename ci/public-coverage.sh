#!/usr/bin/env bash
# Reports the coverage of the public packages from a merged coverage profile
# and fails when an exported function or method of one of them was never
# run.
#
#   ci/public-coverage.sh [profile]    default coverage-root.out
#
# The profile is the textfmt one make coverage-report writes from the unit
# and integration runs. A method counts by its own name, whatever its
# receiver: an exported method of an unexported type is reached through an
# interface. Public packages are those outside internal/, cmd/, tests/, ci/,
# docs/ and examples/. A function with an empty body has no statement to
# cover and is not reported.
set -euo pipefail

profile=${1:-coverage-root.out}
cd "$(dirname "$0")/.."
module=$(go list -m)

public() {
	awk -v prefix="$module/" '
		{
			rel = $1
			if (index(rel, prefix) == 1) rel = substr(rel, length(prefix) + 1)
			if (rel ~ /^(internal|cmd|tests|ci|docs|examples)\//) next
			print
		}'
}

# Per-package statement coverage, from the profile's blocks.
packages=$(tail -n +2 "$profile" | public | awk '
	{
		split($1, loc, ":")
		pkg = loc[1]
		sub(/\/[^\/]*$/, "", pkg)
		total[pkg] += $2
		if ($3 > 0) covered[pkg] += $2
	}
	END {
		for (p in total) printf "| `%s` | %.1f%% |\n", p, 100 * covered[p] / total[p]
	}' | sort)

# go tool cover reports a function with an empty body, which has no
# statement to run, as 0.0%; those are left out.
uncovered=$(go tool cover -func="$profile" | public | awk '$2 ~ /^[A-Z]/ && $3 == "0.0%" { print $1 " " $2 }' |
	while read -r loc name; do
		file=${loc%%:*}
		line=${loc#*:}
		line=${line%%:*}
		if sed -n "${line}p" "${file#"$module"/}" | grep -q '{ *}$'; then
			continue
		fi
		echo "$loc $name"
	done)

report() {
	echo "### Public package coverage"
	echo
	echo "| package | statements |"
	echo "|---|---|"
	echo "$packages"
	echo
	if [[ -n "$uncovered" ]]; then
		echo "Exported functions no test runs:"
		echo
		echo '```'
		echo "$uncovered"
		echo '```'
	else
		echo "Every exported function of the public packages runs in a test."
	fi
}
report
if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
	report >>"$GITHUB_STEP_SUMMARY"
fi
[[ -z "$uncovered" ]]
