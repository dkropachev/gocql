# Sourced by the happy-path gate scripts. HP_DIR holds binaries and results.
: "${HP_DIR:?set HP_DIR to a scratch directory}"
HP_REPO=$(cd "$(dirname "$0")/../../.." && pwd)
HP_GATES=$HP_DIR/gates
mkdir -p "$HP_GATES" "$HP_DIR/prof"
export GOCQL_LOOPBACK_SERVER_CPUS=${GOCQL_LOOPBACK_SERVER_CPUS:-6-7}
# Gate selectors. A top-level '|' separates whole benchmark paths, and each
# '/'-separated level is anchored so that no longer sibling name matches.
HP_T0='^BenchmarkReusableQuery$/^Decoding$/^(point|partition)$/^buffered$|^BenchmarkReusableQuery$/^Decoding$/^point$/^stream$|^BenchmarkReusableQuery$/^Decoding$/^partition$/^each$'
HP_T1='^BenchmarkReusableQuery$/^Loopback$/^(p?(read|write)|pone)$/^w(1|64)$'
HP_MC='^BenchmarkReusableQuery$/^Loopback$/^(p?(read|write)|pone)$/^w64$'
# Paged Each (round 4) and the per-operation-context point read.
HP_PAGE='^BenchmarkReusableQuery$/^Loopback$/^(page(ctx|pto|b)?|preadctx)$/^w(1|4|64)$'
HP_ADM='^BenchmarkReusableQuery$/^Admission$'
# The same cases under the names benchmarks had before they were grouped as
# BenchmarkReusableQuery/<API>. hp_run selects these on binaries built from
# older commits and renames their output with renames.sed, so that benchstat
# lines both sides of a comparison up by name.
HP_T0_LEGACY='^BenchmarkReusableQueryDecoding$/^(point|partition)$/^buffered$|^BenchmarkReusableQueryDecoding$/^point$/^stream$|^BenchmarkReusableQueryDecoding$/^partition$/^each$'
HP_T1_LEGACY='^BenchmarkReusableQueryLoopback$/^(p?(read|write)|pone)$/^w(1|64)$'
HP_MC_LEGACY='^BenchmarkReusableQueryLoopback$/^(p?(read|write)|pone)$/^w64$'
HP_PAGE_LEGACY='^BenchmarkReusableQueryLoopback$/^(page(ctx|pto|b)?|preadctx)$/^w(1|4|64)$'
HP_ADM_LEGACY='^BenchmarkReusableQueryAdmission$'
HP_RENAMES=$(dirname "$0")/renames.sed
# hp_build LABEL builds the gate binaries from the working tree, or from the
# commit HP_REF (checked out in a throwaway worktree) so that editing can go on
# while a comparison runs, optionally with HP_PATCH applied. Builds stay off the benchmark CPUs.
hp_build() { # LABEL
	src=$HP_REPO
	if [ -n "${HP_REF:-}" ]; then
		src=$HP_DIR/src-$1
		git -C "$HP_REPO" worktree remove --force "$src" 2>/dev/null || true
		git -C "$HP_REPO" worktree add -q --detach "$src" "$HP_REF"
		# HP_PATCH (e.g. a benchmark metric) is applied on top of the commit,
		# so that both sides of a comparison measure the same way.
		if [ -n "${HP_PATCH:-}" ]; then git -C "$src" apply "$HP_PATCH"; fi
	fi
	(cd "$src" && taskset -c 10-19 go test -tags unit -c -o "$HP_GATES/$1.root.test" . &&
		taskset -c 10-19 go test -tags unit -c -o "$HP_GATES/$1.exec.test" ./internal/session/)
	if [ -n "${HP_REF:-}" ]; then git -C "$HP_REPO" worktree remove --force "$src"; fi
}
# hp_bin LABEL prints the gate binary that holds the reusable-query benchmarks:
# internal/session now, the root package in commits from before the package split.
hp_bin() { # LABEL
	for b in "$HP_GATES/$1.exec.test" "$HP_GATES/$1.root.test"; do
		if "$b" -test.list '^BenchmarkReusableQuery(Decoding)?$' | grep -q .; then
			echo "$b"
			return
		fi
	done
	echo "no reusable-query benchmarks in $1 binaries" >&2
	return 1
}
# hp_run CPUS BIN SELECTOR FLAGS... runs one gate selector pinned to CPUS, choosing the
# legacy names on binaries that predate BenchmarkReusableQuery and renaming
# their output to the current names.
hp_run() { # CPUS BIN SELECTOR FLAGS...
	cpus=$1 bin=$2 sel=$3
	shift 3
	if "$bin" -test.list '^BenchmarkReusableQuery$' | grep -q .; then
		eval "pat=\$$sel"
		taskset -c "$cpus" "$bin" -test.run '^$' -test.bench "$pat" "$@"
	else
		eval "pat=\$${sel}_LEGACY"
		taskset -c "$cpus" "$bin" -test.run '^$' -test.bench "$pat" "$@" | sed -f "$HP_RENAMES"
	fi
}
hp_round() { # LABEL OUT
	bin=$(hp_bin "$1")
	hp_run 4-5 "$bin" HP_T0 -test.benchmem -test.benchtime 20000x -test.cpu 1 >>"$2"
	hp_run 4-5 "$bin" HP_T1 -test.benchmem -test.benchtime 1s -test.cpu 1 >>"$2"
	hp_run 0-3 "$bin" HP_MC -test.benchmem -test.benchtime 1s -test.cpu 4 >>"$2"
	hp_run 4-5 "$bin" HP_PAGE -test.benchmem -test.benchtime 1s -test.cpu 1 >>"$2"
	hp_run 4-5 "$HP_GATES/$1.exec.test" HP_ADM -test.benchmem -test.benchtime 300000x -test.cpu 1 >>"$2"
}
