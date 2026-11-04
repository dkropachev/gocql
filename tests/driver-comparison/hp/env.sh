# Sourced by the happy-path gate scripts. HP_DIR holds binaries and results.
: "${HP_DIR:?set HP_DIR to a scratch directory}"
HP_REPO=$(cd "$(dirname "$0")/../../.." && pwd)
HP_GATES=$HP_DIR/gates
mkdir -p "$HP_GATES" "$HP_DIR/prof"
export GOCQL_LOOPBACK_SERVER_CPUS=${GOCQL_LOOPBACK_SERVER_CPUS:-6-7}
HP_T0='BenchmarkReusableQueryDecoding/(point|partition)/buffered|BenchmarkReusableQueryDecoding/point/stream|BenchmarkReusableQueryDecoding/partition/each'
HP_T1='BenchmarkReusableQueryLoopback/^(p?(read|write)|pone)$/w(1|64)$'
HP_MC='BenchmarkReusableQueryLoopback/^(p?(read|write)|pone)$/w64$'
# Paged Each (round 4) and the per-operation-context point read.
HP_PAGE='BenchmarkReusableQueryLoopback/^(page(ctx|pto|pf|b)?|preadctx)$/w(1|4|64)$'
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
		taskset -c 10-19 go test -tags unit -c -o "$HP_GATES/$1.exec.test" ./internal/execution/)
	if [ -n "${HP_REF:-}" ]; then git -C "$HP_REPO" worktree remove --force "$src"; fi
}
hp_round() { # LABEL OUT
	taskset -c 4-5 "$HP_GATES/$1.root.test" -test.run '^$' -test.bench "$HP_T0" -test.benchmem -test.benchtime 20000x -test.cpu 1 >>"$2"
	taskset -c 4-5 "$HP_GATES/$1.root.test" -test.run '^$' -test.bench "$HP_T1" -test.benchmem -test.benchtime 1s -test.cpu 1 >>"$2"
	taskset -c 0-3 "$HP_GATES/$1.root.test" -test.run '^$' -test.bench "$HP_MC" -test.benchmem -test.benchtime 1s -test.cpu 4 >>"$2"
	taskset -c 4-5 "$HP_GATES/$1.root.test" -test.run '^$' -test.bench "$HP_PAGE" -test.benchmem -test.benchtime 1s -test.cpu 1 >>"$2"
	taskset -c 4-5 "$HP_GATES/$1.exec.test" -test.run '^$' -test.bench 'BenchmarkReusableQueryAdmission' -test.benchmem -test.benchtime 300000x -test.cpu 1 >>"$2"
}
