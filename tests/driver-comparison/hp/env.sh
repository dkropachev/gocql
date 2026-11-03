# Sourced by the happy-path gate scripts. HP_DIR holds binaries and results.
: "${HP_DIR:?set HP_DIR to a scratch directory}"
HP_REPO=$(cd "$(dirname "$0")/../../.." && pwd)
HP_GATES=$HP_DIR/gates
mkdir -p "$HP_GATES" "$HP_DIR/prof"
export GOCQL_LOOPBACK_SERVER_CPUS=${GOCQL_LOOPBACK_SERVER_CPUS:-6-7}
HP_T0='BenchmarkReusableQueryDecoding/(point|partition)/buffered|BenchmarkReusableQueryDecoding/point/stream'
HP_T1='BenchmarkReusableQueryLoopback/p?(read|write)/w(1|64)$'
HP_MC='BenchmarkReusableQueryLoopback/p?(read|write)/w64$'
hp_build() { # LABEL
	cd "$HP_REPO"
	go test -tags unit -c -o "$HP_GATES/$1.root.test" .
	go test -tags unit -c -o "$HP_GATES/$1.exec.test" ./internal/execution/
}
hp_round() { # LABEL OUT
	taskset -c 4-5 "$HP_GATES/$1.root.test" -test.run '^$' -test.bench "$HP_T0" -test.benchmem -test.benchtime 20000x -test.cpu 1 >>"$2"
	taskset -c 4-5 "$HP_GATES/$1.root.test" -test.run '^$' -test.bench "$HP_T1" -test.benchmem -test.benchtime 1s -test.cpu 1,2 >>"$2"
	taskset -c 0-3 "$HP_GATES/$1.root.test" -test.run '^$' -test.bench "$HP_MC" -test.benchmem -test.benchtime 1s -test.cpu 4 >>"$2"
	taskset -c 4-5 "$HP_GATES/$1.exec.test" -test.run '^$' -test.bench 'BenchmarkReusableQueryAdmission' -test.benchmem -test.benchtime 300000x -test.cpu 1 >>"$2"
}
