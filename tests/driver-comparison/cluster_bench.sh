#!/bin/sh
# usage: cluster_bench.sh OLD_BINARY NEW_BINARY OUTPUT_DIR [PAIRS]
#
# Throughput and latency of the query-execution cases in cases.tsv against
# the cluster from cluster.sh (or provision.py). OLD is the master client
# (built with -tags old), NEW the v2 client. Every client runs on CPU 8 with
# GOMAXPROCS=1.
#
#   check:      every case of both drivers runs 256 validated operations.
#   throughput: closed loop with each case's workers from cases.tsv. For a
#               master case M and its v2 cases B1..Bn, each pair runs
#               M B1..Bn Bn..B1 M.
#   latency:    open loop at FRACTION of M's median throughput, the same
#               scheduled rate for M and all of its v2 cases, same order.
#
# The dataset is created and loaded once with OLD; every client validates
# every row it reads. Summarise with cluster_bench_report.py OUTPUT_DIR.
set -eu
old=$1 new=$2 out=$3 pairs=${4:-2}
here=$(cd "$(dirname "$0")" && pwd)
cases_file=${GOCQL_CLUSTER_CASES_FILE:-$here/cases.tsv}
# CASES limits the run to these master cases (and their v2 cases).
cases=${GOCQL_CLUSTER_CASES:-}
hosts=${GOCQL_CLUSTER_HOSTS:-172.30.7.2,172.30.7.3,172.30.7.4}
warmup=${GOCQL_CLUSTER_WARMUP:-10s} duration=${GOCQL_CLUSTER_DURATION:-30s}
# PHASES selects setup, check, throughput and latency; FRACTION scales the
# latency rate from the master case's median throughput, recorded as
# latency<percent> unless it is 0.7.
phases=${GOCQL_CLUSTER_PHASES:-setup check throughput latency}
fraction=${GOCQL_CLUSTER_LATENCY_FRACTION:-0.7}
has() { case " $phases " in *" $1 "*) return 0 ;; esac; return 1; }
mkdir -p "$out"
cp "$cases_file" "$out/cases.tsv"
client() { # BINARY ARGS...
	bin=$1; shift
	env GOMAXPROCS=1 GOCQL_BENCH_CPUS=8 GOCQL_BENCH_GOMAXPROCS=1 taskset -c 8 "$bin" -hosts "$hosts" "$@"
}
binary() { [ "$1" = A ] && echo "$old" || echo "$new"; }

if has setup; then
	client "$old" -mode schema 2>>"$out/setup.log"
	for sc in read paged lwt; do client "$old" -mode prepare -scenario $sc 2>>"$out/setup.log"; done
	for sc in read paged; do client "$old" -mode warm -scenario $sc 2>>"$out/setup.log"; done
fi

if has check; then
	# shellcheck disable=SC2086
	for c in $(python3 "$here/cluster_cases.py" cases "$cases_file" master $cases); do
		client "$old" -mode check -case "$c" 2>>"$out/check.log"
	done
	# shellcheck disable=SC2086
	for c in $(python3 "$here/cluster_cases.py" cases "$cases_file" v2 $cases); do
		client "$new" -mode check -case "$c" 2>>"$out/check.log"
	done
	echo "check done" >>"$out/setup.log"
fi

run() { # KIND MASTER WORKERS PAIR LABEL CASE RATE
	f="$out/$1-$2-pair$4-$6.json"
	# A failed run (errors, or a warmup that cannot keep the rate) is
	# recorded in failures.log; the remaining runs go on.
	if client "$(binary "$5")" -mode run -case "$6" -concurrency "$3" -warmup "$warmup" -duration "$duration" -rate "$7" -output "$f" 2>"$f.log"; then
		echo "$1 $2 pair$4 $6 done"
	else
		echo "$1 $2 pair$4 $6 FAILED: $(tail -1 "$f.log" | cut -c1-200)" | tee -a "$out/failures.log"
	fi
}

sequence() {
	# shellcheck disable=SC2086
	python3 "$here/cluster_cases.py" sequence "$cases_file" "$pairs" $cases
}

if has throughput; then
	sequence | while read -r master workers pair label case; do
		run throughput "$master" "$workers" "$pair" "$label" "$case" 0
	done
fi

kind=latency
[ "$fraction" = 0.7 ] || kind=latency$(python3 -c "print(round(100*$fraction))")
if has latency; then
	current='' rate=0
	sequence | while read -r master workers pair label case; do
		if [ "$master" != "$current" ]; then
			current=$master
			rate=$(python3 "$here/cluster_cases.py" rate "$out" "$master" "$fraction")
			echo "$master $kind rate $rate/s" >>"$out/setup.log"
		fi
		run "$kind" "$master" "$workers" "$pair" "$label" "$case" "$rate"
	done
fi
echo CLUSTER-BENCH-DONE
