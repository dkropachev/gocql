#!/bin/sh
# usage: cluster_bench.sh OLD_BINARY NEW_BINARY OUTPUT_DIR [PAIRS]
#
# Throughput and latency of two comparison clients against the cluster from
# cluster.sh. Every client runs on CPU 8 with GOMAXPROCS=1.
#
#   throughput: closed loop, 64 workers for point read and write, 4 for
#               paged read, ABBA pairs (A = OLD, B = NEW).
#   latency:    open loop at 70% of OLD's median throughput, the same
#               scheduled rate for both drivers, 64 workers (4 paged).
#
# The dataset is created and loaded once with OLD; both clients validate
# every row they read. Summarise with cluster_bench_report.py OUTPUT_DIR.
set -eu
old=$1 new=$2 out=$3 pairs=${4:-2}
hosts=${GOCQL_CLUSTER_HOSTS:-172.30.7.2,172.30.7.3,172.30.7.4}
warmup=${GOCQL_CLUSTER_WARMUP:-10s} duration=${GOCQL_CLUSTER_DURATION:-30s}
# PHASES selects setup, throughput and latency; FRACTION scales the latency
# rate from the reference's median throughput, recorded as latency<percent>.
phases=${GOCQL_CLUSTER_PHASES:-setup throughput latency}
fraction=${GOCQL_CLUSTER_LATENCY_FRACTION:-0.7}
has() { case " $phases " in *" $1 "*) return 0 ;; esac; return 1; }
mkdir -p "$out"
client() { # BINARY ARGS...
	bin=$1; shift
	env GOMAXPROCS=1 GOCQL_BENCH_CPUS=8 GOCQL_BENCH_GOMAXPROCS=1 taskset -c 8 "$bin" -hosts "$hosts" "$@"
}
workers() { [ "$1" = paged ] && echo 4 || echo 64; }

if has setup; then
	client "$old" -mode schema 2>>"$out/setup.log"
	for sc in read paged; do client "$old" -mode prepare -scenario $sc 2>>"$out/setup.log"; done
	for sc in read paged; do client "$old" -mode warm -scenario $sc 2>>"$out/setup.log"; done
fi

run() { # LABEL BINARY SCENARIO RATE KIND PAIR
	f="$out/$5-$3-pair$6-$1.json"
	client "$2" -mode run -scenario "$3" -concurrency "$(workers "$3")" -warmup "$warmup" -duration "$duration" -rate "$4" -output "$f" 2>"$f.log"
	echo "$5 $3 pair$6 $1 done"
}
has throughput && for sc in read write paged; do
	i=1; while [ $i -le "$pairs" ]; do
		for l in A B B A; do
			if [ $l = A ]; then b=$old; else b=$new; fi
			run $l "$b" $sc 0 throughput $i
		done
		i=$((i + 1))
	done
done
kind=latency
[ "$fraction" = 0.7 ] || kind=latency$(python3 -c "print(round(100*$fraction))")
has latency && for sc in read write paged; do
	rate=$(python3 - "$out" "$sc" "$fraction" <<'EOF'
import glob, json, statistics, sys
out, sc, fraction = sys.argv[1], sys.argv[2], float(sys.argv[3])
rates = []
for f in glob.glob(f'{out}/throughput-{sc}-pair*-A.json'):
    m = json.load(open(f))['measurement']
    rates.append(m['in_window'] / m['duration_seconds'])
print(round(fraction * statistics.median(rates), 2))
EOF
)
	echo "$sc $kind rate $rate/s" >>"$out/setup.log"
	i=1; while [ $i -le "$pairs" ]; do
		for l in A B B A; do
			if [ $l = A ]; then b=$old; else b=$new; fi
			run $l "$b" $sc "$rate" $kind $i
		done
		i=$((i + 1))
	done
done
echo CLUSTER-BENCH-DONE
