#!/bin/sh
# usage: cluster_ab.sh BASE NEW OUTPUT_DIR PAIRS "CASE WORKERS"...
#
# Closed-loop ABBA comparison of two builds of the same driver on the cluster
# from cluster.sh, for gating a driver change: each pair runs
# BASE NEW NEW BASE for every case. Clients run on CPU 8 with GOMAXPROCS=1.
# Prints, per case, medians of ops/s, CPU us/op, allocs/op and p99, and
# NEW/BASE ratios with each pair's ratio, so a change that does not hold
# across pairs is visible.
set -eu
base=$1 new=$2 out=$3 pairs=$4; shift 4
hosts=${GOCQL_CLUSTER_HOSTS:-172.30.7.2,172.30.7.3,172.30.7.4}
warmup=${GOCQL_CLUSTER_WARMUP:-5s} duration=${GOCQL_CLUSTER_DURATION:-20s}
mkdir -p "$out"
for spec in "$@"; do
	set -- $spec
	i=1; while [ "$i" -le "$pairs" ]; do
		for label in A B B A; do
			bin=$base; [ $label = B ] && bin=$new
			n=1; [ -e "$out/$1-pair$i-$label-1.json" ] && n=2
			f="$out/$1-pair$i-$label-$n.json"
			env GOMAXPROCS=1 GOCQL_BENCH_CPUS=8 GOCQL_BENCH_GOMAXPROCS=1 taskset -c 8 "$bin" -hosts "$hosts" -mode run -case "$1" -concurrency "$2" -warmup "$warmup" -duration "$duration" -output "$f" 2>"$f.log" || echo "$1 pair$i $label FAILED: $(tail -1 "$f.log" | cut -c1-160)"
		done
		i=$((i + 1))
	done
done
python3 - "$out" <<'PY'
import glob, json, re, statistics, sys
out = sys.argv[1]
runs = {}
for f in sorted(glob.glob(f'{out}/*-pair*-*.json')):
    m = re.match(r'.*/(\w+)-pair(\d+)-([AB])-\d\.json$', f)
    if not m:
        continue
    d = json.load(open(f))['measurement']
    c = max(d['completed'], 1)
    runs.setdefault(m[1], {}).setdefault(m[3], {}).setdefault(int(m[2]), []).append(
        {'ops': d['in_window'] / d['duration_seconds'], 'cpu': 1e6 * d['cpu_seconds_including_drain'] / c,
         'allocs': d['mallocs'] / c, 'p99': d['latency']['p99_us'], 'errors': d['errors']})
print('| case | metric | base | new | new/base | per pair |')
print('|---|---|---:|---:|---:|---|')
for case, sides in runs.items():
    for key in ('ops', 'cpu', 'allocs', 'p99'):
        med = {s: statistics.median(r[key] for p in sides.get(s, {}).values() for r in p) for s in 'AB'}
        pairs = [statistics.median(r[key] for r in sides['B'][p]) / statistics.median(r[key] for r in sides['A'][p])
                 for p in sorted(sides['A']) if p in sides['B']]
        print(f"| {case} | {key} | {med['A']:,.1f} | {med['B']:,.1f} | {med['B'] / med['A']:.3f} | {' '.join(f'{x:.3f}' for x in pairs)} |")
    errors = sum(r['errors'] for s in sides.values() for p in s.values() for r in p)
    if errors:
        print(f'| {case} | errors | | | | {errors} |')
PY
