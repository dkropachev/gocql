#!/usr/bin/env python3
"""Summarise cluster_bench.sh output: medians per driver (A = old, B = new)."""
import glob
import json
import statistics
import sys


def load(out, kind, scenario, label):
    rows = []
    for path in sorted(glob.glob(f'{out}/{kind}-{scenario}-pair*-{label}.json')):
        data = json.load(open(path))
        m = data['measurement']
        seconds = m['duration_seconds']
        done = m['in_window']
        rows.append({
            'ops': done / seconds,
            'cpu_us': 1e6 * m['cpu_seconds_including_drain'] / max(m['completed'], 1),
            'allocs': m['mallocs'] / max(m['completed'], 1),
            'bytes': m['alloc_bytes'] / max(m['completed'], 1),
            'p90': m['latency']['p90_us'], 'p95': m['latency']['p95_us'], 'p99': m['latency']['p99_us'],
            'errors': m['errors'], 'rate': data.get('rate', m.get('rate', 0)),
        })
    return rows


def med(rows, key):
    return statistics.median(r[key] for r in rows) if rows else float('nan')


def main():
    out = sys.argv[1]
    print('| kind | scenario | metric | master (A) | v2 (B) | B/A |')
    print('|---|---|---|---:|---:|---:|')
    kinds = sorted({p.rsplit('/', 1)[-1].split('-', 1)[0] for p in glob.glob(f'{out}/*-pair*-A.json')}, key=lambda k: (k != 'throughput', k))
    for kind in kinds:
        for scenario in ('read', 'write', 'paged'):
            a, b = load(out, kind, scenario, 'A'), load(out, kind, scenario, 'B')
            if not a or not b:
                continue
            metrics = ['ops', 'cpu_us', 'allocs', 'bytes', 'p90', 'p99'] if kind == 'throughput' else ['ops', 'cpu_us', 'p90', 'p95', 'p99']
            for key in metrics:
                ma, mb = med(a, key), med(b, key)
                ratio = mb / ma if ma else float('nan')
                print(f'| {kind} | {scenario} | {key} | {ma:,.1f} | {mb:,.1f} | {ratio:.3f} |')
            errors = sum(r['errors'] for r in a + b)
            if errors:
                print(f'| {kind} | {scenario} | errors | {sum(r["errors"] for r in a)} | {sum(r["errors"] for r in b)} | |')


if __name__ == '__main__':
    main()
