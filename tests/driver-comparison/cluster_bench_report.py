#!/usr/bin/env python3
"""Summarise cluster_bench.sh output: per master case, medians of the master
case and of every v2 case timed against it, with v2/master ratios."""
import glob
import json
import os
import re
import statistics
import sys

NAME = re.compile(r'^(?P<kind>[a-z]+\d*)-(?P<master>M[A-Z]\d+)-pair(?P<pair>\d+)-(?P<case>[A-Z]+\d+)\.json$')
THROUGHPUT = ['ops', 'cpu_us', 'allocs', 'bytes', 'p90', 'p99']
LATENCY = ['ops', 'cpu_us', 'p90', 'p95', 'p99']


def summarise(data):
    m = data['measurement']
    done = max(m['completed'], 1)
    return {
        'ops': m['in_window'] / m['duration_seconds'],
        'cpu_us': 1e6 * m['cpu_seconds_including_drain'] / done,
        'allocs': m['mallocs'] / done,
        'bytes': m['alloc_bytes'] / done,
        'p90': m['latency']['p90_us'], 'p95': m['latency']['p95_us'], 'p99': m['latency']['p99_us'],
        'errors': m['errors'],
    }


def collect(out):
    """{kind: {master: {case: [run summaries]}}}, in file-name order."""
    runs = {}
    for path in sorted(glob.glob(os.path.join(out, '*-pair*-*.json'))):
        match = NAME.match(os.path.basename(path))
        if not match:
            continue
        with open(path) as f:
            data = json.load(f)
        runs.setdefault(match['kind'], {}).setdefault(match['master'], {}).setdefault(match['case'], []).append(summarise(data))
    return runs


def median(rows, key):
    return statistics.median(r[key] for r in rows) if rows else float('nan')


def table(kind, master, cases):
    """Markdown rows: one per metric; master, then each v2 case and its ratio."""
    v2 = [c for c in cases if c != master]
    metrics = THROUGHPUT if kind == 'throughput' else LATENCY
    lines = [f'#### {kind} {master}', '',
             '| metric | ' + ' | '.join([master] + [f'{c} | {c}/{master}' for c in v2]) + ' |',
             '|---|' + '---:|' * (1 + 2 * len(v2))]
    base_rows = cases.get(master, [])
    for key in metrics:
        base = median(base_rows, key)
        cells = [f'{base:,.1f}']
        for c in v2:
            value = median(cases[c], key)
            cells += [f'{value:,.1f}', f'{value / base:.3f}' if base else 'nan']
        lines.append(f'| {key} | ' + ' | '.join(cells) + ' |')
    errors = {c: sum(r['errors'] for r in rows) for c, rows in cases.items()}
    if any(errors.values()):
        lines.append('| errors | ' + ' | '.join([str(errors.get(master, 0))] + [f'{errors[c]} | ' for c in v2]) + ' |')
    return lines + ['']


def report(out):
    runs = collect(out)
    lines = []
    failures = os.path.join(out, 'failures.log')
    if os.path.exists(failures):
        with open(failures) as f:
            lines += ['Failed runs (no result, or a result with errors):', ''] + [f'- {l.strip()}' for l in f if l.strip()] + ['']
    for kind in sorted(runs, key=lambda k: (k != 'throughput', k)):
        for master, cases in runs[kind].items():
            lines += table(kind, master, cases)
    return '\n'.join(lines)


if __name__ == '__main__':
    print(report(sys.argv[1]))
