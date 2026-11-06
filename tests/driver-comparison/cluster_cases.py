#!/usr/bin/env python3
"""Case table of cluster_bench.sh: master cases and the v2 cases timed against them.

  cluster_cases.py sequence CASES.TSV PAIRS [MASTER...]
      one line per run, in run order: MASTER WORKERS PAIR LABEL CASE
      (LABEL A runs the master binary, B the v2 binary)
  cluster_cases.py cases CASES.TSV DRIVER [MASTER...]
      the case names of one driver (master or v2), once each
  cluster_cases.py rate OUT MASTER FRACTION
      FRACTION times the median throughput of MASTER's throughput runs in OUT
"""
import glob
import json
import statistics
import sys


def load(path):
    """Groups in file order: [(master, workers, [v2 cases])]."""
    groups, index = [], {}
    with open(path) as f:
        for line in f:
            line = line.split('#', 1)[0].strip()
            if not line:
                continue
            master, v2, workers = line.split('\t')[:3]
            workers = int(workers)
            if master not in index:
                index[master] = len(groups)
                groups.append((master, workers, []))
            group = groups[index[master]]
            if group[1] != workers:
                raise ValueError(f'{master}: workers {workers} differ from {group[1]}')
            if v2 in group[2]:
                raise ValueError(f'{master}: duplicate v2 case {v2}')
            group[2].append(v2)
    return groups


def select(groups, masters):
    if not masters:
        return groups
    unknown = set(masters) - {g[0] for g in groups}
    if unknown:
        raise ValueError(f'unknown master cases: {sorted(unknown)}')
    return [g for g in groups if g[0] in masters]


def order(master, v2):
    """One mirrored round: A B1..Bn Bn..B1 A."""
    forward = [('B', c) for c in v2]
    return [('A', master)] + forward + forward[::-1] + [('A', master)]


def sequence(groups, pairs):
    for master, workers, v2 in groups:
        for pair in range(1, pairs + 1):
            for label, case in order(master, v2):
                yield master, workers, pair, label, case


def rate(out, master, fraction):
    rates = []
    for path in glob.glob(f'{out}/throughput-{master}-pair*-{master}.json'):
        with open(path) as f:
            m = json.load(f)['measurement']
        rates.append(m['in_window'] / m['duration_seconds'])
    if not rates:
        raise ValueError(f'no throughput results for {master} in {out}')
    return round(fraction * statistics.median(rates), 2)


def main(argv):
    command = argv[1]
    if command == 'sequence':
        for row in sequence(select(load(argv[2]), argv[4:]), int(argv[3])):
            print(*row)
    elif command == 'cases':
        groups = select(load(argv[2]), argv[4:])
        if argv[3] == 'master':
            print(*(g[0] for g in groups))
        else:
            print(*(c for g in groups for c in g[2]))
    elif command == 'rate':
        print(rate(argv[2], argv[3], float(argv[4])))
    else:
        raise SystemExit(__doc__)


if __name__ == '__main__':
    main(sys.argv)
