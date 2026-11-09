#!/usr/bin/env python3
"""Three-driver comparison: gocql master, gocql v2 and scylla-rust-driver.

  three/bench.py check OUT              validate every case of every driver
  three/bench.py run OUT [FAMILY...]    measure (default: every family)
  three/bench.py report OUT             print the comparison tables

Each family is one use case implemented by all three drivers (README.md in
this directory has the pseudocode). For every family:

  1. reset: truncate and reload the family's table, flush, settle
     compactions, restart all nodes, warm the cache by reading the dataset;
  2. throughput: closed loop with the family's workers, ROUNDS rounds per
     driver, drivers in rotating order (old v2 rust, rust v2 old, ...),
     flushing and settling compactions between runs;
  3. latency: open loop at FRACTION of the lowest driver's median throughput,
     the same scheduled rate for all drivers, same rotation.

Every client runs through three/loader.sh: one dedicated CPU, half a CPU of
budget, GOMAXPROCS=1 (Go) or a current-thread runtime (Rust). three/host.sh
setup must have fixed the clocks and confined other processes first. The binaries are built by
three/build.sh into .codex-cache/driver-comparison/three.
"""
import glob
import json
import os
import statistics
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, '..', '..', '..'))
BIN = os.path.join(ROOT, '.codex-cache', 'driver-comparison', 'three')
HOSTS = os.environ.get('GOCQL_THREE_HOSTS', '172.30.8.2,172.30.8.3,172.30.8.4')
WARMUP = os.environ.get('GOCQL_THREE_WARMUP', '10s')
DURATION = os.environ.get('GOCQL_THREE_DURATION', '30s')
ROUNDS = int(os.environ.get('GOCQL_THREE_ROUNDS', '2'))
FRACTION = float(os.environ.get('GOCQL_THREE_FRACTION', '0.7'))
DRIVERS = ['old', 'v2', 'rust']

# family: (workers, table to reset, {driver: case}, description)
FAMILIES = {
    'A': (64, 'read', {'old': 'MA1', 'v2': 'A1', 'rust': 'RA1'}, 'point read, 1 KiB row, reused prepared statement'),
    'B': (4, 'paged', {'old': 'MB1', 'v2': 'B1', 'rust': 'RB1'}, 'partition read, 128 x 4 KiB rows, page size 32, no prefetch'),
    'C': (64, 'paged', {'old': 'MC1', 'v2': 'C1', 'rust': 'RC1'}, 'application paging, one 32-row page per op from a saved state'),
    'D': (64, 'read', {'old': 'MD1', 'v2': 'D1', 'rust': 'RD1'}, 'point write, 1 KiB row'),
    'E1': (8, 'paged', {'old': 'ME1', 'v2': 'E1', 'rust': 'RE1'}, 'unlogged batch, 8 rows of one partition'),
    'E2': (64, 'read', {'old': 'ME2', 'v2': 'E2', 'rust': 'RE2'}, 'logged batch, 4 point rows'),
    'F': (64, 'lwt', {'old': 'MF1', 'v2': 'F1', 'rust': 'RF1'}, 'LWT UPDATE ... IF, LOCAL_SERIAL'),
    'G': (64, 'read', {'old': 'MG1', 'v2': 'G1', 'rust': 'RG1'}, '50/50 point read/write'),
}


def log(msg):
    print(time.strftime('%H:%M:%S'), msg, flush=True)


def loader(binary, *args, quota='50%', check=True, out=None):
    env = dict(os.environ, GOCQL_LOADER_QUOTA=quota)
    cmd = [os.path.join(HERE, 'loader.sh'), os.path.join(BIN, binary), '-hosts', HOSTS, *args]
    with open(out, 'w') if out else open(os.devnull, 'w') as f:
        p = subprocess.run(cmd, env=env, stdout=subprocess.PIPE, stderr=f if out else subprocess.PIPE, text=True)
    if check and p.returncode != 0:
        detail = open(out).read()[-400:] if out else p.stderr[-400:]
        raise RuntimeError(f'{binary} {" ".join(args)} failed: {detail}')
    return p.returncode


def cluster(*args):
    subprocess.run(['sh', os.path.join(HERE, 'cluster.sh'), *args], check=True, stdout=subprocess.DEVNULL)


def host_snapshot(path):
    """Clocks and temperature of the benchmark CPUs (three/host.sh)."""
    with open(path, 'w') as f:
        subprocess.run(['sh', os.path.join(HERE, 'host.sh'), 'snapshot'], stdout=f, stderr=subprocess.STDOUT)


def reset(table):
    """Truncate and reload table, persist it, settle and restart the nodes,
    then warm the cache by reading the whole dataset."""
    # Setup is untimed; it gets a whole CPU so loading keeps its paced rate.
    loader('old', '-mode', 'prepare', '-scenario', table, quota='100%')
    cluster('clearsnapshot')
    cluster('flush')
    cluster('settle')
    cluster('restart')
    if table in ('read', 'paged'):
        loader('old', '-mode', 'warm', '-scenario', table, quota='100%')


def between_runs():
    """Writes of the previous run go to disk and their compactions finish, so
    that no run inherits another's background work."""
    cluster('flush')
    cluster('settle')


def order(round_index):
    return DRIVERS if round_index % 2 == 0 else DRIVERS[::-1]


def run_one(out, kind, family, round_index, driver, rate):
    workers, _, cases, _ = FAMILIES[family]
    path = os.path.join(out, f'{kind}-{family}-r{round_index + 1}-{driver}.json')
    if os.path.exists(path):
        log(f'{kind} {family} r{round_index + 1} {driver}: kept')
        return
    host_snapshot(path + '.host-before')
    rc = loader(driver, '-mode', 'run', '-case', cases[driver], '-concurrency', str(workers),
                '-warmup', WARMUP, '-duration', DURATION, '-rate', str(rate), '-output', path,
                check=False, out=path + '.log')
    host_snapshot(path + '.host-after')
    m = json.load(open(path))['measurement'] if os.path.exists(path) else None
    if m:
        log(f'{kind} {family} r{round_index + 1} {driver}: {m["in_window"] / m["duration_seconds"]:,.0f} ops/s '
            f'p90 {m["latency"]["p90_us"]} p95 {m["latency"]["p95_us"]} p99 {m["latency"]["p99_us"]} us '
            f'errors {m["errors"]}' + ('' if rc == 0 else ' FAILED'))
    else:
        log(f'{kind} {family} r{round_index + 1} {driver}: FAILED, see {path}.log')
    between_runs()


def measurement(path):
    return json.load(open(path))['measurement']


def throughput(out, family, driver):
    rates = [m['in_window'] / m['duration_seconds'] for m in
             map(measurement, glob.glob(os.path.join(out, f'throughput-{family}-r*-{driver}.json')))]
    return statistics.median(rates) if rates else None


def family_run(out, family):
    workers, table, cases, description = FAMILIES[family]
    log(f'family {family}: {description}, {workers} workers; resetting {table}')
    reset(table)
    for r in range(ROUNDS):
        for driver in order(r):
            run_one(out, 'throughput', family, r, driver, 0)
    maxima = {d: throughput(out, family, d) for d in DRIVERS}
    if any(v is None for v in maxima.values()):
        log(f'family {family}: missing throughput results {maxima}; skipping latency')
        return
    rate = round(FRACTION * min(maxima.values()), 1)
    json.dump({'maxima': maxima, 'fraction': FRACTION, 'rate': rate},
              open(os.path.join(out, f'rate-{family}.json'), 'w'), indent=2)
    log(f'family {family}: latency rate {rate}/s = {FRACTION} x min{maxima}')
    # The latency runs start from the same state as the throughput runs.
    reset(table)
    for r in range(ROUNDS):
        for driver in order(r):
            run_one(out, 'latency', family, r, driver, rate)


def check(out):
    os.makedirs(out, exist_ok=True)
    for table in ('read', 'paged', 'lwt'):
        loader('old', '-mode', 'prepare', '-scenario', table, quota='100%')
    for family, (_, _, cases, _) in FAMILIES.items():
        for driver in DRIVERS:
            loader(driver, '-mode', 'check', '-case', cases[driver], quota='100%',
                   out=os.path.join(out, f'check-{family}-{driver}.log'))
            log(f'check {family} {driver} {cases[driver]}: ok')


def summary(paths):
    rows = []
    for p in paths:
        m = measurement(p)
        rows.append({'ops': m['in_window'] / m['duration_seconds'], 'p90': m['latency']['p90_us'],
                     'p95': m['latency']['p95_us'], 'p99': m['latency']['p99_us'],
                     'svc90': m['dispatch_latency']['p90_us'], 'svc99': m['dispatch_latency']['p99_us'],
                     'cpu_us': 1e6 * m['cpu_seconds_including_drain'] / max(m['completed'], 1),
                     'errors': m['errors'], 'rejected': m.get('rejected', 0)})
    if not rows:
        return None
    return {k: statistics.median(r[k] for r in rows) for k in rows[0]} | {'n': len(rows)}


def report(out):
    print(f'# Three-driver comparison: {out}\n')
    print('Latency p90/p95/p99 run from each arrival\'s scheduled time (client queueing included);')
    print('service latency runs from the moment a worker starts the operation. Medians over rounds.\n')
    for kind in ('throughput', 'latency'):
        print(f'## {kind}\n')
        print('| family | case | workers | rate/s | driver | ops/s | p90 µs | p95 µs | p99 µs | service p90/p99 µs | CPU µs/op | vs master ops | errors | rejected |')
        print('|---|---|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|')
        for family, (workers, _, cases, description) in FAMILIES.items():
            rate = ''
            rf = os.path.join(out, f'rate-{family}.json')
            if kind == 'latency' and os.path.exists(rf):
                rate = f'{json.load(open(rf))["rate"]:,.0f}'
            stats = {d: summary(sorted(glob.glob(os.path.join(out, f'{kind}-{family}-r*-{d}.json')))) for d in DRIVERS}
            base = stats['old']
            for d in DRIVERS:
                s = stats[d]
                if not s:
                    continue
                ratio = f'{s["ops"] / base["ops"]:.2f}x' if base and base['ops'] else ''
                print(f'| {family} | {description} | {workers} | {rate if kind == "latency" else "max"} | {d} | '
                      f'{s["ops"]:,.0f} | {s["p90"]:,.0f} | {s["p95"]:,.0f} | {s["p99"]:,.0f} | {s["svc90"]:,.0f}/{s["svc99"]:,.0f} | '
                      f'{s["cpu_us"]:,.1f} | {ratio} | {s["errors"]:.0f} | {s["rejected"]:.0f} |')
        print()


def main(argv):
    if len(argv) < 3:
        raise SystemExit(__doc__)
    command, out = argv[1], argv[2]
    os.makedirs(out, exist_ok=True)
    if command == 'check':
        check(out)
    elif command == 'run':
        families = argv[3:] or list(FAMILIES)
        for family in families:
            family_run(out, family)
        log('THREE-BENCH-DONE')
    elif command == 'report':
        report(out)
    else:
        raise SystemExit(__doc__)


if __name__ == '__main__':
    main(sys.argv)
