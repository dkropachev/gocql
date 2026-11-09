#!/usr/bin/env python3
"""Three-driver comparison: gocql master, gocql v2 and scylla-rust-driver.

  three/bench.py check OUT              validate every case of every driver
  three/bench.py run OUT [FAMILY...]    measure (default: every family)
  three/bench.py report OUT             print the comparison tables

Each family is one use case implemented by all three drivers (README.md in
this directory has the pseudocode). For every family:

  1. reset: truncate and reload the family's table, flush, settle
     compactions, restart all nodes, warm the cache by reading the dataset,
     then one unmeasured CONDITION run of the workload, flush and settle;
  2. throughput: closed loop with the family's workers, ROUNDS rounds per
     driver, drivers in rotating order (old v2 rust, rust v2 old, ...),
     flushing and settling compactions between runs;
  3. latency: open loop at FRACTION of the lowest driver's median throughput,
     the same scheduled rate for all drivers, same rotation; if any run
     rejects more than MAX_REJECTED of its arrivals, the cluster did not
     sustain the rate, and the phase repeats from a reset at STEP x the rate.

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
# Families whose latency runs at a fixed rate instead of FRACTION of the
# lowest maximum. On the reference host, E1's closed-loop maximum is the
# cluster's: unlogged 32 KiB single-partition batches at 2,000/s and more
# stall the nodes for seconds (commitlog queues, failure-detector RPC
# timeouts, in-flight hint limits), so 70 % of it measures the disk and the
# container network. 1,000/s is sustained by every driver without errors.
FIXED_RATE = {k: float(v) for k, v in (item.split('=', 1) for item in os.environ.get(
    'GOCQL_THREE_FIXED_RATE', 'E1=1000').split(';') if item)}
SKIP_LATENCY = {}
# A latency rate is sustained when no run of any driver rejects more than
# MAX_REJECTED of its scheduled arrivals. The closed-loop maximum can exceed
# what the cluster sustains in an open loop (a closed loop backs off while
# nodes flush and compact; an open loop keeps arriving), so an unsustained
# rate is lowered by STEP and the whole latency phase (reset, conditioning,
# every round of every driver) runs again, up to MAX_ATTEMPTS times. Every
# driver is always measured at the same rate.
MAX_REJECTED = float(os.environ.get('GOCQL_THREE_MAX_REJECTED', '0.005'))
STEP = float(os.environ.get('GOCQL_THREE_STEP', '0.7'))
MAX_ATTEMPTS = int(os.environ.get('GOCQL_THREE_MAX_ATTEMPTS', '8'))
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


CONDITION = os.environ.get('GOCQL_THREE_CONDITION', '20s')


def condition(out, family, kind):
    """One unmeasured closed-loop run of the family's workload (master's
    case) after a reset, then flush and settle. Restarted nodes allocate fresh
    commitlog segments and fill empty memtables on their first heavy writes,
    which stalled the first measured run of every write family; conditioning
    pays that once, so every measured round, whichever driver it is, starts
    from the same warmed state."""
    workers, _, cases, _ = FAMILIES[family]
    rc = loader('old', '-mode', 'run', '-case', cases['old'], '-concurrency', str(workers),
                '-warmup', '0s', '-duration', CONDITION, '-rate', '0',
                '-output', os.path.join(out, f'condition-{kind}-{family}.json'),
                check=False, out=os.path.join(out, f'condition-{kind}-{family}.log'))
    log(f'family {family}: conditioned for {kind} ({CONDITION}, rc={rc})')
    between_runs()


def between_runs():
    """Writes of the previous run go to disk and their compactions finish, so
    that no run inherits another's background work."""
    cluster('flush')
    cluster('settle')


def order(round_index):
    return DRIVERS if round_index % 2 == 0 else DRIVERS[::-1]


LOADER_CPU = os.environ.get('GOCQL_LOADER_CPU', '6')
LOADER_QUOTA = 0.5  # three/loader.sh: CPUQuota=50%, 10 ms period


def loader_check(log_path):
    """The scope's enforced quota and CPU set, and the client's CPU use, as
    three/loader.sh wrote them; problems lists every deviation."""
    info, problems = {}, []
    for line in open(log_path, errors='replace'):
        if line.startswith(('LOADER-CGROUP ', 'LOADER-CPU ')):
            info.update(kv.split('=', 1) for kv in line.split()[1:])
    want_max = f'{int(LOADER_QUOTA * 10000)}/10000'
    if info.get('cpu.max') != want_max:
        problems.append(f'cpu.max {info.get("cpu.max")} != {want_max}')
    if info.get('cpuset') != LOADER_CPU:
        problems.append(f'cpuset {info.get("cpuset")} != {LOADER_CPU}')
    if 'usage_usec' not in info:
        problems.append('no LOADER-CPU accounting')
        return info, problems
    for k in ('usage_usec', 'nr_periods', 'nr_throttled', 'throttled_usec', 'elapsed_usec'):
        info[k] = int(info[k])
    info['cpu_share'] = info['usage_usec'] / info['elapsed_usec']
    info['throttled_share'] = info['nr_throttled'] / max(info['nr_periods'], 1)
    # One period of slack on top of the quota, for the start and end periods.
    if info['cpu_share'] > LOADER_QUOTA * 1.02:
        problems.append(f'CPU share {info["cpu_share"]:.3f} exceeds the {LOADER_QUOTA} quota')
    return info, problems


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
    info, problems = loader_check(path + '.log')
    m = json.load(open(path))['measurement'] if os.path.exists(path) else None
    valid = bool(m) and rc == 0 and not problems
    json.dump({'loader': info, 'problems': problems, 'rc': rc, 'valid': valid}, open(path + '.loader', 'w'), indent=2)
    name = f'{kind} {family} r{round_index + 1} {driver}'
    if valid:
        log(f'{name}: {m["in_window"] / m["duration_seconds"]:,.0f} ops/s '
            f'p90 {m["latency"]["p90_us"]} p95 {m["latency"]["p95_us"]} p99 {m["latency"]["p99_us"]} us '
            f'server errors {m.get("server_errors", 0)}, loader CPU {info["cpu_share"]:.3f} '
            f'throttled {info["throttled_share"]:.1%} of periods')
    else:
        log(f'{name}: INVALID rc={rc} {"; ".join(problems)}, see {path}.log')
    between_runs()


def valid_runs(out, kind, family, driver):
    """Result files of the runs that exited cleanly with the loader quota verified."""
    paths = sorted(glob.glob(os.path.join(out, f'{kind}-{family}-r*-{driver}.json')))
    return [p for p in paths if os.path.exists(p + '.loader') and json.load(open(p + '.loader'))['valid']]


def invalid_runs(out):
    runs = []
    for p in sorted(glob.glob(os.path.join(out, '*-r*-*.json.log'))):
        q = p[:-len('.log')]
        v = json.load(open(q + '.loader')) if os.path.exists(q + '.loader') else None
        if not v or not v['valid']:
            runs.append((os.path.basename(q), v))
    return runs


def measurement(path):
    return json.load(open(path))['measurement']


def throughput(out, family, driver):
    rates = [m['in_window'] / m['duration_seconds'] for m in
             map(measurement, valid_runs(out, 'throughput', family, driver))]
    return statistics.median(rates) if rates else None


def family_run(out, family):
    workers, table, cases, description = FAMILIES[family]
    log(f'family {family}: {description}, {workers} workers')
    done = all(os.path.exists(os.path.join(out, f'throughput-{family}-r{r + 1}-{d}.json'))
               for r in range(ROUNDS) for d in DRIVERS)
    if not done:
        log(f'family {family}: resetting {table}')
        reset(table)
        condition(out, family, 'throughput')
        for r in range(ROUNDS):
            for driver in order(r):
                run_one(out, 'throughput', family, r, driver, 0)
    if family in SKIP_LATENCY:
        log(f'family {family}: latency not measured: {SKIP_LATENCY[family]}')
        return
    maxima = {d: throughput(out, family, d) for d in DRIVERS}
    if any(v is None for v in maxima.values()):
        log(f'family {family}: missing throughput results {maxima}; skipping latency')
        return
    rate = round(FRACTION * min(maxima.values()), 1)
    fixed = family in FIXED_RATE
    if fixed:
        rate = FIXED_RATE[family]
    rate_path = os.path.join(out, f'rate-{family}.json')
    attempts = json.load(open(rate_path)).get('attempts', []) if os.path.exists(rate_path) else []
    for attempt in range(len(attempts), MAX_ATTEMPTS):
        if attempt:
            rate = round(STEP * attempts[-1]['rate'], 1)
        json.dump({'maxima': maxima, 'fraction': None if fixed else FRACTION, 'rate': rate, 'attempts': attempts},
                  open(rate_path, 'w'), indent=2)
        if fixed:
            log(f'family {family}: latency rate {rate}/s (fixed)')
        elif attempt:
            log(f'family {family}: latency rate {rate}/s = {STEP} x {attempts[-1]["rate"]}/s, which was not sustained')
        else:
            log(f'family {family}: latency rate {rate}/s = {FRACTION} x min{maxima}')
        # The latency runs start from the same state as the throughput runs.
        if not all(os.path.exists(os.path.join(out, f'latency-{family}-r{r + 1}-{d}.json'))
                   for r in range(ROUNDS) for d in DRIVERS):
            reset(table)
            condition(out, family, 'latency')
            for r in range(ROUNDS):
                for driver in order(r):
                    run_one(out, 'latency', family, r, driver, rate)
        worst = max(rejected_share(p) for d in DRIVERS
                    for p in glob.glob(os.path.join(out, f'latency-{family}-r*-{d}.json')))
        sustained = fixed or worst <= MAX_REJECTED
        attempts.append({'rate': rate, 'worst_rejected_share': worst, 'sustained': sustained})
        json.dump({'maxima': maxima, 'fraction': None if fixed else FRACTION, 'rate': rate, 'attempts': attempts},
                  open(rate_path, 'w'), indent=2)
        if sustained:
            log(f'family {family}: {rate}/s sustained (worst rejected share {worst:.3%})')
            return
        # Keep the unsustained attempt for the record, out of the report's glob.
        keep = os.path.join(out, 'unsustained', f'{family}-{rate:g}')
        os.makedirs(keep, exist_ok=True)
        for p in glob.glob(os.path.join(out, f'latency-{family}-r*')) + glob.glob(os.path.join(out, f'condition-latency-{family}.*')):
            os.rename(p, os.path.join(keep, os.path.basename(p)))
        log(f'family {family}: {rate}/s not sustained (worst rejected share {worst:.3%} > {MAX_REJECTED:.1%})')
    log(f'family {family}: no sustained latency rate in {MAX_ATTEMPTS} attempts')


def rejected_share(path):
    """Share of scheduled arrivals that a run turned away because its queue
    was full or they were already late: the offered load was not served."""
    m = measurement(path)
    return m.get('rejected', 0) / max(m.get('offered', 0), 1)


def check(out):
    os.makedirs(out, exist_ok=True)
    for table in ('read', 'paged', 'lwt'):
        loader('old', '-mode', 'prepare', '-scenario', table, quota='100%')
    for family, (_, _, cases, _) in FAMILIES.items():
        for driver in DRIVERS:
            loader(driver, '-mode', 'check', '-case', cases[driver], quota='100%',
                   out=os.path.join(out, f'check-{family}-{driver}.log'))
            log(f'check {family} {driver} {cases[driver]}: ok')


def loader_columns(path):
    info = json.load(open(path + '.loader'))['loader'] if os.path.exists(path + '.loader') else {}
    return {'loader_cpu': info.get('cpu_share', float('nan')), 'throttled': info.get('throttled_share', float('nan'))}


def summary(paths):
    rows = []
    for p in paths:
        m = measurement(p)
        rows.append({'ops': m['in_window'] / m['duration_seconds'], 'p90': m['latency']['p90_us'],
                     'p95': m['latency']['p95_us'], 'p99': m['latency']['p99_us'],
                     'svc90': m['dispatch_latency']['p90_us'], 'svc99': m['dispatch_latency']['p99_us'],
                     'cpu_us': 1e6 * m['cpu_seconds_including_drain'] / max(m['completed'], 1),
                     'errors': m['errors'], 'server_errors': m.get('server_errors', 0),
                     'rejected': m.get('rejected', 0), **loader_columns(p)})
    if not rows:
        return None
    return {k: statistics.median(r[k] for r in rows) for k in rows[0]} | {'n': len(rows)}


def report(out):
    print(f'# Three-driver comparison: {out}\n')
    print('Latency p90/p95/p99 run from each arrival\'s scheduled time (client queueing included);')
    print('service latency runs from the moment a worker starts the operation. Medians over rounds.\n')
    for kind in ('throughput', 'latency'):
        print(f'## {kind}\n')
        print('| family | case | workers | rate/s | driver | ops/s | p90 µs | p95 µs | p99 µs | service p90/p99 µs | CPU µs/op | loader CPU / throttled periods | vs master ops | server timeouts/failures | other errors | rejected |')
        print('|---|---|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|')
        for family, (workers, _, cases, description) in FAMILIES.items():
            rate = ''
            rf = os.path.join(out, f'rate-{family}.json')
            if kind == 'latency' and os.path.exists(rf):
                rate = f'{json.load(open(rf))["rate"]:,.0f}'
            if kind == 'latency' and family in SKIP_LATENCY:
                print(f'| {family} | {description} | {workers} | - | all | not measured: {SKIP_LATENCY[family]} |||||||||||')
                continue
            stats = {d: summary(valid_runs(out, kind, family, d)) for d in DRIVERS}
            base = stats['old']
            for d in DRIVERS:
                s = stats[d]
                if not s:
                    continue
                ratio = f'{s["ops"] / base["ops"]:.2f}x' if base and base['ops'] else ''
                print(f'| {family} | {description} | {workers} | {rate if kind == "latency" else "max"} | {d} | '
                      f'{s["ops"]:,.0f} | {s["p90"]:,.0f} | {s["p95"]:,.0f} | {s["p99"]:,.0f} | {s["svc90"]:,.0f}/{s["svc99"]:,.0f} | '
                      f'{s["cpu_us"]:,.1f} | {s["loader_cpu"]:.2f} / {s["throttled"]:.0%} | {ratio} | '
                      f'{s["server_errors"]:.0f} | {s["errors"] - s["server_errors"]:.0f} | {s["rejected"]:.0f} |')
        print()
    print('## latency rates\n')
    print(f'Start at {FRACTION} x the lowest median maximum; a rate is sustained when no run rejects more than '
          f'{MAX_REJECTED:.1%} of its arrivals, otherwise the whole phase repeats from a reset at {STEP} x the rate. '
          'Unsustained attempts are kept under unsustained/.\n')
    print('| family | lowest maximum | attempts (rate/s: worst rejected share) | measured at |')
    print('|---|---:|---|---:|')
    for family in FAMILIES:
        rf = os.path.join(out, f'rate-{family}.json')
        if not os.path.exists(rf):
            continue
        r = json.load(open(rf))
        tries = ', '.join(f'{a["rate"]:,.0f}: {a["worst_rejected_share"]:.2%}{"" if a["sustained"] else " (not sustained)"}'
                          for a in r.get('attempts', []))
        print(f'| {family} | {min(r["maxima"].values()):,.0f} | {tries} | {r["rate"]:,.0f}{"" if r["fraction"] else " (fixed)"} |')
    print()
    bad = invalid_runs(out)
    print('## invalid runs\n')
    print('none: every run exited cleanly with the loader quota and CPU set verified\n' if not bad else
          '\n'.join(f'- {name}: {v and {k: v[k] for k in ("rc", "problems")}}' for name, v in bad) + '\n')


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
