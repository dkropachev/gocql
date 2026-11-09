# Three-driver comparison

gocql master, gocql v2 (this checkout) and scylla-rust-driver, on the same
cluster, with the same dataset, workloads, validation, CPU budget and
measurement code. The Go clients are this directory's parent harness
(`main.go`, `old_cases.go`, `cases.go`); the Rust client (`../rust`) is a
port of the same harness: the same flags, key sequences, row validation,
closed- and open-loop phases and JSON measurement fields.

```sh
sh tests/driver-comparison/three/build.sh
sh tests/driver-comparison/three/host.sh setup        # clocks and CPU isolation
export GOCQL_CONTAINER="sudo podman"                  # or docker
sh tests/driver-comparison/three/cluster.sh start
tests/driver-comparison/three/loader.sh .codex-cache/driver-comparison/three/old \
  -hosts 172.30.8.2,172.30.8.3,172.30.8.4 -mode schema
python3 tests/driver-comparison/three/bench.py check tests/driver-comparison/results/three-check
python3 tests/driver-comparison/three/bench.py run tests/driver-comparison/results/three-full
python3 tests/driver-comparison/three/bench.py report tests/driver-comparison/results/three-full
sh tests/driver-comparison/three/cluster.sh stop
sh tests/driver-comparison/three/host.sh restore
```

## Machine

The defaults fit an AMD Ryzen 9 9950X3D (16 Zen 5 cores, two CCDs). Every
vCPU of the test is on the first CCD, so all are the same core type behind
the same L3; the second CCD runs everything else.

| | |
|---|---|
| cluster | 3 Scylla nodes (`scylladb/scylla:2026.3.3`), `--smp 4` (4 shards), 4 GiB Scylla memory in a 6 GiB container, developer mode, `--overprovisioned 0` |
| node CPUs | two whole cores each, both SMT threads: node1 CPUs 0,1,16,17, node2 2,3,18,19, node3 4,5,20,21 (`GOCQL_THREE_NODE_CPUS`) |
| loader | one client process at a time, in its own systemd scope: CPU 6 (core 6; its SMT sibling 22 stays idle; `GOCQL_LOADER_CPU`), `CPUQuota=50%` with a 10 ms period (5 ms of CPU per 10 ms). Go clients run with `GOMAXPROCS=1`, the Rust client on a current-thread Tokio runtime. Every client checks once a second that all its threads may run only on its CPU |
| host | `host.sh setup`: performance governor and energy policy, boost off (fixed base clock, no thermal or power throttling mid-run); user.slice, system.slice and init.scope confined to the second CCD; the containers and the loader run in `gocqlbench.slice` on the first. Each run records the benchmark CPUs' clocks and the CPU temperature before and after (`*.host-before`, `*.host-after`) |

## Driver settings

All three: protocol v4, `LOCAL_QUORUM`, RF=3 in `datacenter1`, token- and
DC-aware routing, one connection per shard, no compression, no retries, no
speculative execution, client-side timestamps, page size 32 without
prefetch, a 10 s timeout per operation. Every operation validates every row
it reads (key, row id, length and SHA-256 of the payload), copying the blob
into reused owned storage first.

## Cases

| family | workers | master (`old_cases.go`) | v2 (`cases.go`) | rust (`rust/src/main.rs`) |
|---|---:|---|---|---|
| A point read, 1 KiB | 64 | MA1: reused template `Bind(k).Scan` | A1: reused `Query.One` | RA1: `execute_unpaged(prepared, (k,))` |
| B partition read, 128 × 4 KiB, page 32 | 4 | MB1: `Iter` + `Scan` loop | B1: `Query.Each` | RB1: `execute_single_page` loop until no more pages |
| C application paging, one 32-row page per op | 64 | MC1: `PageState(st).Iter()` | C1: `RunPage` + `RestorePageState` | RC1: `execute_single_page` from `PagingState::new_from_raw_bytes` |
| D point write, 1 KiB | 64 | MD1: reused template `Bind(...).Exec` | D1: reused `Query.Exec` | RD1: `execute_unpaged(prepared, (k, 0, p))` |
| E1 unlogged batch, 8 rows of one partition | 8 | ME1: `Batch` of 8 `pageWrite` | E1: 8 `AddToBatch` + `ExecBatch` | RE1: reused `Batch` of 8 prepared statements |
| E2 logged batch, 4 point rows | 64 | ME2 | E2 | RE2 |
| F LWT `UPDATE ... IF v=?`, `LOCAL_SERIAL`, must apply | 64 | MF1: `ScanCAS` | F1: `Query.One` into `CASResult` | RF1: `[applied]` of the first row |
| G 50/50 point read/write | 64 | MG1 | G1 | RG1 |

Workers and keys are those of the parent harness (`README-CASES.md`): B uses
4 workers because each operation reads 512 KiB, E1 8 because 64 concurrent
32 KiB batches exceed Scylla's in-flight hint budget, F at most 64 because
each worker owns 64 of the 4096 LWT keys.

## Protocol

For each family (`bench.py`):

1. Reset: truncate and reload the family's table (`-mode prepare`, paced at
   20,000 point rows/s or 5,000 paged rows/s), `nodetool flush` on every
   node, wait for compactions to finish, restart all nodes, then read the
   whole dataset once to warm the caches.
2. Throughput (maximum): closed loop with the family's workers, 10 s warmup
   and 30 s measurement, `GOCQL_THREE_ROUNDS` (2) rounds per driver in
   rotating order (`old v2 rust`, then `rust v2 old`). Between runs every
   node flushes and compactions settle, so no run inherits another's
   background work.
3. Latency: the same reset, then an open loop at 70 % of the lowest of the
   three drivers' median maximum throughputs: the same scheduled rate for
   all three, so latencies compare at equal offered load. Arrivals are
   evenly scheduled into a bounded queue; latency is measured from each
   arrival's scheduled time, so client queueing counts.

Every run reports throughput (successful operations completed inside the
measurement window per second) and latency p90, p95 and p99 (HDR histogram,
1 µs to 60 s, three significant digits). The report takes medians over
rounds.
