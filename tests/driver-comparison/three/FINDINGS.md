# Three-driver comparison: findings

gocql master (a61c57f), gocql v2 (this branch), scylla-rust-driver 1.10.0.
Reference host: Ryzen 9 9950X3D, 3 Scylla 2026.3.3 nodes x 4 shards, each on
two whole cores of CCD0; loader on CPU 6 with a verified 0.5 CPU quota.
Protocol and settings: `README.md`. Raw results: `results/remote/final3`
(git-ignored; `REPORT.md` there).

## Results

Max throughput, closed loop, ops/s, median of 2 rounds:

| family | master | v2 | rust | v2/master | rust/master |
|---|---:|---:|---:|---:|---:|
| A point read 1 KiB | 70,460 | 93,613 | 154,910 | 1.33x | 2.20x |
| B partition read 128 x 4 KiB | 1,538 | 1,564 | 1,485 | 1.02x | 0.97x |
| C application paging | 7,725 | 7,824 | 8,477 | 1.01x | 1.10x |
| D point write 1 KiB | 73,955 | 97,032 | 147,401 | 1.31x | 1.99x |
| E1 unlogged batch x 8 | 32,911 | 32,326 | 44,304 | 0.98x | 1.35x |
| E2 logged batch x 4 | 19,227 | 20,263 | 19,636 | 1.05x | 1.02x |
| F LWT | 42,487 | 42,559 | 42,221 | 1.00x | 0.99x |
| G 50/50 read/write | 70,467 | 93,691 | 157,226 | 1.33x | 2.23x |

Client CPU per operation (A, D, G): master ~7.0 µs, v2 ~5.2 µs, rust ~3.2 µs.

Latency, open loop, at the highest stall-free rate (see below), µs
p90 / p95 / p99. A-D and G are at 70 % of the lowest maximum; E1, E2 and F
had to step down:

| family | rate/s | master | v2 | rust |
|---|---:|---|---|---|
| A | 49,322 | 72 / 78 / 1,092 | 72 / 76 / 859 | 95 / 98 / 107 |
| B | 1,040 | 686 / 703 / 744 | 683 / 698 / 724 | 752 / 762 / 779 |
| C | 5,408 | 186 / 192 / 261 | 188 / 193 / 306 | 230 / 235 / 243 |
| D | 51,769 | 110 / 680 / 1,683 | 80 / 457 / 1,799 | 96 / 100 / 1,458 |
| E1 | 3,803 | 81 / 86 / 126 | 82 / 86 / 119 | 124 / 132 / 164 |
| E2 | 9,421 | 167 / 2,234 / 3,451 | 140 / 2,183 / 3,456 | 626 / 2,429 / 3,676 |
| F | 10,137 | 250 / 276 / 2,327 | 245 / 266 / 482 | 291 / 310 / 463 |
| G | 49,327 | 79 / 354 / 1,494 | 78 / 156 / 1,582 | 99 / 106 / 756 |

These latency rows come from a pass where the 50 ms p99 rule was applied to
existing results; a fresh pass under the final rule was started 2026-11-09
(`results/final3-fresh.log`) and replaces them when it finishes.

- Client-bound families (A, D, G): v2 is 1.31-1.33x master, rust ~2x; the
  ratio follows CPU per operation under the 0.5 CPU budget.
- At equal offered load the Go drivers have lower p90 than rust everywhere;
  rust has the tightest tail on A, D and G. v2 is equal to or better than master at
  p90/p95 in every family.
- B, C, E2, F are cluster-bound: throughput within +/-10 % across drivers.

## Methodology findings

- **Closed-loop maximum hides cluster stalls.** Write families (E1, E2, F)
  have Scylla-side stalls where individual requests wait 1-2 s at the
  server (failure-detector pings time out, `Too long queue accumulated`).
  In a closed loop a stall is a handful of slow operations out of ~10^6 and
  vanishes below p99. In an open loop every worker blocks and 10-30 k
  arrivals queue behind it, so p99 becomes the stall and lands on whichever
  driver happens to run. 70 % of the closed-loop maximum is therefore not
  stall-free for these families; stall-free rates were E1 3,803/s,
  E2 9,421/s, F 10,137/s. Data at strict 70 % is kept under
  `unsustained/` for reference.
- **Sustained-rate rule.** A latency rate counts as sustained when no run of
  any driver rejects > 0.5 % of arrivals and no run has p99 > 50 ms;
  otherwise the whole phase repeats from a reset at 0.7x the rate. All
  drivers always run at the same rate.
- **Queue size must scale with rate.** A fixed 1,024-entry arrival queue
  rejected arrivals on any stall longer than 1,024 / rate (0.19 s at
  5,400/s), so rejection counted stalls, not capacity. Latency runs now
  queue 2 s of arrivals.
- **Condition after every reset.** The first heavy write run after a node
  restart stalled (fresh commitlog segments, empty memtables); a 20 s
  unmeasured run of the family's workload after each reset removed those
  failures.
- **Shared disk.** The host NVMe is shared with other tenants (Postgres,
  MinIO); with the commitlog on it, E2 throughput swung 3-9 k ops/s between
  rounds. Commitlog on `/dev/shm` (1 GiB cap per node) made it 19-20 k for
  all drivers. Data files stay on disk; remaining stalls are most likely
  memtable flush backpressure (not confirmed).
- **Server errors are not driver failures.** Scylla's `Operation timed out
  for` / `Operation failed for` responses are counted (`server_errors`) and
  kept in the measurement; any other error invalidates the run.
- **Loader quota is verified per run.** `loader.sh` logs the scope's
  `cpu.max`, `cpuset.cpus.effective` and `cpu.stat`; every run showed
  5000/10000, CPU 6, and at most 0.50 CPU (throttled in ~100 % of periods
  at maximum load).

## Tooling pitfalls

- `systemd-run` expands `${...}` in the command line unless
  `--expand-environment=no` is passed.
- Scylla refuses a commitlog directory it does not own (uid 998); podman
  `--tmpfs` cannot set the owner, so `cluster.sh` bind-mounts a
  pre-chowned `/dev/shm` directory.
- Podman restarts of SIGKILLed nodes leave hints and commitlog work; drain
  before stop.
- `pkill -f` patterns match the invoking shell's own command line; use
  `pgrep -f "three/bench[.]py run"`-style patterns.
- The remote's default rustc (1.93) is too old for scylla 1.10;
  build with `RUSTUP_TOOLCHAIN=1.98.1`.
