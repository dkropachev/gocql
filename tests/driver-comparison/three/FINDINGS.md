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

Latency, open loop, at the highest sustained rate (rule below), µs
p90 / p95 / p99, median of 2 rounds. A-D, F and G held 70 % of the lowest
maximum; E1 and E2 stepped down (attempts in the next table):

| family | rate/s | % of lowest max | master | v2 | rust |
|---|---:|---:|---|---|---|
| A | 49,322 | 70 % | 72 / 78 / 1,122 | 72 / 76 / 846 | 95 / 99 / 107 |
| B | 1,040 | 70 % | 692 / 707 / 747 | 690 / 705 / 731 | 748 / 758 / 775 |
| C | 5,408 | 70 % | 187 / 192 / 291 | 188 / 194 / 323 | 230 / 236 / 243 |
| D | 51,769 | 70 % | 113 / 688 / 1,656 | 80 / 444 / 1,745 | 97 / 101 / 1,465 |
| E1 | 3,803 | 12 % | 83 / 92 / 822 | 82 / 86 / 117 | 126 / 133 / 161 |
| E2 | 6,595 | 34 % | 126 / 541 / 3,413 | 132 / 880 / 3,662 | 170 / 601 / 3,558 |
| F | 29,555 | 70 % | 982 / 1,946 / 10,336 | 934 / 1,747 / 13,220 | 920 / 1,554 / 8,895 |
| G | 49,327 | 70 % | 79 / 366 / 1,499 | 78 / 155 / 1,604 | 100 / 106 / 758 |

Rate attempts (worst rejected share, worst p99 over all runs of all drivers):

| family | attempts |
|---|---|
| E1 | 22,628: 6.1 %, 3.7 s; 15,840: 30.1 %, 6.0 s; 11,088: 0 %, 1.6 s; 7,762: 0 %, 516 ms; 5,433: 0 %, 76 ms; 3,803: 0 %, 1.5 ms |
| E2 | 13,459: 64.7 %, 9.6 s; 9,421: 0 %, 80 ms; 6,595: 0 %, 3.7 ms |

Strict 70 % for E1 and E2, measured in the same pass and the same way
(first attempt, all runs valid), ops/s and p90 / p95 / p99 in ms, median of
2 rounds, rejected share per round:

| family | rate/s | master | v2 | rust |
|---|---:|---|---|---|
| E1 | 22,628 | 21,408; 1,102 / 2,462 / 2,802; 0.8 %, 3.4 % | 21,111; 1,744 / 3,080 / 3,624; 5.4 %, 6.1 % | 21,587; 1,151 / 2,333 / 2,833; 3.1 %, 3.4 % |
| E2 | 13,459 | 8,598; 3,970 / 4,517 / 5,102; 0 %, 64.7 % | 13,458; 96 / 217 / 407; 0 %, 0 % | 13,458; 390 / 840 / 1,206; 0 %, 0 % |

At this rate every driver sits in multi-second cluster stalls and the
ranking follows which round a stall hit (E2 master lost one round to a
stall), so these rows compare the cluster, not the drivers.

All rates were found in one pass under the final rule (2026-11-09,
`results/final3-fresh.log`); every run was valid. Data from unsustained
attempts is under `unsustained/`.

- Client-bound families (A, D, G): v2 is 1.31-1.33x master, rust ~2x; the
  ratio follows CPU per operation under the 0.5 CPU budget.
- At equal offered load the Go drivers have lower p90 than rust in every
  family except F (LWT, where rust is lowest at every percentile); rust has
  the tightest tail on A, D and G. v2 matches or beats master at p90/p95
  everywhere except E2, where p95 (880 vs 541 µs) falls in a cluster-bound
  tail (service p99 3.4-3.7 ms for all three drivers).
- B, C, E2, F are cluster-bound: throughput within +/-10 % across drivers.

## Methodology findings

- **Closed-loop maximum hides cluster stalls.** Write families (E1, E2,
  at times F) have Scylla-side stalls where individual requests wait 1-2 s at the
  server (failure-detector pings time out, `Too long queue accumulated`).
  In a closed loop a stall is a handful of slow operations out of ~10^6 and
  vanishes below p99. In an open loop every worker blocks and 10-30 k
  arrivals queue behind it, so p99 becomes the stall and lands on whichever
  driver happens to run. 70 % of the closed-loop maximum is therefore not
  stall-free for these families; stall-free rates were E1 3,803/s and
  E2 6,595/s; F held 70 % (29,555/s) in the final pass, though its p99
  (9-13 ms) is the highest of all families. Data at strict 70 % is kept
  under `unsustained/` for reference.
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
