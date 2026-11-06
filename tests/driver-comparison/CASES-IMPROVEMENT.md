# Case improvement ledger: writes, partition reads, LWT

Source: the case matrix run `cases-20261105T193147Z` (README-CASES.md). Where
v2 trails or only matches master:

| case | v2/master ops | CPU/op | p99 (throughput run) | allocs v2/master |
|---|---:|---:|---:|---:|
| D1 reused write | 1.00 | 0.79 | 1.64 | 6/26 |
| D2 one-shot write | 1.07 | 0.75 | 1.37 | 6/29 |
| D3 idempotent + timestamp | 1.08 | 0.83 | 1.18 | 9/26 |
| B1 partition Each | 0.85 | 1.12 | 1.85 | 10/101 |
| B2 partition Rows | 0.98 | 1.01 | 1.07 | 20/101 |
| B3 partition stream | 0.86 | 1.15 | 1.26 | 79/101 |
| B4 partition stream, incremental | 0.002 | 45.6 | 1009 | 10341/101 |
| B5 partition prefetch | 0.94 | 1.02 | 1.00 | 10/105 |
| B8 partition, abandon after 16 rows | 0.80 | 1.19 | 3.14 | 8/32 |
| F1 LWT | 1.07 | 0.99 | 1.16 | 6/37 |

## Plan

1. **Attribute.** On the pinned cluster, take 30 s CPU and heap profiles
   (`-profile-prefix`) of every case above and of its master case, and a
   per-request trace where the shape matters (requests per operation,
   pages, cancellation). Writes are server-bound at equal ops/s, so for
   D1-D3 also compare routing (replica/shard of each write) and the
   client-side write pattern that drives tail latency.
2. **Hypothesise** one mechanism per finding, with the profile share or
   wire evidence that supports it.
3. **Implement** each candidate as a separate change in the driver.
4. **Gate.** Unit tests (`make test-unit` scope for touched packages) must
   pass. Then:
   - the happy-path gates (`hp/compare.sh`, 5×ABBA) must show no
     regression worse than 2% at p<0.05;
   - cluster A/B of the target cases and their neighbours with
     `cluster_bench.sh` (`GOCQL_CLUSTER_CASES`, 2 pairs, OLD = base v2
     build, NEW = candidate build).
5. **Decide.** Keep a change when its mechanism is shown (profile, alloc
   or wire delta) and the target case improves beyond run-to-run spread
   (both pairs agree) with no regression elsewhere; otherwise reject and
   record why.

Host load is checked before each timed run (see the gate pitfalls); runs
on a loaded host are repeated.

## Results

### Attribution

Profiles and server counters (`/metrics` deltas per operation) on the
pinned cluster, host load below 2 unless noted.

- **Writes (D1-D3).** Throughput is set by the server. Back-to-back runs
  of one build moved from 1.18x to 0.69x master, and Scylla's own
  coordinator write latency moved with them (249 to 572 us). Requests,
  request bytes and cross-shard operations per write are identical for
  both drivers, and writes are token routed to the same replica and
  shard. v2 already uses 0.75-0.83x master's client CPU and 0.58
  syscalls per write against 1.22. The p99 tail follows the server.
  No driver change; the matrix's write rows are server noise.
- **LWT (F1).** v2 routes conditional statements to the primary replica
  on a strict shard without shuffling, as master does. Server CPU per
  LWT is equal (0.375 ms), client syscalls 2.18 against 2.63, and v2's
  p99 is half of master's. Throughput alternates between drivers run
  to run. No driver change.
- **Partition reads, harness.** B1 and B5 sent 5 requests per partition,
  master and B2/B3 sent 4: the harness stopped reading at 128 rows,
  which skipped the terminal empty page for every reader except
  `Each`. Fixed in the harness (`allRows`); all now send 5. With the
  fix, B1's driver-side CPU (excluding the harness's SHA-256, 70% of
  the operation) is 118 us/op against master's 125. B1, B2 and B5 need
  no change.
- **Incremental stream (B4).** The 0.002x result came from the same
  harness cut: closing an incremental stream before its end closed the
  connection, so every operation waited for a reconnect. Reproduced
  with a new case, B9 (incremental, 16 of 128 rows, then Close):
  17 operations in 5 s. Each reservation also logged
  `read header timeout`.
- **Buffered stream (B3).** 186 us/op of driver CPU against 118 for
  `Each`. A third of the stream decoder's overhead was cancellation
  checks: every scope operation of `wire.Stream` and both codec readers
  called `ctx.Err()`, which takes a lock, several times per cell.

### P1: keep an incremental stream's connection on early Close (kept, 3e90d70b)

An explicit Close that no NextInto is running discards the rest of the
current response (up to 1 MiB), finishes it and returns the reservation
for reuse before cancelling, whose callback would otherwise close the
connection. The intentional pause of the shared reader is no longer
logged as a timeout. New unit test
`TestStreamAbandonedIncrementalCloseKeepsConnection` (v4, v5;
mid-response, boundary, over the limit), 30x under -race.

Cluster, 2xABBA against e2268627:

| case | ops/s base -> new | CPU/op | per-pair ops ratio |
|---|---|---:|---|
| B9 incremental, abandon | 1.2 -> 9,635 | 0.006 | 8066, 13751 |
| B4 incremental, full | 1,714 -> 1,722 | 0.995 | 1.005, 1.004 |
| B3, B8, A5 | unchanged | 1.000-1.004 | 0.945-1.005 |

All 13 errors were base runs timing out.

### P3: poll the context's Done channel instead of Err (rejected)

`BenchmarkReusableQueryDecoding/partition/stream` +5.33% (5xABBA). The
profile diff shows the removed `cancelCtx.Err` (-0.20 s) replaced by an
equal `selectnbrecv`/`chanrecv` cost (+0.21 s): no mechanism gain.

### P3b: check cancellation where a stream reads its source (kept, 1bc89515)

`wire.Stream.check` no longer calls `ctx.Err()`; `Read` checks it before
and after reading the source, the only place a stream can block. The
codec stream readers' checks, which always reach a `wire.Stream` read,
drop theirs. A cancelled page now fails at its next read rather than at
the next scope operation; the codec test asserts that.

Microbenchmark, 5xABBA against P1 (CPU 12): partition/stream -4.23%,
point/stream -1.69% (buffered -0.6% to -1.75%, the run's drift).
Cluster, 2xABBA against P1:

| case | ops/s | CPU/op | per-pair ops ratio |
|---|---:|---:|---|
| B3 buffered stream | 1.026 | 0.976 | 1.021, 1.027 |
| B4 incremental stream | 1.014 | 0.985 | 1.017, 1.014 |
| A5 point stream | 1.010 | 0.989 | 1.010, 1.006 |
| B1 Each (control) | 0.999 | 1.001 | 0.995, 1.004 |

Happy-path gates (`hp/compare.sh`, 5xABBA against P1, cluster stopped):
no change beyond 2%; flagged pone/w1 -2.08% sec/op, -5.29% cpu-sec/op,
and pagectx/w4 +1.03% sec/op (p=0.043). Unit suite with -race: only the
known `TestProcessFrameFailedDeadlineArmIsFatal`, which also fails on
e2268627~1.

### Not done

- Incremental streams cost 557 us and 204 allocations per partition
  against 474 us and 87 for the buffered stream: per-page exclusive
  reservation (context callbacks, channels, reader pause/resume). A
  bounded-memory mode's price; not attributed further here.
- The buffered stream decoder still costs about 1.5x `Each` per cell
  (closures and scope bookkeeping per cell); a structural rewrite of
  `wire.Stream` children, not a local change.
- Per-operation client timestamps on a reused `Query` need one-shot
  `Options` per call (D3); master sets them on a reused query. An API
  addition, out of scope.
