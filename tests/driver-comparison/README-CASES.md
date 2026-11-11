# Query-execution cases

`cluster_bench.sh` times every case of `cases.tsv` on the pinned three-node
cluster (`cluster.sh`, or `provision.py`, which can add 200 µs of netem delay).
Each v2 case (`cases.go`) is matched to one master case (`old_cases.go`);
several v2 cases can share one master case. Clients run on CPU 8 with
GOMAXPROCS=1.

Common settings: protocol v4, LOCAL_QUORUM, RF=3 in `datacenter1`, token- and
DC-aware routing, no compression, retries or speculation, one connection per
shard, page size 32, no prefetch unless stated. Every operation has a 10 s
context and validates every row it reads (key, row id, length, SHA-256).
Writes store the same deterministic payload the dataset holds, so reads stay
valid after write cases.

Tables: `point (k int PRIMARY KEY, r int, p blob)` with 100,000 1 KiB rows;
`paged (k, r, p, PRIMARY KEY (k, r))` with 1,024 partitions of 128 × 4 KiB
rows; `lwt (k int PRIMARY KEY, v int)` with 4,096 rows. Keys come from
`pointKey(seq)` / `partKey(seq)`.

Notation: `w.*` is per-worker state built before timing; `check` validates rows.

## A. Single-row read (`point`), 64 workers

| v2 | pseudocode | master | pseudocode |
|---|---|---|---|
| A1 | `w.q.One(ctx, k, &out)` on a reused `Query` | MA1 | `w.q.Bind(k).WithContext(ctx).Scan(&k, &r, &p)` on a reused template |
| A2 | `prepared.One(ctx, k, &out, opts)` | MA2 | `s.Query(pointRead, k).WithContext(ctx).Scan(...)`: a new query per operation |
| A7 | `session.UnpreparedOne(ctx, pointRead, k, &dst)` | MA2 | master always prepares DML |
| A3 | `w.q.Each(ctx, k, &out, check)` | MA3 | `it := w.q.Bind(k).WithContext(ctx).Iter(); for it.Scan(...) {check}; it.Close()` |
| A4 | `rows := w.q.Run(ctx, k); for rows.NextInto(&out) {check}; rows.Close()` | MA3 | |
| A5 | `rs := w.stream.Run(ctx, k); for rs.NextInto(&out) {check}; rs.Close(); w.stream.Wait(ctx)` | MA3 | |
| A6 | `page := w.q.RunUnpaged(ctx, k); for page.NextInto(&out) {check}; page.Close()` | MA4 | MA3 with `PageSize(0)` |

## B. Partition read (`paged`, 128 × 4 KiB), 4 workers

| v2 | pseudocode | master | pseudocode |
|---|---|---|---|
| B1 | `w.q.Each(ctx, k, &out, check)` | MB1 | `it := w.q.Bind(k).WithContext(ctx).Iter(); for it.Scan(...) {check}; it.Close()` |
| B2 | `Run` + `Rows.NextInto` loop + `Close` | MB1 | |
| B3 | `StreamQuery.Run` (page buffered) + `NextInto` loop | MB1 | |
| B4 | `StreamQuery` with `StreamOptions{Incremental: true}` | MB1 | |
| B5 | B1 with `Options{Prefetch: 1}` | MB2 | MB1 with `Prefetch(0.25)` (master's default; nearest to one page) |
| B6 | `RunUnpaged`: the whole partition in one response | MB3 | MB1 with `PageSize(0)` |
| B7 | B1 with `Options{PageSize: 5000}` | MB4 | MB1 with `PageSize(5000)` |
| B8 | `rows := Run(...)`; read 16 rows; `rows.Close(); w.q.Wait(ctx)` | MB5 | `it := Iter()`; scan 16 rows; `it.Close()` |
| B9 | incremental `StreamQuery`: read 16 rows; `rs.Close(); Wait` | MB5 | |

## C. Application-driven paging, 64 workers

One operation fetches one 32-row page of a partition from a saved state, as a
REST cursor would. The worker keeps `(k, state, next row)`; the page without
a state ends the partition, and the worker moves to its next partition.

| v2 | pseudocode | master | pseudocode |
|---|---|---|---|
| C1 | `page := w.q.RunPage(ctx, k, PageStateFromToken(st)); NextInto loop; st = page.State().Token(); page.Close()` | MC1 | `it := w.q.Bind(k).PageState(st).WithContext(ctx).Iter(); st = it.PageState(); scan loop; it.Close()` |

## D. Single-statement write (`point`), 64 workers

| v2 | pseudocode | master | pseudocode |
|---|---|---|---|
| D1 | `w.q.Exec(ctx, row)` on a reused `Query` | MD1 | `w.q.Bind(k, r, p).WithContext(ctx).Exec()` on a reused template |
| D2 | `prepared.Exec(ctx, row, opts)` | MD2 | `s.Query(pointWrite, k, r, p).WithContext(ctx).Exec()` |
| D4 | `session.UnpreparedExec(ctx, pointWrite, row, metadata)` | MD2 | |
| D3 | `prepared.Exec(ctx, row, CompileOptions(session, {Idempotent, Timestamp: now}))`: a reused `Query` has fixed options, so per-operation timestamps need one-shot options | MD3 | MD1 with `Idempotent(true)` and `WithTimestamp(now)` |

## E. Batches, 64 workers (E1: 8; at 64, 32 KiB batches exceed Scylla's in-flight hint budget)

| v2 | pseudocode | master | pseudocode |
|---|---|---|---|
| E1 | 8 × `q.AddToBatch(&batch, row)` of one `paged` partition; `q.ExecBatch(ctx, UnloggedBatch, &batch, opts)` on a reused `Batch` | ME1 | `b := s.BatchWithContext(ctx, UnloggedBatch)`; 8 × `b.Query(pageWrite, ...)`; `s.ExecuteBatch(b)` |
| E2 | 4 `point` keys, `LoggedBatch` | ME2 | 4 `point` keys, `LoggedBatch` |

## F. Lightweight transaction (`lwt`, LOCAL_SERIAL), 64 workers

Worker `id` of `n` owns keys `id + n·j`, j < 64, and reads their values when it
is built, so each `UPDATE lwt SET v=? WHERE k=? IF v=?` must apply; an update
that does not apply fails the operation.

| v2 | pseudocode | master | pseudocode |
|---|---|---|---|
| F1 | `w.q.One(ctx, {cur+1, k, cur}, &casResult)`; require `Applied` | MF1 | `w.q.Bind(cur+1, k, cur).WithContext(ctx).ScanCAS(&prev)`; require applied |

## G. Mixed 50/50 read/write (`point`), 64 workers

| v2 | pseudocode | master | pseudocode |
|---|---|---|---|
| G1 | even seq: A1, odd: D1 | MG1 | even: MA1, odd: MD1 |
| G2 | even seq: A2, odd: D2 | MG2 | even: MA2, odd: MD2 |

## Protocol

As before: setup once with the master binary (`schema`, `prepare` for read,
paged and lwt, `warm`). `-mode check -case X` runs 256 validated operations of
every case of both binaries. Throughput is closed loop (10 s warmup, 30 s
measurement) with each case's workers. For a master case `M` with v2 cases
`B1..Bn`, a pair runs `M B1..Bn Bn..B1 M`; `PAIRS` pairs (default 2). Latency is
open loop at `GOCQL_CLUSTER_LATENCY_FRACTION` (default 0.7) of `M`'s median
throughput, the same scheduled rate for `M` and its v2 cases, in the same
order. `GOCQL_CLUSTER_CASES="MA1 MB1"` restricts a run to those master cases.

```sh
sh tests/driver-comparison/cluster.sh start
sh tests/driver-comparison/cluster_bench.sh OLD_BIN NEW_BIN OUT [PAIRS]
python3 tests/driver-comparison/cluster_bench_report.py OUT
```

Results are `OUT/{kind}-{M}-pair{i}-{case}.json`; the report prints one table
per kind and master case with medians and v2/master ratios.
