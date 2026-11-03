# Happy-path optimization ledger

Branch `dk/v2-happy-path`, starting from the parked redesign `f363e371`. The
goal is per-request fixed cost when the first attempt succeeds. Plan and rationale:
`~/.claude/plans/typed-gliding-steele.md`.

## Gates

- **T0**: `BenchmarkReusableQueryDecoding` (net.Pipe, one caller) and
  `BenchmarkReusableQueryAdmission`. Exact allocations; CPU on an unbatched pipe.
- **T1**: `BenchmarkReusableQueryLoopback` over loopback TCP with the production
  8 KiB reader. The fake server is a child process pinned to CPUs 6-7, and the
  client is pinned to 4-5. At `w64 -cpu 1` the single P is saturated, so ns/op
  is client CPU per operation. `w1` is a round-trip latency figure (±5-8%).
- **Smoke**: the emulator from `tests/driver-comparison/emulator`, with
  `driver-comparison -mode check` for read, write and paged in buffered and
  stream modes. Emulator failure counter must not change.

A change is kept when its mechanism is proven exactly and an interleaved A/B
benchstat (10 rounds, alternating binaries) shows no regression worse than 2%
at p<0.05. The first comparison ran the two sets sequentially and showed a
false `write/w1` +4.8% from drift; interleaving removed it.

Known baseline failure, unrelated to this work:
`TestProcessFrameFailedDeadlineArmIsFatal` fails deterministically on `f363e371`.

## Baseline `f363e371`

| benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| Decoding point/buffered | 13,262 | 4,865 | 46 |
| Decoding point/stream | 14,431 | 5,953 | 59 |
| Decoding partition/buffered | 114,534 | 16,250 | 156 |
| Loopback read/w64 (1 CPU) | 12,001 | 4,407 | 41 |
| Loopback read/w64-2 | 9,846 | 4,419 | 40 |
| Loopback write/w64 (1 CPU) | 11,206 | 4,038 | 37 |
| Loopback write/w64-2 | 8,969 | 4,065 | 36 |
| Admission | 1,625 | 1,584 | 13 |

## I1: skip unused per-execution allocations (kept)

Admission always allocated an `InternalExecutionRequest` and then replaced it
with the query's embedded request. Every logical execution also created a
lifetime `done` channel, which only session-owned executions are joined on
(`InternalActiveExecutionDrain`). Both are now allocated only for session-owned
executions.

Mechanism: −2 allocs/op, −816 B/op on every reusable-query case.

| benchmark | before | after | Δ |
|---|---:|---:|---:|
| Loopback read/w64 ns/op | 12.00µ | 11.41µ | −4.95% |
| Loopback write/w64 ns/op | 11.21µ | 10.70µ | −4.52% |
| Loopback write/w64-2 ns/op | 8.97µ | 8.48µ | −5.42% |
| Decoding point/buffered B/op | 4,865 | 4,049 | −16.8% |
| Admission ns/op | 1.625µ | 1.464µ | −9.9% |

w1 interleaved rerun: no difference (−0.4%, p≥0.28). Unit race suite passes
except the known baseline failure; emulator smoke OK.

## I1b: reuse a query-embedded execution owner (kept)

Every execution registered a fresh input owner: the struct, a `done` channel
that is never waited on for execution owners, a drain closure, and a bound
release closure for each writer lease. It also took the session-global
`rt.InternalMu` lock, which the reusable path never needed.

Reusable queries now reset an embedded `execOwner` each generation. Its drain
callback is the query owner's pre-bound `InternalRelease`, and the writer lease
reuses one bound release function. To make the reset safe,
`InternalRelease`/`InternalClose` run the drain callback after unlocking, as
their final action. The next generation can only be admitted through that
callback, and nothing touches the owner afterwards. The session-global lock is
now taken only for session-owned executions.

Mechanism: −4 allocs/op and −224 B/op on read and write; −3 allocs in admission.

| benchmark (interleaved vs I1) | before | after | Δ |
|---|---:|---:|---:|
| Loopback read/w64-2 ns/op | 9.298µ | 9.002µ | −3.18% |
| Loopback write/w64-2 ns/op | 8.431µ | 8.129µ | −3.58% |
| Loopback read/w64 ns/op | 11.39µ | 11.18µ | ~ (p=0.12) |
| Decoding point/buffered ns/op | 12.91µ | 12.76µ | −1.12% |
| Admission ns/op | 1.346µ | 1.219µ | −9.44% |
| Loopback read allocs/op | 39 | 35 | −4 |
| Loopback write allocs/op | 35 | 31 | −4 |

`-race -count=20` on `internal/execution`; unit race suite clean except the
known baseline failure; emulator smoke OK.
