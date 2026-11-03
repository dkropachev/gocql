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

## I2: lock-free shared-connection admission (rejected)

`beginShared`/`endShared` take the per-connection mutex four times per request,
only to count shared operations for the rare exclusive streaming reservation.
The prototype replaced the count with an atomic. It published the operation,
then checked the exclusive flag, and the reservation armed its notification
before reading the count. Lock and unlock remained only when a waiter was armed.
A 200× race test of the protocol passed, and a mutation (no release signal)
made it fail.

There was no measurable CPU benefit at any tested parallelism, interleaved ABBA:

| benchmark | I1b | I2 | Δ |
|---|---:|---:|---:|
| Loopback read/w64 | 11.41µ | 11.28µ | ~ (p=0.14) |
| Loopback read/w64-2 | 9.023µ | 9.024µ | ~ |
| Loopback read/w64-4 | 4.821µ | 4.781µ | ~ (p=0.27) |
| Loopback write/w64-4 | 4.430µ | 4.461µ | ~ (p=0.79) |
| Loopback read/w1-2 (ABBA, n=20) | 27.08µ | 27.80µ | +2.66% (p=0.010) |

Rejected under the rule: there was no benefit, one latency case was flagged,
and the protocol is harder to reason about. Four uncontended lock pairs are
about 60 ns of an 11 µs request. The earlier lock-heavy profile came from
`net.Pipe`. It does not represent a socket, and plan items I3 (call table) and
I4/I5 (channel work) need a fresh real-TCP profile before they are attempted.

The w1 comparison also exposed order bias: the new binary ran second in every
round. `compare.sh` now runs ABBA rounds.

## I3: yield once before flushing so submitters batch (kept)

Plan deviation, chosen from a real-TCP profile at read/w64, one P:
`Syscall6` (`writev`) was 32% of client CPU, `io.ReadAtLeast` 2.4%,
mutex lock/unlock about 7.5%. strace counted **48,655 `writev` for ~50,000
requests: 0.97 per request**. A woken writer runs next on its P, ahead of every
runnable submitter, so the zero-coalescing drain found one request each time.

The writer now yields once (`runtime.Gosched`) when its queue looks empty and
another request is in flight on the connection. Runnable submitters enqueue
before it flushes. There is no timer and no wait for future requests, and a lone
caller never yields. Batches are still bounded at 16 requests and 64 KiB.
Larger batches overflowed the two 16/128-span descriptor scratch buffers, which
were then reallocated every batch (+6% / +30% B/op). Both now retain up to
`maxRetainedWriteSpans` = 512 descriptors (12 KiB per connection), and
`TestTypedWriterScratchBoundAndConcurrentOrdering` follows the new bound.

Mechanism: 7,028 `writev` for ~100,000 read+write requests, **0.07 per
request** (about 14 per batch). Also −2 allocs/op at w64.

| benchmark (interleaved ABBA vs I1b) | before | after | Δ |
|---|---:|---:|---:|
| Loopback read/w64 | 11.00µ | 7.198µ | −34.55% |
| Loopback write/w64 | 10.56µ | 6.614µ | −37.34% |
| Loopback read/w64-2 | 9.050µ | 7.115µ | −21.38% |
| Loopback write/w64-2 | 8.201µ | 6.405µ | −21.90% |
| Loopback w1 (all four) | | | ~ |
| Decoding point/buffered | 12.82µ | 12.83µ | ~ |

Without the in-flight condition, the single-caller net.Pipe T0 cases paid
+1.1–1.6% for a pointless yield; the condition removed it. Transport
`-race -count=20`, unit race suite clean except the known baseline failure;
emulator smoke OK.

## I4: lock-free session runtime lookup (kept)

The post-batching real-TCP profile shows `sync.Mutex` Lock+Unlock at 9.6% flat,
spread over dozens of sites. The largest single site was `InternalRuntimeFor`,
at about 1.9% of CPU. It is called several times per request, and each call
takes `SessionData(s).InternalTypedMu` to look up a runtime that is created
once and never replaced. The initialized runtime is now published through an
`atomic.Value`, and later calls skip the lock.

| benchmark (ABBA vs I3) | before | after | Δ |
|---|---:|---:|---:|
| Loopback read/w64 | 7.252µ | 7.156µ | −1.33% (p=0.028) |
| Loopback read/w64-2 | 7.109µ | 7.063µ | −0.64% (p=0.001) |
| Loopback write/w64 | 6.618µ | 6.590µ | ~ |

The remaining lock sites are each ≤1% (session `Closed`, query cursor checks,
page close, input owner, connection reader disarm), so they are not worth
individual changes. The post-batching profile puts allocation and GC first:
`mallocgc` is 17.4% cumulative plus about 12% marking and scanning, and the
execution deadline context alone is 4.4%.

## I5a: create the query completion channel only for a waiter (kept)

Every `queryState.begin` allocated a completion channel, and the matching
`idle` closed it, even when nothing ever called `Wait`. `wait` now returns at
once for an idle query. A busy query creates the channel on its first waiter,
and `idle` closes and clears it, so each generation's waiters still observe
that generation's completion.

| benchmark (ABBA vs I4) | before | after | Δ |
|---|---:|---:|---:|
| allocs/op (all reusable cases) | | | −1 |
| B/op (all reusable cases) | | | −112 |
| Loopback write/w64 | 6.665µ | 6.609µ | −0.84% (p=0.037) |
| Decoding point/buffered | 12.86µ | 12.69µ | −1.30% (p=0.007) |
| Loopback read/w64 | 7.244µ | 7.079µ | ~ (p=0.089) |

## I8: closure-free rows release, co-allocated first page, embedded cursor (kept)

Each buffered `Run` built a `Rows` and a separate first `Page`. It also built
two closures (the rows release, and the stop hook for query or session
shutdown) and a heap `wire.Cursor` for the page.

- The rows now record their logical execution. Close moves it to a pending slot,
  and it is finished and released after the unlock, as the closure was. The
  result info and error are now captured under the lock. The old closure read
  them after unlocking.
- `installStop` takes an `executionStopper` interface, which `*Rows`
  implements directly. Other callers use `stopFunc`, a func adapter that needs
  no allocation because func values are stored in interfaces directly.
- The first page shares the rows allocation. Continuation pages still replace
  it, so no handle object is reused across executions, and stale `*Rows`
  handles cannot observe a later generation (plan gap 6 is avoided rather than
  solved).
- `PageStorage.InternalCursor` is a `wire.Cursor` value, built with the new
  `wire.CursorOver`.

The first measurement carried two unused fields in `RowsStorage` (+3.4% B/op).
They were removed before the measurement below.

| benchmark (ABBA vs I5a) | before | after | Δ |
|---|---:|---:|---:|
| Loopback read allocs/op | 32 | 28 | −4 |
| Decoding point/buffered allocs/op | 39 | 35 | −4 |
| Decoding partition allocs/op | 145 | 137 | −8 |
| Loopback read/w64 B/op | 3,254 | 3,174 | −2.46% |
| Loopback read/w64 ns/op | 7.387µ | 7.130µ | −3.47% (p=0.035) |
| Loopback write | | | unchanged (Exec builds no rows) |

## I9a: stop success-path escapes (kept)

Line-level allocation profiles found four allocations that happen on every
successful request only because of escape analysis:

- `executeTypedAttempt` declared its `errors.As` target for UNPREPARED
  responses before checking for an error. The target now lives in
  `unpreparedError`, which runs only on failure.
- `receiveTypedCall` moved `output`/`outputEpoch` to the heap, because the
  compressed-frame lease closure captured them by reference. That closure now
  captures copies.
- `InternalProcessFrameSource` moved `head` to the heap, because the legacy
  framer path passes `&head`. The legacy path now passes a branch-local copy.
- `InternalResultPlanCache.InternalSelectPlan` minted a throwaway schema identity
  for every response without one (unprepared queries). Such responses now
  compare columns with the cached plan directly. This keeps the existing reuse
  of a plan cached under an identified entry, as required by
  `TestV2ResultPlanCacheScopedReuseAndNoCompileLock`. The first version wrongly
  required an unidentified cache entry and failed that test.

| benchmark (ABBA vs I8) | before | after | Δ |
|---|---:|---:|---:|
| Loopback read allocs/op | 28 | 23 | −5 |
| Loopback write allocs/op | 28 | 24 | −4 |
| Decoding partition allocs/op | 137 | 112 | −25 |
| Loopback read/w64 | 7.030µ | 6.819µ | −3.02% |
| Loopback read/w64-2 | 6.897µ | 6.635µ | −3.81% |
| Loopback write/w64 | 6.516µ | 6.436µ | −1.24% |
| Loopback read/w64 B/op | 3,174 | 3,022 | −4.79% |

Gate gap found here: T0/T1 use unprepared QUERY, while production and the
emulator use prepared EXECUTE. The plan-identity allocation exists only on the
unprepared path, and the prepared path may hide costs T1 cannot see. T1 needs a
prepared variant before further allocation work is ranked.

## T1 prepared variants

`BenchmarkReusableQueryLoopback/pread` and `/pwrite` use `Prepare` + `NewQuery`
with an `int32` key, as the canonical point workloads do. The fake server
answers PREPARE with a Prepared result and every EXECUTE with the workload
body. The same benchmark file was compiled against the T1 commit (before any
driver change) for a baseline, interleaved ABBA:

| benchmark | baseline | after I9a | Δ |
|---|---:|---:|---:|
| pread/w64 ns/op | 12.835µ | 7.695µ | −40.04% |
| pread/w64-2 ns/op | 10.694µ | 7.450µ | −30.34% |
| pwrite/w64 ns/op | 11.805µ | 6.698µ | −43.26% |
| pwrite/w64-2 ns/op | 9.306µ | 6.619µ | −28.87% |
| pread/w1 ns/op | 23.24µ | 22.98µ | ~ |
| pwrite/w1 ns/op | 23.63µ | 22.05µ | −6.69% |
| pread allocs/op (w64) | 40 | 23 | −17 |
| pwrite allocs/op (w64) | 36 | 23 | −13 |
| pread B/op (w64) | 4,632 | 3,302 | −28.73% |
| pwrite B/op (w64) | 3,929 | 2,677 | −31.87% |

## I9b: received frames release their buffer without a closure (kept)

`wire.Lease` documents that leases are never recycled, so that an old pointer
cannot retain a new owner's storage. That rules out the plan's pooled or
embedded leases, and the invariant is kept. What could go is the release
closure each received frame allocated to return its buffer to the receive
cache.

`Lease.release func()` is now `releaser leaseReleaser`, a one-method interface.
`NewLease` wraps a callback in `releaseFunc`; storing a func value in an
interface does not allocate. `NewCachedLease` allocates a `cachedLease`, which
embeds `Lease`, holds the cache, buffer and epoch, and is its own releaser.

The first version added the cache fields to `Lease` itself, growing every lease
from 96 B to 128 B for +1.4–1.8% B/op. With the embedded type, a plain `Lease`
stays at 96 B, and a cached lease (136 B) replaces a lease plus closure
(96 + 48 B).

| benchmark (ABBA vs HEAD) | before | after | Δ |
|---|---:|---:|---:|
| allocs/op, every loopback case | | | −1 |
| Decoding partition allocs/op | 112 | 107 | −5 |
| B/op, every case | | | unchanged |
| ns/op | | | ~ (noisy host, ±5–20%) |

## I6: reusable execution context for direct-path queries (kept)

Correction to the plan: typed attempts do not set a transport timeout
(`attemptRequest.Timeout` is never assigned). The execution context created by
`context.WithTimeout` in admission is therefore the only timeout mechanism. It
spans continuation pages and retries, and it also carries query-close and
session-close cancellation. The fix is a cheaper context with the same
behaviour, not removing one of two timeouts.

`executionContext` (`internal/execution/execution_context.go`) belongs to a
reusable `Query`. It is used under the condition that already reuses the
embedded logical execution: no observer, no speculation, no routing-latency
observation, zero coalescing. `StreamQuery` opts out, because it hands its
context to exclusive transport I/O.

- One `time.Timer` per query, re-armed per execution. `expire` re-checks the
  current deadline under the lock, so a late callback from an earlier
  generation is discarded.
- A cancellable parent is followed with `context.AfterFunc` and a callback bound
  once per query. The callback cancels only when the current generation's parent
  is cancelled.
- Cancellation closes `Done`, as before, and stops the timer and the parent
  registration under the lock, so a delayed stop cannot disarm the next
  generation.
- The type implements `AfterFunc`, so derived contexts and `context.AfterFunc`
  need no watcher goroutine.
- `Deadline` includes the parent's deadline, and `Value` forwards to the parent.
  The driver does not use `context.Cause`, and users never receive this context.

Tests cover deadline expiry, parent cancellation, an earlier parent deadline,
an already-cancelled parent, value forwarding, `AfterFunc` and its stop,
derived contexts, stale stops, and late timer and parent callbacks, with a
concurrent race loop. Mutating the deadline re-check or the parent check makes
`TestExecutionContextLateCallbacksSpareNextGeneration` fail.

The first version captured the generation in a per-execution parent callback.
That cost one allocation more than `context.WithTimeout` when the parent is
cancellable (T0 point/buffered +1 alloc, +2.3% B/op); the bound callback
removed it.

| benchmark (ABBA vs HEAD) | before | after | Δ |
|---|---:|---:|---:|
| Loopback read/w64 | 6.764µ | 6.342µ | −6.24% |
| Loopback write/w64-2 | 6.202µ | 5.732µ | −7.59% |
| Loopback pread/w64 | 7.522µ | 7.072µ | −5.98% |
| Loopback pwrite/w64 | 6.603µ | 6.138µ | −7.05% |
| Loopback allocs/op (Background parent) | 22 | 18 | −4 |
| Loopback B/op | | | −8% to −10% |
| Decoding point/buffered (cancellable parent) | 29 allocs | 29 allocs | +0.92% B/op |
| Admission ns/op | 1,087 | 790 | −27.35% |

I7 (first-attempt result path) was not attempted: `mergeResponseResult` only
checks empty slices, and the `time.Now` pair feeds the public `Result.Latency`.
I10 (shared reader 8/16/32/64 KiB) changed w64 by at most −1.2%, so the reader
stays at 8 KiB.

## Emulator comparison against master (screen)

The same session ran both screens, back to back: `redesign_compare.py
--stage screen --load maximum`, five canonical cases, two ABBA pairs × 30 s,
client CPU 8 with GOMAXPROCS=1, emulator CPUs 0-1,4-5. Builds were frozen with
`roadmap_build.py`: master `1ceaac81`, baseline `f363e371`, candidate `5f0d852f`.
Results are in `results/hp-screen-baseline-20261103T160542Z` and
`results/hp-screen-master-20261103T160542Z`. A screen cannot accept a change.
Six-pair confirmation, matched-rate loads and real Scylla remain outstanding.

| case | baseline / master | candidate / master | candidate CPU µs/op (master) | candidate allocs/op (master) | candidate p99 µs (master) |
|---|---:|---:|---:|---:|---:|
| point-buffered w64 | 0.701 | **0.976** | 10.02 (9.78) | 27.2 (32.0) | 1,826 (1,532) |
| write w64 | 0.694 | **1.015** | 8.30 (8.42) | 25.2 (25.0) | 1,623 (1,371) |
| paged-buffered w4 | 0.868 | 0.930 | 439 (411) | 85.5 (150) | 2,623 (2,554) |
| paged-stream w4 | 0.866 | 0.832 | 487 (408) | 137 (150) | 2,829 (2,399) |
| point-stream w4 | 0.672 | 0.699 | 15.7 (11.0) | 52.7 (32.0) | 225 (122) |

- Point-buffered went from 0.701 to 0.976 of master. The runner's CI for it,
  [0.968, 0.984], still marks a small regression against master. Writes went
  from 0.694 to parity (1.015).
- Maximum-load p99 is still 18–19% above master for point reads and writes.
  This is a closed-loop measurement. Matched-rate latency, which the
  convergence policy treats as primary, was not measured.
- Streaming was out of scope: stream queries keep the standard context and
  their own receive path. point-stream remains the largest gap, at 0.699.
  paged-stream at 0.832 against 0.866 is within the noise of two pairs.

## Round 2

Plan: `~/.claude/plans/validated-chasing-mccarthy.md`. Gates now run from
`tests/driver-comparison/hp/` (`compare.sh OLD NEW`, 5×ABBA). The gate set
adds the prepared loopback cases and a `-cpu 4` run (client on CPUs 0-3,
server on 6-7) for cross-core costs that one P cannot show. The admission
benchmark runs 300,000 iterations; at 20,000 it was bimodal (≈800 and
≈1,300 ns in both binaries) and produced a false +58%.

Fresh profile of `pread/w64 -cpu 1` at `c641e86b` (6.6 µs, 18 allocs,
3,029 B): GC ≈16–20% of CPU, mutex lock/unlock 12.5% flat over ~15 sites,
RWMutex reader atomics 5%, `selectgo` 7%, `duffcopy` 3.5%. Largest byte
sources: rows+page holder ≈690 B, response page ≈565 B, request framing
≈800 B over 5–6 allocations.

## C1: keep an unwatched execution Done channel (kept)

I6 made the execution context reusable, but every generation still
allocated its Done channel, because the normal end of an execution closed
it. Two kinds of waiters use it: transport waits that end before the
attempt returns, and watchers that may outlive it (prefetch, `AfterFunc`,
derived contexts). Transport waits now take the channel through
`InternalTransientDone`. The CancelFunc returned by `begin` finishes the
generation. When no `Done` watcher or `AfterFunc` exists, it marks the
generation cancelled without closing the channel, and the next generation
reuses it. Done and Err still report cancellation for the finished
generation. Real cancellation (query or session stop, deadline, parent)
always closes it, so query and session stop now use `cancelFunc` instead of
the finishing func.
`TestExecutionContextFinishReusesUnsharedDone` fails if finish ignores a
`Done` watcher.

| benchmark (ABBA vs `c641e86b`) | before | after | Δ |
|---|---:|---:|---:|
| allocs/op, every reusable case | | | −1 |
| B/op, loopback cases | | | −110 (−3.7% to −4.7%) |
| Loopback write/w64 | 6.002µ | 5.853µ | −2.48% |
| Loopback read/w64 | 6.327µ | 6.231µ | −1.52% |
| Loopback pread/w64-2 | 6.808µ | 6.719µ | −1.31% |
| Loopback read/w64-4 | 4.437µ | 4.370µ | −1.49% |
| Admission (n=20 ABBA) | 824.9n | 829.4n | ~ (p=0.53) |

Execution and transport `-race -count=20`, unit race suite (only the known
baseline failure), emulator smoke OK.

## C2: small per-request allocations (kept)

- `binding.EncodeInto` allocated a `ParamWriter`, and `WriteParam` a fresh
  `ValueWriter` per parameter (it escapes through the encoder interface).
  Both now live in the query-owned `ParameterBuffer`; the cell writer is
  re-initialised for each parameter.
- Retiring an inline execution assigned a new no-op closure; it now uses a
  package-level `noopCancel`.
- The submission writer built a `wire.Chain` over each batch's spans only for
  the socket writer to flatten it again. `InternalDeadlineContextWriter` now
  accepts the span slice directly (`InternalWriteSpanSlice`).

| benchmark (ABBA vs C1) | before | after | Δ |
|---|---:|---:|---:|
| allocs/op read, pread / write, pwrite (w64) | 17 / 18 | 15, 14 / 15, 13 | −2 to −4 |
| Admission allocs/op | 3 | 0 | −3 |
| Loopback pwrite/w64 | 6.375µ | 5.842µ | −8.36% |
| Loopback pread/w64-2 | 7.037µ | 6.638µ | −5.67% |
| Loopback pread/w64-4 | 4.944µ | 4.564µ | −7.69% |
| Loopback pwrite/w64-4 | 4.399µ | 4.000µ | −9.07% |

This run was noisier than usual (some cases ±40–60%): race suites for the
next item ran on other CPUs during it. No case regressed. Codec, binding,
transport and execution `-race -count=20`, unit race suite (only the known
baseline failure), emulator smoke OK.

## Measuring on a busy host

During round 2 the host also ran a SentinelOne scan (about two cores) and
an unrelated C++ build, and wall-clock A/B spread reached ±20–130%.
`BenchmarkReusableQueryLoopback` now reports `cpu-ns/op`, the client
process's CPU time per operation from getrusage. It excludes the fake
server and time spent preempted, and stayed within ±1–2% at 64 workers
while wall time did not. `compare.sh` gained `HP_REF`/`HP_PATCH` so each
item is built from a pinned commit; the queue ran every item in order,
each against the previous one, 4×ABBA (n=8), with the unit race suite and
emulator smoke before each comparison. Results below are against the
previous item.

## C3: lock-free closed check, cached host list (kept)

`Session.Closed` read-locked the session state on every request; it now
reads `closedOrClosing`, an atomic set under that lock with either flag.
`InternalHostsView` returns a host list cached per ring revision, so the
no-policy fallback in `InternalSessionSelectTypedConn` stops building one
per request. Host UUID reads keep their lock: an empty host ID can still
be filled in later, so a lock-free read would need atomics at every
writer for about 1% of CPU. Tests that marked a session closed by
assigning the field now call `InternalSessionMarkClosed`.

| vs m0 (metric only) | before | after | Δ |
|---|---:|---:|---:|
| allocs/op, every loopback and decoding case | | | −1 |
| pread/w64 cpu-ns/op | 6.772µ | 6.520µ | −3.73% (p=0.050) |
| read/w1 cpu-ns/op | 15.36µ | 14.91µ | −2.93% (p=0.038) |
| other cases | | | ~ |

## T2: one-slot response hand-off without a timeout channel (kept)

`InternalCallReq` loses its per-call `InternalTimeout` channel. Its pooled
`InternalResp` has one slot, and `state` (waiting, delivered, stopped)
decides who owns a response: `deliver` succeeds only for a waiting
caller, and `InternalStopWaiting` either withdraws or returns the response
that won the race. The typed and legacy readers and `CloseWithError`
deliver without a select. `TestCallReqStopWaitingDecidesResponseOwnership`
and `TestCallReqReuseStartsWaitingWithAnEmptySlot` cover both outcomes and
reuse.

| vs C3 | before | after | Δ |
|---|---:|---:|---:|
| allocs/op | | | −1 |
| cpu-ns/op read / write / pread / pwrite w64 | | | −3.23 / −3.44 / −2.84 / −3.59% |
| cpu-ns/op w64-4 (four cases) | | | −7.85% to −10.02% |
| w1 | | | ~ |

## T1: no separate wait for write completion (kept)

`submitTyped` returns the queued ticket; `unsent` withdraws a ticket the
writer has not claimed, and `written` reads the writer's result, published
before it closes the connection. On a response the caller still calls
`awaitWritten`: the first chain, which lacked it, made `pwrite/w64-4`
fail with `ErrBusy`, because a response could arrive before the writer
released the query's input. `TestTypedAttemptReturnsAfterWriterReleasesInput`
holds the writer after its bytes are sent and fails without the wait.

| vs T2 | before | after | Δ |
|---|---:|---:|---:|
| cpu-ns/op read/w64 | 5.762µ | 5.662µ | −1.74% |
| cpu-ns/op write/w64 | 5.281µ | 5.219µ | −1.18% |
| cpu-ns/op w64-4 (four cases) | | | −1.17% to −1.99% |
| sec/op decoding (three cases) | | | −0.8% to −1.1% |
| pread / pwrite w64, w1 | | | ~ |

## F1: contiguous owned frames (rejected), F1b: pooled envelope scratch (kept)

F1 encoded small requests, values included, into one pooled contiguous
buffer. It saved as much as F1b below, but it copied borrowed values, which
`feature-specs/gocql-v2-implementation.md` forbids for borrowed text and
blob of every size, and it failed
`TestV2PreparedRetryRepreparePagingNeverReencodes`. F1b keeps the borrowed
spans and pools only what the envelope owns: `wire.BuilderScratch` holds
the scalar slab, span descriptors and the frozen chain's index, and the
request lease returns it after the writer finishes.
`TestScratchFrameMatchesAndBorrowsValues` checks byte equality with the
plain builder for QUERY, EXECUTE and BATCH on v3–v5, and that the value is
still referenced.

| vs T1 | before | after | Δ |
|---|---:|---:|---:|
| allocs/op read / write / pread / pwrite w64 | 13 / 13 / 12 / 11 | 8 / 7 / 9 / 8 | −3 to −6 |
| allocs/op partition decoding | 89 | 60 | −29 |
| B/op pread / pwrite w64 | 2,450 / 1,746 | 1,830 / 1,123 | −25.3% / −35.7% |
| cpu-ns/op w64 (four cases) | | | −5.35% to −10.42% |
| cpu-ns/op w64-4 (four cases) | | | −4.77% to −9.54% |
| sec/op w1 read / write | | | −3.01% / −3.84% |

## F2a: embedded parse cursor (kept)

`ParseTypedResponseInto` used `wire.NewCursor`, which escaped through the
parser's cursor interface. The cursor is now `ResponseData.InternalParseCursor`.

| vs F1b | before | after | Δ |
|---|---:|---:|---:|
| allocs/op pread/w64 | 9 | 8 | −1 |
| cpu-ns/op | | | ~ (pwrite/w64 +0.67%, p=0.035, under the 2% bar) |

## C5: one shared admission per attempt (kept)

`InternalExecuteTyped` hands its admission, input reference, payload owners
and frame scratch to `typedRequestLease`, which embeds `wire.Lease`
(`wire.InitLease`) and is its own releaser. Two connection lock pairs, two
atomic lease operations and the release closure go away.

| vs F2a | before | after | Δ |
|---|---:|---:|---:|
| allocs/op write / pread / pwrite w64 | 6 / 8 / 7 | 5 / 7 / 6 | −1 |
| cpu-ns/op pread/w1 | 15.32µ | 14.57µ | −4.90% |
| cpu-ns/op w64-4 (four cases) | | | −2.30% to −4.04% |
| cpu-ns/op pread / pwrite w64 | | | −1.11% / −1.47% |
| Admission | 683.8n | 688.9n | +0.75% (p=0.035, under 2%, path untouched) |

## rt: no partition key without a partitioner (kept)

| vs C5 | before | after | Δ |
|---|---:|---:|---:|
| allocs/op pread / pwrite w64 | 7 / 6 | 6 / 5 | −1 |
| cpu-ns/op prepared w64 and w64-4 | | | −0.61% to −1.55% |

## A2: Query.Each and Query.One (kept)

`internal/execution/each.go`. `RunEach`/`RunOne` are the session-level
forms. Values that borrow response storage are valid only during the
callback, or until One returns. A new loopback workload, `pone`, is a
prepared point read through One.

| pone vs pread, same binary | pread | pone | Δ |
|---|---:|---:|---:|
| sec/op w64 | 5.840µ | 5.130µ | −12.16% |
| sec/op w64-4 | 3.585µ | 3.189µ | −11.03% |
| cpu-ns/op w64 / w64-4 | | | −12.14% / −10.50% |
| B/op | 1,750 | 1,044 | −40% |
| allocs/op | 6 | 5 | −1 |

Against rt the first run flagged write/w1 cpu-ns/op +4.0% (p=0.021), on a
path A2 does not touch. A 10×ABBA (n=20) rerun of the write cases showed no
difference (p≥0.54).

## A1: slim Result (kept, breaking)

`model.Result` keeps HostID, Attempts, Latency, Rows and
AchievedConsistency; warnings, trace IDs and custom payload move to
`resultDiagnostics` behind accessor methods, with `AddWarnings`,
`AddTraceIDs`, `AddCustomPayload` and `SetDiagnostics` for the driver.
`TestResultDiagnosticsAreCopyOnWrite` covers copies, merge and clone.
Making Latency opt-in was left out: its two clock reads are under 1% of a
request.

| vs A2 | before | after | Δ |
|---|---:|---:|---:|
| B/op pread/w64 / pone/w64 / point decoding | | | −7.37% / −6.23% / −5.03% |
| cpu-ns/op w64 (five cases) | | | −1.07% to −3.07% |
| cpu-ns/op w64-4 (five cases) | | | −1.59% to −2.40% |

## F2: response hot/cold split (kept)

`ResponseData.Extras` holds Keyspace, SchemaChange, PreparedID,
BindColumns, PkIndexes, TraceID, Payload and Warnings; the parser writes
them through `Extra()`.

| vs A1 | before | after | Δ |
|---|---:|---:|---:|
| B/op read and pread w64 | 1,621 | 1,429 | −11.84% |
| B/op pone/w64 | 980 | 787 | −19.69% |
| cpu-ns/op w64 (five cases) | | | −1.82% to −4.08% |
| cpu-ns/op w64-4 (five cases) | | | −2.25% to −2.74% |
| cpu-ns/op pone/w1 | 14.19µ | 13.27µ | −6.45% |
| Admission | 675.1n | 678.5n | +0.50% (p=0.007, under 2%) |
