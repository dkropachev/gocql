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

## Reader pause flag (kept)

| vs F2 | Δ |
|---|---:|
| cpu-ns/op pread / pwrite w64 | −1.23% / −1.40% |
| cpu-ns/op w64-4 (three cases) | −0.61% to −1.30% |
| other cases | ~ |

## Round 2 cumulative

`c641e86b` (with the `cpu-ns/op` patch) against `e91f8ae5`, 5×ABBA (n=10),
quiet host. Every row is p=0.000.

| benchmark | before | after | Δ |
|---|---:|---:|---:|
| Loopback read/w64 cpu-ns/op | 6.222µ | 5.099µ | −18.05% |
| Loopback write/w64 cpu-ns/op | 5.826µ | 4.368µ | −25.02% |
| Loopback pread/w64 cpu-ns/op | 6.953µ | 5.575µ | −19.83% |
| Loopback pwrite/w64 cpu-ns/op | 5.969µ | 4.598µ | −22.98% |
| Loopback w64-4 cpu-ns/op (four cases) | | | −25.54% to −32.52% |
| Loopback w1 sec/op (four cases) | | | −5.90% to −8.03% |
| Loopback w64-4 sec/op (four cases) | | | −27.14% to −33.71% |
| Decoding point / stream / partition sec/op | | | −9.58 / −8.12 / −10.91% |
| allocs/op read, pread / write, pwrite (w64) | 18 / 19, 18 | 6 / 5, 5 | −12 to −14 |
| allocs/op partition decoding | 107 | 50 | −53% |
| B/op pread/w64 / pwrite/w64 | 3,030 / 2,405 | 1,430 / 787 | −52.8% / −67.3% |
| Admission | 815.5n, 3 allocs | 684.5n, 0 allocs | −16.06% |

A prepared point read through `Query.One` costs 5 allocations, 787 B and
4.88µs client CPU at w64, against 6.95µs, 18 allocations and 3,030 B
through `Run` before this round.

Not attempted this round: a query-owned `Rows` for `Run` (needs a
value-type handle to keep stale handles from seeing a later execution;
`One`/`Each` cover point reads instead), removing locks that only guard
handle misuse, and the stream-query path.

## Emulator screen against master after round 2

Same setup as the earlier screen: `redesign_compare.py --stage screen --load
maximum`, five canonical cases, two ABBA pairs × 30 s, client CPU 8 with
GOMAXPROCS=1, emulator CPUs 0-1,4-5. Master is the same frozen `1ceaac81`
build; the candidate is `466a6542`, frozen with `roadmap_build.py`. Results
are in `results/hp2-screen-master-20261103T205219Z`. The point case still
reads through `Run`, not `Query.One`.

| case | candidate / master, before round 2 | after | CPU µs/op (master) | allocs/op (master) | p99 µs (master) |
|---|---:|---:|---:|---:|---:|
| point-buffered w64 | 0.976 | **1.098** | 8.84 (9.71) | 17.1 (32.0) | 1,681 (1,498) |
| write w64 | 1.015 | **1.196** | 7.07 (8.46) | 13.1 (25.0) | 1,446 (1,376) |
| paged-buffered w4 | 0.930 | 0.942 | 428 (407) | 47.3 (150) | 2,456 (2,384) |
| paged-stream w4 | 0.832 | 0.843 | 478 (406) | 100 (150) | 2,724 (2,371) |
| point-stream w4 | 0.699 | 0.752 | 14.2 (10.7) | 42.8 (32.0) | 175 (109) |

Point reads and writes now beat master: throughput +9.8% and +19.6%, CPU
per operation −9% and −16%, allocations about half (CIs exclude 1). The
runner's overall verdict is still `rejected`, because the stream and paged
cases, which this round did not target, remain regressions. Maximum-load
p99 is 5–12% above master for point reads and writes; matched-rate latency
was not measured. The point case's bytes/op is 0.25% above master, which
the runner flags.

## Round 3

Plan: `~/.claude/plans/modular-questing-axolotl.md`. Gates as in round 2
(`compare.sh`, 5×ABBA, `cpu-ns/op`). Fresh profile of `pread/w64 -cpu 1` at
`89c18b3b`: allocations 1,429 B in 5 driver objects (rows+page holder 640 B,
response ≈370 B, request lease, receive lease, writer lease); mutexes 12%
flat plus 4% RWMutex reader atomics; `selectgo` and channel locks ≈10%;
`duffcopy` 5.7%.

### R1: cached protocol settings and resolved options (kept)

`protocol.Settings` is built once when the framer cache is initialised and
passed by pointer; its methods take pointer receivers. A query resolves its
options once and re-resolves only when a profile handle publishes a new
profile (options with a custom payload are still resolved per execution, to
check borrowed values for liveness). Capture reads the immutable prepared
fields without the prepared-state lock; routing partitioners are published
for lock-free reads. Tests that change framer defaults after initialisation
call `InternalRefreshNativeProtocol`.

| vs `89c18b3b` | Δ cpu-ns/op |
|---|---:|
| w64 (five cases) | −1.14% to −2.40% |
| pread / pwrite / pone w64-4 | −1.21% / −1.80% / −1.72% |

### R2: lock-free host view, host ID and plan hits (kept)

The ring host view is cleared under the write lock on every revision bump
and read without the lock. A connection caches its host ID once known.
The result plan cache serves identified hits from an atomic snapshot,
withdrawn when the cache switches to an unidentified plan
(`TestV2ResultPlanCacheUnidentifiedSwitchRetiresIdentityHit` fails without
the withdrawal). Host state atomics, a picker size counter and a lock-free
tablet feedback stamp were left out: each was under 0.5% of CPU, and the
stamp would have had to invalidate on any schema change instead of per table.

| vs R1 | Δ cpu-ns/op |
|---|---:|
| pread/w64 | −1.42% |
| write / pread w64-4 | −1.48% / −0.79% |
| other cases | ~ |

### R3: submission queue without a channel rendezvous (kept)

`submitTyped` appends to a mutex-guarded queue and wakes the writer only
when it is parked, instead of a three-way select on an unbuffered channel.
The writer takes the whole queue, yields once when more requests are in
flight than queued, and still writes at most 16 requests and 64 KiB per
socket write. On close it finishes every queued ticket unsent. A ticket
publishes its write result through an atomic state and signals its channel
only when the caller is blocked in `awaitWritten`. The response wait keeps
its connection-closed case: a parent context can cancel the connection
without `CloseWithError` delivering to pending calls.

| vs R2 | Δ cpu-ns/op |
|---|---:|
| w64 (five cases) | −6.31% to −8.53% |
| w64-4 (five cases) | −5.27% to −8.81% |
| w1 | ~ |

### R4–R8: cursor/owner atomics, Rows handle, leases, response storage (kept)

- Query generation is atomic; cursors check it without the query lock. An
  input owner keeps refs and the closed flag in one atomic word. The
  execution context replaces its Done channel only in `begin`, so transport
  waits read it without the lock. The reader's disarm flag is set without
  the deadline lock (the reader re-reads it under that lock before every
  socket read).
- Response diagnostics are merged only when a response has extras.
- **Breaking:** `Query.Run` and `Run` return `Rows[R]`, a value handle over
  iterator storage that a reusable query reuses each execution. A stale
  handle reports no rows, `Err` returns `ErrStaleRows`, `Close` does
  nothing (`TestQueryRowsHandleGoesStaleOnNextExecution`).
- The request lease lives in its pooled write ticket; an input owner reuses
  one writer lease once its final release finished, and a writer that
  overlaps it gets its own.
- A reusable query lends its response storage to the first page's attempt on
  the direct path. The reader parses into it only after claiming the call
  while the caller waits; a caller that stops waiting after the claim
  receives that response. Continuation, prefetched, speculative,
  single-page and stream responses keep their own storage.

| vs R3 | before | after | Δ |
|---|---:|---:|---:|
| cpu-ns/op read / pread w64 | 4.646µ / 5.110µ | 3.899µ / 4.316µ | −16.08% / −15.53% |
| cpu-ns/op write / pwrite / pone w64 | | | −8.54% / −8.83% / −7.07% |
| cpu-ns/op w64-4 (five cases) | | | −10.71% to −18.12% |
| B/op pread / pwrite w64 | 1,429 / 787 | 162 / 162 | −88.7% / −79.4% |
| allocs/op pread / pwrite w64 | 6 / 5 | 2 / 2 | |
| Admission | 619.5n | 587.5n | −5.16% |

Rejected: removing the Rows/Page mutexes. It breaks closing rows from
another goroutine to interrupt a blocked page fetch
(`TestV2RowsNextIntoPagingAndCloseInterrupt`), and keeping that would need a
busy flag costing about as much as the mutex. Not done: the breaking lazy
`Result` (`Exec`/`One`/`Each` without `Result`): building it is ≈2% of CPU,
mostly clock reads that a lazy result still needs. The two remaining
allocations are the benchmark's own and the receive-frame lease, which
reaches borrowed result values and so needs generation-checked leases to be
recycled. Response parsing still runs on the reader goroutine; moving it to
callers needs the metadata-reuse cache made safe for concurrent parsers.

Unit race suite (only the known baseline failure), `-race -count=20` on
`execution`, `transport`, `wire`, and emulator smoke OK.

## Round 3 cumulative

`89c18b3b` against `fccc29f4`, 5×ABBA (n=10). Every row p≤0.023.

| benchmark | before | after | Δ |
|---|---:|---:|---:|
| Loopback read/w64 cpu-ns/op | 5.148µ | 3.833µ | −25.54% |
| Loopback write/w64 cpu-ns/op | 4.438µ | 3.500µ | −21.15% |
| Loopback pread/w64 cpu-ns/op | 5.629µ | 4.223µ | −24.98% |
| Loopback pwrite/w64 cpu-ns/op | 4.695µ | 3.690µ | −21.42% |
| Loopback pone/w64 cpu-ns/op | 4.977µ | 4.003µ | −19.56% |
| Loopback w64-4 cpu-ns/op (five cases) | | | −18.96% to −25.81% |
| Loopback w1 cpu-ns/op (read, pread, pwrite, pone) | | | −5.14% to −7.80% |
| B/op every w64 case | 787–1,429 | 162 | −79% to −89% |
| allocs/op every w64 case | 5–6 | 2 | |
| Admission | 692.7n | 588.1n | −15.09% |

## Round 3b: the rest of the plan

Each item against the previous one, 5×ABBA unless noted. The host ran an
endpoint security agent (`s1-agent`, about one core) and other sessions
throughout, so several gates were noisy; decisions rely on the
reproducible ones.

- **R9, breaking:** `Query.Exec` and `Each` return only an error; `One` returns
  `(bool, error)`; `Query.LastResult()` reports the latest outcome. Building
  a `Result` was ≈2% of CPU, mostly clock reads it still needs, so this is an
  API change more than a CPU one.
- **R10:** the page lock is gone. A page is used from one goroutine, and
  within Rows only under the rows' lock, which also covers a blocking fetch,
  so closing rows from another goroutine still interrupts it.
- **R11:** received-frame leases are recycled. A lease's state word holds a
  generation next to its reference count; borrowed views (`cqltype.Bytes`
  and the codec readers built from it) carry a `wire.Ref` naming their
  generation, so a released view stays dead after its lease is reused
  (`TestCachedLeaseRecyclingKeepsStaleRefsDead`, which fails if `Ref.Alive`
  ignores the generation).

| R9–R11 vs R8 (`fccc29f4`) | Δ |
|---|---:|
| cpu-ns/op read / write / pread w64 | −4.09% / −4.89% / −6.48% |
| allocs/op every loopback w64 case | 2 → 1 (the benchmark's own) |
| B/op every loopback w64 case | 162 → 18 |

- **R12, rejected:** parsing typed responses on the calling goroutine. A
  focused 10×ABBA of the `-cpu 4` cases showed no gain and a consistent cost
  (geomean +1.48%, pread +1.84% at p=0.039): the reader has just pulled the
  frame into its core's cache. Reverted (`8cdbcb62`); the immutable
  metadata-reuse entry went with it.
- **R13:** host state is an atomic cell, a host's ID is cached once known,
  and tablet feedback captures take a read lock. The picker size counter was
  left out: `Pick` still needs the picker lock for its slice.
- **R14:** a dialed connection closes itself when its context is cancelled,
  and a close always delivers to waiting typed calls, so the typed response
  wait selects on the response and the attempt only
  (`TestTypedWaitEndsWhenConnectionContextIsCancelled` fails without the
  hook).

R13–R14 against R12's predecessor: no case moved outside the noise (all
p>0.05; geomean −1.2%).

The v2 comparison client now reads through `Query.Each` (exact row counts,
no iterator) and joins a query with `Wait` only after a failure.

## Round 3 against master

### Emulator screen

`redesign_compare.py --stage screen --load maximum`, the same setup as the
earlier screens (frozen master `1ceaac81`, client CPU 8, GOMAXPROCS=1,
emulator CPUs 0-1,4-5, two ABBA pairs × 30 s). Ratios are v2/master with
95% CIs.

`hp3-screen-master-20261104T002535Z`: candidate `8cdbcb62`, client unchanged
(reads through `Run`).

| case | throughput | CPU/op | allocs/op | bytes/op | p99 |
|---|---:|---:|---:|---:|---:|
| point-buffered w64 | 1.232 [1.215, 1.250] | 0.808 [0.800, 0.816] | 0.378 | 0.363 | 0.865 [0.807, 0.926] |
| point-stream w4 | 0.789 [0.787, 0.792] | 1.269 [1.264, 1.274] | 1.245 | 1.534 | 1.417 [1.410, 1.424] |
| paged-buffered w4 | 0.964 [0.962, 0.967] | 1.030 [1.026, 1.034] | 0.203 | 0.338 | 1.029 [0.959, 1.104] |
| paged-stream w4 | 0.845 [0.800, 0.894] | 1.220 [1.207, 1.233] | 0.569 | 0.634 | 0.817 [0.486, 1.373] |
| write w64 | 1.402 [1.370, 1.436] | 0.714 [0.697, 0.732] | 0.364 | 0.420 | 0.723 [0.712, 0.734] |

`hp3c-screen-master-20261104T003057Z`: candidate `ea52b7b7`, client reads
through `Query.Each`.

| case | throughput | CPU/op | allocs/op | bytes/op | p99 |
|---|---:|---:|---:|---:|---:|
| point-buffered w64 | 1.370 [1.369, 1.371] | 0.730 [0.730, 0.730] | 0.378 | 0.363 | 0.769 [0.765, 0.772] |
| point-stream w4 | 0.778 [0.776, 0.780] | 1.246 [1.214, 1.279] | 1.245 | 1.534 | 1.608 [1.537, 1.683] |
| paged-buffered w4 | 0.971 [0.966, 0.977] | 1.025 [1.025, 1.025] | 0.176 | 0.229 | 0.950 [0.895, 1.008] |
| paged-stream w4 | 0.844 [0.838, 0.850] | 1.176 [1.169, 1.184] | 0.568 | 0.632 | 1.166 [1.119, 1.216] |
| write w64 | 1.361 [1.355, 1.368] | 0.735 [0.731, 0.738] | 0.364 | 0.420 | 0.749 [0.745, 0.754] |

Before round 3 (`hp2-screen`), point reads and writes were 1.098 and 1.196
of master. Streaming, which these rounds did not target, still regresses;
paged buffered reads are 3% short on throughput with a fifth of master's
allocations.

### Three-node Scylla cluster

`cluster.sh`: Scylla 2026.3.3 in Docker, nodes pinned to CPUs 0-1, 4-5 and
6-7, two shards each, 3 GiB, developer mode (no I/O calibration). The
emulator was paused. `cluster_bench.sh` ran every client on CPU 8 with
GOMAXPROCS=1, ABBA pairs, 10 s warmup and 30 s measurement, RF=3
LOCAL_QUORUM. Master is the frozen `1ceaac81` build; v2 is `ea52b7b7`
(client reads through `Query.Each`). Medians; all runs had zero errors.
Results: `results/cluster-master-vs-v2-20261104T003057Z`.

Closed-loop throughput (2 pairs; 64 workers, paged 4):

| scenario | ops/s master | ops/s v2 | v2/master | CPU µs/op | allocs/op | B/op | p99 µs |
|---|---:|---:|---:|---:|---:|---:|---:|
| point read | 100,821 | 129,169 | **1.281** | 9.9 → 7.7 | 26.0 → 12.2 | 1,993 → 839 | 1,623 → 1,204 |
| write | 50,319 | 63,005 | **1.252** | 9.9 → 8.6 | 25.0 → 9.4 | 1,838 → 780 | 20,047 → 12,615 |
| paged read (128 × 4 KiB) | 2,275 | 2,103 | 0.925 | 432 → 451 | 120 → 26.7 | 11,949 → 3,677 | 3,078 → 3,814 |

The server dominated write variance (master's two write runs: 60k and
40k ops/s).

Open-loop latency at 40% of master's throughput, the same scheduled rate
for both (3 pairs):

| scenario | rate/s | p90 µs | p95 µs | p99 µs | CPU µs/op |
|---|---:|---:|---:|---:|---:|
| point read | 40,328 | 147 → 141 | 442 → 185 | 1,539 → 1,631 | 16.1 → 17.2 |
| write | 20,128 | 1,175 → 981 | 2,006 → 1,849 | 2,675 → 2,649 | 15.4 → 17.2 |
| paged read | 910 | 2,331 → 2,327 | 2,535 → 2,517 | 4,055 → 3,959 | 506 → 519 |

At 70% of master's throughput (2 pairs), a read rate of 70.6k/s keeps one
client vCPU about 97% busy for both drivers, so latency there is client
queueing; v2's read p90/p99 were 1,411/14,179 µs against master's
9,847/20,399, and its write p90/p99 were 5,211/24,239 against 2,405/17,447.

v2 needs less CPU per operation at saturation but more at 40% load:
with fewer requests in flight, the writer batches fewer per socket write
and the woken-writer hand-off costs more per request. Low-load write
batching is the next target, together with the streaming and paged
regressions.

## Round 4: paged Query.Each

Target: paged reads, the remaining regression against master on the cluster
(0.925× throughput, 451 vs 432 CPU µs/op). Plan:
`~/.claude/plans/compressed-wondering-parnas.md`. Each item is measured
against its predecessor with `hp/compare.sh` (5×ABBA, n=10) and kept under
the round 1 rule.

### P0: gates

The loopback server answers an unprepared QUERY with the page its paging
state names: four pages of 32 × 4 KiB rows, then an empty page. New gates
(`HP_PAGE`): `page`, `pagectx` (a per-operation `WithTimeout`, as the
latency client passes), `pagepto` (`PageTimeout` 2 s under `Timeout` 10 s),
`pagepf` (Prefetch 1), `pageb` (borrowed blob, measurement only) and
`preadctx` (pread under a per-operation context); T0 gains
`partition/each`. Baseline `r4base` (`0ed808ee`): `page/w1` 16 allocs,
2,140 B/op; `pagepto/w1` 63 allocs, 4,740 B/op; `preadctx/w1` 11 allocs.

The paged loopback case is dominated by `read` syscalls (65%) and the
owned-blob copy (13%); the driver's control path is a few percent, so most
items below show as allocations at neutral CPU.

`compare.sh` resets the old side's sample file, so chained comparisons read
each result from the `*.cmp.out` written when that comparison finished.

### P1: lend query response storage to every Each page (kept)

`executePageDirect` cleared the lent response storage after the first page.
Each releases a page before requesting the next, so continuation pages now
parse into the query's storage too. Paging tokens are copied out of the
frame, so the history keeps them across the reuse
(`TestQueryEachReusedStorageKeepsTokensAndDetectsRepeats`,
`TestQueryEachUnpreparedContinuationRepairs`).

| vs r4base | before | after |
|---|---:|---:|
| allocs/op page/w1, pagectx/w1, partition/each | 16 / 25 / 29 | 12 / 21 / 25 |
| B/op page/w1 | 2,140 | 604 |
| cpu-ns/op, every case | | ~ (pagectx/w4 −1.5%, p=0.019) |

### Writev header in the connection writer (kept, found while profiling)

`net.Buffers.WriteTo` takes its receiver by pointer, so the header passed
by value moved to the heap on every socket write: one allocation per
request at w1 and per page on a paged read. The writer keeps the header
next to its span scratch, under the same semaphore.

| vs P2 | before | after |
|---|---:|---:|
| allocs/op every loopback w1 case | 2 | 1 |
| allocs/op page/w1 | 10 | 5 |
| B/op page/w1 / read/w1 | 140 / 40 | 20 / 16 |
| cpu-ns/op | | ~ (geomean +0.06%) |

### P2: inline paging history (kept after a revision)

The first eight tokens are compared inline; the ninth spills them into the
hashed map (`TestV2PagingHistoryInlineSpill`; a mutation that halves the
inline scan fails it and the fuzz test).

As first written (`25ae33a4`) the inline array was part of every history:
−2 allocs on every paged case including Rows, but `point/stream` +281 B/op
(+9.2%, a history per stream execution) and `read/w1` cpu +2.85%
(p=0.035), over the 2% rule. The revision (`4d0a4dde`) makes the inline
storage a pointer lent by reusable owners only: Each from its frame, a
query's iterator from the query; streams keep the map.

| P2 vs P1 (`25ae33a4`) | before | after |
|---|---:|---:|
| allocs/op page/w1, partition/buffered, partition/each | 12 / 33 / 25 | 10 / 31 / 23 |
| B/op page/w1 | 604 | 140 |
| B/op point/stream | 3,121 | 3,409 (+9.2%) |
| cpu-ns/op read/w1 | 12.25µ | 12.60µ (+2.85%, p=0.035) |

| P2 revision vs P9 (`4d0a4dde` vs `177b08d1`) | before | after |
|---|---:|---:|
| B/op point/stream | 3,385 | 3,097 (−8.5%) |
| allocs, B/op paged cases | | unchanged |
| pwrite/w64-4 sec/op | | +3.88% (p=0.019, ±25%), see the rerun below |

### P3: pin continuation pages to the previous page's connection (kept, marginal)

A continuation page goes to the connection that served the previous page
while it is open and its host is up. A plan is built only if that attempt
is retried, and it skips the pinned host, as the plan would have moved past
its first target (`TestQueryEachPinsContinuationPages`,
`TestQueryEachRePlansWhenPinnedHostIsDown`,
`TestQueryEachPinnedRetryNextMovesHost`, `TestSkipHostPlanOmitsPinnedHost`).

The loopback session has one host and one connection, so the selection it
removes is already cheap there; the benefit is meant for token-aware plans
over several hosts.

| vs writev | Δ |
|---|---:|
| partition/each sec/op | −0.50% (p=0.023) |
| pagectx/w4 cpu-ns/op | −0.82% (p=0.029) |
| read/write/pread/pone w64 and w64-4 cpu-ns/op | +0.67% to +1.09% (p<0.05) |
| allocs, B/op | unchanged |

### P4: reuse a query-owned page timeout context (kept)

With `PageTimeout` below the remaining deadline every direct page derived
`context.WithTimeout` from the execution context, about nine allocations
per page. A second reusable execution context per query now carries the
page timeout. It links to the execution context directly instead of
through `AfterFunc`: the parent cancels the linked generation, a
generation check keeps a late cancellation off the next page, and finishing
the parent cancels a page still linked (`TestExecutionContextLinkedChild`,
`TestExecutionContextLinkedChildRaces`, `TestQueryEachPageTimeoutPerPage`,
`TestQueryEachParentCancelReachesPageContext`). A page that overlaps
another takes a context of its own.

| vs P3 | before | after |
|---|---:|---:|
| allocs/op pagepto/w1, w4 | 52 / 52 | 5 / 5 |
| B/op pagepto/w1 | 2,620 | 20 |
| pwrite/w64 sec/op | | +1.59% (p=0.001) |
| pone/w1 sec/op | | +6.62% (p=0.029), see the rerun below |

### P5: watch a cancellable caller context lazily (kept)

Every execution under a cancellable context registered
`context.AfterFunc` on it. The execution context now only remembers such a
parent: `Err` checks it and cancels the generation once it is done, the
typed response wait selects on the parent's Done channel next to the
attempt's, and `Done` or `AfterFunc` register a watcher only once something
may wait on the execution context's own channel. Linked contexts lock
child, then parent (`TestExecutionContextLazyParent`,
`TestQueryEachParentCancelInterruptsWait` — fails after 5 s without the
parent case — and `TestQueryEachParentCancelInterruptsRetryDelay`).

| vs P4 | before | after | Δ |
|---|---:|---:|---:|
| allocs/op preadctx/w1, pagectx/w1, point/buffered | 10 / 14 / 10 | 6 / 10 / 6 | |
| B/op preadctx/w1 | 784 | 400 | −49% |
| preadctx/w64 cpu-ns/op | 5.233µ | 4.780µ | −8.66% |
| pagectx/w1 sec/op | 211.5µ | 197.7µ | −6.55% (p=0.035) |
| point/buffered sec/op | 10.26µ | 9.91µ | −3.44% |
| Admission | 592.4n | 583.4n | −1.52% |
| partition/buffered sec/op | | | +0.81% |

### Reruns of flagged cases

Two compare runs flagged a case the change does not touch; a focused
10×ABBA (n=20) of each, on the gate CPUs:

| case | pair | sec/op | cpu-ns/op |
|---|---|---:|---:|
| pone/w1 | P4 vs P3 | −3.78% (p=0.049) | ~ (p=0.072) |
| pwrite/w64-4 | P2 revision vs P9 | ~ (p=0.189) | ~ (p=0.578) |

Both were noise; P4 and the revision are kept.

### P9: prefetch in Each (rejected)

With Prefetch 1, Each requested the next page as soon as a page arrived and
parsed it into a second query-owned response slot while the current page's
rows were delivered; an early stop cancelled the execution for real and
joined the outstanding page (`177b08d1`). Gates at Prefetch 0 did not move
(pone/w64 +1.42%, p=0.027), but the target did not improve:

| | page/w1 | pagepf/w1 |
|---|---:|---:|
| sec/op, -cpu 1 (compare run) | 202.8µ | 215.0µ |
| cpu-ns/op, -cpu 1 | 158.3µ | 170.3µ |
| sec/op, -cpu 2 (6 rounds, CPUs 12-15) | 422–432µ | 462–479µ |
| allocs/op | 5 | 9 |

A loopback round trip leaves nothing for the overlap to hide, so the
prefetch goroutine is only cost. It may pay off over a real network round
trip, which these gates do not measure; the comparison client uses
Prefetch 0 for both drivers. Reverted in `65c7dfa6`.

### Not done

- **P6** (skip redundant `SetReadDeadline`) and **P7** (per-connection
  receive-buffer slot): `armDeadline` is 0.19% and `ReceiveCache`
  0.4% of `page/w1` CPU, under the 1% prerequisite.
- **P8** (per-page instead of per-row cancellation checks): the per-row
  checks are what stop a cancellation inside the callback at the next row;
  they cost about 0.4%.

## Round 4 cumulative

`65c7dfa6` against `r4base` (`0ed808ee`), 5×ABBA. Allocations and bytes are
exact; the timing side of this first run is unusable (±20–60% per row),
because a Scylla build used all 20 CPUs during it, and is repeated below.

| benchmark | allocs/op before → after | B/op before → after |
|---|---:|---:|
| page/w1 | 16 → 5 | 2,140 → 20 |
| pagectx/w1 | 25 → 10 | 2,908 → 404 |
| pagepto/w1 | 63 → 5 | 4,740 → 20 |
| partition/each (T0) | 29 → 14 | 3,020 → 516 |
| partition/buffered (T0, Rows) | 33 → 22 | 4,300 → 3,332 |
| point/buffered (T0) | 11 → 6 | 817 → 409 |
| preadctx/w1, w64 | 11, 10 → 6, 6 | 808, 788 → 400, 401 |
| every point read/write loopback case w1 | 2 → 1 | 40 → 16 |
| every point read/write loopback case w64 | 1 → 1 | 18 → 16 |
| pageb/w1 (borrowed, new) | — → 5 | — → 20 |

A third run on a quiet host (`r4final3`, ±1–3% per row) gives the timing:

| benchmark | before | after | Δ |
|---|---:|---:|---:|
| preadctx/w64 cpu-ns/op | 5.141µ | 4.737µ | −7.86% |
| pagepto/w1, w4 cpu-ns/op | 161.8µ / 109.8µ | 154.4µ / 103.3µ | −4.58% / −5.92% |
| pagectx/w4 cpu-ns/op | 105.5µ | 103.7µ | −1.75% |
| partition/each sec/op (T0) | 90.29µ | 87.81µ | −2.75% |
| point/buffered sec/op (T0) | 10.24µ | 9.85µ | −3.79% |
| page/w1, page/w4 cpu-ns/op | 147.5µ / 100.5µ | 146.5µ / 98.5µ | ~ |
| read, pread, pone w64-4 cpu-ns/op | | | +0.64% to +1.35% |
| pwrite/w64 cpu-ns/op | 3.509µ | 3.557µ | +1.37% |
| point/stream sec/op | 11.83µ | 11.91µ | +0.66% |
| Admission | 579.4n | 585.8n | +1.10% |

No case regressed by more than 2%. The small w64 costs come from P3 (pinned
connection fields and checks on every direct page). `pageb` (borrowed
values) runs at 132.6µ cpu-ns/op against `page`'s 146.5µ: the owned-blob
copy is about 10% of a paged read's client CPU.

## Round 4 against master

### Emulator screen

`hp4-screen-master-20261104T034637Z`: candidate `65c7dfa6`, the same setup
as `hp3c` (frozen master `1ceaac81`, client CPU 8, GOMAXPROCS=1, emulator
CPUs 0-1,4-5, two ABBA pairs × 30 s). Ratios are v2/master with 95% CIs;
the `hp3c` column is the end of round 3.

| case | throughput hp3c → hp4 | CPU/op hp3c → hp4 | allocs/op | bytes/op | p99 hp3c → hp4 |
|---|---:|---:|---:|---:|---:|
| point-buffered w64 | 1.370 → 1.428 [1.421, 1.434] | 0.730 → 0.699 | 0.378 → 0.250 | 0.363 → 0.195 | 0.769 → 0.548 |
| point-stream w4 | 0.778 → 0.770 [0.765, 0.776] | 1.246 → 1.299 | 1.245 → 1.219 | 1.534 → 1.526 | 1.608 → 1.521 |
| paged-buffered w4 | 0.971 → 0.970 [0.960, 0.979] | 1.025 → 1.023 | 0.176 → 0.081 | 0.229 → 0.038 | 0.950 → 0.974 |
| paged-stream w4 | 0.844 → 0.844 [0.831, 0.857] | 1.176 → 1.175 | 0.568 → 0.568 | 0.632 → 0.641 | 1.166 → 1.161 |
| write w64 | 1.361 → 1.476 [1.465, 1.487] | 0.735 → 0.677 | 0.364 → 0.200 | 0.420 → 0.210 | 0.749 → 0.479 |

Paged buffered reads now allocate 8% of master's objects and 4% of its
bytes, but throughput and CPU per operation did not move: allocation is not
what keeps paged reads 3% behind master on the emulator. Point reads and
writes gained throughput and tail latency, mostly from the writev header
(one allocation per socket write) and the lazily watched caller context.

### Three-node Scylla cluster

Same setup as round 3 (`cluster.sh`, Scylla 2026.3.3, nodes on CPUs 0-1,
4-5 and 6-7, clients on CPU 8 with GOMAXPROCS=1, RF=3 LOCAL_QUORUM, 10 s
warmup and 30 s measurement), emulator paused. Master is the frozen
`1ceaac81` build; v2 is `65c7dfa6`. Medians; every run had zero errors.
Results: `results/cluster-master-vs-v2-hp4-20261104T034637Z`. Absolute
rates for both drivers were lower than in the round 3 run (master paged
1,910 against 2,275 ops/s), so compare ratios, not rates, across runs.

Closed-loop throughput (2 pairs; 64 workers, paged 4):

| scenario | v2/master round 3 | v2/master round 4 | CPU µs/op master → v2 | allocs/op | B/op | p99 µs |
|---|---:|---:|---:|---:|---:|---:|
| point read | 1.281 | **1.257** | 11.5 → 9.1 | 26.0 → 8.0 | 1,993 → 449 | 1,976 → 1,290 |
| write | 1.252 | **1.089** | 12.5 → 10.4 | 25.0 → 5.0 | 1,838 → 386 | 10,749 → 9,953 |
| paged read (128 × 4 KiB) | 0.925 | **1.019** | 505 → 484 | 120 → 12.2 | 12,014 → 1,187 | 4,283 → 4,394 |

Per pair the paged ratio was 0.96 and 1.08, so paged reads are at parity
with master within this run's noise, against a 7.5% deficit before; v2's
paged CPU per operation is now 4% below master's (it was 4% above). Point
reads were 1.26 and 1.25 per pair; writes 1.11 and 1.07 (round 3's write
runs varied 40k–60k ops/s for master alone).

Open-loop latency at 40% of master's throughput, the same scheduled rate
for both (3 pairs):

| scenario | rate/s | p90 µs | p95 µs | p99 µs | CPU µs/op |
|---|---:|---:|---:|---:|---:|
| point read | 35,106 | 701 → 249 | 1,644 → 401 | 2,977 → 2,395 | 22.0 → 23.2 |
| write | 21,707 | 762 → 444 | 2,057 → 1,839 | 2,645 → 2,625 | 16.2 → 17.1 |
| paged read | 764 | 2,659 → 2,889 | 3,085 → 3,703 | 4,599 → 5,675 | 548 → 574 |

Paged tails are noise-dominated: per pair v2's p99 was 5,707/5,331,
5,675/4,025 and 4,009/4,599 against master. CPU per operation at 40% load
stays about 5% above master in every scenario and every pair, the low-load
cost round 3 attributed to smaller write batches and the woken-writer
hand-off; this round did not target it.

### Outcome

Round 4 removed most per-page and per-execution allocations from paged
reads (16 → 5 allocations and 2,140 → 20 B per loopback paged read; 120 →
12 allocations per cluster paged read against master's 120) and four per
execution under a cancellable context. On the cluster this closed the
paged throughput gap (0.925 → 1.019, parity within noise) and turned paged
CPU at saturation from +4% to −4% against master; on the emulator paged
throughput did not move (0.970), so the remaining emulator gap is not
allocation. The open items are unchanged from round 3: low-load CPU (write
batching at few requests in flight) and streaming.
