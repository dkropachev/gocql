# Execution API surface

The public execution API is a grid: **statement kind** times **output shape**.
Each cell holds exactly one method. Streaming sits outside the grid as its own
API. Apply this whenever you add, rename, remove or review a public method that
executes a statement or reads its results.

## Statement kinds

1. **Throwaway**: CQL text executed once, unprepared. UX is the only concern:
   the fewest calls, no handle to close, options optional. Add no
   optimisations that only this path uses; it shares execution code with the
   reusable kinds, so that code keeps its full performance.
2. **Reusable**: a prepared statement. It gets every performance optimisation.
   It comes in two forms:
   - **Worker-owned**: one goroutine owns it, it keeps its buffers and
     resolved options across executions, and the caller closes it.
   - **Shared**: safe for any number of goroutines at once, with no close
     obligation; per-call state comes from an internal pool.

## Output shapes

1. **None**: success or error only.
2. **One row**.
3. **Many rows**: every row across all pages, in one call.
4. **Pages**: one page per call, resumed from a saved page state. It serves
   callers whose position must outlive the call: a paginated endpoint that
   hands the client a cursor, or a long scan that checkpoints and resumes after
   a restart. The other shapes page under the hood and expose no state.

## The grid

| | None | One row | Many rows | Pages |
|---|---|---|---|---|
| Throwaway | `Session.UnpreparedExec` | `Session.UnpreparedOne` | `Session.UnpreparedEach`, `Session.UnpreparedAll` | empty by design |
| Worker-owned | `Query.Exec` | `Query.One` | `Query.Each`, `Query.All`, `Query.Run` | `Query.RunPage` |
| Shared | `Prepared.Exec` | `Prepared.One` | `Prepared.Each`, `Prepared.All`, `Prepared.Run` | `Prepared.RunPage` |

Throwaway pages stay empty: a caller that pages through results is executing
the statement repeatedly, so it prepares the statement.

## Options

- Worker-owned: options are fixed when the query is created.
- Shared: each call ends in `options ...CompiledOptions`, made once by
  `Session.CompileOptions`. The variadic is Go's form of an optional
  parameter, so the common call carries no options at all (`p.Exec(ctx, v)`)
  and uses the session defaults. At most one value is accepted; a second
  fails the call at once with a validation error. The type is
  `CompiledOptions`, not `Options`, because the shared form is the hot path:
  validation, payload copying and default resolution happen once at setup,
  and each call passes a pointer to the frozen result.
- Throwaway: each call ends in `options ...Options`, the same optional
  parameter with at most one value. The call validates and resolves it: a
  throwaway statement has no setup step to compile options in, and its path
  is not optimised.

A new cell follows the options form of its row.

## Streaming

Streaming is a standalone, performance-first API that decodes into caller
storage. It is always prepared, worker-owned, and has one output shape, many
rows: `Session.PrepareStream` → `PreparedStream.Query` → `StreamQuery.Run` →
`RowStream`. Adding another shape or entry point to streaming is a change to
this rule, made with the maintainer.

Streaming and the many-rows methods fetch pages under the hood and expose no
page state. Explicit paging is the pages column only.

## Rules

- A new public execution method fills an empty cell or replaces the method in
  its cell. A second method in an occupied cell is a duplicate: merge the two
  or remove one. The many-rows column is the exception: it holds one method per
  consumption mode, and no others.
  - **Push**, `Each`: the driver calls back with each row.
  - **Range**, `All`: a range-over-func loop; `break` releases the execution.
    It takes a destination like `Each`, yields `(*R, error)`, and returns no
    `Result`; callers that need the result use `Each` or `Run`.
  - **Pull**, `Run` → `Rows`: the caller advances, picks the destination of
    each row, and can pin a page with `Retain`. Absent from the throwaway row,
    because it returns a handle.
- A handle returned by the shared form (`Rows`, `Page`) holds a pooled query
  until it is closed. The session registers that query while the handle is
  open, so `Session.Close` stops the handle instead of waiting for it, and a
  `runtime.AddCleanup` releases the query if the handle is dropped unclosed.
- A setting that changes how a cell executes (consistency, timeouts) is an
  option, not a new method. The page size of the pages column is the
  exception: it is a `RunPage` argument, because a paginated endpoint takes it
  from each request, and an option would mean compiling options per request.
  Zero uses the resolved page size. Unpaged execution is the page size
  `cql.Unpaged`, as an option or as that argument.
- The none column (`Exec`, `UnpreparedExec`, `ExecBatch`) rejects a statement
  or batch the driver knows to be conditional with `cqlerr.ErrConditionalExec`
  before sending it. A conditional result reports whether the write applied,
  and the none shape cannot return that. The kind is known when the statement
  declares it or the server marks it, as ScyllaDB does in prepared metadata.
  An undeclared kind on Cassandra stays unknown: the driver never parses CQL,
  so such a request is sent and its result columns are rejected afterwards.
- Each column uses one verb in every row: `Exec`, `One`, `Each`, `RunPage`.
  Throwaway methods carry the `Unprepared` prefix in front of that verb.
- `AddToBatch` appears once per reusable form. `Query.Routing` reports where
  an execution with the given values would go, and lives on `Query`.
- Batches follow the batch rules below, not the grid.

## Batches

A batch is a reusable request: it gets the same performance treatment as the
reusable kinds, and its happy path is a reused `Batch` sent with `ExecBatch`
and no options.

- **Building.** A member joins with `AddToBatch` on the statement that
  supplies it: one method per reusable form (`Prepared.AddToBatch`,
  `Query.AddToBatch`). `AddToBatch` encodes the value straight into storage
  the `Batch` owns and touches no execution state. The zero `Batch` is ready,
  and `Reset` keeps the storage for the next batch.
- **Sending.** The session sends a batch, and the batch kind is a call
  argument, not batch state:
  - None: `Session.ExecBatch`. A batch whose members the driver knows to be
    conditional is rejected before it is sent, because the result of an
    applied-or-not write cannot be returned as success or error alone.
  - Rows: one method, for conditional batches. The response is a single page
    with no continuation, so there is no paging, prefetch or page state.
- **Options.** As for the shared form: an optional `CompiledOptions`, so the
  send resolves nothing per call. Paging options are rejected.
- **Performance contract.** Once a reused batch has reached its working size,
  appending, `Reset` and `ExecBatch` allocate nothing
  (`internal/session/batch_allocs_test.go`), and `BenchmarkBatch` is the
  gate for changes to the path. To keep that:
  - `ExecBatch` runs on a pooled session query and takes the void-result
    path. It returns no handle, and keeps no pooled query once it returns.
  - The wire batch of an attempt reuses the batch's item storage. Only a
    concurrent speculative attempt builds its own.
  - Per-member work at send time is a lookup of the prepared ID on the
    attempt's host. Encoding, routing and validation happen at `AddToBatch`.
  - A cancelled execution that may still read the batch keeps its storage,
    and `Reset` abandons the storage to that execution instead of copying it
    on every send.
- **Routing.** The token is the first routable member's token. Members must
  share one session and one keyspace, checked at `AddToBatch`.

## Current deviations

- The rows method is `Session.RunBatch`. It returns a `*Page` that holds a
  pooled query until it is closed, which breaks the no-handle rule and reuses
  the pages-column type for a response that has a single page. Resolved by
  replacing it with a push method that has no handle.
- There is no throwaway member: constant CQL must be prepared before it can
  join a batch. Resolved by adding an unprepared `AddToBatch`.
