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

Cells marked *planned* do not exist yet.

| | None | One row | Many rows | Pages |
|---|---|---|---|---|
| Throwaway | `Session.UnpreparedExec` | `Session.UnpreparedOne` | `Session.UnpreparedEach`, `Session.UnpreparedAll` *planned* | empty by design |
| Worker-owned | `Query.Exec` | `Query.One` | `Query.Each`, `Query.All` *planned*, `Query.Run` | `Query.RunPage` |
| Shared | `Prepared.Exec` | `Prepared.One` | `Prepared.Each`, `Prepared.All` *planned*, `Prepared.Run` *planned* | `Prepared.RunPage` *planned* |

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
- A handle returned by the shared form (`Rows`, `Page`) holds a pooled query.
  It carries a `runtime.AddCleanup` that releases the query if the handle is
  dropped unclosed, so a leaked handle cannot hold `Session.Close` open.
- A setting that changes how a cell executes (page size, consistency) is an
  option, not a new method. Unpaged execution is the page size
  `PageSize: cql.Unpaged`.
- Each column uses one verb in every row: `Exec`, `One`, `Each`, `RunPage`.
  Throwaway methods carry the `Unprepared` prefix in front of that verb.
- `AddToBatch` appears once per reusable form. `Query.Routing` reports where
  an execution with the given values would go, and lives on `Query`.
- Batches are outside this rule.

## Current deviations

Update this list as each one is resolved.

- `Query.All`, `Prepared.All` and `Session.UnpreparedAll` are missing from the
  many-rows range mode. They must benchmark as fast as `Each` before they ship.
- `Prepared.Run` is missing from the shared pull mode.
- `Prepared.RunPage` is missing from the shared pages cell.
