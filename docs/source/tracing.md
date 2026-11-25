# Tracing

The driver supports two unrelated kinds of tracing. CQL tracing asks the
cluster to record what it did for a request. Client-side tracing, such as
OpenTelemetry, records what the driver did, in your own tracing system.

## CQL tracing

Set `exec.Options.CQLTrace` and the cluster records a trace session of every
attempt in `system_traces`. `Result.CQLTraces` lists them in the order the
attempts finished, page after page. Each `cql.CQLTraceRef` carries the
session ID and the attempt it records: its coordinator `HostID`, its
`Attempt` number within the page, and whether it `Failed` or was
`Speculative`. The `Result` of a failed request lists its failed attempts'
sessions too, so tracing a timeout needs no error unwrapping. The IDs also
come back on each `cqlerr.AttemptFailure.CQLTraceID` and each
`routing.AttemptEvent.CQLTraceID`. Package `cqltrace` reads a session:

```go
insert := cql.Statement[int32, cql.EmptyRow]{
	CQL:         "INSERT INTO app.items (id) VALUES (?)",
	Params:      cql.OneParam(cql.Int32Codec),
	BindMarkers: cql.NewColumns(cql.ColumnSpec{Name: "id", Type: cql.IntType}),
}
result, err := session.UnpreparedExec(ctx, insert, 42, exec.Options{CQLTrace: true})
if err != nil {
	return err
}
for _, trace := range result.CQLTraces() {
	info, err := cqltrace.Fetch(ctx, session, trace.ID)
	if err != nil {
		return err
	}
	fmt.Print(info) // prints the session the way cqlsh does
}
```

The cluster writes a trace session after it answers, so `Fetch` polls until
the session's duration and at least one event are visible, within `ctx`.
Events can still arrive after that, so `Fetch` then waits until the event
count stays the same for 200ms; `Info.Settled` reports whether it did, and is
false when `ctx` ended first. A `cqltrace.Fetcher` changes that wait with
`Settle` (negative skips it), the poll interval and the read consistency.
When `ctx` ends before the session is ready, the error matches
`cqltrace.ErrNotReady`.

`cqltrace.FetchAll` reads every session a list of references names, which is
how to trace a request that failed: its `*cqlerr.ExecutionError` carries the
`Result` with the failed attempts' sessions. It returns one `Info` per
reference, and the errors of the sessions it could not read joined:

```go
_, err := session.UnpreparedExec(ctx, insert, 42, exec.Options{CQLTrace: true})
if failure, ok := errors.AsType[*cqlerr.ExecutionError](err); ok {
	infos, err := cqltrace.FetchAll(ctx, session, failure.Result.CQLTraces())
	if err != nil {
		log.Print(err)
	}
	for _, info := range infos {
		fmt.Print(info)
	}
}
```

`exec.PrepareOptions.CQLTrace` traces the PREPARE request itself; the driver
does not report that session's ID, so find it in `system_traces.sessions`.

## Client-side tracing

`observe.Hooks` is the hook for client-side tracing. Each of its fields is
one hook, a function the driver calls:

- `Request` receives the caller's context as a request is admitted and
  returns the context for its attempts, so a span started for the request
  can parent one span per attempt.
- `Attempt` is called for each attempt, retry or speculative attempt.

Each returns the function the driver calls when that request or attempt
finishes. The driver skips a nil hook and the work of filling its event, so
hooks that only need attempts build no request events. Later releases add
hooks as new fields, so existing `Hooks` keep compiling.

```go
hooks := observe.Hooks{
	Attempt: func(ctx context.Context, start observe.AttemptStart) func(routing.AttemptEvent) {
		return func(e routing.AttemptEvent) {
			log.Printf("attempt %d on %s at %v: %v (%v)", e.Attempt, e.Address, e.Consistency, e.Err, e.ExecutionStatus)
		}
	},
}
cfg, err := config.New(config.ScyllaV4, "localhost:9042").WithObserver(hooks).Build()
```

Set the hooks for the session with `config.ConfigBuilder.WithObserver`;
`Session.Observer` returns them. To run several, such as a tracer and a
metrics exporter, combine them with `observe.Join`: each hook of the result
calls the same hook of every argument in order, passes each `Request` the
context the one before returned, and keeps a panic in one from skipping the
others.

`exec.Options.Observer` adds hooks to one execution: they are joined after
the session's, as `observe.Join(session, extra)` would join them, so each
hook calls the session's first and the added one gets the context the
session's `Request` returned. Set `exec.Options.NoSessionObserver` to leave
the session's hooks out of the execution, so that only
`exec.Options.Observer`, or nothing, observes it.

Attempt hooks usually run on the goroutine that called the execution, the
start hook before each attempt and its end function after it, so a hook
that blocks delays that call. Keep them short and hand slow work to a
goroutine of your own.

The driver itself depends on no tracing library. To link the two kinds, an
`Attempt` hook can record `AttemptEvent.CQLTraceID` on its attempt span when
the request runs with `CQLTrace`.
