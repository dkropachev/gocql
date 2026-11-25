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

`exec.PrepareOptions.CQLTrace` traces the PREPARE request itself; the driver
does not report that session's ID, so find it in `system_traces.sessions`.

## Client-side tracing

`observe.Observer` is the hook for client-side tracing. An observer is any
value that implements one or more hook interfaces:

- `observe.RequestObserver`: its `StartRequest` receives the caller's
  context as a request is admitted and returns the context for its attempts,
  so a span started for the request can parent one span per attempt.
- `observe.AttemptObserver`: its `StartAttempt` is called for each attempt,
  retry or speculative attempt.

Each start returns the function the driver calls when that request or
attempt finishes. The driver looks the hooks up once, when the observer is
installed, and skips the call and its event for a hook the observer does
not implement, so an observer that only needs attempts builds no request
events. An observer that implements no hook is rejected. Later releases add
hooks as new interfaces, so an existing observer keeps compiling.
`observe.RequestObserverFunc` and `observe.AttemptObserverFunc` make an
observer from a function.

Set the observer for the session with `config.ConfigBuilder.WithObserver`;
`Session.Observer` returns it. To run several observers, such as a tracer and
a metrics exporter, combine them with `observe.Join`: it calls each
observer's hooks in order, passes each `StartRequest` the context the one
before returned, and keeps a panic in one from skipping the others.

`exec.Options.Observer` adds an observer to one execution: it is joined
after the session's, as `observe.Join(session, extra)` would join them, so
each hook calls the session's observer first and the added one gets the
context the session's `StartRequest` returned. Set
`exec.Options.NoSessionObserver` to leave the session's observer out of the
execution, so that only `exec.Options.Observer`, or nothing, observes it.

Attempt hooks usually run on the goroutine that called the execution, the
start hook before each attempt and its end function after it, so an observer
that blocks delays that call. Keep them short and hand slow work to a
goroutine of your own.

The driver itself depends on no tracing library. To link the two kinds, an
observer can record `AttemptEvent.CQLTraceID` on its attempt span when the
request runs with `CQLTrace`.
