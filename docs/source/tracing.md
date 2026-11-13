# Tracing

The driver supports two unrelated kinds of tracing. CQL tracing asks the
cluster to record what it did for a request. Client-side tracing, such as
OpenTelemetry, records what the driver did, in your own tracing system.

## CQL tracing

Set `exec.Options.CQLTrace` and the cluster records a trace session of every
attempt in `system_traces`. `Result.CQLTraces` lists them in the order the
attempts finished, page after page. Each `gocql.CQLTraceRef` carries the
session ID and the attempt it records: its coordinator `HostID`, its
`Attempt` number within the page, and whether it `Failed` or was
`Speculative`. The `Result` of a failed request lists its failed attempts'
sessions too, so tracing a timeout needs no error unwrapping. The IDs also
come back on each `cqlerr.AttemptFailure.CQLTraceID` and each
`config.AttemptEvent.CQLTraceID`. Package `cqltrace` reads a session:

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

`config.Observer` is the hook for client-side tracing. `StartRequest`
receives the caller's context as a request is admitted and returns the
context for its attempts, so a span started for the request can parent one
span per attempt, retry or speculative attempt from `StartAttempt`. Each start
returns the function the driver calls when that request or attempt finishes.
Set the observer for the session with `config.ConfigBuilder.WithObserver`, or
per execution with `exec.Options.Observer`.

Attempt hooks usually run on the goroutine that called the execution, the
start hook before each attempt and its end function after it, so an observer
that blocks delays that call. Keep them short and hand slow work to a
goroutine of your own.

The driver itself depends on no tracing library. To link the two kinds, an
observer can record `AttemptEvent.CQLTraceID` on its attempt span when the
request runs with `CQLTrace`.
