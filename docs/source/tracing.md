# Tracing

The driver supports two unrelated kinds of tracing. CQL tracing asks the
cluster to record what it did for a request. Client-side tracing, such as
OpenTelemetry, records what the driver did, in your own tracing system.

## CQL tracing

Set `execopt.Options.CQLTrace` and the cluster records a trace session of every
attempt in `system_traces`. To trace a sample of the traffic instead, give
the profile a sampler with `config.ProfileBuilder.WithCQLTraceSampler`: the
driver asks it once per request, with the request's `routing.Info`, and
traces the request when it returns true. `Result.CQLTraces` lists the
sessions in the order the attempts finished, page after page. Each
`cql.CQLTraceRef` carries the session ID and the attempt it records: its
coordinator `HostID`, the `Page` it fetched, its `Attempt` number within
that page, and whether it `Failed` or was `Speculative`. The `Result` of a failed request lists its failed attempts'
sessions too, so tracing a timeout needs no error unwrapping. The IDs also
come back on each `cqlerr.AttemptFailure.CQLTraceID` and each
`routing.AttemptEvent.CQLTraceID`. Package `cqltrace` reads a session:

```go
insert := cql.Statement[int32, cql.EmptyRow]{
	CQL:         "INSERT INTO app.items (id) VALUES (?)",
	Params:      cql.OneParam(cql.Int32Codec),
	BindMarkers: cql.NewColumns(cql.ColumnSpec{Name: "id", Type: cql.IntType}),
}
traced, err := session.CompileOptions(execopt.Options{CQLTrace: true})
if err != nil {
	return err
}
result, err := session.UnpreparedExec(ctx, insert, 42, traced)
if err != nil {
	return err
}
for _, trace := range result.CQLTraces() {
	info, err := cqltrace.Fetch(ctx, session, trace)
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
`cqltrace.ErrNotReady`. `Fetch` reads from the reference's `HostID`, the
coordinator, which has the session's row and its own events first; once a
read there fails, as when the host is down, it reads from any replica. Its
reads run without the session's hooks and stay out of `Session.Metrics` and
`Session.HostMetrics`.

`cqltrace.FetchAll` reads every session a list of references names, which is
how to trace a request that failed: its `*cqlerr.ExecutionError` carries the
`Result` with the failed attempts' sessions. It returns one `Info` per
reference, and the errors of the sessions it could not read joined:

<!-- snippet
var insert cql.Statement[int32, cql.EmptyRow]
var traced execopt.CompiledOptions
-->
```go
_, err := session.UnpreparedExec(ctx, insert, 42, traced)
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

## Client-side tracing

`observe.Hooks` is the hook for client-side tracing. Each of its fields is
one hook, a function the driver calls:

- `Request` receives the caller's context as a request is admitted and
  returns the context for its attempts, so a span started for the request
  can parent one span per attempt. Its `RequestStart` names the statement,
  the session (`SessionID`), the page size and the bound values; the
  `RequestEvent` it ends with carries only the outcome: the error, the
  `Result`, when it `Finished` and how many `Pages` it fetched.
- `Attempt` is called for each attempt, retry or speculative attempt. A
  statement that returns rows over several pages sends each page's attempts
  in turn: `Page` numbers the page within the request, and `Attempt` the
  attempt within the page, so every page's first attempt is 1. `Address`
  and `AddrPort` name the connection, the latter as an OpenTelemetry span's
  `server.address` and `server.port` take it.

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

`execopt.Options.Observer` adds hooks to one execution: they are joined after
the session's, as `observe.Join(session, extra)` would join them, so each
hook calls the session's first and the added one gets the context the
session's `Request` returned. Set `execopt.Options.NoSessionObserver` to leave
the session's hooks out of the execution, so that only
`execopt.Options.Observer`, or nothing, observes it.

Attempt hooks usually run on the goroutine that called the execution, the
start hook before each attempt and its end function after it, so a hook
that blocks delays that call. Keep them short and hand slow work to a
goroutine of your own. A page that rows prefetch runs on a goroutine of its
own, and the speculative attempts of a page each run on one, concurrently
with the page's first attempt, so their hooks can run at the same time; a
speculative attempt that lost can end after its request did.

`RequestStart.Values`, and each `RequestStart.BatchStatements` entry's
`Values`, view the values bound to the statement. They are borrowed from
the request, which reuses or releases them once it ends, so read them, or
`Format` them, before the request's end function returns. Bound values often
hold personal data: `Format` takes a function that redacts the columns it
chooses.

<!-- snippet
var hooks observe.Hooks
-->
```go
hooks.Request = func(ctx context.Context, start observe.RequestStart) (context.Context, func(observe.RequestEvent)) {
	var values strings.Builder
	_ = start.Values.Format(&values, func(c cql.ColumnSpec) bool { return c.Name == "password" })
	log.Printf("%s [%s]", start.Statement, values.String())
	return ctx, nil
}
```

The driver itself depends on no tracing library. To link the two kinds, an
`Attempt` hook can record `AttemptEvent.CQLTraceID` on its attempt span when
the request runs with `CQLTrace`.

## Metrics

`Session.Metrics` returns a `metrics.Snapshot` of the session's counters:
requests, pages, attempts, errors by kind, retries, speculations, orphaned
streams, executions in flight and the connections the pools hold.
`Canceled` and `Timeouts` break two kinds out of `Errors`; they repeat what
`ErrorsByKind` (or `ErrorsOf`) counts for `cqlerr.KindCanceled` and
`cqlerr.KindTimeout`, so do not add them to it. `Session.HostMetrics`
appends a `metrics.Host` per host the session has a pool for: its attempts,
failed attempts and timeouts, connections and requests in flight. Pass it
the slice of the previous call, `hosts = session.HostMetrics(hosts[:0])`, to
poll without allocating.

The latency percentiles cover the pages that succeeded in the last ten
seconds, each from sending its first attempt to its response, so the time
the application spends reading rows stays out of them. They are meant for
decisions that follow the cluster's current latency, such as a speculation
delay, not for charts. To export latency, count it per request from a
`Request` hook into a histogram of your metrics library, as
`observe.Example_prometheus` does with a Prometheus `HistogramVec`; a
request that returns rows ends when its rows are closed, so its latency
includes the reading:

<!-- snippet
// latency is a Prometheus HistogramVec.
var latency interface {
	WithLabelValues(...string) interface{ Observe(float64) }
}
-->
```go
hooks := observe.Hooks{
	Request: func(ctx context.Context, start observe.RequestStart) (context.Context, func(observe.RequestEvent)) {
		return ctx, func(e observe.RequestEvent) {
			outcome := "ok"
			if e.Err != nil {
				outcome = cqlerr.KindOf(e.Err).String()
			}
			latency.WithLabelValues(outcome).Observe(e.Finished.Sub(start.Started).Seconds())
		}
	},
}
```
