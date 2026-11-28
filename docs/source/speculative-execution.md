# Speculative execution

Speculation starts another attempt of a request on the next host of its plan
when the first one is slow, and returns whichever answers first. It is off by
default. Set `Profile.Speculation`, with `ProfileBuilder.WithSpeculation`, to a
`retry.SpeculationPolicy`; nil disables it. The driver ships no policy:
`Delay` receives a `retry.Speculation` and returns how long to wait before
starting another attempt and whether to start one at all. The `Speculation`
holds the attempts started so far, the time since the first, the page's
deadline, the statement's keyspace, table and consistency, and the session's
latency percentiles. Its `Context` is the execution's context, so a value the
application attached with `context.WithValue`, such as a request's priority,
reaches the policy; `retry.Attempt.Context` does the same for a retry policy.

`retry.SpeculationPolicyFunc` makes one from a function:

```go
// Start a second attempt once the first has taken the session's p95
// latency, and only when it can still finish before the deadline.
speculation := retry.SpeculationPolicyFunc(func(s retry.Speculation) (time.Duration, bool) {
	delay := max(s.LatencyP95, 10*time.Millisecond)
	if s.Started >= 2 || !s.Deadline.IsZero() && time.Until(s.Deadline) < delay {
		return 0, false
	}
	return delay, true
})
cfg, err := config.New(config.ScyllaV4, "10.0.0.1").
	WithDefaultProfile(config.NewProfile().WithSpeculation(speculation)).
	Build()
```

`exec.Options.Speculation` replaces the profile's policy for the executions
that carry it, nil keeps the profile's, and `retry.NoSpeculation{}` turns
speculation off. To speculate only some statements, leave the profile without
a policy and set one on those statements; to exempt a few, do the opposite:

<!-- snippet
var speculation retry.SpeculationPolicy
var lookups, reports *gocql.Prepared[int32, string]
-->
```go
speculative, err := session.CompileOptions(exec.Options{Speculation: speculation})
if err != nil {
	return err
}
lookups = lookups.With(speculative) // every execution of lookups speculates

quiet, err := session.CompileOptions(exec.Options{Speculation: retry.NoSpeculation{}})
if err != nil {
	return err
}
reports = reports.With(quiet) // executions of reports never speculate
```

To decide per call without extra options, have the policy read a value the
caller put on the context, from `Speculation.Context`, and return false when
it is absent.

A request speculates only when it is idempotent and known to be ordinary.
Mark a statement idempotent with `Statement.Idempotent`, which its prepared
statement, workers and batch members carry, or every statement a profile runs
with `ProfileBuilder.WithIdempotent(true)`; `exec.Options.Idempotent`, when
set, overrides both for one execution. A statement left
`cql.UnknownStatement` is classified when it is prepared or bound: one read
with `cql.CASRows`, or an INSERT, UPDATE, DELETE or BATCH with `IF` in its
CQL, is conditional, and anything else, a SELECT included, is ordinary (see
`cql.UnknownStatement`); ScyllaDB also marks conditional statements in
prepared metadata.
Conditional statements, requests with SERIAL or LOCAL_SERIAL
consistency, and batches that hold a conditional member never
speculate; an idempotent batch of ordinary members can. Streams never
speculate or prefetch, whatever their profile says.

Concurrent branches share serialized plan advancement and the same encoded
input owner. Losing attempts are cancelled and drain before borrowed input can
be reused. Request results and observers account for every started attempt:
`routing.AttemptEvent.Speculative` marks the extra ones, and
`metrics.Snapshot.Speculations` counts them. Disabled speculation allocates no
scheduler or timer.
