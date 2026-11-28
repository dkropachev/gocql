# Speculative execution

Speculation starts another attempt of a request on the next host of its plan
when the first one is slow, and returns whichever answers first. It is off by
default. Set `Profile.Speculation`, with `ProfileBuilder.WithSpeculation`, to a
`retry.SpeculationPolicy`; nil disables it. `Delay` receives a
`retry.Speculation` and returns how long to wait before starting another
attempt and whether to start one at all. The `Speculation` holds the attempts
started so far, the time since the first, the statement's keyspace, table and
consistency, and the session's latency percentiles. Its `Context` is the
execution's context, so a value the application attached with
`context.WithValue`, such as a request's priority, reaches the policy, and
`Context.Deadline` reports when the page's context ends;
`retry.Attempt.Context` does the same for a retry policy. A negative delay
fails the execution with a `cql.ValidationError`, as a negative retry delay
does.

`retry.FixedSpeculation{After: d, MaxAttempts: n}` starts another attempt
every `d` until `n` attempts have started, as v1's
`SimpleSpeculativeExecution` did; a `MaxAttempts` of zero means two, one
speculative attempt.
`retry.SpeculationPolicyFunc` makes a policy from a function:

```go
// Start a second attempt once the first has taken the session's p95
// latency, and only when it can still finish before the deadline.
speculation := retry.SpeculationPolicyFunc(func(s retry.Speculation) (time.Duration, bool) {
	delay := max(s.LatencyP95, 10*time.Millisecond)
	deadline, bounded := s.Context.Deadline()
	if s.Started >= 2 || bounded && time.Until(deadline) < delay {
		return 0, false
	}
	return delay, true
})
cfg, err := config.New(config.ScyllaV4, "10.0.0.1").
	WithDefaultProfile(config.NewProfile().WithSpeculation(speculation)).
	Build()
```

`execopt.Options.Speculation` replaces the profile's policy for the executions
that carry it, nil keeps the profile's, and `retry.NoSpeculation{}` turns
speculation off. To speculate only some statements, leave the profile without
a policy and set one on those statements; to exempt a few, do the opposite:

<!-- snippet
var speculation retry.SpeculationPolicy
var lookups, reports *gocql.Prepared[int32, string]
-->
```go
speculative, err := session.CompileOptions(execopt.Options{Speculation: speculation})
if err != nil {
	return err
}
lookups = lookups.With(speculative) // every execution of lookups speculates

quiet, err := session.CompileOptions(execopt.Options{Speculation: retry.NoSpeculation{}})
if err != nil {
	return err
}
reports = reports.With(quiet) // executions of reports never speculate
```

To decide per call without extra options, have the policy read a value the
caller put on the context, from `Speculation.Context`, and return false when
it is absent.

A request speculates only when it is idempotent and known to be ordinary.
Mark a statement idempotent with `Statement.WithIdempotent(true)`, which its prepared
statement, workers and batch members carry, or every statement a profile runs
with `ProfileBuilder.WithIdempotent(true)`; `execopt.Options.Idempotent`, when
set, overrides both for one execution. A statement is classified when it is
prepared or bound: one read
with `cql.CASRows`, or an INSERT, UPDATE, DELETE or BATCH with `IF` in its
CQL, is conditional, and anything else, a SELECT included, is ordinary (see
`cql.StatementKind`); ScyllaDB also marks conditional statements in
prepared metadata.
Conditional statements, requests with SERIAL or LOCAL_SERIAL
consistency, and batches that hold a conditional member never
speculate; an idempotent batch of ordinary members can. Streams never
speculate or prefetch, whatever their profile says.

A request whose policy is set but that cannot speculate says why in
`cql.Result.SpeculationSkipped`, on success and in `ExecutionError.Result`:
`cql.SpeculationSkippedConditional` or `cql.SpeculationSkippedNotIdempotent`.
A `Prepared.With` view whose `execopt.Options.Speculation` names a policy for a
statement that cannot speculate, by the view's own options, the statement and
the profile, is a mistake found once: every call of the view, and its
`Worker`, fails with a `cql.ValidationError` before sending. Speculation that
comes from a profile serves many statements, so it is only reported per call.

Concurrent branches share serialized plan advancement and the same encoded
input owner. Losing attempts are cancelled and drain before borrowed input can
be reused. Request results and observers account for every started attempt:
`routing.AttemptEvent.Speculative` marks the extra ones, and
`metrics.Snapshot.Speculations` counts them. Disabled speculation allocates no
scheduler or timer.
