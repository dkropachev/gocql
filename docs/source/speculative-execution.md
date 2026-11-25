# Speculative execution

Speculation starts another attempt of a request on the next host of its plan
when the first one is slow, and returns whichever answers first. It is off by
default. Set `Profile.Speculation`, with `ProfileBuilder.WithSpeculation`, to a
`retry.SpeculationPolicy`; nil disables it. The driver ships no policy:
`Delay(started)` receives the number of attempts started so far and returns how
long to wait before starting another and whether to start one at all.

`retry.SpeculationPolicyFunc` makes one from a function:

```go
// Start up to two attempts, 50ms apart.
speculation := retry.SpeculationPolicyFunc(func(started int) (time.Duration, bool) {
	return 50 * time.Millisecond, started < 2
})
cfg, err := config.New(config.ScyllaV4, "10.0.0.1").
	WithDefaultProfile(config.NewProfile().WithSpeculation(speculation).MustBuild()).
	Build()
```

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
