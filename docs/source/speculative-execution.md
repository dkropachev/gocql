# Speculative execution

Speculation starts another attempt of a request on the next host of its plan
when the first one is slow, and returns whichever answers first. It is off by
default. Set `Profile.Speculation`, with `ProfileBuilder.WithSpeculation`, to a
`retry.SpeculationPolicy`; nil disables it. The driver ships no policy:
`Delay(started)` receives the number of attempts started so far and returns how
long to wait before starting another and whether to start one at all.

```go
// fixedSpeculation starts up to max attempts, delay apart.
type fixedSpeculation struct {
	delay time.Duration
	max   int
}

func (p fixedSpeculation) Delay(started int) (time.Duration, bool) {
	return p.delay, started < p.max
}

cfg, err := config.New(config.ScyllaV4, "10.0.0.1").
	ConfigureDefaultProfile(func(p *config.ProfileBuilder) {
		p.WithSpeculation(fixedSpeculation{delay: 50 * time.Millisecond, max: 2})
	}).
	Build()
```

A request speculates only when it is idempotent and known to be ordinary.
Mark a statement idempotent with `Statement.Idempotent`, which its prepared
statement, workers and batch members carry; `exec.Options.Idempotent`, when
set, overrides it for one execution. ScyllaDB marks conditional statements in
prepared metadata, so its prepared statements are classified. Elsewhere a
statement whose row type is `cql.EmptyRow` is ordinary unless its `Kind` says
otherwise; for any other, on Cassandra or unprepared, set `Statement.Kind` to
`cql.OrdinaryStatement`.
Unknown and conditional statements, requests with SERIAL or LOCAL_SERIAL
consistency, and batches that hold an unknown or conditional member never
speculate; an idempotent batch of ordinary members can. Streams never
speculate or prefetch, whatever their profile says.

Concurrent branches share serialized plan advancement and the same encoded
input owner. Losing attempts are cancelled and drain before borrowed input can
be reused. Request results and observers account for every started attempt:
`routing.AttemptEvent.Speculative` marks the extra ones, and
`metrics.Snapshot.Speculations` counts them. Disabled speculation allocates no
scheduler or timer.
