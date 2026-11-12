# Routing

Set `Profile.Routing` to a typed `routing.Policy` in `Config.DefaultProfile` or in
the `Profile` passed with a query's options. `routing.NewPolicy(routing.Options{...})` configures
token awareness, datacenter/rack preference, remote fallback, replica shuffling,
leader-aware routing and latency ordering. Missing required locality is an
explicit validation error. `routing.SingleTarget` pins a typed host/shard target.

A custom `routing.Policy` implements Validate, Pick, Fallback and Observe against
immutable `metadata.Snapshot` and `routing.Info` values. The split serves the happy
path, where the first route serves the request:

- `Pick` returns the first route without building a plan or allocating, or
  false when it has no cheap answer.
- `Fallback` returns a `routing.Plan` of every route the request may take, in
  order. The driver calls it only when the picked route fails or `Pick`
  returned false, and skips the picked route if the plan returns it again.
- `Observe` receives every attempt's outcome.

`routing.NewPlan(policy, info, snapshot)` combines Pick and Fallback into the plan
a request follows. `Plan.Next` lazily returns `routing.Route` values; Pick and Next
must not perform I/O or mutate inputs. A Route is a host and a shard,
or `AnyShard`, resolved by the plan: a request takes a connection for exactly
that shard of that host and does not move to another shard or host on its own;
moving on is the plan's next Route. A plan takes routes from the snapshot, with
`HostRoutes` (every host, in the order of `Hosts`) and `ReplicaRoutes` (the
replicas of a request's token), and narrows them with `WithShard` and `ShardOf`.
Built-in routing follows a route to a replica's shard with a route to any shard
of the same host, except for conditional requests, requests that name a shard
and leaders. Its Pick routes to one random replica when that replica is up and in the
preferred tier (the local rack, else the local datacenter); conditional,
leader-aware, latency-penalized and unshuffled requests go straight to
Fallback. It keeps private immutable indices and does not copy all hosts to
choose its first target.

Config.Partitioners maps server names to TokenPartitioner implementations.
PartitionKey.ForEach streams canonical encoded components without gathering;
consume borrowed spans synchronously. Unknown partitioners disable token routing
with a diagnostic. Tablet placement and opaque version identities survive in
immutable snapshots; retained snapshots remain valid after refresh.

## Where did my request go?

Each stage of a request has its own answer:

- **Before any statement**: `session.Metadata()` returns a `metadata.Snapshot`.
  `Snapshot.Token(keyspace, table, key...)` computes a partition's token from
  its partition-key values, `Snapshot.Replicas(keyspace, table, token)` lists
  its replicas as host and shard targets, and `Snapshot.TokenRanges` and
  `Snapshot.Hosts` describe the ring.
- **Statement and values, before executing**: `Query.Routing(value)` returns
  the keyspace, table, token, statement kind and any host or shard pinned in
  the options, without sending anything. Pass its token to `Snapshot.Replicas`
  for the candidate replicas. These are the candidates, not a promise: the
  routing policy picks among them when the request runs, and the built-in
  policy picks a random replica.
- **After executing**: `Result.HostID` names the host of the attempt that
  produced the result, and `Result.Attempts` how many attempts ran. The shard
  is not in `Result`: storing it on every call slowed the w64 read path by
  about 7%, so it comes from the observer below. A
  failed call's `*cqlerr.ExecutionError` lists each attempt's host, shard,
  status and error (see [Errors](errors.md)). For every attempt as it happens,
  including speculative and cancelled ones, implement
  `config.Observer.ObserveAttempt`.

## Writing your own policy

`routing.Policy` is a public interface, so a health-weighted or pool-based
policy, such as an adapter for a host-pool library, can live outside the
driver. Embed the built-in policy to keep token awareness and override what
you need; `Observe` sees each attempt's outcome:

```go
// skipFailed avoids, for its first route, a host whose last attempt failed.
type skipFailed struct {
	routing.Policy // from routing.NewPolicy
	failed         sync.Map // host ID -> struct{}
}

func (p *skipFailed) Pick(info routing.Info, m *metadata.Snapshot) (routing.Route, bool) {
	r, ok := p.Policy.Pick(info, m)
	if _, bad := p.failed.Load(r.HostID()); ok && bad {
		return routing.Route{}, false // the driver asks Fallback instead
	}
	return r, ok
}

func (p *skipFailed) Observe(e config.AttemptEvent) {
	switch {
	case e.Cancelled:
	case e.Err != nil:
		p.failed.Store(e.Target.HostID, struct{}{})
	default:
		p.failed.Delete(e.Target.HostID)
	}
	p.Policy.Observe(e)
}
```

Pick, Fallback, `Plan.Next` and Observe run concurrently for many requests,
so a policy's own state must be safe for concurrent use.
