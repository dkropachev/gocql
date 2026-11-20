# Routing

Set `Profile.Routing` to a typed `routing.Policy` with
`ProfileBuilder.WithRouting`, in the session's default profile (`config.ConfigBuilder.WithDefaultProfile` or
`ConfigureDefaultProfile`) or in the `exec.Options.Profile` of an execution. The
default is token-aware with shuffled replicas; nil routes without a
request-specific plan. `routing.NewPolicy(routing.Options{...})` configures
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
  A plan returns its routes lazily; `routing.PlanOf(routes...)` makes one
  from routes already in a slice.
- `Observe` receives every attempt's outcome.

The session combines Pick and Fallback into the plan a request follows.
`Plan.Next` lazily returns `metadata.Route` values; Pick and Next must not
perform I/O or mutate inputs. A Route is a host and, optionally, a shard
(`Route.Shard`, a `cql.Option[uint32]`), resolved by the plan: a request takes a
connection for exactly that shard of that host, or any connection of the host
when the route has no shard, and does not move to another shard or host on its own;
moving on is the plan's next Route. A plan takes routes from the snapshot, with
`routing.HostRoutes` (every host, in the order of `Hosts`) and
`routing.ReplicaRoutes` (the replicas of a request's token), and narrows them
with `WithShard` and `ShardOf`.
Built-in routing follows a route to a replica's shard with a route to any shard
of the same host, except for conditional requests, requests that name a shard
and leaders. Its Pick routes to one random replica when that replica is up and in the
preferred tier (the local rack, else the local datacenter); conditional,
leader-aware, latency-penalized and unshuffled requests go straight to
Fallback. It keeps private immutable indices and does not copy all hosts to
choose its first target.

## Built-in plan order

A plan of `routing.NewPolicy` tries, in order: the tablet's leader when
`LeaderAware` applies, the replicas in the local rack (`Rack`), the other
replicas in the local datacenter (`Datacenter`, or every datacenter without
one), the replicas in other datacenters when `AllowRemote` is set, then the
hosts that are no replicas in the same tiers: local rack, local datacenter,
other datacenters. So with `AllowRemote` a remote replica comes before a local
host that is no replica. Hosts that are down or have no connection come after
every reachable host that latency-aware routing does not hold back.
`ShuffleReplicas` orders the replicas of a tier at random per request, and
otherwise in an order fixed for each replica set; the hosts that are no
replicas take turns. A conditional request (LWT), one whose statement is not
known to be ordinary, and one whose consistency is `SERIAL` or `LOCAL_SERIAL`
keep the replicas' order and do not rotate the hosts.

- Without `TokenAware`, or without a token, a plan has no replicas and starts
  with the hosts.
- A host the host filter (`config.ConfigBuilder.WithHostFilter`) skips is in
  no plan, a `SingleTarget` one included, and does not count for `Validate`.
- With `Latency.Enabled`, a host measured much slower than the fastest is
  tried after every other host until `RetryPeriod` passes.
- A table with tablets is placed by its tablets, not by the token ring. A
  token no known tablet holds has no replicas, and the plan starts with the
  hosts; the response then carries the tablet, which later plans use.
- A replica's route goes to its tablet's shard, or to the shard that owns the
  token, followed by a route to any shard of the same host, except for a
  conditional request, a statement not known to be ordinary, a request that
  names its shard, and a leader.
  `SingleTarget` routes only to its host: to its shard first when it names one
  and the host has it, then to any shard, unless `StrictShard` is set.

A tablet hint in a response replaces every tablet its range overlaps, so a
migrated, split or merged tablet takes effect once a background worker
publishes it, unless the schema or the table's tablets changed since the
request was sent; a plan already built keeps the tablets it was built with.
When more hints arrive than the driver can queue, it forgets every tablet it
knows and relearns them from later responses; a plan already built then skips
its remaining replicas, of tablets or of the token ring, and goes on with the
hosts.

`config.ConfigBuilder.WithPartitioner(name, partitioner)` maps a partitioner
class name to a `routing.TokenPartitioner`, next to the built-in
`routing.Murmur3TokenPartitioner` and `routing.CDCTokenPartitioner`.
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
  `observe.AttemptObserver.StartAttempt` (see [Tracing](tracing.md)).

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

func (p *skipFailed) Pick(info routing.Info, m *metadata.Snapshot) (metadata.Route, bool) {
	r, ok := p.Policy.Pick(info, m)
	if _, bad := p.failed.Load(r.HostID()); ok && bad {
		return metadata.Route{}, false // the driver asks Fallback instead
	}
	return r, ok
}

func (p *skipFailed) Observe(e routing.AttemptEvent) {
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
