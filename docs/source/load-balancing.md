# Routing

Set `Profile.Routing` to a typed `routing.Policy` with
`ProfileBuilder.WithRouting`, in the session's default profile
(`config.ConfigBuilder.WithDefaultProfile` or `ConfigureDefaultProfile`) or in
the `exec.Options.Profile` of an execution. The default is token-aware and
leader-aware with shuffled replicas, and `WithRouting(nil)` restores it.
`routing.NewPolicy(routing.Options{...})` configures
datacenter/rack preference, remote fallback, latency ordering, and turns off
token awareness (`NoTokenAware`), replica shuffling (`FixedReplicaOrder`) or
leader-aware routing (`NoLeaderAware`). The zero `routing.Options` is the
default policy. `routing.SingleTarget` pins a typed host/shard target.

## Local datacenter

`config.ConfigBuilder.WithLocalDatacenter(dc, rack)` sets the session's local
datacenter, and optionally rack. Every built-in policy whose `routing.Options`
name no `Datacenter` of their own takes it: the default profile's, and those
of every profile built with `config.NewProfile`. Such a policy prefers
replicas in that datacenter and, unless `AllowRemote` is set, routes only
there. The default consistency, `LOCAL_QUORUM`, is local to the datacenter of
the coordinator, so set it on a cluster with more than one datacenter.
Without it every datacenter is local, and Connect logs a warning when the
cluster has more than one. Connect fails when the local datacenter, or rack,
has no eligible host; `Build` rejects a rack without a datacenter.

```go
cfg, err := config.New(config.ScyllaV4, "10.0.0.1").
	WithLocalDatacenter("dc1", "").
	Build()
```

## Custom policies

A custom `routing.Policy` implements Pick and Fallback against
immutable `metadata.Snapshot` and `routing.Info` values. A request pinned to a
host with `exec.Options.HostID` goes there, to `ShardID` when set, without
consulting the policy. The split serves the happy
path, where the first route serves the request:

- `Pick` returns the first route without building a plan or allocating, or
  false when it has no cheap answer.
- `Fallback` returns a `routing.Plan` of every route the request may take, in
  order. The driver calls it only when the picked route fails or `Pick`
  returned false, and skips the picked route if the plan returns it again.
  A plan returns its routes lazily; `routing.PlanOf(routes...)` makes one
  from routes already in a slice.

A policy may also implement two optional interfaces:

- `routing.Validator`: `Validate` checks the policy against a snapshot. The
  session calls it once per snapshot before routing with the policy; while it
  fails, so do the requests the policy would route, and Connect fails when it
  fails for the default profile's policy.
- `routing.AttemptObserver`: `Observe` receives every attempt's outcome.

Package `routing/routingtest` builds snapshots from hosts, schema, token
ranges and tablets, so a policy can be tested without a cluster.

The session combines Pick and Fallback into the plan a request follows.
`Plan.Next` lazily returns `metadata.Route` values; Pick and Next must not
perform I/O or mutate inputs. A Route is a host and, optionally, a shard
(`Route.ShardID`, a `cql.Option[uint32]`), resolved by the plan: a request takes a
connection for exactly that shard of that host, or any connection of the host
when the route has no shard, and does not move to another shard or host on its own;
moving on is the plan's next Route. A plan takes routes from the snapshot, with
`routing.HostRoutes` (every host, in the order of `Hosts`) and
`routing.ReplicaRoutes` (the replicas of a request's token, each to the shard
that owns the token when the host's sharding is known), and narrows them
with `WithShardID` and `ShardOf`.
Built-in routing follows a route to a replica's shard with a route to any shard
of the same host, except for conditional requests, requests that name a shard
and leaders. Its Pick routes to one random replica when that replica is up and in the
preferred tier (the local rack, else the local datacenter); conditional,
leader-aware, latency-penalized and unshuffled requests go straight to
Fallback. It keeps private immutable indices and does not copy all hosts to
choose its first target.

## Built-in plan order

A plan of `routing.NewPolicy` tries, in order: the tablet's leader when
leader-aware routing applies, the replicas in the local rack (`Rack`), the other
replicas in the local datacenter (`Datacenter`, else the session's local
datacenter, or every datacenter without one), the replicas in other datacenters when `AllowRemote` is set, then the
hosts that are no replicas in the same tiers: local rack, local datacenter,
other datacenters. So with `AllowRemote` a remote replica comes before a local
host that is no replica. Hosts that are down or have no connection come after
every reachable host that latency-aware routing does not hold back.
The replicas of a tier come in an order random per request, or with
`FixedReplicaOrder` in an order fixed for each replica set; the hosts that are no
replicas take turns. A conditional request (LWT), one whose statement is not
known to be ordinary, and one whose consistency is `SERIAL` or `LOCAL_SERIAL`
keep the replicas' order and do not rotate the hosts.

- With `NoTokenAware`, or without a token, a plan has no replicas and starts
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

## Strongly consistent tables

A ScyllaDB keyspace created with `consistency = 'global'` keeps each tablet
strongly consistent: one replica, the tablet's Raft leader, coordinates its
writes and its linearizable reads, and any other replica only forwards them
there. Unless `NoLeaderAware` is set, the built-in policy
tries the leader first, ahead of nearer replicas:

- Each connection negotiates `TABLETS_ROUTING_V2_EXPERIMENTAL` when the server
  offers it (ScyllaDB 2026.3 and later started with the
  `strongly-consistent-tables` experimental feature), and
  `TABLETS_ROUTING_V1` otherwise; a server offering neither, such as
  Cassandra, gets no tablet routing.
- On a V2 connection every prepared EXECUTE carries one byte sampled from the
  version of the tablet the driver knows for the request's token. When the
  server's tablet differs, the response reports the tablet's range, replicas
  and shards, leader first, and its version, which later plans use. A
  response with no report means the driver's tablet is current.
- `metadata.Keyspace.ConsistencyMode` is `"global"` for such a keyspace; it is
  read from `system_schema.scylla_keyspaces` on every schema refresh when the
  session's connections negotiate V2.
- The leader comes first for every request, write, read or conditional
  statement, whose consistency is not `ONE` or `LOCAL_ONE`. Requests at `ONE`
  or `LOCAL_ONE` keep the usual replica order, since any replica can serve
  such a read, and so do eventually consistent keyspaces and tablets learned
  over V1, which report no leader.
- The leader is not preferred when the policy would not otherwise use it: a
  host the host filter skips, one outside the local datacenter without
  `AllowRemote`, or one that is down or has no connection. With
  `Latency.Enabled`, a leader measured slow is tried last, like any slow
  host. The plan then
  continues as usual and a replica forwards the request. Retries and
  speculative attempts follow the rest of the plan.
- A batch is routed like any other batch, by its first statement with a
  token, so it goes to the leader of that statement's tablet; the driver does
  not consider the tablets of its other statements.

## Where did my request go?

Each stage of a request has its own answer:

- **Before any statement**: `session.Metadata()` returns a `metadata.Snapshot`.
  `Snapshot.Token(keyspace, table, key...)` computes a partition's token from
  its partition-key values, `Snapshot.Replicas(keyspace, table, token)` lists
  its replicas as host and shard targets, and `Snapshot.TokenRanges` and
  `Snapshot.Hosts` describe the ring.
- **Statement and values, before executing**: `Prepared.Routing(value)`
  returns the keyspace, table, token and statement kind, and the consistency
  and any host or shard pinned by the options passed after the value, without
  sending anything. Pass its token to `Snapshot.Replicas`
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
driver. Embed the `*routing.Builtin` that `routing.NewPolicy` returns to keep
token awareness and override what you need; its `Validate` and `Observe` are
promoted, so the wrapper keeps the built-in validation and latency tracking.
A wrapper that overrides `Observe` calls the built-in one itself:

```go
// skipFailed avoids, for its first route, a host whose last attempt failed.
type skipFailed struct {
	*routing.Builtin // from routing.NewPolicy
	failed           sync.Map // host ID -> struct{}
}

func (p *skipFailed) Pick(info routing.Info, m *metadata.Snapshot) (metadata.Route, bool) {
	r, ok := p.Builtin.Pick(info, m)
	if _, bad := p.failed.Load(r.HostID()); ok && bad {
		return metadata.Route{}, false // the driver asks Fallback instead
	}
	return r, ok
}

func (p *skipFailed) Observe(e routing.AttemptEvent) {
	switch {
	case e.Canceled:
	case e.Err != nil:
		p.failed.Store(e.Target.HostID, struct{}{})
	default:
		p.failed.Delete(e.Target.HostID)
	}
	p.Builtin.Observe(e)
}
```

Pick, Fallback, `Plan.Next` and Observe run concurrently for many requests,
so a policy's own state must be safe for concurrent use.
