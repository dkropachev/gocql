# Routing

Set `Profile.Routing` to a typed `RoutingPolicy` in `Config.DefaultProfile` or in
the `Profile` passed with a query's options. `NewRoutingPolicy(RoutingOptions{...})` configures
token awareness, datacenter/rack preference, remote fallback, replica shuffling,
leader-aware routing and latency ordering. Missing required locality is an
explicit validation error. `SingleTargetRouting` pins a typed host/shard target.

A custom RoutingPolicy implements Validate, Pick, Fallback and Observe against
immutable MetadataSnapshot and RoutingInfo values. The split serves the happy
path, where the first route serves the request:

- `Pick` returns the first route without building a plan or allocating, or
  false when it has no cheap answer.
- `Fallback` returns a QueryPlan of every route the request may take, in
  order. The driver calls it only when the picked route fails or `Pick`
  returned false, and skips the picked route if the plan returns it again.
- `Observe` receives every attempt's outcome.

`NewQueryPlan(policy, info, metadata)` combines Pick and Fallback into the plan
a request follows. QueryPlan.Next lazily returns Route values; Pick and Next
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

The optional hostpolicy subpackage adapts go-hostpool to the typed RoutingPolicy
and attempt observation seams.
