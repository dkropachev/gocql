# Lightweight transactions

A statement is conditional when its `Rows` is a `cql.CASRows` decoder or its
CQL is an INSERT, UPDATE, DELETE or BATCH with an `IF` outside string
literals, quoted identifiers and comments; the driver infers it, and there
is no kind to declare. Only an INSERT, UPDATE, DELETE or BATCH can be
conditional: a SELECT or other statement decoded with `CASRows` fails with a
`cql.ValidationError` before it is sent. When the server returns result
metadata at prepare time, as ScyllaDB does, `Prepare` also fails if it lacks
the leading `[applied]` column `CASRows` needs, or if ScyllaDB reports the
statement is not a lightweight transaction. `Exec` and
`ExecBatch` reject a statement or batch known to be conditional with
`cqlerr.ErrConditionalExec` before sending it, because the result of a
conditional write reports whether it applied. `prepared.Apply(ctx, params)`
returns `(applied, result, err)` for a statement, as `session.ApplyBatch`
does for a batch, whatever the statement's row decoder; `Apply` rejects a
statement known not to be conditional before sending it. Read the previous
values with `One`, `Each`, `All`, `Run` or `RunConditionalBatch` and a
`cql.CASRows` decoder. `One` with a nil destination on a conditional
statement fails before sending: the row exists whether or not the write
applied, so it reports nothing. ScyllaDB marks conditional statements in
prepared metadata. Server conditional
metadata and SERIAL/LOCAL_SERIAL consistency also force safe conditional
classification. Conditional requests never speculate and use deterministic
replica ordering: a client tries a partition's replicas in the same order
for every conditional request, preferred datacenter and rack first, without
shuffling or random picks. Replicas that are down, unconnected or outside the
allowed datacenters are skipped, and latency avoidance moves a replica it
marked slow to the end of the plan. Otherwise concurrent requests from a
client share one coordinator and its Paxos rounds instead of contending from
several.
The driver has no other notion of a Paxos leader. For a table in a strongly
consistent (`consistency = 'global'`) keyspace, the built-in policy puts
the tablet's Raft leader ahead of that order, unless
`routing.Options.NoLeaderAware` is set or the consistency is ONE or LOCAL_ONE (see "Strongly consistent tables" in [Routing](../load-balancing.md)). A statement
executed with SERIAL or LOCAL_SERIAL consistency is a Paxos read, not a
conditional statement. Other statements whose prepare metadata has result
columns, such as a SELECT, fail `Exec` before they are sent; a SERIAL read
is sent anyway, since it completes the Paxos rounds in progress on its
partition, and fails on its result columns.

A profile's `Idempotent` does not mark a conditional statement, a SERIAL
read or a counter write: replaying one could apply it twice. The statement's
own `Idempotent` or `execopt.Options.Idempotent` still does.

Use `CASRows(decoder)` to decode the applied flag and the returned row through a
typed `CASResult[R]`. `cql.CASApplied`, which is `CASRows[cql.EmptyRow](nil)`,
decodes the applied flag alone and skips the returned row. A conditional
batch is sent with `session.ApplyBatch`, which reports whether it applied
and leaves nothing to close, or, to read its rows, with
`session.RunConditionalBatch`, whose rows iterate like `Rows` and whose
`Applied` reports whether the batch applied. Both reject a batch whose
members are all known not to be conditional.
The ordinary `execopt.Options` carries regular and serial consistency; profile defaults
apply when an explicit override is absent. `execopt.Options.SerialConsistency` is a
`cql.Option[cql.Consistency]`: unset keeps the profile's, and `cql.Some(cql.Serial)` or
`cql.Some(cql.LocalSerial)` sends SERIAL or LOCAL_SERIAL; another level is rejected
with a `cql.ValidationError` when the options are compiled. A profile whose
`SerialConsistency` is unset sends none, and the server applies its default. Inspect every returned row and close
the result rows even when the condition was not applied.

Cassandra and ScyllaDB return different rows and columns for conditional
statements and batches. See [Cassandra and ScyllaDB differences](../server-differences.md)
for what each one returns and how to decode it portably.
