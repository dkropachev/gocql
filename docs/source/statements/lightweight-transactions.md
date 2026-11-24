# Lightweight transactions

Set statement Kind to `ConditionalStatement` when known. `Exec` and
`ExecBatch` reject a statement or batch known to be conditional with
`cqlerr.ErrConditionalExec` before sending it, because the result of a
conditional write reports whether it applied: read it with `One`, `Each`,
`All`, `Run` or `RunConditionalBatch`. ScyllaDB marks conditional statements in
prepared metadata. Cassandra does not, so declare the kind there: a statement
left `UnknownStatement` whose row type is `cql.EmptyRow` executes as an
ordinary one, since it cannot read the `[applied]` column anyway. Server conditional
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
conditional statement: `Exec` does not reject it up front, and fails on its
result columns like any other row-returning statement.

Use `CASRows(decoder)` to decode the applied flag and the returned row through a
typed `CASResult[R]`. For conditional batches, pass this decoder to
`session.RunConditionalBatch`, whose rows iterate like `Rows` and whose
`Applied` reports whether the batch applied. `RunConditionalBatch`
rejects a batch whose members are all known not to be conditional.
The ordinary `exec.Options` carries regular and serial consistency; profile defaults
apply when an explicit override is absent. `exec.Options.SerialConsistency` is an
`exec.SerialOverride`: `exec.SerialInherit` (the zero value) keeps the profile's,
`exec.SerialSerial` and `exec.SerialLocal` send SERIAL or LOCAL_SERIAL, and `exec.SerialOmit`
sends none. Inspect every returned row and close
the result rows even when the condition was not applied.

Cassandra and ScyllaDB return different rows and columns for conditional
statements and batches. See [Cassandra and ScyllaDB differences](../server-differences.md)
for what each one returns and how to decode it portably.
