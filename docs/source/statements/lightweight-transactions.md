# Lightweight transactions

Set statement Kind to `ConditionalStatement` when known. `Exec` and
`ExecBatch` reject a statement or batch known to be conditional with
`cqlerr.ErrConditionalExec` before sending it, because the result of a
conditional write reports whether it applied: read it with `One`, `Each`,
`All`, `Run` or `RunBatch`. ScyllaDB marks conditional statements in
prepared metadata. Cassandra does not, so declare the kind there. Server conditional
metadata and SERIAL/LOCAL_SERIAL consistency also force safe conditional
classification. Conditional requests never speculate and use deterministic
replica ordering.

Use `CASRows(decoder)` to decode the applied flag and the returned row through a
typed `CASResult[R]`. For conditional batches, pass this decoder to `session.RunBatch`.
The ordinary `exec.Options` carries regular and serial consistency; profile defaults
apply when an explicit override is absent. `exec.Options.SerialConsistency` is an
`exec.SerialOverride`: `exec.SerialInherit` (the zero value) keeps the profile's,
`exec.SerialSerial` and `exec.SerialLocal` send SERIAL or LOCAL_SERIAL, and `exec.SerialOmit`
sends none. Inspect every returned row and close
the result rows even when the condition was not applied.

Cassandra and ScyllaDB return different rows and columns for conditional
statements and batches. See [Cassandra and ScyllaDB differences](../server-differences.md)
for what each one returns and how to decode it portably.
