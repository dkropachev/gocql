# Quick start

Install the single v2 module:

```sh
go get github.com/scylladb/gocql/v2@v2.0.0-rc.1
```

Import `github.com/scylladb/gocql/v2` (`gocql`),
`github.com/scylladb/gocql/v2/config` and `github.com/scylladb/gocql/v2/cql`.
Build a configuration with `config.New(dialect, contactPoints...).Build()`,
naming the database and protocol version you connect to: `config.ScyllaV4`,
`config.CassandraV4` or `config.CassandraV5` (see
[connecting](connecting/index.md)), and open it with `gocql.Connect(ctx, cfg)`. Close the session when finished.

Declare a `cql.Statement[P,R]` with `CQL`, `Params` and `Rows`.
`session.Prepare(ctx, statement)` prepares it and validates its codecs
against server metadata. The prepared statement is safe to share between
goroutines and needs no Close: `prepared.Exec` runs a statement without
results, `prepared.One` reads one row, and `prepared.Each` or a range loop over
`prepared.All` visits every row.
A statement without results has the row type `cql.EmptyRow`; one with no bind
markers takes `cql.EmptyParams`. Leave `Params` or `Rows` nil for those empty
types; `cql.Command(text)` builds a statement with neither, such as DDL.
`cql.Select(text, params, rows)`, `cql.Write(text, params)` and
`cql.NewStatement(text, params, rows)` build a statement whose type arguments
Go infers from the codecs; pass `cql.NoParams` for a statement without bind
markers. `Select` marks the statement ordinary and idempotent.
Parameters are copied on admission.

A goroutine that runs one statement many times can own a reusable `Worker`:
`prepared.Worker(options)` keeps parameter buffers and resolved options across
executions, and several workers can share one preparation. A worker has the
same `Exec`, `One`, `Each` and `All`, plus `worker.Run`, which returns typed rows:
check `Rows.Err()` after iteration and close the rows. A worker call while a
`Rows` of that worker is open, or while another goroutine's call runs, returns
`cqlerr.ErrBusy`. Close the worker when the goroutine finishes with
`defer worker.Close()`.
Close returns at once; transport writes and callbacks still in flight drain in
the background, and `Session.Close` joins them. Call `worker.Wait(ctx)` after
Close only when the following code must not overlap those callbacks.

The repository's external-package `Example` and other `example_*_test.go` files
compile against the normal public API. The complete `examples/streaming` command
creates its own schema and validates its results:

```sh
go run ./examples/streaming -hosts 127.0.0.1:9042
```

Use `session.PrepareStream` with `cql.StreamStatement[P,R]` for a streaming-only
statement, then `prepared.Worker(streamOptions)` per goroutine. No buffered decoder
is needed. `Each` callbacks can return `gocql.StopIteration` to stop successfully.

See [streaming](statements/streaming.md) for capacity and ownership requirements,
and [configuration](connecting/index.md) for TLS/authentication.
