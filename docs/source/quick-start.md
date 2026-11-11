# Quick start

Install the single v2 module:

```sh
go get github.com/scylladb/gocql/v2@v2.0.0
```

Import `github.com/scylladb/gocql/v2` (`gocql`) and
`github.com/scylladb/gocql/v2/cql`. Build a configuration with
`gocql.NewConfig(contactPoints...).Build()` and open it with
`gocql.Connect[gocql.ScyllaV4](ctx, config)`, naming the database and protocol
version you connect to: `ScyllaV4`, `CassandraV4` or `CassandraV5` (see
[connecting](connecting/index.md)). Close the session when finished.

Declare a `cql.Statement[P,R]` with `CQL`, `Params` and `Rows`.
`session.Prepare(ctx, statement)` prepares it and validates its codecs
against server metadata. The prepared statement is safe to share between
goroutines and needs no Close: `prepared.Exec` runs a statement without
results, `prepared.One` reads one row, and `prepared.Each` or a range loop over
`prepared.All` visits every row.
A statement without results has the row type `cql.EmptyRow`; one with no bind
markers takes `cql.EmptyParams`. Leave `Params` or `Rows` nil for those empty
types; `cql.Command(text)` builds a statement with neither, such as DDL.
Parameters are copied on admission.

A worker that runs one statement many times can own a reusable query:
`prepared.Query(options)` keeps parameter buffers and resolved options across
executions, and several queries can share one preparation. A query has the
same `Exec`, `One`, `Each` and `All`, plus `query.Run`, which returns typed rows:
check `Rows.Err()` after iteration and close the rows. A query call while a
`Rows` of that query is open, or while another goroutine's call runs, returns
`ErrBusy`. Close the query when the worker finishes with `defer query.Close()`.
Close returns at once; transport writes and callbacks still in flight drain in
the background, and `Session.Close` joins them. Call `query.Wait(ctx)` after
Close only when the following code must not overlap those callbacks.

The repository's external-package `Example` and other `example_*_test.go` files
compile against the normal public API. The complete `examples/streaming` command
creates its own schema and validates its results:

```sh
go run ./examples/streaming -hosts 127.0.0.1:9042
```

Use `session.PrepareStream` with `cql.StreamStatement[P,R]` for a streaming-only
statement, then `prepared.Query(streamOptions)` per worker. No buffered decoder
is needed. `Each` callbacks can return `gocql.StopIteration` to stop successfully.

See [streaming](statements/streaming.md) for capacity and ownership requirements,
and [configuration](connecting/index.md) for TLS/authentication.
