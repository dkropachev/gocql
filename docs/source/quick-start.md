# Quick start

Install the single v2 module:

```sh
go get github.com/scylladb/gocql/v2@v2.0.0
```

Import `github.com/scylladb/gocql/v2` (`gocql`) and
`github.com/scylladb/gocql/v2/cql`. Create `gocql.DefaultConfig(contactPoints...)`
and open it with `gocql.Connect(ctx, config)`. Close the session when finished.

Declare a `cql.Statement[P,R]` with `CQL`, `Params` and `Rows`.
`Prepare(ctx, session, statement)` prepares it and validates its codecs
against server metadata; `prepared.Query(options)` returns a worker-owned
reusable query, and several queries can share one preparation. `query.One` reads one row, `query.Each` visits every row and
`query.Run` returns typed rows: check `Rows.Err()` after iteration and close
the rows. Use `query.Exec` for statements without results: their row type is
`cql.EmptyRow`. A statement with no bind markers takes `cql.EmptyParams`. Leave
`Params` or `Rows` nil for those empty types; `cql.Command(text)` builds a
statement with neither, such as DDL. Parameters are copied on admission.
Overlapping executions return `ErrBusy`; `query.Wait(ctx)` waits for transport
and callbacks to drain. Close queries when finished, or call
`query.CloseAndWait(ctx)` to close and wait in one step.

The repository's external-package `Example` and other `example_*_test.go` files
compile against the normal public API. The complete `examples/streaming` command
creates its own schema and validates its results:

```sh
go run ./examples/streaming -hosts 127.0.0.1:9042
```

See [streaming](statements/streaming.md) for capacity and ownership requirements,
and [configuration](connecting/index.md) for TLS/authentication.
