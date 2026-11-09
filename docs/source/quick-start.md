# Quick start

Install the single v2 module:

```sh
go get github.com/scylladb/gocql/v2@v2.0.0
```

Import `github.com/scylladb/gocql/v2` (`gocql`) and
`github.com/scylladb/gocql/v2/cql`. Build a configuration with
`gocql.NewConfig(contactPoints...).Build()` and open it with
`gocql.Connect(ctx, config)`. Close the session when finished.

Declare a `cql.Statement[P,R]` with `CQL`, `Params` and `Rows`.
`session.Prepare(ctx, statement)` prepares it and validates its codecs
against server metadata; `prepared.Query(options)` returns a worker-owned
reusable query, and several queries can share one preparation. `query.One` reads one row, `query.Each` visits every row and
`query.Run` returns typed rows: check `Rows.Err()` after iteration and close
the rows. Use `query.Exec` for statements without results: their row type is
`cql.EmptyRow`. A statement with no bind markers takes `cql.EmptyParams`. Leave
`Params` or `Rows` nil for those empty types; `cql.Command(text)` builds a
statement with neither, such as DDL. Parameters are copied on admission.
`prepared.One`, `prepared.Each` and `prepared.Exec` run a prepared statement
once from any goroutine without creating a query. A query call while a `Rows`
of that query is open, or while another goroutine's call runs, returns
`ErrBusy`. Close queries when finished with `query.Close(ctx)`, which also waits
for transport writes and callbacks to drain.

The repository's external-package `Example` and other `example_*_test.go` files
compile against the normal public API. The complete `examples/streaming` command
creates its own schema and validates its results:

```sh
go run ./examples/streaming -hosts 127.0.0.1:9042
```

See [streaming](statements/streaming.md) for capacity and ownership requirements,
and [configuration](connecting/index.md) for TLS/authentication.
