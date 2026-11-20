# Flaky tests

This page tracks every test or CI step that has failed intermittently on
`gocql-v2`, or failed only on some lanes of the test matrix, and what was done
about it. A test listed here is either fixed or explicitly accepted; an
accepted one says why and what a failure of it looks like, so that it is not
mistaken for a regression.

Add an entry when a test fails in CI for a reason unrelated to the change under
test. Give the first failing run, the failure, the cause once known, and the
fix or the reason to accept it. Move an entry to "Fixed" only once the fix has
merged and the failure no longer reproduces under stress (`go test -race
-count=N -cpu 1,2,4`, several copies at once to load the machine).

The history below was collected from the failed `PR CI` and `Code coverage`
runs of `dkropachev/gocql` between 2026-11-13 and 2026-11-20, and from the
first run of the release candidate matrix (dkropachev/gocql#70). Failures on
branches that were still changing the code a test covers are not listed.

## Accepted

| Test | Where | Behavior |
| --- | --- | --- |
| `TestBatch/Exec/UnpreparedCounter`, `TestStructMapping/Batch`, `TestStructMapping/Conditional`, `TestLWTDialect/Single`, `TestLWTDialect/Batch`, `TestMetadata/Recreate/{Materialized_Views,Index,Secondary_Index}` | `tests/integration`, ScyllaDB LTS-PRIOR (2025.1) | ScyllaDB 2025.1 refuses counters, LWT, materialized views and secondary indexes in a keyspace with tablets, which the suite's keyspace has. These tests skip, naming the server's error, when the server refuses the feature; newer versions run them. |

## Fixed

| Test or step | Where | Failure | Cause and fix |
| --- | --- | --- | --- |
| `cmd/cqlgen` tests | Code coverage job | `panic: test timed out after 5m0s` in 19 runs between 2026-11-19 and 2026-11-20 | `TestGenerator` and `TestBuiltins` run `go list -export`, which also compiled the test variant of `cql` (70s, 7 GB cold); under `-cover` the package needed more than 5m. dkropachev/gocql#65 raised the coverage run's per-package timeout to 10m and skips `TestBuiltins/Compile` under `-cover`; `cqlgen` now lists the table's packages without `-test` (cold `go test -race ./cmd/cqlgen`: 64s at 2.8 GB, was 110s at 7.5 GB). |
| Run linters step | Build job | The runner lost with "The runner has received a shutdown signal" (exit 143) during `make check`, twice on 2026-11-19/20; reported earlier as the step hanging for about 50 minutes against a usual 5 | Memory. A cold `golangci-lint run` compiles every test variant for its export data; with `GOMAXPROCS=4` it peaked at 13.9 GB summed over its processes, close to the runner's 16 GB. The step now runs with `GOGC=50` (11.4 GB peak, same time). In the 200 PR CI runs before the change the step took 4–5.5 minutes when it completed. |
| `TestCloseStopsBoundExecutions` | `internal/session` | "execution ended with gocql: request: gocql: no hosts available in the pool", in coverage runs until 2026-11-19 and once in 240 stressed runs of the code before this fix | A request that passed the connection selector's closed check just before `Session.Close` could find every pool already closed and report `ErrNoConnections`. A request that finds no connection now reports its context's error or `ErrClosed` when the session is closing. |
| `TestRunStreamCoordinatorStop` | `tests/integration`, ScyllaDB LATEST | Seven failures from 2026-11-18 to 2026-11-19: after the node restart, the deferred `DROP KEYSPACE` failed with "metadata generation … retained stale data … connection closed waiting for response" | The control connection reconnected on its own schedule, after the test's pinned probe had already recovered. Fixed by e34215f9 (dkropachev/gocql#62): the control connection retries with a backoff, and the test waits for `RefreshMetadata` to succeed before cleanup. No failure in the 19 successful PR CI and 19 coverage runs since. |
| `TestRunStreamCoordinatorStop`, `TestFixture/Identity` | `tests/integration`, every Cassandra lane | `ccm start` of the node-control cluster failed with "The version of java available on PATH 17 does not match the Java version of the distribution provided via JAVA_HOME 11", and the suite read the server version as v0.0.0 for the same reason | The suite runs `ccm` itself, and `make test-integration-cassandra` did not put SDKMAN's java on `PATH` as `cassandra-start` does. It now does. Cassandra runs only in the release candidate matrix and the merge queue, so PR CI never showed it. |
| `TestMetadata/Control` | `tests/integration`, ScyllaDB | Two failures on 2026-11-19 and one in the first release candidate matrix (LATEST, arm64): `CREATE TYPE`/`CREATE TABLE` failed with "Failed to apply group 0 change due to concurrent modification" | ScyllaDB aborts the losing one of two concurrent schema changes, and the tests running beside this one change the schema. The server applied nothing, so the test resends the statement, as `recreateExec` already did. |
| `TestPaging/Rows/NextIntoPagingAndCloseInterrupt`, `TestExecution/Speculation/LoserWriterKeepsInput`, `TestQueryRegistry/CollectsUnclosedQuery` | `internal/session` | Test races (the first two) and a registry cleanup order (the third) | Fixed in dkropachev/gocql#49. |
| `TestCallTableCloseRace` | `internal/transport` | Three failures under the coverage job, last on 2026-11-19 | Calls drained by close were recycled before the whole drain finished; fixed by 9cec3e89 ("Recycle calls drained by close only after the whole drain"). No failure on `gocql-v2` since 2026-11-19. |
| `TestOneOwnsBorrowedResult`, `TestInlineSpeculation/SpeculativeWinnerEndsInlineWait`, `TestConn/Exec/StreamIDMismatchFailsConnection` | `internal/session` | Failed in most coverage runs of 2026-11-13 (`TestOneOwnsBorrowedResult`) | No failure on `gocql-v2` since 2026-11-13, nor in 240 stressed runs (`-race -cover -count=20 -cpu 1,2,4`, two copies at once). |
| `TestMetadataReuseInterleaved`, `TestPreparedAlternationAllocs` | `internal/protocol`, `internal/session` | Allocation comparisons failed under `-race` on 2026-11-18 | fa2ddf94 skips allocation comparisons in `-race` builds. No failure in 10 stressed runs (`-race -cover -count=20 -cpu 1,2,4`). |
