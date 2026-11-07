# Retry policy

`Profile.Retry` is a `retry.Policy`: `Decide(retry.Attempt) retry.Decision`.
`retry.Attempt` carries the attempt's error, number, idempotence, consistency
and `ExecutionStatus`, which reports whether the attempt may have changed data
(see [Errors](errors.md)). Decisions select `retry.Stop`, `retry.Same`,
`retry.Next` or `retry.IgnoreWrite`, with optional consistency and delay;
achieved consistency is reported.

`retry.Safe` bounds attempts and retries only when replay is safe: it moves to
the next host when the server rejected the request before applying it
(Unavailable, Overloaded, Bootstrapping), retries a read timeout that had
enough replicas, and replays other failures only for idempotent requests or
attempts whose status is `cqlerr.NotExecuted`. Deterministic errors never
retry. A custom policy can build on `cqlerr.KindOf` and the attempt's
`ExecutionStatus`.

Retries reuse encoded binding inputs and compiled result plans. They do not
invoke application encoders again. Streaming may retry only before any
application output begins and never replays a delivered row.

Request observers see one terminal request event and one per started attempt.
Callbacks are synchronous and must be prompt; disabled observation avoids event
construction.
