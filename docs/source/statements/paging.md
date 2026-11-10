# Paging and borrowed results

Prefer `query.One` for a single row and `query.Each` to visit every row:
they reuse the query's scratch space and leave nothing to close.
`query.Run(ctx, parameters)` returns `Rows`, which must be closed; iterate it
with `NextInto`, which decodes each row into a caller-owned destination and
fetches continuation pages as needed. Payload fields still borrow the receive page. Their lifetime ends on
the next advancement or Close unless a retained `PageLease` pins that page.
Clone explicitly detaches a value. A retained page survives Session.Close and
does not retain query input storage.

For explicit paging use `query.RunPage(ctx, parameters, StartPage)` and
carry the returned page state to the next call. `query.RunUnpaged` explicitly requests
one unpaged response. Check iteration errors and close all delivered pages.
Cyclic/nonprogressing state and retained paging-history limits report errors.

`page.State()` remains available after the page closes. A state distinguishes
the initial page, a continuation, and completion. `state.Token()` returns an
owned copy of the server continuation token; it does not serialize the full
state. Check `state.More()` before exporting a token, and use
`gocql.PageStateFromToken(token)` to resume it. Empty continuation tokens are
valid: nil or empty input to `PageStateFromToken` always creates a continuation.
The initial and complete states both return nil from `Token()`; use `More()`
and `Complete()` to distinguish them. An HTTP API can omit its next-page token
when `More()` is false, representing completion separately from token bytes.

Use `RowInto` with `OwnedBlobCodec` for reusable owned buffered destinations.
See [streaming](streaming.md) for seeded streaming decoders and explicit
incremental receive before a complete page arrives.
