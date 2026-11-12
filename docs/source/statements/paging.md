# Paging and borrowed results

Prefer `One` for a single row and `Each` or `All` to visit every row: they
reuse the query's scratch space and leave nothing to close. `All` is the range
form, `for row, err := range prepared.All(ctx, parameters, &dst)`; each row is
valid until the next iteration and `break` releases the execution.
`Run(ctx, parameters)`, on a prepared statement or a query, returns `Rows`,
which must be closed; iterate it
with `NextInto`, which decodes each row into a caller-owned destination and
fetches continuation pages as needed. Payload fields still borrow the receive page. Their lifetime ends on
the next advancement or Close unless a retained `PageLease` pins that page.
Clone explicitly detaches a value. A retained page survives Session.Close and
does not retain query input storage.

For explicit paging use `RunPage(ctx, parameters, cql.StartPage, pageSize)` and
carry the returned page state to the next call. `prepared.RunPage` serves
concurrent handlers, such as a paginated endpoint: keep `page.State()`, not the
page. The page size is an argument so it can come from each request; zero
uses the resolved page size. The page size `cql.Unpaged`, as that argument or
as `Options{PageSize: cql.Some(cql.Unpaged)}`, sends no page size, so the
server returns the whole result in one response; keep it for small results. Check iteration errors and close all delivered pages.
Cyclic/nonprogressing state and retained paging-history limits report errors.

`page.State()` remains available after the page closes. A state distinguishes
the initial page, a continuation, and completion. `state.Token()` returns an
owned copy of the server continuation token; it does not serialize the full
state. Check `state.More()` before exporting a token, and use
`cql.PageStateFromToken(token)` to resume it. Empty continuation tokens are
valid: nil or empty input to `PageStateFromToken` always creates a continuation.
The initial and complete states both return nil from `Token()`; use `More()`
and `Complete()` to distinguish them. An HTTP API can omit its next-page token
when `More()` is false, representing completion separately from token bytes.
A page can hold fewer rows than the page size, or none, while `More()` is
still true, so decide whether to continue with `More()`, never with the
number of rows on the page.

Use `RowInto` with `ByteSliceCodec` for reusable owned buffered destinations.
See [streaming](streaming.md) for seeded streaming decoders and explicit
incremental receive before a complete page arrives.
