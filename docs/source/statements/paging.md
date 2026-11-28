# Paging and borrowed results

Prefer `One` for a single row and `Each` or `All` to visit every row: they
reuse the worker's scratch space and leave nothing to close. `All` is the range
form, `for row, err := range prepared.All(ctx, parameters, &dst)`; each row is
valid until the next iteration and `break` releases the execution. The
yielded pointer is `dst` for every row, so `append(rows, *row)` keeps a copy
while `append(ptrs, row)` keeps the same row many times. Pass a nil `dst` to
decode each row into a new value the loop may keep; `One` with a nil `dst`
only reports whether a row exists.
`Run(ctx, parameters)`, on a prepared statement or a worker, returns `Rows`,
which must be closed; iterate it
with `NextInto`, which decodes each row into a caller-owned destination and
fetches continuation pages as needed, or range over `rows.All(&dst)`, which
closes the rows when the loop ends. Payload fields still borrow the receive page. Their lifetime ends on
the next advancement or Close unless a retained `PageLease` pins that page.
Clone explicitly detaches a value. A retained page survives Session.Close and
does not retain worker input storage.

For explicit paging use `RunPage(ctx, parameters, cql.StartPage(), pageSize)` and
carry the returned page state to the next call:

<!-- snippet
var prepared *gocql.Prepared[int32, string]
var parameters int32
var state cql.PageState
func use(*string) {}
-->
```go
page, err := prepared.RunPage(ctx, parameters, state, 50)
if err != nil {
	return err
}
for row, err := range page.All(nil) { // closes the page when the loop ends
	if err != nil {
		return err
	}
	use(row)
}
state = page.State()
```
 `prepared.RunPage` serves
concurrent handlers, such as a paginated endpoint: keep `page.State()`, not the
page. The page size is an argument so it can come from each request; zero
uses the resolved page size. The page size `cql.Unpaged`, as that argument or
as `execopt.Options{PageSize: cql.Unpaged}`, sends no page size, so the
server returns the whole result in one response; keep it for small results. Check iteration errors and close all delivered pages.
Cyclic/nonprogressing state and retained paging-history limits report errors.

`page.State()` remains available after the page closes. A state distinguishes
the initial page, a continuation, and completion. `cql.PageState` implements
`encoding.BinaryMarshaler` and `encoding.TextMarshaler` and their
unmarshalers, which encode the whole state, so a cursor survives a round trip
through JSON or a URL; the text form is unpadded URL-safe base64. The start
state encodes as empty, and empty input decodes to it, so a request without a
cursor starts paging; `"AA"`, the start state of earlier v2 releases, still
decodes to it. Both carry
the server token, so authenticate them as the `cql.PageState` doc describes
before trusting one from a client. `cql.ParsePageState(cursor)` decodes a
text cursor in one call. The raw server token is not exported: it cannot tell
the start from completion, so a cursor built from it alone would restart a
finished query. Use `More()` and `Complete()` to tell the states apart.
A page can hold fewer rows than the page size, or none, while `More()` is
still true, so decide whether to continue with `More()`, never with the
number of rows on the page.

Use `RowInto` with `ByteSliceCodec` for reusable owned buffered destinations.
See [streaming](streaming.md) for seeded streaming decoders and explicit
incremental receive before a complete page arrives.
