# Streaming into caller storage

`session.PrepareStream(ctx, cql.StreamStatement[P,R]{CQL: text, Params: params,
Rows: decoder})` prepares directly for caller-storage decoding. Its immutable
`PreparedStream` handle can be shared; `prepared.Query(StreamOptions{})` creates
a query for one worker. Streaming is always prepared; `Rows` supplies the
streaming decoder. The query's `Run(ctx, parameters)` method returns a cursor; `NextInto(&row)` decodes
into seeded caller storage. By default it buffers a complete response page on
normal multiplexed connections. `StreamOptions{Incremental: true}` selects
bounded incremental receive through an exclusive connection reservation. The standalone
[`examples/streaming/main.go`](../../../examples/streaming/main.go) program
creates its schema, checks exact results and backing identity, and runs through
the normal public package build.

```go
type Download struct {
    ID int32
    Payload []byte
    Pieces [][]byte
    ApplicationState string
}
decoder := cql.StreamRow(func(r *Download) { r.ApplicationState = "" },
    cql.StreamColumn("id", cql.StreamInt32, func(r *Download) *int32 { return &r.ID }),
    cql.StreamColumn("payload", cql.StreamBlob, func(r *Download) *[]byte { return &r.Payload }),
    cql.StreamColumn("pieces", cql.StreamList(cql.StreamBlob), func(r *Download) *[][]byte { return &r.Pieces }),
)
row := Download{
    Payload: make([]byte, 0, 1<<20),
    Pieces: [][]byte{make([]byte, 0, 1024), make([]byte, 0, 1024)},
}
prepared, err := session.PrepareStream(ctx, cql.StreamStatement[cql.EmptyParams, Download]{
    CQL: "SELECT id,payload,pieces FROM app.downloads",
    Rows: decoder,
})
if err != nil { return err }
query, err := prepared.Query(gocql.StreamOptions{})
if err != nil { return err }
defer query.Close()
stream, err := query.Run(ctx, cql.EmptyParams{})
if err != nil { return err }
defer stream.Close()
for stream.NextInto(&row) {
    consume(row) // Finish synchronous access before reusing the destination.
}
return stream.Err()
```

Choose execution and receive behavior independently:

| Use case | API and behavior |
| --- | --- |
| Ordinary application reads | `One` for an owned result, `Each` for synchronous consumption, or `Run` for a borrowed cursor |
| Reusing caller output buffers with normal connection multiplexing | Page-buffered `StreamQuery`, the default |
| Large responses needing bounded receive scratch | `StreamQuery` with `Incremental: true`, reserving one connection |

Both stream modes decode into caller-owned output. Page buffering holds a full
page; incremental mode bounds receive scratch and has the protocol/compression
restrictions below. Caller-buffer capacity is a separate budget in both modes.

Seed the outer slice slots **and each nested slice's capacity** before the first
NextInto. Decoders reuse capacity without growing or reallocating payloads. A
value exceeding available capacity returns StreamCapacityError before writing
that value body; earlier fields may already have changed. Provision capacity
from the application's bounds, not from untrusted remote lengths. Reset clears
logical values recursively while preserving mapped capacity. StreamRow's
explicit callback clears unmapped application state; it must preserve mapped
buffers. Custom plans/resetters have the same responsibility.

Only a true NextInto return publishes a complete validated row. Truncation,
capacity errors, cancellation, malformed text or integrity failure are terminal
and can leave partial destination data. Rows already delivered are never
replayed. Close cancels and joins in-progress NextInto destination access;
caller storage stays valid after Close. Concurrent NextInto calls are rejected.

One query permits one active execution; overlapping `Run` calls return `ErrBusy`.
Parameters are copied on admission and retries/pages share that owned snapshot.
Close the cursor on early termination; the query's next `Run` waits for the
previous execution's accepted writes to drain. `query.Close()` permanently
stops admission and cancels the active execution; `query.Wait(ctx)` waits for
its drain. Session close cancels and joins
queries and writers. Output already decoded remains caller-owned.

Page-buffered memory is proportional to concurrent response pages and remains
bounded by configured frame/decompression limits. Incremental mode reserves
one configured connection through idle time and all pages; independent queries
may wait for pool capacity. Both modes disable prefetch/speculation.

The following receive capability and scratch limits apply to explicit
**incremental** mode. Default page buffering supports all normal buffered
protocol/compression combinations.

Protocol 3/4 uncompressed raw/TLS and protocol 5 uncompressed or LZ4 raw/TLS are
supported. Raw eligible payload reads target caller arrays directly; TLS,
segment integrity and LZ4 use bounded staging/transform units. Protocol 3/4 LZ4
or Snappy frame compression is explicitly rejected with StreamCapabilityError
before streaming submission. Incremental mode reports its capability error explicitly.
Set StreamOptions.Limits for metadata, read-ahead, discard and transform budgets;
The configuration builder's `WithMaxFrameSize`, `WithMaxDecompressedSize`
and `WithMaxMetadataDepth` bound response sizes and nesting. A limit error is terminal. StreamInfo reports delivered rows, pages and attempts.

Lists, sets, and maps decode NULL as empty, matching buffered decoding, while
preserving the destination's seeded capacity. `StreamNullable` preserves the
NULL/present distinction. Vectors remain strict about their dimension and need
`StreamNullable` for NULL. This policy does not permit NULL collection elements.
Preparation validates the streaming decoder against available result metadata;
conditional statements whose PREPARE response omits result metadata are checked
on execution. Actual changed metadata always triggers plan revalidation.
