# Custom codec contracts

Most applications compose `Params`, `Row`, `StreamRow`, or their struct-mapping
counterparts. Custom implementations use the following public contracts from
`github.com/scylladb/gocql/v2/cql`. Some low-level plan types share their
implementation identity through aliases; their full contracts are listed here.

```go
type Encoder[T any] interface {
    CheckType(DataType) error
    Encode(*ValueWriter, T) error
}
type Decoder[T any] interface {
    CheckType(DataType) error
    Decode(ValueReader) (T, error)
    Clone(T) T
}
type Codec[T any] interface {
    Encoder[T]
    Decoder[T]
}

type ParamEncoder[P any] interface {
    Compile(Columns) (ParamPlan[P], error)
}
type ParamPlan[P any] interface {
    Encode(*ParamWriter, P) error
}
type RowDecoder[R any] interface {
    Compile(Columns) (RowPlan[R], error)
    Clone(R) R
}
type RowPlan[R any] interface {
    Decode(*RowReader) (R, error)
    DecodeInto(*RowReader, *R) error
    Clone(R) R
}

type StreamDecoder[T any] interface {
    Compile(DataType) (StreamValuePlan[T], error)
}
type StreamValuePlan[T any] interface {
    Reset(*T)
    DecodeInto(*StreamValueReader, *T) error
}
type StreamRowDecoder[R any] interface {
    Compile(Columns) (StreamRowPlan[R], error)
}
type StreamRowPlan[R any] interface {
    Reset(*R)
    DecodeInto(*StreamRowReader, *R) error
}
type StreamResetter[T any] interface {
    Reset(*T)
}
```

`CheckType` and `Compile` validate complete metadata, including nested types
even when the eventual value is NULL or empty. Return an error for an
unsupported descriptor or mapping, and never return a nil plan on success.
Resolve column names, positions, child codecs and field mappings during
compilation. A plan is immutable and safe to share across independent
executions. It must not retain a reader, writer, destination, or mutable
per-execution state.

Use `WriteParam` for each parameter in physical server order and `ReadColumn`
for each buffered result column. `RowReader.SkipColumn` skips an ignored
column. Scalar encoders write a cell's body using `ValueWriter`; the driver
owns length framing and rolls back failed encodes. `ValueReader` is a value
snapshot; its cursors are independent. Width, state and payload validation
still runs for each value after metadata compilation.

`RowPlan.DecodeInto` replaces a non-nil destination, including clearing
unmapped application fields. Return a validation error for a nil destination.
It may partially update a destination on error. `Decode` provides the same
logical decoding as a returned value. Avoid retaining the destination or
building field mappings repeatedly inside the row loop.

`Decoder.Clone`, `RowDecoder.Clone`, and `RowPlan.Clone` recursively detach
borrowed storage, including nested collection elements. `RowDecoder.Clone`
must work before any successful `Compile`; do not implement it by remembering
the last compiled plan. `One` detaches borrowed fields before returning.
`Each` callback fields expire when the callback returns; a cursor's borrowed
fields expire on advancement or Close unless a `PageLease` retains their page.
Clone retained values explicitly.

Streaming plans write into seeded caller storage. `Reset` clears logical
values while preserving mapped buffers and nested capacity. It runs before
decoding each row or value. An optional declaration-level `StreamResetter`
also resets fields absent from actual metadata without needing a compiled
database type. Custom streaming declarations mapping such absent fields must
provide it. Use `ReadStreamColumn` with a checked child plan. Do not adapt a
buffered decoder by accumulating an entire streamed cell.

Streaming decoders must report `cqlerr.StreamCapacityError` when caller
capacity is insufficient rather than growing payload buffers. Only a true
`NextInto` publishes a complete row; a failure can leave earlier destination
fields changed. See [streaming](statements/streaming.md) for cursor ownership,
receive modes and complete caller-storage examples.

For request observations, implement both `gocql.Observer.ObserveRequest` and
`ObserveAttempt`. The driver shares observer and policy objects across
executions; implementations must support concurrent calls. `Each` treats a
callback's `gocql.StopIteration` as success before notifying observers. Actual
callback errors remain failures.
