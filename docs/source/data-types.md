# Typed codecs

Use immutable `DataType` and `Columns` descriptors. `ScalarType`, `ListType`,
`SetType`, `MapType`, `TupleType`, `UDTType` and `VectorType` construct them;
predeclared scalars such as `TextType` and `IntType` need no error check.
`UDTOptionsFor` derives UDT codec options from a UDT `DataType`.
`Encoder[T]`/`Decoder[T]` compile typed value plans; `ParamEncoder[P]` and
`RowDecoder[R]` compose statement plans. `Params(Param(...))` and
`Row(Column(...))` map struct fields explicitly.

Native scalars have explicit codecs such as Int32Codec, Int64Codec, BoolCodec, UUIDCodec,
TimestampCodec and DurationCodec. `StringCodec` and `ByteSliceCodec` encode a Go string and []byte and decode
an owned copy; TextCodec and BlobCodec decode a borrowed Text and Bytes
without copying. Collection decoders eagerly produce slices whose payload elements may borrow pages;
nullable/empty wrappers distinguish null, empty and present values. Unset is
an explicit binding state and is invalid in results.

Package `cql/dynamic` (`dynamic.Params`, `dynamic.Rows`, `dynamic.Value`)
and `EncodedValue` support metadata-driven
applications. Standalone `EncodeValue` (copies caller payloads),
`EncodeValueBorrowed` (zero-copy) and `DecodeValue` use the same codec
implementation as execution. Explicit owned
conversion/Clone detaches payloads. Custom codecs use checked reader/writer
interfaces and preserve metadata validation and lifetime rules.

Streaming has separate `stream.Decoder` and `stream.RowDecoder` plans; these
incremental readers cannot call buffered decoders. See [streaming](statements/streaming.md).

`MapCodec` decodes a map into `[]Entry[K, V]` in server order and accepts keys
that Go cannot compare; `GoMapCodec` decodes into a Go `map[K]V`, rejects a
repeated key, and is what struct mappings use for `map[K]V` fields of built-in
key and value types. `ListCodec`, `SetCodec`, `MapCodec` and `GoMapCodec`, and
the streaming counterparts, accept NULL as an empty collection. Use `NullableCodec` or `stream.Nullable` to
preserve NULL independently of a present empty collection. Vectors retain their
declared dimension and reject NULL without a nullable wrapper.

`TimeUUID` is distinct from `UUID`, so struct mappings choose the appropriate
codec automatically. `NewTimeUUID`, `TimeUUIDFromTime`, `MinTimeUUID`,
`MaxTimeUUID`, and `TimeUUIDWith` return `TimeUUID`; `RandomUUID` returns a
version 4 `UUID`. It supports canonical string, JSON and text
formatting and timestamp extraction. `ParseTimeUUID` and `TimeUUIDFromUUID`
validate version 1. Convert with `cql.UUID(id)` when storing an identifier in a
regular UUID column. The zero value can be formatted with `String`, but checked JSON/text
serialization and CQL binding require version 1; use `Nullable` for NULL.

`ParamsOf`, `RowOf`, and `stream.RowOf` honor codec options for generated and
reflected mappings. A recognized built-in field tag takes precedence over a
type-wide override; the override takes precedence over the generated/default
codec, including a custom-expression default. Other generated expressions keep
their codecs. Each call constructs an independent mapping; options never mutate
generated defaults. Each option has a scope, and the other functions ignore it:
`WithCodec` applies to `ParamsOf` and `RowOf`, `stream.WithCodec` to
`stream.RowOf`, and `ReuseBuffers` to `RowOf`, keeping slice field storage
between rows; `stream.RowOf` always reuses that storage. `BindValue` binds
Present, Null and Unset only; binding fails for `EmptyCell`, so bind an
`EmptyValueOf` through `EmptyCodec` for an empty value.

See [custom codec contracts](custom-codecs.md) for extension interfaces and plan
ownership requirements.
