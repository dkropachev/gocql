# Typed codecs

Use immutable `DataType` and `Columns` descriptors. `ScalarType`, `ListType`,
`SetType`, `MapType`, `TupleType`, `UDTType` and `VectorDataType` construct them;
predeclared scalars such as `TextType` and `IntType` need no error check.
`UDTOptionsFor` derives UDT codec options from a UDT `DataType`.
`Encoder[T]`/`Decoder[T]` compile typed value plans; `ParamEncoder[P]` and
`RowDecoder[R]` compose statement plans. `Params(Param(...))` and
`Row(Column(...))` map struct fields explicitly.

Native scalars have explicit codecs such as Int32, Int64, Bool, UUIDCodec,
TimestampCodec and DurationCodec. `StringEncoder` encodes a Go string;
TextCodec decodes a borrowed Text. BytesEncoder accepts []byte and BlobCodec
uses borrowed Bytes. Collection decoders eagerly produce slices whose payload elements may borrow pages;
nullable/empty wrappers distinguish null, empty and present values. Unset is
an explicit binding state and is invalid in results.

`DynamicParams`/`DynamicRows`, `Value` and `EncodedValue` support metadata-driven
applications. Standalone `EncodeValue` (copies caller payloads),
`EncodeValueBorrowed` (zero-copy) and `DecodeValue` use the same codec
implementation as execution. Explicit owned
conversion/Clone detaches payloads. Custom codecs use checked reader/writer
interfaces and preserve metadata validation and lifetime rules.

Streaming has separate `StreamDecoder` and `StreamRowDecoder` plans; these
incremental readers cannot call buffered decoders. See [streaming](statements/streaming.md).

`ListCodec`, `SetCodec`, and `MapCodec`, and their streaming counterparts,
accept NULL as an empty collection. Use `NullableCodec` or `StreamNullable` to
preserve NULL independently of a present empty collection. Vectors retain their
declared dimension and reject NULL without a nullable wrapper.

`TimeUUID` is distinct from `UUID`, so struct mappings choose the appropriate
codec automatically. `NewTimeUUID`, `MinTimeUUID`, `MaxTimeUUID`, and
`TimeUUIDWith` return `TimeUUID`. It supports canonical string, JSON and text
formatting and timestamp extraction. `ParseTimeUUID` and `TimeUUIDFromUUID`
validate version 1. Convert with `cql.UUID(id)` when storing an identifier in a
regular UUID column. The zero value can be formatted with `String`, but checked JSON/text
serialization and CQL binding require version 1; use `Nullable` for NULL.

`ParamsOf`, `RowOf`, and `StreamRowOf` honor codec options for generated and
reflected mappings. A recognized built-in field tag takes precedence over a
type-wide override; the override takes precedence over the generated/default
codec, including a custom-expression default. Other generated expressions keep
their codecs. Each call constructs an independent mapping; options never mutate
registered defaults. `ReuseBuffers` independently controls buffered destination
reuse.

See [custom codec contracts](custom-codecs.md) for extension interfaces and plan
ownership requirements.
