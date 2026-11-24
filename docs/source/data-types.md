# Typed codecs

Use immutable `DataType` and `Columns` descriptors. `ScalarType`, `ListType`,
`SetType`, `MapType`, `TupleType`, `UDTType` and `VectorType` construct them;
predeclared scalars such as `TextType` and `IntType` need no error check.
`UDTOptionsFor` derives UDT codec options from a UDT `DataType`.
`Encoder[T]`/`Decoder[T]` compile typed value plans; `ParamEncoder[P]` and
`RowDecoder[R]` compose statement plans. `Params(Param(...))` and
`Row(Column(...))` map struct fields explicitly.

Native scalars have explicit codecs such as Int32Codec, Int64Codec, BoolCodec, UUIDCodec,
TimestampCodec and DurationCodec. `Int64Codec` also reads and writes a counter
column, so an `int64` field maps one. A non-nullable destination, such as an
`int64` field, fails on NULL with an error matching `cql.ErrNull`; use
`Nullable[T]` (or `cql.NonNull(v)` to build a present one) for a column that
may be NULL. `StringCodec` and `ByteSliceCodec` encode a Go string and []byte and decode
an owned copy; TextCodec and BlobCodec decode a borrowed Text and Bytes
without copying. Collection decoders eagerly produce slices whose payload elements may borrow pages;
nullable/empty wrappers distinguish null, empty and present values. Unset is
an explicit binding state and is invalid in results.

Dynamic values (`cql.DynamicParams`, `cql.DynamicRows`, `cql.Value`)
and `EncodedValue` support metadata-driven
applications. Scalar constructors such as `cql.TextValue("a")` return a
`Value` with no error, so a parameter list is written inline; an invalid
payload, such as a `TextValue` that is not UTF-8, fails when it is bound.
`BigIntValue` also binds a counter. Accessors such as `Signed`, `Text` and
`Time` read a present value; `Len`, `At(i)` and `Field(name)` read one member
of a collection, tuple or UDT, and `Elements`, `Entries` and `Fields` return
an iterator whose `All` ranges over every member. `String` prints a `Value`,
and `Varint`, `Decimal`, `Date`, `TimeOfDay`, `Timestamp` and `Duration`
print, in a CQL literal form. A vector type has kind `TypeVector`, the
driver's own kind: the protocol sends a vector as a custom type. Standalone
`EncodeValue` (copies caller payloads),
`EncodeValueBorrowed` (zero-copy) and `DecodeValue` use the same codec
implementation as execution. A decoder sees an empty value as a response holds
it, Present with an empty body, including one `DecodeValue` reads from an
`EncodedValue` in `EmptyCell`. Explicit owned
conversion/Clone detaches payloads. Custom codecs use checked reader/writer
interfaces and preserve metadata validation and lifetime rules.

Streaming has separate `cql.StreamDecoder` and `cql.StreamRowDecoder` plans; these
incremental readers cannot call buffered decoders. See [streaming](statements/streaming.md).

`MapCodec` decodes a map into `[]Entry[K, V]` in server order and accepts keys
that Go cannot compare; `GoMapCodec` decodes into a Go `map[K]V`, rejects a
repeated key, and is what struct mappings use for `map[K]V` fields of built-in
key and value types. `ListCodec`, `SetCodec`, `MapCodec` and `GoMapCodec`, and
the streaming counterparts, accept NULL as an empty collection. Use `NullableCodec` or `cql.StreamNullable` to
preserve NULL independently of a present empty collection. Vectors retain their
declared dimension and reject NULL without a nullable wrapper.

`TimeUUID` is distinct from `UUID`, so struct mappings choose the appropriate
codec automatically. `NewTimeUUID`, `TimeUUIDFromTime`, `MinTimeUUID`,
`MaxTimeUUID`, and `TimeUUIDWith` return `TimeUUID`; `RandomUUID` returns a
version 4 `UUID`. Both support canonical string, JSON and text formatting;
only `TimeUUID` has the version 1 accessors `Time`, `Timestamp`, `Clock` and
`Node`. `ParseTimeUUID` and `TimeUUIDFromUUID` validate version 1; use
`TimeUUIDFromUUID` to read the timestamp of a `UUID`, and `cql.UUID(id)` to
store a `TimeUUID` in a regular UUID column. The zero value can be formatted with `String`, but checked JSON/text
serialization and CQL binding require version 1; use `Nullable` for NULL.

Struct mappings (`ParamsOf`, `RowOf`, `cql.StreamRowOf`, `UDTCodecOf`) name
a field's column by its `cql:"name"` tag or, untagged, by its name in
snake_case as gocqlx does: `CreatedAt` maps `created_at` and `UserID`
`user_id`. A `time.Time` field maps a timestamp with `TimestampTimeCodec`,
which decodes a UTC time and encodes the instant truncated to the
millisecond; `Nullable[time.Time]`, `[]time.Time` and `map[K]time.Time`
fields map too. A `BindValue[V]` field binds Present, Null or Unset for a `V`
with a built-in codec or one `WithCodec` or `WithEncoder` maps; it only binds
parameters, so `RowOf` rejects it. Binding fails for `EmptyCell`, so bind a
`MaybeEmpty` through `EmptyCodec` for an empty value.

A UDT field maps with `cql.WithCodec(cql.MustUDTCodecOf[Address](cql.UDTOptions{Name: "address"}))`:
`UDTCodecOf` maps the UDT's fields to the struct's as `RowOf` maps columns,
and an empty `UDTOptions.Keyspace` matches the UDT in any keyspace. A tuple
field maps with `cql.WithCodec(cql.TupleCodec(params, rows))`.

Options are not generic: `cql.WithCodec(statusCodec)` infers the field type
from the codec. They honor generated and reflected mappings alike. A
recognized built-in field tag takes precedence over a type-wide override; the
override takes precedence over the generated/default codec, including a
custom-expression default. Other generated expressions keep their codecs.
Each call constructs an independent mapping; options never mutate generated
defaults. Each option applies to some constructors, and the others reject it
with a `ValidationError`: `WithCodec` applies to `ParamsOf`, `RowOf` and
`UDTCodecOf`; `WithEncoder` to `ParamsOf`; `ReuseBuffers` to `RowOf`,
keeping slice field storage between rows (`cql.StreamRowOf` always reuses
that storage); `IgnoreExtraColumns` to `RowOf` and `cql.StreamRowOf`, which
otherwise require a field for every result column, so `SELECT *` keeps
working after `ALTER TABLE ADD`; and `cql.WithStreamCodec` to
`cql.StreamRowOf`.

See [custom codec contracts](custom-codecs.md) for extension interfaces and plan
ownership requirements.
