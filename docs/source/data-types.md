# Typed codecs

Use immutable `DataType` and `Columns` descriptors. `ScalarType`, `ListType`,
`SetType`, `MapType`, `TupleType`, `UDTType` and `VectorType` construct them;
predeclared scalars such as `TextType` and `IntType` need no error check.
`ParseDataType("map<text, frozen<list<int>>>")` parses a type as CQL writes
it; a user-defined type needs its fields, so it is built with `UDTType` or
read from metadata.
Text and varchar are one CQL type, `KindText`: a column declared either way
has that kind.
`UDTNameOf` derives the `UDTName` option of a UDT `DataType`.
`Encoder[T]`/`Decoder[T]` compile typed value plans; `ParamEncoder[P]` and
`RowDecoder[R]` compose statement plans. `Params(Param(...))` and
`Row(Column(...))` map struct fields explicitly. `CodecOf[T]()` returns the
codec a struct field of type T derives, such as `[]int32` or
`map[string]Address`, and `ParamOf[T]()` and `ColumnOf[T]()` bind or decode
one value with it, as `OneParam` and `OneColumn` do with an explicit codec.

Native scalars have explicit codecs such as Int32Codec, Int64Codec, BoolCodec, UUIDCodec,
TimestampCodec and DurationCodec. `Int64Codec` also reads and writes a counter
column, so an `int64` field maps one. An integer codec widens: `Int64Codec`
and `IntCodec` also decode int, smallint and tinyint, `Int32Codec` smallint
and tinyint, and `Int16Codec` tinyint, losslessly; encoding into the narrower
column fails with a `CodecError` for a value outside its range.
`Float64Codec` likewise decodes float, and encodes into it rounding to the
nearest float32, failing beyond float32's range. A non-nullable destination,
such as an `int64` field, fails on NULL with an error matching `cql.ErrNull`;
use `Nullable[T]` (or `cql.NonNull(v)` to build a present one) for a column
that may be NULL, or `cql.NullAsZero()` or a `nullzero` tag to read NULL as
the zero value. `StringCodec` and `ByteSliceCodec` encode a Go string and []byte and decode
an owned copy; TextViewCodec and BytesViewCodec decode a borrowed Text and Bytes
without copying. The text codecs, `StreamTextBytes` included, map both text and
ascii, and check a value is valid UTF-8 for text and ASCII for ascii.
`UUIDCodec` maps uuid and timeuuid; it rejects encoding a UUID that is not
version 1 into a timeuuid. Collection decoders eagerly produce slices whose payload elements may borrow pages;
nullable/empty wrappers distinguish null, empty and present values. Unset is
an explicit binding state and is invalid in results.

Dynamic values (`cql.DynamicParams`, `cql.DynamicRows`, `cql.Value`)
and `EncodedValue` support metadata-driven
applications. Scalar constructors such as `cql.TextValue("a")` return a
`Value` with no error, so a parameter list is written inline; an invalid
payload, such as a `TextValue` that is not UTF-8, fails when it is bound.
`BigIntValue` also binds a counter. `NullValue()` and `UnsetValue()` have no
type: they bind NULL or UNSET of whatever type the marker has. Accessors such
as `Int64`, `Float64`, `Text` and `Time` read a present value; `Text` copies
text into a string and `TextView` borrows it; `Len` and
`Field(name)` read a collection's size and a UDT's field, and `Elements`,
`Entries` and `Fields` return an `iter.Seq2` that ranges over every member,
ending with an error for a malformed member or a value of another kind.
`ListValue`, `SetValue` and `VectorValue` take the element type and the
values: `cql.ListValue(cql.TextType, cql.TextValue("a"))`; `MapValue` takes
the key and value types and the entries, and `TupleValue` and `UDTValue` the
type and the members. `UDTValue` may name some of the fields, in any order;
the others bind NULL. A dynamic value binds a marker or member by the rules
of the typed codecs: an integer binds any integer type and a float either
float type, checking the range when it is encoded; text and ascii bind each
other, validated for the marker's kind; and a UDT value binds its UDT after
`ALTER TYPE ADD`, the added fields as NULL. `Bytes` copies a blob and
`BytesView` borrows it. `String` prints a `Value`,
and `Varint`, `Decimal`, `Date`, `TimeOfDay`, `Timestamp` and `Duration`
print, in a CQL literal form; their `MarshalText` and `UnmarshalText` use
that form, so `encoding/json` writes a date as `"2026-01-02"`. `VarintFromInt64`
and `Varint.Int64` convert a varint that fits an `int64`, `VarintFromBigInt`
and `BigInt` any other, and `Cmp` orders varints and decimals. A `Varint`, and
so a `Decimal`, owns its bytes: a decoded one outlives its response, and two
of one integer are `==`; `VarintViewCodec` reads a varint's bytes without
copying them. `ParseDecimal` reads a decimal as `Decimal.String` prints it, and
`Decimal.Rat` returns its exact value. `Duration.TimeDuration` and
`DurationFromTime` convert a duration without months or days to and from
`time.Duration`. A vector type has kind `KindVector`, the
driver's own kind: the protocol sends a vector as a custom type. Standalone
`EncodeValue`, which copies caller payloads, and `DecodeValue` use the same
codec implementation as execution; `NewEncodedValue` wraps an encoded body,
borrowing it. A decoder sees an empty value as a response holds
it, CellPresent with an empty body, including one `DecodeValue` reads from an
`EncodedValue` in `CellEmpty`. Explicit owned
conversion/Clone detaches payloads. Custom codecs use checked reader/writer
interfaces and preserve metadata validation and lifetime rules.

Streaming has separate `cql.StreamDecoder` and `cql.StreamRowDecoder` plans; these
incremental readers cannot call buffered decoders. See [streaming](statements/streaming.md).

`MapEntriesCodec` decodes a map into `[]Entry[K, V]` in server order and accepts keys
that Go cannot compare; `MapCodec` decodes into a Go `map[K]V`, rejects a
repeated key, and is what struct mappings use for `map[K]V` fields of built-in
key and value types. `ListCodec`, `SetCodec`, `MapEntriesCodec` and `MapCodec`, and
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
`user_id`. A field's codec is derived from its Go type, recursively. A leaf
type has a built-in codec: the Go scalars, `string`, `[]byte`, `cql.Text`
and `cql.Bytes` (borrowed views, `TextViewCodec` and `BytesViewCodec`), the
cql scalar types, `netip.Addr`, `*big.Int`, `time.Time` (a timestamp with
`TimestampTimeCodec`, which decodes a UTC time, encodes the instant truncated
to the millisecond, and rejects the zero `time.Time`, almost always a field
left unset; bind a `Nullable[time.Time]` or nil `*time.Time` for NULL) and
`time.Duration` (a CQL duration with `TimeDurationCodec`, never a bigint;
decoding a duration with months or days fails). An `int` field maps a bigint
with `IntCodec`. A named type defined from a predeclared one, such as `type
Status string`, maps like it, and any array of 16 bytes maps a uuid or
timeuuid like `UUID`. A type with a `CQLCodec() cql.Codec[T]` method, as
`cqlgen -udt` generates for a UDT struct, maps with its codec. From those,
`[]V` and `[N]V` map a list, a set or a vector, whichever the column is
(`cql.SliceCodec`; an array needs exactly N elements), and `codec=ListCodec`,
`codec=SetCodec` or `codec=VectorCodec` requires one; `*V` maps `V` with
NULL as nil, `Nullable[V]` with NULL as invalid, `map[K]V` a map
(`MapCodec`), `cql.Pair[A, B]` a two-member tuple (`PairCodec`), and the
forms nest: `[][]int32`, `map[string][]Status` or `*Nullable[[]string]`. A
`codec=` tag naming a leaf codec, such as `codec=ASCIIStringCodec`, applies
to the leaves it maps. A `BindValue[V]` field binds a `V`, NULL or UNSET;
it only binds parameters, so `RowOf` rejects it, and it is only ever a
field's own type. `cql.Bind(v)`, `cql.BindNull[T]()` and `cql.BindUnset[T]()`
build one and `Get` reads it; its zero value binds UNSET, so a struct of
`BindValue` fields writes only the fields the application set. Bind a
`MaybeEmpty` through `MaybeEmptyCodec` for an empty value. A nil pointer, slice or
map field binds NULL, or an empty collection, unless its tag has `nilunset`
(`cql:"name,nilunset"`) or the mapping has `cql.NilAsUnset()`: then it binds
UNSET, which leaves the column unchanged.

A field that cannot hold NULL fails to decode a NULL cell with an error
matching `cql.ErrNull`, unless its tag has `nullzero` or the mapping has
`cql.NullAsZero()`, which leave it at its zero value. A UDT's fields always
decode NULL so, and so does a trailing field the server leaves out of a value
written before `ALTER TYPE ADD` added it, so a UDT keeps decoding after its
type grows.

`ParamsOf` binds a named marker (`:name`) that a statement repeats from one
field. The server names a `?` marker after its column, so the two markers of
`WHERE ck > ? AND ck < ?` are both `ck`; `ParamsOf` rejects them when the
statement is prepared rather than bind one field to both. Name them, as in
`WHERE ck > :low AND ck < :high`, or bind them by position with `ParamAt`.

A UDT field maps with its type's `CQLCodec` method, which
`go run github.com/scylladb/gocql/v2/cmd/cqlgen -udt=Address` generates, or
with `cql.WithCodec(cql.MustUDTCodecOf[Address](cql.UDTName("", "address")))`.
`UDTCodecOf` maps the UDT's fields to the struct's as `RowOf` maps columns;
`UDTName(keyspace, name)` names the UDT it expects, an empty keyspace
matching the UDT in any keyspace and an empty name a UDT of any name.
`cql.IgnoreUnknownFields()` accepts UDT fields the struct lacks, and
`cql.AllowMissingFields()` struct fields the UDT lacks. Either way the codec
also maps `[]Address`, `*Address` and map values of `Address`. A tuple field maps with
`cql.WithCodec(cql.MustTupleCodecOf[Point]())`, which maps the tuple's
members to the struct's fields by position, `cql.PairCodec(a, b)` for a
two-member tuple, or `cql.TupleCodec(params, rows)`.

Options are not generic: `cql.WithCodec(statusCodec)` infers the field type
from the codec. They honor generated and reflected mappings alike. A
recognized built-in field tag takes precedence over a type-wide override; the
override takes precedence over the generated/default codec, including a
custom-expression default. Other generated expressions keep their codecs.
Each call constructs an independent mapping; options never mutate generated
defaults. Each option applies to some constructors, and the others reject it
with a `ValidationError`: `WithCodec` applies to `ParamsOf`, `RowOf`,
`UDTCodecOf`, `TupleCodecOf` and `CodecOf`; `WithEncoder` and `NilAsUnset` to
`ParamsOf`; `ReuseBuffers` to `RowOf`, keeping slice field storage between
rows (`cql.StreamRowOf` always reuses that storage); `NullAsZero` to
`RowOf`, `cql.StreamRowOf`, `UDTCodecOf` and `TupleCodecOf`;
`IgnoreUnknownFields` to `RowOf`, `cql.StreamRowOf`, `UDTCodecOf` and the UDT
encoder and decoders, which otherwise require a field for every result
column or UDT field, so `SELECT *` keeps working after `ALTER TABLE ADD`;
`UDTName` and `AllowMissingFields` to `UDTCodecOf`, `UDTEncoder`,
`UDTDecoder` and `cql.StreamUDT`; and `cql.WithStreamDecoder` to
`cql.StreamRowOf`.

See [custom codec contracts](custom-codecs.md) for extension interfaces and plan
ownership requirements.
