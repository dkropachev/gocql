# gocql v2 API contract

Status: implementation contract, distinct from the six reviewed Rust reference
specifications. Performance acceptance is incomplete; see
[redesign status](../tests/driver-comparison/REDESIGN.md). Integration tests
exercise typed behavior with focused testkit hooks.

Module: `github.com/scylladb/gocql/v2`. The public packages are:

| Package | Contents |
|---|---|
| `gocql` (module root) | dialects, `Connect`, `Session[D]`, `Prepared`, `Query`, `Batch`, cursors, results |
| `config` | cluster configuration and profile builders, observers |
| `exec` | per-execution options: `Options`, `CompiledOptions`, `PrepareOptions`, `StreamOptions`, `StreamLimits`, `SerialOverride` |
| `event` | cluster events delivered by `Session.Subscribe` |
| `trace` | query traces read by `Session.Trace` |
| `cql` | statements, data types, values, buffered codecs, parameter and row mappings, consistency |
| `cql/stream` | decoders that write streamed rows into caller storage |
| `cql/dynamic` | values whose types are known only from metadata at run time |
| `cqlerr` | errors and sentinels the driver returns |
| `routing` | routing policies, plans, targets and token partitioners |
| `metadata` | cluster topology and schema snapshots |
| `metrics` | session-wide counter snapshot |
| `retry` | retry policies |
| `endpoint` | address translation, DNS resolution and dialers |
| `auth` | authentication providers |
| `scylla` | API only ScyllaDB supports |

Everything else lives under `internal/`. In the code blocks below, unqualified
names belong to the package the block names, or to `gocql` when it names none;
`cql` names are unqualified inside the `cql`, `stream` and `dynamic` blocks.

## Type metadata and codec contracts

The signatures below are normative. Existing `Type` constants denote CQL kinds;
`DataType` is the immutable descriptor of all wire-available type structure. It
contains nested element/key and value types, ordered tuple/UDT fields, UDT
identity, vector dimension and frozen state. Construction and access copy
descriptor slices, never value payloads. An invalid/zero descriptor fails
validation. Native protocol metadata does not carry collection/UDT frozen
annotations: FrozenKnown is false for that source; WithFrozen records an
explicitly known annotation. Equal and codec compatibility compare nested wire
structure/identity/names and ignore frozen annotations. Schema validation may
separately compare annotations when both are known. Fingerprints include all
compared structure and names; equality never relies solely on a hash.

```go
// package cql
type DataType struct { /* immutable private descriptor */ }
type ColumnSpec struct { Keyspace, Table, Name string; Type DataType }
type Columns struct { /* immutable private ordered column specs */ }
func ScalarType(kind Type) (DataType, error) // shared descriptor; fails only for non-scalar kinds
var ASCIIType, BigIntType, BlobType, BooleanType, CounterType, DecimalType,
    DoubleType, FloatType, IntType, TextType, TimestampType, UUIDType,
    VarcharType, VarintType, TimeUUIDType, InetType, DateType, TimeType,
    SmallIntType, TinyIntType, DurationType DataType // predeclared scalars; treat as constants
func ListType(element DataType) (DataType, error)
func SetType(element DataType) (DataType, error)
func MapType(key, value DataType) (DataType, error)
func TupleType(fields ...DataType) (DataType, error)
func UDTType(keyspace, name string, fields ...ColumnSpec) (DataType, error)
func VectorType(element DataType, dimension uint16) (DataType, error)
func NewColumns(columns ...ColumnSpec) Columns
func (t DataType) Kind() Type
func (t DataType) Equal(other DataType) bool
func (t DataType) Frozen() bool
func (t DataType) FrozenKnown() bool
func (t DataType) WithFrozen(frozen bool) DataType
func (t DataType) Element() (DataType, bool)
func (t DataType) KeyValue() (DataType, DataType, bool)
func (t DataType) Fields() Columns
func (t DataType) UDTName() (keyspace, name string, ok bool)
func (t DataType) Dimension() (uint16, bool)
func (c Columns) Len() int
func (c Columns) At(index int) (ColumnSpec, bool)
func (c Columns) Equal(other Columns) bool
func (c Columns) Same(other Columns) bool

type Encoder[T any] interface {
    CheckType(DataType) error
    Encode(*ValueWriter, T) error
}
type Decoder[T any] interface {
    CheckType(DataType) error
    Decode(ValueReader) (T, error)
    Clone(T) T
}
type Codec[T any] interface { Encoder[T]; Decoder[T] }

type ParamEncoder[P any] interface {
    Compile(Columns) (ParamPlan[P], error)
}
type ParamPlan[P any] interface { Encode(*ParamWriter, P) error }
type RowDecoder[R any] interface {
    Compile(Columns) (RowPlan[R], error)
    Clone(R) R
}
type RowPlan[R any] interface {
    Decode(*RowReader) (R, error)
    DecodeInto(*RowReader, *R) error
    Clone(R) R
}
```

`CheckType` runs when compiling a plan, including all children even for empty,
null or unset values. It runs again when the actual metadata changes. A compiled
plan is immutable, safe for concurrent use, and does not capture mutable
per-call state. Encode/Decode get the checked full descriptor. Custom
implementations must not assume their callers have run CheckType; public entry
points always do. Custom `Clone` must recursively detach all borrowed storage;
scalar implementations return the value unchanged. Compilation may allocate
descriptor/mapping storage; row and scalar decoding must not repeat it. Built-in
compositions recursively bind child codecs and tuple/UDT field mappings during
Compile, including children of collections/vectors. Execution of those checked
plans reuses the bindings without recursive CheckType, Fields copies, name
lookup or nested plan construction per value/element. Value-dependent width,
state, text and framing validation still runs on every value. Standalone public
encode/decode calls validate their supplied descriptor on each call; arbitrary
custom codec work is accounted for separately.

RowPlan.DecodeInto is required for built-in and custom row plans. It fills a
non-nil caller-supplied destination using the checked metadata without retaining
the destination in the shared plan. It replaces the previous value, including
zeroing unmapped Go fields; custom plans must honor the same rule. Decode is
the value-returning convenience over the same decoding implementation. Built-in
Row field plans decode directly into destination fields: implementing DecodeInto
as Decode followed by assignment of an entire temporary R is not sufficient.
Compositions propagate destination fields to child plans where their semantics
permit; ConvertRows still invokes its explicitly supplied value-returning mapper,
whose work is accounted for separately. On an error, low-level DecodeInto may
have partially written the destination; the public iterator clears it before
returning false. Nil returns ValidationError. Plans must never retain reader or
destination pointers or capture per-call mutable state.

RowDecoder.Clone is required independently of Compile: it recursively detaches
the logical Go value using its declared field codecs, without needing actual
server metadata, a prior successful plan, or retained decode state. This makes
TupleDecoder/UDTDecoder.Clone correct after standalone DecodeValue as well as
normal row execution. RowPlan.Clone remains available for an already compiled
plan and must provide the same ownership guarantee. Row, OneColumn, PairRows,
ConvertRows, CASRows, dynamic.Rows and every custom RowDecoder implement the
schema-independent method; custom decoders must supply explicit clone logic.
Caching the first successful plan to implement Clone or panicking because no
plan was previously compiled is not permitted.

```go
// package cql
type ValueWriter struct { /* current checked type, framed sink */ }
func (w *ValueWriter) Type() DataType
func (w *ValueWriter) Null() error
func (w *ValueWriter) Unset() error
func (w *ValueWriter) Empty() error
func (w *ValueWriter) BorrowBytes([]byte) error
func (w *ValueWriter) BorrowString(string) error
func (w *ValueWriter) Uint8(uint8) error
func (w *ValueWriter) Uint16(uint16) error
func (w *ValueWriter) Uint32(uint32) error
func (w *ValueWriter) Uint64(uint64) error
func (w *ValueWriter) Child(DataType, func(*ValueWriter) error) error

type ValueReader struct { /* checked type, cell state, segmented bytes, lease */ }
func (r ValueReader) Type() DataType
func (r ValueReader) State() CellState
func (r ValueReader) Bytes() Bytes
func (r ValueReader) Cursor() *ValueCursor

type ValueCursor struct { /* checked segmented cursor */ }
func (c *ValueCursor) Remaining() int64
func (c *ValueCursor) Uint8() (uint8, error)
func (c *ValueCursor) Uint16() (uint16, error)
func (c *ValueCursor) Uint32() (uint32, error)
func (c *ValueCursor) Uint64() (uint64, error)
func (c *ValueCursor) Take(int64) (Bytes, error)
func (c *ValueCursor) Child(DataType) (ValueReader, error)
func (c *ValueCursor) RequireEnd() error

type ParamWriter struct { /* exact metadata count, framing transaction */ }
func (w *ParamWriter) WriteParam[T any](encoder Encoder[T], value T) error
type RowReader struct { /* current physical row, checked cursor */ }
func (r *RowReader) ReadColumn[T any](decoder Decoder[T]) (T, error)
func (r *RowReader) SkipColumn() error
```

Native cells have three framing states: present, null (-1), and unset (-2).
EmptyCell is a logical supported-type state over present zero-length wire
bytes, never a fourth negative sentinel. Empty() checks type support, writes no
body, and cannot coexist with payload/Null/Unset writes. Unknown CellState
values fail validation. Null/Unset cannot follow or precede payload writes in a
cell. A value encoder writes the body; the enclosing writer owns the length.
Child writes a length-framed nested cell; raw scalar methods write network byte
order. Decode readers are value snapshots; independent cursors do not advance
other readers. Invalid custom framing or a failed encoder rolls back the entire
cell and causes binding to fail before network submission. A failed complete
parameter encoding never sends a request.

## Standalone serialization

Binary fixtures and applications can use the same checked generic codecs without
opening a session. EncodedValue holds an immutable descriptor, state and
segmented body. Its constructor validates state/body consistency, not scalar
semantics; DecodeValue validates type and payload. NewEncodedValue requires zero
body for Null/Unset/Empty, validates descriptors and rejects unknown states;
Empty also requires an EmptyCodec-supported type. Its logical Empty state is
normalized to Present zero bytes for ValueReader, so the selected decoder
determines meaning. Wire readers likewise report Present for length zero;
text/blob zero bytes always remain ordinary present values. EmptyCodec
interprets supported zero-byte bodies as Empty, while ordinary fixed-width
codecs reject invalid zero widths. Dynamic Value exposes logical Empty only for
its explicitly supported empty kinds. Outbound unset is permitted only in this
standalone representation or a top-level binding, never in a result decode.

```go
// package cql
type EncodedValue struct { /* private type, state and Bytes */ }
func NewEncodedValue(DataType, CellState, Bytes) (EncodedValue, error)
func (v EncodedValue) Type() DataType
func (v EncodedValue) State() CellState
func (v EncodedValue) Bytes() Bytes
func (v EncodedValue) Clone() EncodedValue
func EncodeValueBorrowed[T any](DataType, Encoder[T], T) (EncodedValue, error)
func EncodeValue[T any](DataType, Encoder[T], T) (EncodedValue, error) // owned
func DecodeValue[T any](Decoder[T], EncodedValue) (T, error)
```

Borrowed encoded values require caller payload lifetime and immutability while
any encoded value/decoded view is used; these synchronous helpers start no
background work. They do not retain source page leases: the caller must retain
those leases separately. Original received views detect a released owner, but a
derived standalone encoded span cannot detect that its externally owned source
was released. Use EncodeValue or Clone to detach. EncodeValue clones
caller-backed payload once, sharing already-owned scalar encoding. Clone
detaches bytes explicitly. These functions call the exact
ValueWriter/ValueReader used in request binding and result decoding; no second
serialization implementation is permitted.

## Typed parameter and result composition

Applications compose typed accessors (`Params`, `Row`, `RowInto`), generate
them with cmd/cqlgen, or map a struct with `ParamsOf`/`RowOf`, which reflect
the struct once at construction; encoding and decoding never reflect. Named
parameters are
mapped to server order once. Named result plans likewise resolve names once and
decode in physical wire order, assigning each value through its compiled field
accessor. Fragmented cells remain immutable range views; sequential traversal
does not rescan earlier spans or allocate descriptor arrays per cell. Duplicate
marker names may reuse one input field;
unused or missing source names fail with deterministic diagnostics. Result
aliases are the column names. Duplicate selected names require positional
mapping. All selected columns must be accounted for, including explicit ignored
fields.

```go
// package cql
type Statement[P, R any] struct {
    CQL string
    Params ParamEncoder[P]   // nil when P is EmptyParams
    Rows RowDecoder[R]       // nil when R is EmptyRow
    BindMarkers Columns      // parameter metadata for unprepared execution
    Kind StatementKind
}
type StatementKind uint8
const (UnknownStatement StatementKind = iota; OrdinaryStatement; ConditionalStatement)
func Command(text string) Statement[EmptyParams, EmptyRow]

type EmptyParams = EmptyRecord   // EmptyRecord is struct{}
type EmptyRow = EmptyRecord
type Pair[A, B any] struct { First A; Second B }
type InputField[P any] interface { /* sealed descriptor */ }
type OutputField[R any] interface { /* sealed descriptor */ }
func Param[P, V any](name string, codec Encoder[V], get func(P) V) InputField[P]
func ParamAt[P, V any](index int, codec Encoder[V], get func(P) V) InputField[P]
func Params[P any](fields ...InputField[P]) ParamEncoder[P]
func Column[R, V any](name string, codec Decoder[V], field func(*R) *V) OutputField[R]
func ColumnAt[R, V any](index int, codec Decoder[V], field func(*R) *V) OutputField[R]
func IgnoreColumn[R any](name string) OutputField[R]
func Row[R any](fields ...OutputField[R]) RowDecoder[R]
func RowInto[R any](reset func(*R), fields ...OutputField[R]) RowDecoder[R]
func OneParam[T any](Encoder[T]) ParamEncoder[T]
func OneColumn[T any](Decoder[T]) RowDecoder[T]
func PairParams[A, B any](a ParamEncoder[A], b ParamEncoder[B]) ParamEncoder[Pair[A, B]]
func PairRows[A, B any](a RowDecoder[A], b RowDecoder[B]) RowDecoder[Pair[A, B]]
func ConvertParams[A, B any](ParamEncoder[B], func(A) B) ParamEncoder[A]
func ConvertRows[A, B any](RowDecoder[A], func(A) B, func(B) B) RowDecoder[B]
var EmptyParamsEncoder ParamEncoder[EmptyParams]
var EmptyRowDecoder RowDecoder[EmptyRow]

// Struct mapping.
type StructFields[P any] struct { Params []InputField[P]; Columns []OutputField[P] }
type StructOption[P any] /* opaque */
func ParamsOf[P any](...StructOption[P]) (ParamEncoder[P], error)
func MustParamsOf[P any](...StructOption[P]) ParamEncoder[P]
func RowOf[R any](...StructOption[R]) (RowDecoder[R], error)
func MustRowOf[R any](...StructOption[R]) RowDecoder[R]
func WithCodec[P, V any](Codec[V]) StructOption[P]
func ReuseBuffers[P any]() StructOption[P]
```

`RowInto` calls its reset callback before decoding each row; the callback
clears application state and may keep owned slices for reuse. `ParamsOf` and
`RowOf` map exported fields by column name: the `cql:"name"` tag or the
lowercased field name, `cql:"-"` to skip, `cql:"name,codec=..."` to pick a
built-in codec. `ParamsOf` skips fields without a matching marker but requires
a field for every marker; `RowOf` leaves fields of unselected columns zero.

`Column`/`ColumnAt` use a typed field-address accessor so decoding and recursive
cloning both reach the same field without reflection. Accessors must return a
non-nil address belonging to the supplied row; they must not capture an external
mutable destination. `PairParams`/`PairRows` split Columns at the left side's fixed arity
(OneParam/OneColumn: one; Params/Row/RowInto of only ParamAt/ColumnAt: the
highest index plus one; a nested pair: the sum of its sides), computed at
Compile, and concatenate positional plans. Compile rejects any other left side
with "left side has no fixed arity"; named composition
uses `Params`/`Row` with accessors. `ConvertRows`' final function is its
ownership-detaching clone; omitting correct clone semantics is a custom codec
contract violation. `EmptyRowDecoder` accepts only results with zero physical columns;
use a real decoder for conditional writes. Ignored result fields still have
validated framing.

## Scalar values, null, unset and empty

```go
// package cql
type CellState uint8
const (Present CellState = iota; Null; Unset; EmptyCell)
type Option[T any] struct { Value T; Set bool }
func Some[T any](T) Option[T]
type Nullable[T any] struct { Value T; Valid bool }
type BindValue[T any] struct { Value T; State CellState }
func NullableCodec[T any](Codec[T]) Codec[Nullable[T]]
func NullableEncoder[T any](Encoder[T]) Encoder[Nullable[T]]
func NullableDecoder[T any](Decoder[T]) Decoder[Nullable[T]]
func BindEncoder[T any](Encoder[T]) Encoder[BindValue[T]]
type EmptyValueOf[T any] struct { Value T; Empty bool }
func EmptyCodec[T any](Codec[T]) Codec[EmptyValueOf[T]]

type UUID [16]byte
type Duration struct { Months, Days int32; Nanoseconds int64 }
type Counter int64
type Date int32                  // days relative to Unix epoch
type TimeOfDay int64             // nanoseconds since midnight
type Timestamp int64             // milliseconds relative to Unix epoch
type TimeUUID UUID
type Varint struct { /* canonical/sign-preserving segmented two's complement */ }
type Decimal struct { Scale int32; Unscaled Varint }

var BoolCodec Codec[bool]
var Int8Codec Codec[int8]
var Int16Codec Codec[int16]
var Int32Codec Codec[int32]
var Int64Codec Codec[int64]
var Float32Codec Codec[float32]
var Float64Codec Codec[float64]
var CounterCodec Codec[Counter]
var DateCodec Codec[Date]
var TimeCodec Codec[TimeOfDay]
var TimestampCodec Codec[Timestamp]
var DurationCodec Codec[Duration]
var UUIDCodec Codec[UUID]
var TimeUUIDCodec Codec[TimeUUID]
var InetCodec Codec[netip.Addr]
var VarintCodec Codec[Varint]
var DecimalCodec Codec[Decimal]
var StringCodec Codec[string]                 // text/varchar; decodes an owned copy
var ASCIIStringCodec Codec[string]
var ByteSliceCodec Codec[[]byte]               // blob; borrows input, decodes an owned copy
var TextCodec Codec[Text]                     // text/varchar borrowed view
var ASCIICodec Codec[Text]
var BlobCodec Codec[Bytes]
func NamedScalar[A, B any](Codec[B], func(A) B, func(B) A) Codec[A]
func VarintFromBigInt(*big.Int) Varint
func (v Varint) BigInt() *big.Int
func VarintFromBytes([]byte) (Varint, error)
func DateFromTime(time.Time) (Date, error)
func (d Date) Time() (time.Time, error)
func TimestampFromTime(time.Time) (Timestamp, error)
func (t Timestamp) Time() (time.Time, error)
func ParseUUID(string) (UUID, error)
func MustParseUUID(string) UUID
func UUIDFromBytes([]byte) (UUID, error)
func RandomUUID() UUID
func NewTimeUUID() TimeUUID
func TimeUUIDFromTime(time.Time) TimeUUID
func TimeUUIDWith(t time.Time, clock uint16, node [6]byte) TimeUUID
func MinTimeUUID(time.Time) TimeUUID
func MaxTimeUUID(time.Time) TimeUUID
func ParseTimeUUID(string) (TimeUUID, error)
func MustParseTimeUUID(string) TimeUUID
func TimeUUIDFromUUID(UUID) (TimeUUID, error)
```

`TimeUUID` is distinct from `UUID`. `NewTimeUUID`, `TimeUUIDFromTime`,
`MinTimeUUID`, `MaxTimeUUID` and `TimeUUIDWith` return `TimeUUID`;
`RandomUUID()` returns a version 4 `UUID` and no error, since crypto/rand does
not fail. TimeUUID provides canonical string/JSON/text and
UUID timestamp/time/byte helpers. ParseTimeUUID and TimeUUIDFromUUID validate
version 1; codecs and checked unmarshaling do likewise. The zero value is not
an encodable timeuuid; Nullable represents NULL.
VectorType has Kind TypeCustom plus Dimension; ScalarType(TypeCustom)
rejects an incomplete custom descriptor.

Nil `[]byte` passed to ByteSliceCodec is present empty blob. Null is always
explicit; nil collections likewise encode a zero count. Nonnullable scalar and vector decoders
reject null. List/set/map decoders, buffered and streaming, interpret NULL as
empty while preserving destination capacity; Nullable preserves null independently
of empty. Unset is only an outer bound parameter on v4+; reject unset in results
and nested values. Empty is distinct from zero-byte text/blob. EmptyCodec
rejects ASCII/text/blob, whose zero-length payload is Present. It accepts
boolean, integers/floats, date, time, timestamp, UUID/timeuuid, inet, varint,
decimal and tuple, but never counter, duration, collection, UDT or vector. The
outbound type remains checked even when null/unset skips payload encoding.

Integers use exact widths. Fixed values reject wrong lengths. ASCII/UTF-8 are
validated in both directions, including across chunks. Boolean accepts 0 and 1
only. TimeOfDay must be within the day; TimeUUID must be version 1; duration
components have compatible signs and int32/int32/int64 ranges. Checked signed
vints consume the entire duration. Varint retains full signed precision; decimal
preserves the signed scale and the exact unscaled integer. Explicit big.Int,
calendar and owned-string conversions are allocation/representation operations,
not implicit work in typed execution. No numeric coercion silently narrows
values.

Public type descriptions use DataType and ColumnSpec only; protocol metadata
descriptors are internal.
ParseConsistency and ParseUUID return errors; MustParseConsistency and
MustParseUUID panic on invalid input. EncodeValue owns its output payloads;
EncodeValueBorrowed preserves borrowed inputs.

The wrapper roles are separate: Option is for inherited settings; Nullable is
for nullable application data and decoded results; BindValue controls binding
with Present, Null or Unset; EmptyValueOf represents supported empty CQL values.
Their zero values mean inherited, NULL, present zero value and ordinary zero
value, respectively. NullableEncoder remains useful for nullable struct fields;
BindEncoder is preferred when a parameter also needs UNSET. BindValue does not
accept EmptyCell; wrap its underlying encoder with EmptyCodec when needed.

## Borrowed bytes, text and container values

```go
// package cql
type Bytes struct { /* chain range + stable page-owner generation */ }
type Text struct { /* validated UTF-8/ASCII Bytes */ }
func BorrowBytes([]byte) Bytes
func BorrowText(string) (Text, error)
func (b Bytes) Len() int64
func (b Bytes) At(int64) (byte, error)
func (b Bytes) Slice(offset, length int64) (Bytes, error)
func (b Bytes) WriteTo(io.Writer) (int64, error)
func (b Bytes) CopyTo([]byte) (int, error)
func (b Bytes) Clone() Bytes          // panics with cqlerr.ErrReleased on an expired view
func (b Bytes) OwnedBytes() []byte    // panics likewise
func (b Bytes) Copy() ([]byte, error) // checked form: returns cqlerr.ErrReleased
func (t Text) Bytes() Bytes
func (t Text) Clone() Text
func (t Text) String() string
func (t Text) Copy() (string, error)

type Entry[K, V any] struct { Key K; Value V }
func ListCodec[T any](Codec[T]) Codec[[]T]
func SetCodec[T any](Codec[T]) Codec[[]T]
func MapCodec[K, V any](Codec[K], Codec[V]) Codec[[]Entry[K, V]]
func GoMapCodec[K comparable, V any](Codec[K], Codec[V]) Codec[map[K]V]
func VectorCodec[T any](Codec[T]) Codec[[]T]
func ListEncoder[T any](Encoder[T]) Encoder[[]T]
func SetEncoder[T any](Encoder[T]) Encoder[[]T]
func MapEncoder[K, V any](Encoder[K], Encoder[V]) Encoder[[]Entry[K, V]]
func ListDecoder[T any](Decoder[T]) Decoder[[]T]
func SetDecoder[T any](Decoder[T]) Decoder[[]T]
func MapDecoder[K, V any](Decoder[K], Decoder[V]) Decoder[[]Entry[K, V]]
func VectorEncoder[T any](Encoder[T]) Encoder[[]T]
func VectorDecoder[T any](Decoder[T]) Decoder[[]T]
func TupleEncoder[T any](ParamEncoder[T]) Encoder[T]
func TupleDecoder[T any](RowDecoder[T]) Decoder[T]
type UDTOptions struct {
    Keyspace, Name string // required expected UDT identity, exact metadata names
    AllowExtraDatabaseFields, AllowMissingFields bool
}
func UDTOptionsFor(DataType) (UDTOptions, error) // identity from a UDT DataType
func UDTEncoder[T any](ParamEncoder[T], UDTOptions) Encoder[T]
func UDTDecoder[T any](RowDecoder[T], UDTOptions) Decoder[T]
```

GoMapCodec decodes into a Go map: a repeated key fails decoding, NULL is an
empty map, and DecodeInto clears and reuses the destination map. Encoding
follows Go's map iteration order. Struct mappings use it for map[K]V fields
whose key and value types have owning built-in codecs; MapCodec keeps
[]Entry[K, V] for non-comparable keys, server order and streaming slots.

BorrowText validates UTF-8 and returns ValidationError for invalid input.
dynamic.Text likewise returns an error for invalid UTF-8. StringCodec and
ASCIIStringCodec return ordinary codec errors for invalid encoded input. Malformed
input is never a reason for a constructor panic.

Buffered Bytes and Text retain fragments; a cell is not flattened when it
crosses a v5 segment. Typed list, set, map and vector decoders eagerly decode
all elements into slices. Codec and decoder-only forms return the same Go
container types; stream forms also use slices and the shared Entry type.
There are no typed lazy collection views. List/set/map NULL cells decode as
empty collections; Nullable preserves NULL when required. Vectors require the
declared dimension and reject NULL without a Nullable wrapper.

Eager materialization does not detach borrowed elements. Decoder.Clone
recursively copies collection storage and borrowed Text, Bytes, Value and
nested aggregates so they survive page release. String(), OwnedBytes(), Clone()
and explicit big.Int/calendar adapters are requested materializations.
Collection framing and all selected child decoders run before Decode succeeds;
a malformed element returns an error and no partial slice. DecodeInto may reuse
caller storage, whose contents must be discarded after an error. Fixed-width
vectors check the compiled aggregate width before decoding elements; variable
vectors use checked unsigned-vint lengths.

Dynamic Value retains explicit cursor traversal through Elements, Entries and
Fields. dynamic.Iterator stops permanently at the first error, enforces framing
and counts, and does not publish the failed value. Its Collect method allocates
an output slice and recursively detaches yielded values; errors return no
partial slice. Dynamic traversal does not imply typed lazy collection decoders.

Tuples preserve physical columns: they never expand implicitly into destination
arguments. Missing serialized trailing tuple/UDT members are null only where the
protocol permits; a nonnullable destination rejects them. UDT matching defaults
to exact field names and full identity/type validation. UDTOptions.Keyspace/Name
must supply the expected identity; missing names fail CheckType/Compile with a
validation error, and equal layouts under a different identity are incompatible.
Explicit options permit extra database fields (encode explicit null; decode
checked skips) or application fields absent from the database (encode ignores
those fields; decode sets explicit zero defaults only when allowed).
Null-default behavior requires Nullable or a custom codec. Unknown application
fields are errors unless AllowMissingFields is explicitly set. All actual
framing is consumed even for ignored suffixes. For a present tuple with an empty
body, ordinary TupleDecoder applies the omitted nullable-suffix rule. Explicit
EmptyCodec(TupleDecoder/Encoder composition) instead intercepts the zero-byte
body as Empty=true before the tuple codec. These are codec-selected
interpretations of identical wire bytes, not distinguishable native states.
TupleEncoder emits a frame for every declared field, including explicit null
suffix frames, so an ordinary nonzero-arity tuple encoded through EmptyCodec
with Empty=false cannot accidentally become zero bytes. Zero-arity tuples have
no distinct ordinary/Empty wire representation and an Empty wrapper
canonicalizes their zero bytes to Empty=true. Dynamic Value's tuple-empty
variant uses that explicit empty interpretation. Tuple Empty support is
preserved.

Vectors validate dimension and checked aggregate width; zero dimension is
rejected. They use the server's fixed/variable vector layouts, including nested
fixed vectors; no short vectors, implicit null padding, or surplus elements are
accepted.

## Metadata-driven values

Dynamic behavior uses a tagged representation, never reflection-created Go
values or `map[string]any`. Value's private representation contains the actual
DataType, state, scalar payload or a lazy encoded aggregate. Constructors store
borrowed input payloads; normal binding lifetime rules apply. Decoded Value and
dynamic.Row borrow the current page owner without acquiring an independent lease
reference. Their views expire under the same NextInto/Close rule as typed rows; only
an explicit PageLease or Clone extends their usable lifetime. An aggregate may
hold explicitly supplied `[]Value` descriptors on input or encoded bytes on
output, but neither form causes eager payload flattening.

```go
// package dynamic
type Value struct { /* explicit discriminant; no any field */ }
type Field struct { Name string; Value Value }
type Entry struct { Key, Value Value }
func (v Value) Type() DataType
func (v Value) State() CellState
func (v Value) Clone() Value
func (v Value) Bool() (bool, error)
func (v Value) Signed() (int64, error)
func (v Value) Float() (float64, error)
func (v Value) Text() (Text, error)
func (v Value) Bytes() (Bytes, error)
func (v Value) UUID() (UUID, error)
func (v Value) Inet() (netip.Addr, error)
func (v Value) Duration() (Duration, error)
func (v Value) Varint() (Varint, error)
func (v Value) Decimal() (Decimal, error)
func (v Value) Elements() (*Iterator[Value], error)
func (v Value) Entries() (*Iterator[Entry], error)
func (v Value) Fields() (*Iterator[Field], error)
type Iterator[T any] struct { /* bounded cursor for Value traversal */ }
func (i *Iterator[T]) Next() bool
func (i *Iterator[T]) Value() T
func (i *Iterator[T]) Err() error
func (i *Iterator[T]) Collect() ([]T, error)
func Null(DataType) Value
func Unset(DataType) Value
func Empty(DataType) (Value, error)
func Bool(bool) Value
func TinyInt(int8) Value
func SmallInt(int16) Value
func Int(int32) Value
func BigInt(int64) Value
func Counter(Counter) Value
func Float(float32) Value
func Double(float64) Value
func Text(string) (Value, error)
func ASCII(string) (Value, error)
func Blob([]byte) Value
func UUID(UUID) Value
func TimeUUID(TimeUUID) (Value, error)
func Inet(netip.Addr) (Value, error)
func Date(Date) Value
func Time(TimeOfDay) (Value, error)
func Timestamp(Timestamp) Value
func Duration(Duration) (Value, error)
func Varint(Varint) Value
func Decimal(Decimal) Value
func List(DataType, []Value) (Value, error)
func Set(DataType, []Value) (Value, error)
func Map(DataType, []Entry) (Value, error)
func Tuple(DataType, []Value) (Value, error)
func UDT(DataType, []Field) (Value, error)
func Vector(DataType, []Value) (Value, error)
var Codec cql.Codec[Value]
var Params cql.ParamEncoder[[]Value]
var NamedParams cql.ParamEncoder[[]Field]
var Rows cql.RowDecoder[Row]
type Row struct { /* ordered Columns and lazy Values */ }
func (r Row) Columns() Columns
func (r Row) At(int) (Value, error)
func (r Row) Get(string) (Value, error)
func (r Row) Clone() Row
```

Named dynamic binding uses `cql.Params[[]dynamic.Field]` accessors or
`dynamic.NamedParams`, which maps unique input names to server marker order;
repeated server markers reuse one input field, and duplicate or unused input
names are rejected.
dynamic.Rows compiles column name lookup once per result shape. At/Get use a
single lazily built physical-cell offset index per current row for repeated
random access, without eagerly decoding values; there is no whole-row rescan
for each access. Its descriptors remain owned by that row/page and cannot alias
scratch reused by later rows. Dynamic aggregate variants must match the actual
outer type, including list versus set versus vector, and a UDT's identity.
Dynamic malformed scalars are
rejected with the same rules as typed codecs; no implicit universal Empty
fallback.

## Package-level functions of gocql

Package-level functions exist only where a method cannot express them. Go 1.27
methods may declare their own type parameters, so the generic operations are
methods on the type that owns them (`Session.Prepare`, `Session.PrepareStream`,
`Session.RunBatch`, `Session.Unprepared*`, `Prepared.Query`,
`PreparedStream.Query`). Settings that only refine a builder are methods of
the builder, not values made by a function: pool size and a fixed TLS
configuration are `ConfigBuilder` methods. Client routes are one read-only
`config.ClientRoutes`, built with `config.NewClientRoutes().WithRoute(id,
hostname)...Build()` and passed to `WithClientRoutes`, so a hostname override
belongs to its route and one call replaces the whole setting. Routed
connections dial the route's `tls_port` exactly when TLS is configured.

The root package has one function, `Connect[D Dialect](context.Context,
config.Config) (*Session[D], error)`, which opens a session of dialect D. It
cannot be a method of `config.Config`: package `config` cannot import the
`Session` it would return. Every other constructor lives in the package that
owns its type:

| Function | Purpose |
|---|---|
| `config.New(contactPoints ...string) *config.ConfigBuilder` | starts a configuration |
| `config.NewProfile() *config.ProfileBuilder` | starts an execution profile |
| `config.NewClientRoutes() *config.ClientRoutesBuilder` | starts a client routes setting |
| `config.NewTLSFiles(*tls.Config, config.TLSFiles) (*config.TLSFileSource, error)` | a TLS configuration reloaded from files |
| `cql.PageStateFromToken([]byte) cql.PageState` | a continuation from a saved token; `PageState` fields are private |

Package `cql` also exports `var StartPage PageState` (the zero `PageState`);
the root package exports `var StopIteration` (see below).

## Statements, preparation, queries and execution

A session is generic over its dialect, the database and native protocol
version it speaks. The caller chooses it; the driver negotiates no protocol
version. Connect fails with `cqlerr.ServerMismatchError` when the cluster does
not speak that version or is not that database. API only one dialect supports
takes a session of that dialect (package `scylla` takes
`*Session[ScyllaV4]`). Prepared statements, queries and batches do not carry
the dialect. See docs/source/server-differences.md.

```go
type Dialect interface { /* sealed */ }
type ScyllaV4 struct{}    // ScyllaDB, native protocol 4
type CassandraV4 struct{} // Apache Cassandra, native protocol 4
type CassandraV5 struct{} // Apache Cassandra 4.0+, native protocol 5
type Session[D Dialect] struct { /* opaque */ }
func Connect[D Dialect](context.Context, config.Config) (*Session[D], error)
func (s *Session[D]) ID() string
```

Prepared statements and compiled plans are immutable and shareable. Each
worker creates a reusable query; it permits one active execution and owns all
captured parameters through retries, pages, accepted writes and callbacks.

```go
// package exec
type PrepareOptions struct {
    Keyspace string
    HostID cql.Option[cql.UUID]
    Trace bool
}

// package gocql
func (s *Session[D]) Prepare[P, R any](context.Context, cql.Statement[P, R], ...exec.PrepareOptions) (*Prepared[P, R], error) // at most one
func (p *Prepared[P, R]) Parameters() cql.Columns
func (p *Prepared[P, R]) Columns() cql.Columns
func (p *Prepared[P, R]) Info() PreparedInfo
type PreparedInfo struct {
    Keyspace, Table string
    PartitionKeyIndexes []int
    Kind cql.StatementKind
    Conditional bool
    Generation uint64
}

// One-shot calls, safe for concurrent use; at most one exec.CompiledOptions.
func (p *Prepared[P, R]) Exec(context.Context, P, ...exec.CompiledOptions) (Result, error)
func (p *Prepared[P, R]) One(context.Context, P, *R, ...exec.CompiledOptions) (bool, Result, error)
func (p *Prepared[P, R]) Each(context.Context, P, *R, func(*R) error, ...exec.CompiledOptions) (Result, error)
func (p *Prepared[P, R]) All(context.Context, P, *R, ...exec.CompiledOptions) iter.Seq2[*R, error]
func (p *Prepared[P, R]) Run(context.Context, P, ...exec.CompiledOptions) (Rows[R], error)                // holds a pooled query until closed
func (p *Prepared[P, R]) RunPage(context.Context, P, cql.PageState, int32, ...exec.CompiledOptions) (*Page[R], error)
func (p *Prepared[P, R]) AddToBatch(*Batch, P) error

// Worker-owned reusable queries.
func (p *Prepared[P, R]) Query(exec.Options) (*Query[P, R], error)
func (s *Session[D]) UnpreparedExec[P any](context.Context, cql.Statement[P, cql.EmptyRow], P, ...exec.Options) (Result, error)
func (s *Session[D]) UnpreparedOne[P, R any](context.Context, cql.Statement[P, R], P, *R, ...exec.Options) (bool, Result, error)
func (s *Session[D]) UnpreparedEach[P, R any](context.Context, cql.Statement[P, R], P, *R, func(*R) error, ...exec.Options) (Result, error)
func (s *Session[D]) UnpreparedAll[P, R any](context.Context, cql.Statement[P, R], P, *R, ...exec.Options) iter.Seq2[*R, error]
func (s *Session[D]) UnpreparedAddToBatch[P, R any](*Batch, cql.Statement[P, R], P) error
func (q *Query[P, R]) Exec(context.Context, P) (Result, error)
func (q *Query[P, R]) Each(context.Context, P, *R, func(*R) error) (Result, error)
func (q *Query[P, R]) One(context.Context, P, *R) (bool, Result, error)
func (q *Query[P, R]) All(context.Context, P, *R) iter.Seq2[*R, error]
func (q *Query[P, R]) Run(context.Context, P) (Rows[R], error)
func (q *Query[P, R]) RunPage(context.Context, P, cql.PageState, int32) (*Page[R], error) // page size: 0 resolved, cql.Unpaged
func (q *Query[P, R]) Routing(P) (routing.Info, error)
func (q *Query[P, R]) AddToBatch(*Batch, P) error
func (q *Query[P, R]) Close()                       // stop at once
func (q *Query[P, R]) Wait(context.Context) error   // join the drain

func (s *Session[D]) ExecBatch(context.Context, *Batch, ...exec.CompiledOptions) (Result, error)          // sends batch.Kind
func (s *Session[D]) RunBatch[R any](context.Context, *Batch, cql.RowDecoder[R], ...exec.CompiledOptions) (Rows[R], error)
```

Prepared.Exec, One, Each and All run on queries the prepared statement keeps in
a lock-free pool, so they allocate no per-call execution state. Prepared.Run and
RunPage return a handle that holds a pooled query until it is closed: the
session registers that query while the handle is open, so Session.Close stops
the handle, and a handle dropped without Close releases its query once the
garbage collector finds it. A worker-owned Query skips the pool hand-off (about
2 to 4 percent of client CPU at 64 workers on the loopback benchmark) and
resolves its exec.Options once; it keeps the same calls for workers that run one
statement repeatedly.

All is the range-over-func form of Each: it yields dst for every row across
all pages, valid until the next iteration, then any error once as (nil, err);
breaking out of the loop releases the execution. It returns no Result. RunPage
takes the page size as an argument rather than an option, so a paginated
endpoint can take it from each request without compiling options: zero uses
the resolved page size and cql.Unpaged requests the whole result. Both forms report results the same way. One detaches borrowed fields before
releasing the response, preserving already-owned built-in destination buffers.
Each lends borrowed fields only during its callback. Custom row/value decoders
fall back to their recursive Clone contract when One detaches results.
Session.UnpreparedExec, UnpreparedOne, UnpreparedEach and UnpreparedAll provide occasional typed
CQL text execution without a caller-owned query, and UnpreparedAddToBatch
appends an unprepared statement to a batch. They take at most one plain
exec.Options and validate it during the call. Their cleanup stops admission
on every return and keeps any remaining drain registered with the session.

Input is copied during execution admission,
including custom codec borrowed spans. Encoding records cell boundaries and
computes routing from the same owned bytes; encoders are not called again for
routing, retries, reprepare or paging. The Unprepared calls take parameter
Columns from Statement.BindMarkers. Query options are captured at construction, including owned
custom payload bytes; the selected profile is loaded per execution. Profile
replacement never changes an active execution's retry or continuation options.

A call while a Rows, Page or RowStream of the query is open, or while another
goroutine's call runs, returns cqlerr.ErrBusy. Cancellation may return while accepted
transport writes or callbacks still drain; the query's next execution waits for
that drain within its own context instead of returning ErrBusy. Routing and
AddToBatch use storage of their own and work during the drain. Close() stops
the query, cancels a running call (which returns an error matching
context.Canceled), stops an open handle with cqlerr.ErrClosed, and returns at once; Wait(ctx) joins the drain, returning ctx.Err() when ctx ends
first, and Session.Close joins the rest. The session refers to an open query
weakly: a query dropped without Close is closed by a garbage-collector cleanup,
and a session logging at Debug logs where it was created. Cursor handles capture their execution
generation; stale handles cannot advance, close or modify another execution.
Rows is a value handle over iterator storage that a reusable query keeps and
reuses for its next execution. A stale Rows reports no rows, Err returns
cqlerr.ErrStaleRows and Close does nothing; the zero Rows has no rows. Rows is not
safe for concurrent use.
Close is idempotent. Close a cursor on early termination before reusing its
query. Session
shutdown stops all queries before joining their transport/callback drains.

Prepare performs schema-scoped dependency validation and keeps per-host IDs,
LRU eviction independence, codec validation, conditional classification and
routing contracts. Statement.Kind is a caller classification; server-confirmed
conditional metadata takes precedence. Unknown statements disable speculation.
SERIAL/LOCAL_SERIAL execution is conditional. Routing uses captured parameters
and the currently selected metadata snapshot, preserving token distribution,
strict shard targeting and tablet feedback.

On reprepare, validate bind metadata against the encoded schema, not merely
marker count. Incompatible type/order/identity yields `cqlerr.SchemaChangedError`
before resend; the application explicitly prepares and binds again. Routing
schema compatibility includes partition-key index order, effective table
identity, selected partitioner and conditional classification, in addition to
parameter types/order/identity. A change to any of these requires a new explicit
prepare/query and fails the old query execution with SchemaChangedError before resend.
Compatible metadata may update per-host prepared IDs without re-encoding or
mutating the query's canonical routing inputs. Actual result metadata always
selects a compatible RowPlan before rows are exposed, including empty pages.
Reuse the immutable plan when the exact validated result entry is unchanged;
compile only on first use or a changed result shape. A new entry with the same
shape may reuse a plan after structural equality is established once. Plan reuse
is scoped to this handle's RowDecoder, never just CQL or a type fingerprint;
different application decoders cannot share an unchecked plan. Changed or
NO_METADATA responses still pass the generation/ID fences below. Pages retain
their selected plan independently of later cache replacement.
Cached NO_METADATA requires a matching generation, column count and negotiated
ID; otherwise obtain valid metadata or return a protocol error. Atomic
generation/CAS updates prevent a stale response from overwriting newer local
publications. Each attempt captures an immutable result entry and its local
revision plus schema epoch. Publication compares that exact entry/revision and
epoch; on CAS failure it never retries the stale publication against a freshly
loaded entry. A losing response still decodes with its matching retained or
response-supplied metadata. Prepared IDs and result metadata IDs are opaque
equality tokens, not ordered server schema versions. These fences prevent local
stale-update races; conflicting remote observations require a typed metadata
refresh and cannot be represented as proven global schema chronology. Conflict
reconciliation is bounded: it coalesces at most 64 distinct pending keyspaces and queries a known statement's
relevant keyspace through the typed schema path. Required internal validation
can run when optional schema metadata is disabled or filtered, but publication
continues to honor that mode/filter; it does not expose unrelated schema.
Only an unknown statement scope requires full internal enumeration under the
existing metadata limits. Queue exhaustion returns a ProtocolError with stage
"prepared schema reconciliation" through the logical operation. Earlier delivered
matching pages remain valid. Unscheduled/failed conflicts stay dirty, so later
calls can retry and scheduled failures are retried without broadening scope.
No unsafe cache-metadata opt-in bypasses these rules.

## Rows, pages, leases and conditional results

```go
type Rows[R any] struct { /* value handle: iterator storage + generation */ }
func (r Rows[R]) NextInto(dst *R) bool
func (r Rows[R]) Err() error
func (r Rows[R]) Close() error
func (r Rows[R]) Retain() (*PageLease, error)
func (r Rows[R]) Clone() (R, error)
func (r Rows[R]) Info() Result
func (r Rows[R]) Columns() cql.Columns

type Page[R any] struct { /* exactly one response page */ }
func (p *Page[R]) NextInto(dst *R) bool
func (p *Page[R]) Err() error
func (p *Page[R]) Close() error
func (p *Page[R]) Retain() (*PageLease, error)
func (p *Page[R]) Clone() (R, error)
func (p *Page[R]) State() cql.PageState
func (p *Page[R]) Info() Result
func (p *Page[R]) Columns() cql.Columns

type PageLease struct { /* stable shared reference to receive storage */ }
func (l *PageLease) Close()

// package cql
type PageState struct { /* start / more / complete, opaque owned token */ }
var StartPage PageState // the zero PageState
func PageStateFromToken([]byte) PageState
func (p PageState) More() bool
func (p PageState) Complete() bool
func (p PageState) Token() []byte

// package cql
type CASResult[R any] struct { Applied bool; Previous Option[R] }
func CASRows[R any](RowDecoder[R]) RowDecoder[CASResult[R]]
```

NextInto is the only row iterator. Views it decodes into the destination
remain valid until the **next** NextInto call or Close, whichever comes first. Retain pins the
current physical page before advancing; its owner has stable identity independent
of pooled parsers.
All views from that page remain valid until that lease closes. Clone detaches
the row the last NextInto decoded, using RowPlan.Clone. Copying a Go row struct does not detach it.
Access through a released view reports cqlerr.ErrReleased where a method returns error;
methods without error results panic with cqlerr.ErrReleased rather than reading
recycled storage. Len/type metadata remain inspectable. Do not concurrently use
a view while closing its last lease. Retained leases are independent of
Session.Close.

### Caller-supplied result destinations

This section decodes from received page storage. Decoding into final caller
storage as rows arrive uses the separate streaming API described under
"Streaming receive into final caller storage"; this NextInto contract is the
buffered path. Applications define the row representation and may provide a
reusable instance for decoding:

```go
type UserView struct {
    ID int32
    Name cql.Text
    Payload cql.Bytes
}

var user UserView
for rows.NextInto(&user) {
    consume(user) // Text and Bytes reference the leased receive storage
}
if err := rows.Err(); err != nil {
    return err
}
```

NextInto advances exactly one logical row, applying framing, plan selection,
metadata validation and automatic (Rows) or explicit (Page) paging rules.
The compiled RowPlan.DecodeInto fills dst directly. The driver does not first
decode a complete temporary row, copy payloads, flatten split cells, or discover
fields through reflection. Text, Bytes and borrowed collection elements preserve
the original immutable receive/decompression backing and its page lease identity.
Fixed-width Go fields hold decoded values; compression/TLS transformations
retain their separately documented cost and ownership boundaries. Neither a Go
string nor an owned byte slice is implicitly materialized.

Every successful call replaces dst, including resetting fields not populated by
the declared decoder. On false, any non-nil dst is set to its zero value, whether
the cause is exhaustion, closure, cancellation, metadata/framing failure or a
codec error. Partially decoded rows are never published as successful. The first
error is terminal and available through Err; exhaustion alone is not an error.
On an active iterator, nil dst produces terminal ValidationError before consuming
a row or performing paging I/O. A terminal iterator stays terminal and preserves
its existing error. A failed call does not promise rollback of cursor progress.

Every NextInto call advances. After successful NextInto, Clone detaches the
current contents of *dst with the selected RowPlan.
The iterator may retain that destination pointer only until the next advance or
Close; no shared plan, prefetch worker or other background task may retain or
write it. Caller changes to the current destination are reflected by Clone.
Advancing with a different destination or closing the iterator must not modify
the earlier destination, although its borrowed views still expire under the
advance/Close rule. Iteration and access/mutation of its current destination
are not concurrent operations; independent iterators may share compiled plans,
but require separate destination storage.

Retain after successful NextInto pins the current page; it does not pin the Go destination or preserve fields overwritten on reuse. To keep
a borrowed row while reusing dst, copy its view descriptors into another value
and retain the page before advancing, then close that lease after use. Use Clone
instead for fully detached data. Simply storing dst or copying its Go struct
without a page lease does not extend buffer lifetime.

### Paging and conditional results

Rows owns one current page and at most the configured bounded prefetch. NextInto
consumes empty pages with continuations, propagates the first error terminally,
and never loses an error behind partial results. Context governs the entire
pager lifetime; a separate configured per-page timeout may shorten it. Repeated
continuation states are compared against every previously returned continuation
in that automatic pager, including empty tokens: A → B → A therefore returns
cqlerr.ErrPagingNoProgress before another fetch of A. Token equality checks full bytes;
a hash alone is insufficient. Use a hash-indexed history with full-byte collision
checks, giving expected O(token bytes) insertion/lookup rather than scanning all
prior states on every page. Retain one immutable owned token representation for
history, next-page sending and PageState; internal use never calls the public
copying Token() accessor. Count the history entry/index storage under the count
budget and token bytes under the byte budget. Explicit Token() calls still copy.
Retained history is bounded by ConfigBuilder.WithMaxPagingStates (default
65,536) and WithMaxPagingStateSize (default 16 MiB);
exhausting either returns terminal cqlerr.ErrPagingStateLimit before storing/fetching
the next state. Both budgets must be positive. Manual RunPage keeps no
cross-call history. RunPage returns an empty page and its continuation as-is.
PageState distinguishes an absent start token, present empty continuation, and
completion. Tokens returned by State/Token remain available after page closure;
Token returns an explicit owned copy of the server token, not a serialized state
tag. The zero PageState and StartPage mean start. PageStateFromToken always
constructs a continuation, including for nil and empty input, and copies its
argument. Token is nil for start and complete; use More/Complete to distinguish
them. A continuation with zero token bytes still has More()==true. Complete is
output-only: passing it to RunPage returns cqlerr.ErrPagingComplete before
admission/I/O. This continuation-only import API does not round-trip start or
complete through Token. The page size cql.Unpaged (math.MaxInt32) resolves to
a request without a page size and rejects an explicit nonzero prefetch. Exec drains and
closes all rows and fails if result columns exist; conditional writes use
CASRows and consume every row, including multi-row conditional batches. CASRows
handles an applied-only response and checked previous-value metadata; Previous
is absent only if no previous columns were returned.

## Streaming receive into final caller storage

Queries are page-buffered by default and use shared multiplexed connections.
Memory scales with concurrent response pages, bounded by frame/decompression
limits. Streaming codecs still write seeded caller destinations. Set
exec.StreamOptions.Incremental for explicit bounded receive; the exclusive-connection,
raw receive, transform, read-ahead and working-storage clauses below describe
that explicit mode. exec.StreamLimits receive budgets apply to incremental mode.
All framing, malformed-value, cancellation and decoder checks remain mandatory
in both modes. Incremental native read-ahead is bounded by its configured limit
and 4 KiB. Shared connections retain an 8 KiB reader, transferred without losing
unread bytes at a checked frame/unit boundary and restored before shared reads.
The user-facing description is docs/source/statements/streaming.md.

```go
// Streaming is always prepared: Session.PrepareStream, then PreparedStream.Query.
func (s *Session[D]) PrepareStream[P, R any](context.Context, stream.Statement[P, R], ...exec.PrepareOptions) (*PreparedStream[P, R], error)
func (p *PreparedStream[P, R]) Parameters() cql.Columns
func (p *PreparedStream[P, R]) Columns() cql.Columns
func (p *PreparedStream[P, R]) Info() PreparedInfo
func (p *PreparedStream[P, R]) Query(exec.StreamOptions) (*StreamQuery[P, R], error)
func (q *StreamQuery[P, R]) Run(context.Context, P) (*RowStream[R], error)
func (q *StreamQuery[P, R]) Close()
func (q *StreamQuery[P, R]) Wait(context.Context) error
type RowStream[R any] struct { /* exclusive pull receiver, automatic paging */ }
func (s *RowStream[R]) NextInto(*R) bool
func (s *RowStream[R]) Err() error
func (s *RowStream[R]) Close() error
func (s *RowStream[R]) Columns() cql.Columns
func (s *RowStream[R]) Info() StreamInfo

// package exec
type StreamOptions struct {
    Incremental bool
    Options Options
    Limits StreamLimits
}
type StreamLimits struct {
    MaxMetadataBytes int64
    ReadAheadBytes int
    DiscardBytes int
    MaxTransformBytes int64
}

// package gocql
type StreamInfo struct {
    Result Result
    DeliveredRows int64
    Pages int64
    OutputStarted bool
}

// package stream
type Statement[P, R any] struct {
    CQL string
    Params cql.ParamEncoder[P] // nil when P is cql.EmptyParams
    Rows RowDecoder[R]         // must not be nil
    Kind cql.StatementKind
}
type RowDecoder[R any] interface {
    Compile(Columns) (RowPlan[R], error)
}
type RowPlan[R any] interface {
    Reset(*R)
    DecodeInto(*RowReader, *R) error
}
type Decoder[T any] interface {
    Compile(DataType) (ValuePlan[T], error)
}
// Optional declaration capability; no server DataType is needed.
type Resetter[T any] interface { Reset(*T) }
type ValuePlan[T any] interface {
    Reset(*T)
    DecodeInto(*ValueReader, *T) error
}
type OutputField[R any] struct { /* immutable checked field declaration */ }
func Column[R, V any](string, Decoder[V], func(*R) *V) OutputField[R]
func ColumnAt[R, V any](int, Decoder[V], func(*R) *V) OutputField[R]
func RowInto[R any](reset func(*R), fields ...OutputField[R]) RowDecoder[R]
func OneColumn[T any](Decoder[T]) RowDecoder[T]
func RowOf[R any](...cql.StructOption[R]) (RowDecoder[R], error)
func MustRowOf[R any](...cql.StructOption[R]) RowDecoder[R]
func WithCodec[P, V any](Decoder[V]) cql.StructOption[P]

// Readers are scoped mutable limited readers, never retained-byte views.
type RowReader struct { /* checked count and column cursor */ }
func (r *RowReader) ReadColumn[T any](ValuePlan[T], *T) error
func (r *RowReader) Context() context.Context
func (r *RowReader) SkipColumn() error
func (r *RowReader) RequireEnd() error

type ValueReader struct { /* checked type/state and exact body boundary */ }
func (r *ValueReader) Context() context.Context
func (r *ValueReader) Type() DataType
func (r *ValueReader) State() CellState
func (r *ValueReader) Remaining() int64
func (r *ValueReader) Read([]byte) (int, error)
func (r *ValueReader) Uint8() (uint8, error)
func (r *ValueReader) Uint16() (uint16, error)
func (r *ValueReader) Uint32() (uint32, error)
func (r *ValueReader) Uint64() (uint64, error)
func (r *ValueReader) Child(DataType, func(*ValueReader) error) error
func (r *ValueReader) Skip() error
func (r *ValueReader) RequireEnd() error

var Bool Decoder[bool]
var Int8 Decoder[int8]
var Int16 Decoder[int16]
var Int32 Decoder[int32]
var Int64 Decoder[int64]
var Float32 Decoder[float32]
var Float64 Decoder[float64]
var Counter Decoder[Counter]
var Date Decoder[Date]
var Time Decoder[TimeOfDay]
var Timestamp Decoder[Timestamp]
var Duration Decoder[Duration]
var UUID Decoder[UUID]
var TimeUUID Decoder[TimeUUID]
var Inet Decoder[netip.Addr]
var Text Decoder[[]byte] // text/varchar; incremental UTF-8 check
var ASCII Decoder[[]byte]
var Blob Decoder[[]byte]
var Varint Decoder[[]byte] // exact signed two's-complement bytes
// Decimal's unscaled bytes have the same caller-capacity rule.
type DecimalValue struct { Scale int32; Unscaled []byte }
var Decimal Decoder[DecimalValue]
func Nullable[T any](Decoder[T]) Decoder[Nullable[T]]
func Empty[T any](Decoder[T]) Decoder[EmptyValueOf[T]]
func List[T any](Decoder[T]) Decoder[[]T]
func Set[T any](Decoder[T]) Decoder[[]T]
func Map[K, V any](Decoder[K], Decoder[V]) Decoder[[]Entry[K, V]]
func Vector[T any](Decoder[T]) Decoder[[]T]
func Tuple[R any](RowDecoder[R]) Decoder[R]
func UDT[R any](RowDecoder[R], UDTOptions) Decoder[R]
// Explicit final encoded storage for dynamic/custom representations.
type EncodedValue struct { Type DataType; State CellState; Bytes []byte }
var Encoded Decoder[EncodedValue]

// package cqlerr
type StreamCapabilityError struct { Protocol int; Compression config.Compression; Reason string }
type StreamCapacityError struct { Path string; Required, Available int64 }
func (e *StreamCapabilityError) Error() string
func (e *StreamCapacityError) Error() string
```

stream.OutputField[R] is opaque, as for OutputField[R]. Unmapped physical columns
are skipped with recursive structural/type validation; no ignore field is
needed in stream.RowInto.
Compile validates actual metadata recursively, including children of null and
empty values, and returns immutable plans safe for independent cursors. Plans
capture neither readers nor destinations. Scalar rules match the buffered
codecs, including exact widths, UTF-8/ASCII, numeric ranges, null/unset and
logical Empty. stream.Empty accepts exactly the types accepted by EmptyCodec.
No adapter may run an arbitrary buffered Decoder by accumulating its input.
stream.Encoded explicitly writes the entire encoded body into caller storage,
then exposes that caller-owned representation; nested framing/type validation
runs incrementally and does not allocate a complete cell in driver storage.

A streaming execution uses the same parameter encoding, routing, per-host
prepared IDs, metadata revisions and Close/Wait admission as a buffered query.
Actual streaming response metadata selects this query's stream.RowPlan; cache
reuse is scoped to this decoder and exact metadata. Applications using both modes prepare the statement twice: Session.Prepare with
an ordinary typed decoder, and Session.PrepareStream with a stream.RowInto decoder.
Session.PrepareStream takes stream.Statement[P,R] (CQL, Params, Rows as a
stream.RowDecoder, Kind) and returns an immutable PreparedStream[P,R]. Its
Query(exec.StreamOptions) creates a worker-owned StreamQuery. Preparation validates
the streaming plan against available metadata, deferring only absent conditional
result metadata. No buffered decoder is needed. Query-local caches revalidate
actual changed result metadata without changing the shared preparation.

Streaming query execution installs the stream receiver before sending and returns after checked
response headers/metadata and bounded integrity staging, without waiting for the
complete page. It does not consume row payload waiting for an application
destination. NextInto reads one row into that destination and performs automatic
paging on demand, including empty-page progress checks. RowStream has no
Clone or Retain: output already belongs to the caller and no PageLease is needed.
Columns returns current-page checked metadata. Info counters are cumulative;
its Result warnings, trace IDs and custom payload describe only the latest
page/attempt, bounded by MaxMetadataBytes, rather than accumulating per-page
payloads forever. OutputStarted becomes true before the first decoder operation
that can modify a destination, including Reset; it never becomes false again.
DeliveredRows increments only after complete checked rows. Pages counts checked
response pages, including empty pages. Errors retain this information through
Info even when a streaming query has already returned a cursor. An error before cursor
construction returns normally through the query Run method's error result.

Reset is called on a non-nil destination before each active NextInto attempt;
terminal/exhausted calls return false without touching it. Built-in value Reset
clears logical values while preserving byte/element slice capacity and nested
storage. stream.RowInto requires a non-nil application reset callback for unmapped
Go state, then resets mapped fields through child plans. The callback must
preserve mapped storage. A custom plan has the same explicit responsibility;
there is no promise to zero an arbitrary R while preserving its unknown storage.
An application UDT field absent from database metadata has no DataType with
which to compile its value plan. stream.Resetter[T]
is an optional capability on decoder declarations (including row declarations),
providing metadata-independent Reset while preserving caller-seeded byte and
nested storage. Built-in scalar, text/blob/numeric payload, nullable/empty,
collection/map/vector, tuple/UDT, encoded-value and row compositions provide this
reset behavior recursively. For compositions with custom descendants, the
required declaration reset capability must be available for every absent
application mapping; it cannot be inferred from the custom Go representation.

With UDTOptions.AllowMissingFields, compilation permits an application field
missing from database metadata only when its decoder declaration supports
stream.Resetter for that destination. Missing custom-field capability produces a
clear ValidationError during Compile, before row decoding; implementing
stream.Resetter allows that custom field to participate. The declaration Reset
clears its logical value while preserving seeded storage, just like built-in
reset. This also applies recursively to missing fields inside compositions.
Mapped custom fields continue to use their successfully compiled
stream.ValuePlan.Reset and need not implement declaration stream.Resetter.
Custom row declarations used as tuple/UDT compositions follow the same rule
when metadata-independent reset is required. No synthetic DataType, reflection,
whole-row temporary or zero-assignment of arbitrary V supplies a substitute.
stream.RowInto's application callback retains responsibility for unmapped Go state
and must preserve mapped storage; this optional capability does not shift that
responsibility or relax the complete streaming type-support requirements.

Unlike buffered NextInto, false does not clear the whole destination. After a
failed attempt its contents may be partial and are invalid. Nil on an active
cursor is terminal ValidationError before row consumption. A terminal cursor
preserves its first error; exhaustion alone has nil Err.

Text/blob output reslices the supplied array without growing it. A declared
length beyond capacity yields StreamCapacityError before any body bytes of
that value are written. A null nullable value resets its logical Value and Valid
while preserving Value's capacity; an empty present value has Valid true and
zero length. For slices of nested elements, count/dimension must fit cap(dst)
before element bodies are read; decoding reslices the existing backing and
resets each element immediately before use, preserving its seeded child storage.
Map entries similarly preserve key/value storage. The caller must preseed each
nested destination it wants to reuse, including slots beyond its current length.
Tuple and UDT builders reuse the checked positional/name/evolution rules and
write into application fields; no driver-owned complete-row temporary is used.
Reader Child owns nested length boundaries, and RequireEnd rejects trailing
bytes. Read has io.Reader semantics bounded by Remaining and may write directly
to final caller ranges; custom codecs must honor the same no-gather/capacity and
lifetime rules. Custom callbacks/plans must return promptly on cancellation
using the reader context and must not start asynchronous destination accesses;
Close waits for active custom calls rather than returning with a writable
destination. Skip and SkipColumn validate discarded values with bounded
scratch, including nested structure, rather than trusting lengths alone.

The mode disables inherited prefetch and speculation regardless of profile
defaults. Explicit nonzero exec.Options.Prefetch fails validation before submission;
zero or inherited Prefetch is accepted. Retry and reprepare are permitted only
before OutputStarted and any delivered row, under the ordinary safe-request
rules. A row decoder/reset error or later I/O failure is terminal, with no
transparent replay. StreamQuery.Close() stops the cursor and StreamQuery.Wait(ctx) covers final transport and callback drain;
streamed output remains caller-owned.

exec.StreamLimits zero fields select defaults: 1 MiB simultaneously retained
metadata/control bytes,
32 KiB connection read-ahead, 32 KiB discard scratch, and 262,158 transform bytes
(two maximum 131,071-byte v5 units plus 16 bytes of framing). Negative/overflowing
values fail validation. Positive values are upper bounds, never a reason to
grow with a response. Limits are copied at admission; lower configured protocol limits
(WithMaxFrameSize, WithMaxDecompressedSize, WithMaxMetadataDepth and the paging
state limits) still apply to total frame/decompressed work, nesting, counts and paging history.
MaxMetadataBytes bounds simultaneously retained current-page metadata/control,
including old data still live while replacement metadata is allocated or
installed. Declared lengths accumulate within an envelope and are checked before
allocation; accounting resets only after the corresponding storage is released.
It is not a lifetime/page-total limit. Paging tokens and retained continuation
history use their separate paging state budgets rather than this counter.
MaxTransformBytes covers simultaneous compressed input and plaintext output plus
unit framing; oversized units fail before allocation. No list of consumed units
is retained. Raw v3/v4 does not allocate transform units. Metadata compilation
and paging control/history have their own explicit limits; caller output is
accounted for separately.

Initial supported modes are native v3/v4 without frame compression, with or
without TLS, and native v5 raw or LZ4 segments, with or without TLS. Validate a
candidate connection's negotiated mode and the decoder's metadata before request
submission when known; otherwise select only eligible candidates and validate
actual response metadata before output. Unsupported protocol/compression or
custom mapping combinations produce StreamCapabilityError, never buffered
fallback. v3/v4 Snappy/LZ4 frame compression is rejected before sending the query.
A pool containing only incompatible connections fails promptly; the driver does
not silently change negotiated compression or create unlimited connections.

The cursor reserves one connection exclusively from the configured pool budget.
A connection is eligible only after its shared writer/read loop is quiescent,
all pending/in-flight/orphan/control operations are absent, and read ownership
has transferred atomically to the stream receiver. Pool acquisition honors
context, routing and strict host/shard targets. Idle cursors retain that bounded
capacity; other work waits or uses other pool connections. Control/event frames
encountered during streaming use bounded nonblocking dispatch and cannot steal
result bytes. No background operation writes caller storage between NextInto
calls. Close is idempotent, may interrupt NextInto, closes an incomplete
connection to unblock reads, and waits for destination access and input writers
to stop before returning. Context cancellation provides the same boundary when
the active NextInto returns. Concurrent NextInto calls are invalid and rejected
before the second call accesses storage. Exact response completion permits pool
reuse only after the shared reader resumes at a proven protocol boundary; early
close, capacity failure, malformed framing, timeout or cancellation discards the
reserved connection and schedules ordinary bounded pool repair. Session.Close
also interrupts active streams; already written caller output remains valid.

## Options, profiles, batches and results

```go
// exec.Options are plain values; the zero exec.Options uses the session default
// profile. Query constructors take exec.Options and validate, resolve and encode
// them for their session once. One-shot executions (Prepared.Exec/One/Each,
// Session.ExecBatch, Session.RunBatch) take exec.CompiledOptions: exec.Options compiled by
// Session.CompileOptions for one session's default profile and keyspace. On
// another session, exec.CompiledOptions are resolved again for that session on each
// execution. The zero exec.CompiledOptions uses the executing session's default profile.
func (s *Session[D]) CompileOptions(exec.Options) (exec.CompiledOptions, error)

// package exec
type CompiledOptions struct { /* frozen, resolved options */ }
func (CompiledOptions) Values() Options          // a copy, to derive other options

type Options struct {
    Profile *config.Profile                       // copied when resolved
    Consistency cql.Option[cql.Consistency]
    SerialConsistency SerialOverride              // SerialInherit (zero), SerialSerial, SerialLocal, SerialOmit
    Timeout cql.Option[time.Duration]             // 0 disables profile timeout
    PageTimeout cql.Option[time.Duration]
    PageSize cql.Option[int32]                    // cql.Unpaged: whole result
    Prefetch cql.Option[int]                      // 0 or 1 page
    Idempotent bool
    Timestamp cql.Option[int64]                   // microseconds
    NowInSeconds cql.Option[int32]
    Keyspace string
    Trace bool
    CustomPayload []cql.PayloadEntry
    IncludeMetadata bool                          // request full result metadata
    HostID cql.Option[cql.UUID]
    ShardID cql.Option[uint32]
    Retry retry.Policy                            // nil inherits; retry.Never{} disables
    Observer config.Observer
}
// package cql
type PayloadEntry struct { Key string; Value Nullable[Bytes] }

// package exec
type SerialOverride uint8                         // one field, so no invalid combination exists
const (SerialInherit SerialOverride = iota; SerialSerial; SerialLocal; SerialOmit)

// package config
type Profile struct {
    Consistency cql.Consistency
    SerialConsistency cql.Option[cql.Consistency]
    Timeout, PageTimeout time.Duration
    PageSize int32
    Prefetch int
    Retry retry.Policy
    Routing routing.Policy
    Speculation SpeculationPolicy
    /* unexported built mark */
}
func NewProfile() *ProfileBuilder
func (*ProfileBuilder) Build() (Profile, error)
// With* setters: WithConsistency, WithSerialConsistency, WithTimeout,
// WithPageTimeout, WithPageSize, WithPrefetch, WithRetry, WithRouting,
// WithSpeculation; NoTimeout, NoPrefetch and NoSerialConsistency clear
// the corresponding setting.

// package cql
type BatchKind byte
const (LoggedBatch BatchKind = 0; UnloggedBatch BatchKind = 1; CounterBatch BatchKind = 2)

// package gocql
const MaxBatchStatements                          // 65,535
type Batch struct {
    Kind cql.BatchKind // zero: cql.LoggedBatch; set while building, sent with every execution
    /* reusable owned members appended by AddToBatch */
}
func (b *Batch) Len() int
func (b *Batch) Reset() // keeps storage and Kind

type Result struct {
    HostID cql.UUID
    Attempts int
    Latency time.Duration
    Rows int64
    AchievedConsistency cql.Consistency
    /* unexported diagnostics */
}
func (r Result) Warnings() []string
func (r Result) TraceIDs() []cql.UUID
func (r Result) CustomPayload() []cql.PayloadEntry
```

Invalid values fail in the query constructor or Session.CompileOptions, before any
execution. A query uses its options as resolved when it is created; options
passed per call use the resolution CompileOptions made. A zero exec.Options inherits the selected session profile, is non-idempotent, and has
no tracing/timestamp/keyspace override. Override validation returns typed
errors, not panics. A profile is a plain value, made by `ProfileBuilder.Build`:
it carries an unexported built mark, so a `Profile{...}` literal, whose unset
fields would be zero rather than the defaults, fails `WithDefaultProfile` at
Build and `exec.Options.Profile` at compile time. A copy of a built profile stays
built. Build copies
the default profile and compiling copies exec.Options.Profile, so changing a
Profile after it was handed over has no effect. Admission validates nothing and
shares the resolved options across attempts/pages; CustomPayload is copied once,
when the options are compiled.
Defaults:
LOCAL_QUORUM, LOCAL_SERIAL, 30-second request timeout, 5,000-row pages, one-page
prefetch, no speculation, and the safe retry policy. Caller context always
bounds request work, including preparation, metadata refresh, agreement,
queueing and cleanup waits. Cleanup continues independently where required to
release an orphan stream; returning a context error never authorizes reuse of
still-borrowed input before Wait. Query construction copies custom payload bytes and checks source views while
capturing options. Public batch execution also owns its options payload; batch
parameters are copied into the batch when AddToBatch appends them. Returned Result
payloads use owned storage, because Result outlives pages; this explicit escape
is an ownership conversion, not a per-cell receive copy.

Keyspace affects prepare/cache identity and v5 QUERY/BATCH flags; an
incompatible post-bind override is rejected. v3/v4 nondefault keyspace requires
a qualified CQL statement or a session established in that keyspace. No
concurrently mutable session-wide USE operation exists. Host pinning still
validates host eligibility; a shard ID is a hint unless routing.Target.StrictShard is
set by the routing policy. StrictShard requires a present shard ID; absent,
out-of-range or unavailable strict shards return a typed target error without
same-host shard fallback. HostID pinning never falls back to a different host.
Target strictness survives plan, attempt and pool seams; routing.SingleTarget
propagates its strictShard argument unchanged. Unsupported version options fail
before send. Timestamps use one value per logical execution reused across
retries; generated timestamps are concurrency-safe.

A Batch owns its members' encoded parameters in storage it reuses after Reset:
appending to a batch at its working size does not allocate. AddToBatch validates
the session and keyspace of each member as it is appended. An execution that
returns before its writer finished, after cancellation, keeps the storage it
reads: a later Reset gives the batch fresh storage instead. At most
MaxBatchStatements (65,535) entries and 65,535 parameters/member; reject size violations before
preparation/I/O. Mixed prepared and QUERY members are legal. Encoded member
parameters are reused unchanged; UNPREPARED repairs only the matching host statement and
validates its new schema. All member statements must have compatible effective
keyspace. Routing chooses the first member with a complete valid partition key,
records this choice, and never claims a cross-partition batch has only one
target replica set. Counters and LWT retain server restrictions. Batch
idempotence defaults false and must be explicit. Batch classification is
conditional if any member is conditional (or regular consistency is serial),
otherwise unknown if any member is unknown, otherwise ordinary. Unknown and
conditional batches are never speculated.

## Policies, metadata, events, configuration and observation

The following seams make policy and lifecycle behavior available to
applications.

```go
// package routing
type Info struct {
    Keyspace, Table string
    Token cql.Option[int64]
    Kind cql.StatementKind
    Conditional bool
    Consistency cql.Consistency
    HostID cql.Option[cql.UUID]
    ShardID cql.Option[uint32]
}
type Target struct { HostID cql.UUID; ShardID cql.Option[uint32]; StrictShard bool }
type Route struct { Shard int32; /* unexported host route */ }
func (r Route) HostID() cql.UUID
func (r Route) Target() Target
func (r Route) Usable() bool
func (r Route) ShardOf(token int64) int32
func (r Route) WithShard(shard int32) Route
type Plan interface { Next() (Route, bool) }
type Policy interface {
    Validate(*metadata.Snapshot) error
    Pick(Info, *metadata.Snapshot) (Route, bool)     // cheap first route; false: start with Fallback
    Fallback(Info, *metadata.Snapshot) Plan          // every route, in order
    Observe(config.AttemptEvent)
}
func NewPlan(Policy, Info, *metadata.Snapshot) Plan  // combines Pick and Fallback

// package config
type AttemptEvent struct {
    RequestID uint64
    Attempt int
    Target Target
    Started time.Time
    Latency time.Duration
    Err error
    Cancelled, Speculative bool
}
type SpeculationPolicy interface { Delay(attempt int) (time.Duration, bool) }
type Observer interface { ObserveRequest(RequestEvent); ObserveAttempt(AttemptEvent) }
type RequestEvent struct {
    ID uint64
    Statement string
    BatchStatements []string
    Parameters cql.Columns
    Routing routing.Info
    Result Result
    Err error
}
type LifecycleObserver interface { ObserveLifecycle(LifecycleEvent) }
type LifecycleEvent struct {
    At time.Time
    Err error
    Kind, Stage string
    Stream int16
    HostID cql.UUID
}
```

Retry types live in the public package `github.com/scylladb/gocql/v2/retry`.
A retry policy depends on packages `cql` and `cqlerr` only, not on the root
package:

```go
package retry

type Action uint8
const (Stop Action = iota; Same; Next; IgnoreWrite)
type Attempt struct {
    Err error
    Number int // 1-based
    Idempotent bool
    Consistency cql.Consistency
    Conditional bool
    BytesWritten bool                        // Deprecated: use ExecutionStatus
    ExecutionStatus cqlerr.ExecutionStatus
}
type Decision struct {
    Action Action
    Consistency cql.Option[cql.Consistency]
    Delay time.Duration
}
type Policy interface { Decide(Attempt) Decision }
type PolicyFunc func(Attempt) Decision
type Safe struct { MaxAttempts int }
type Never struct{}                          // stops after the first failed attempt
```

Built-in routing has a concrete configuration surface. The default profile selects a
token-aware policy with local preference when configured, replica shuffling for
ordinary requests, deterministic conditional ordering and no remote fallback
unless allowed. Validate runs at session startup and on eligible-topology
changes; missing preferred DC/rack returns a configuration error rather than an
empty plan. Pick and Fallback never do I/O or report a hidden failure through a
panic. The built-in policy reads private immutable host/replica indices and
produces its first eligible replica without enumerating/copying the full host
list or constructing all fallback targets. It advances fallback lazily only
when needed. Public defensive metadata accessors are inspection APIs, not the
built-in routing path. A plan captures a placement/topology snapshot; pool
acquisition still rechecks current availability and target strictness. Custom
policies implement Pick and Fallback themselves and account for their own work.

```go
// package routing
type Options struct {
    TokenAware bool
    Datacenter, Rack string
    AllowRemote bool
    ShuffleReplicas bool
    LeaderAware bool
    Latency LatencyOptions
}
type LatencyOptions struct {
    Enabled bool
    ExclusionThreshold float64
    MinimumSamples int
    Scale, RetryPeriod, UpdateInterval time.Duration
}
func NewPolicy(Options) (Policy, error)
func SingleTarget(Target, strictShard bool) Policy
const AnyShard
type PartitionKey struct { /* logical canonical encoded component stream */ }
func (k PartitionKey) Len() int64
func (k PartitionKey) ForEach(func(cql.Bytes) error) error
type TokenPartitioner interface { Token(PartitionKey) (int64, error) }
var Murmur3TokenPartitioner TokenPartitioner
var CDCTokenPartitioner TokenPartitioner
```

Tablet v2 takes precedence over v1 when both are negotiated. Every EXECUTE on a
v2 connection writes exactly one trailing version-block byte; QUERY/BATCH never
do. For a known opaque 64-bit version, choose nibble index 0..15 and encode it
in the high nibble with that version's selected least-significant-first nibble
in the low nibble. Unknown range with table/token uses a random byte; absent
table/token uses zero. Retries may reuse the chosen byte. Opaque versions have
equality only, never numeric age ordering. Valid ranges are nonwrapping
(first,last] with first<last; an accepted update replaces every overlapping
cached range entirely. Retain original replica order and shard hints, reject
malformed declared entries as a whole, and never fall back to vnode placement
for unknown tablet ranges.

Feedback carries the request's table identity and schema/placement epochs.
Reject feedback for dropped/recreated or invalidated identities; receipt order
cannot prove server-version freshness. Negotiate/apply only known payload
schemas; malformed optional feedback produces a diagnostic without replacing a
valid query result. For globally strong-consistent tablet keyspaces, a
token-aware plan at regular consistency other than ONE/LOCAL_ONE promotes the
first original v2 replica as leader only when connected, enabled and
locality-eligible. An unresolved original leader never promotes the first
resolved follower. Unknown/ineligible leaders use ordinary replica/nonreplica
fallback. LWT replica ordering remains deterministic; ordinary replica shuffling
and configured latency ordering apply without admitting excluded hosts.
routing.Options.LeaderAware enables this leader path.

ConfigBuilder.WithPartitioner(name, routing.TokenPartitioner) maps a server
partitioner class name to a TokenPartitioner. Built-in Murmur3 and CDC
implementations are installed by default. Unknown
partitioners disable token routing and expose a diagnostic; they do not use the
wrong hash. PartitionKey includes composite length/terminator framing as needed;
ForEach streams it in canonical order without materialization. Custom
partitioners must consume it synchronously and not retain borrowed spans.

Policies may not mutate request inputs; no observer receives `[]any`. Observer
callbacks are synchronous and must not block; panic containment logs observer
failure without corrupting execution. There is one terminal request event and
one terminal event per started attempt, including cancellation and speculative
losers, for each configured observer. When no observer is configured, skip event
construction, batch-statement arrays and diagnostic formatting. Required policy
Observe feedback, metrics and Result accounting still run. Latency percentile
snapshots are maintained outside individual request completion; disabled
speculation does not create a scheduler/timer. Row
cardinality is separate from successful server execution. Trace
IDs/warnings are preserved per response; Result aggregates across consumed
pages. Tracing lookup uses typed internal queries and context-bounded readiness
polling.

The safe retry policy only retries unknown-outcome writes when explicitly
idempotent. Read timeout/unavailable repair follows server evidence with bounded
attempts; deterministic failures never retry. Optional downgrading is separately
named/configured and reports achieved consistency. Only explicitly or reliably
server-confirmed ordinary statements may speculate, and only when idempotent.
Unknown/conditional statements and batches never speculate; regular
SERIAL/LOCAL_SERIAL and prepared LWT markers force conditional classification
regardless of declarations. Plan advancement is serialized across speculative
branches. Valid feedback accepted from completed responses is applied at most
once even if the branch loses; bounded coalescing may supersede equivalent
updates. Feedback must not block successful execution. Equivalent feedback with
the same
validated identity/epochs, range, version and ordered replicas/shards is a no-op;
version equality alone is insufficient. Publish changed placement with
structural sharing of unaffected tables/ranges instead of rebuilding complete
cluster/schema snapshots for every response. Queued feedback owns only its
bounded decoded control data and does not pin the response page. On queue
saturation,
advance a placement invalidation epoch (globally if necessary), mark affected
mappings unknown and surface a diagnostic; later EXECUTE responses relearn
placement. Do not retain known-stale mappings, leak page owners, or fail a
successful response due solely to saturation. Invalidation takes effect through
a bounded atomic signal even when the normal queue is full.

Package cqltrace reads server-side CQL trace sessions through the public
Session API; it is not a Session method because it needs nothing internal.

```go
// package cqltrace
type Info struct {
    ID cql.UUID
    Coordinator, Client netip.Addr
    Started time.Time
    Duration time.Duration
    Command, Request string
    Parameters map[string]string
    RequestSize, ResponseSize cql.Option[int32] // ScyllaDB only
    Events []Event
}
type Event struct {
    ID cql.TimeUUID
    Source netip.Addr
    Activity, Thread string
    Elapsed time.Duration
}
func (i Info) Nodes() []netip.Addr
func (i Info) String() string
type Fetcher struct {
    Poll time.Duration          // 0: 100ms
    Settle time.Duration        // >0: wait until the event count is stable
    Consistency cql.Consistency // 0 (Any): One
}
func (f Fetcher) Fetch[D gocql.Dialect](context.Context, *gocql.Session[D], cql.UUID) (Info, error)
func Fetch[D gocql.Dialect](context.Context, *gocql.Session[D], cql.UUID) (Info, error)
var ErrNotReady error // ctx ended before the session was ready
```

```go
func (s *Session[D]) Metadata() *metadata.Snapshot
func (s *Session[D]) RefreshMetadata(context.Context) error
func (s *Session[D]) AwaitSchemaAgreement(context.Context) error
func (s *Session[D]) Subscribe(event.Filter) (*event.Subscription, error)
func (s *Session[D]) Metrics() metrics.Snapshot // session-wide counters
func (s *Session[D]) Trace(context.Context, cql.UUID) (trace.Info, error)
func (s *Session[D]) Close() error
func (s *Session[D]) Closed() bool

// package event
type Kind uint8
const (Topology Kind = iota; Status; Schema; Control; ClientRoute; Overflow)
type Filter struct { Keyspace string; Kinds []Kind; Capacity int }
type Event struct {
    Kind Kind
    Generation uint64
    Address netip.AddrPort
    HostID cql.UUID
    Change, Target, Keyspace, Object string
    ArgumentTypes []string
}
func (s *Subscription) Events() <-chan Event
func (s *Subscription) Close()

// package metadata
type Snapshot struct { /* immutable */ }
func (m *Snapshot) Hosts() []routing.HostSnapshot
func (m *Snapshot) Keyspace(string) (*Keyspace, bool)
func (m *Snapshot) Replicas(keyspace, table string, token int64) ([]routing.Target, error)
func (m *Snapshot) TokenRanges(keyspace, table string) ([]routing.TokenRange, error)
func (m *Snapshot) Token(keyspace, table string, key ...dynamic.Value) (int64, error)
func (m *Snapshot) HostRoutes() []routing.Route
func (m *Snapshot) ReplicaRoutes(routing.Info) []routing.Route
func (m *Snapshot) Diagnostics() []error
func (m *Snapshot) Generation() uint64
// Schema types: Keyspace, Table, TableOptions, Column, ColumnKind, ColumnOrder
// (ASC, DESC), ColumnIndex, Index, IndexKind, View, Type, Function, Aggregate.
type Mode /* SchemaNone, SchemaKeyspaces, SchemaFull (default) */

// package metrics
type Snapshot struct {
    ReceiveCacheHits, ReceiveCacheMisses uint64
    ReceiveCacheBytes, ReceiveCacheObjects int
    Requests, Attempts uint64
    Errors, Cancelled uint64
    Retries, Speculations uint64
    Orphans uint64
    InFlight int64
    LatencyP50, LatencyP95, LatencyP99 time.Duration
}

// package routing
type HostSnapshot struct {
    Address netip.AddrPort
    Datacenter, Rack string
    Tokens []int64
    Shards uint32
    ShardingIgnoreMSB uint8
    ID cql.UUID
    Enabled, Connected, Up bool
}
type TokenRange struct { Start, End int64; Replicas []Target }
```

ConfigBuilder.WithMetadataMode selects how much schema a session loads
(SchemaFull by default); topology always loads. WithMetadataKeyspaces limits
schema to the named keyspaces.

metadata.Snapshot exposes immutable snapshots, with full existing schema
features: keyspaces, replication, tables, columns, ordered keys, UDTs, views,
indexes, functions/aggregates, recreation CQL, node addresses/tokens and
tablets. Retained snapshots survive refresh. Accessors returning existing
mutable metadata structs or exported slice/map fields return defensive deep
metadata copies; private hot routing uses immutable internal snapshots directly.
HostSnapshot separates enabled/connected/up state. Events have explicit
topology/status/schema/control/client-route variants and carry a generation.
Publication events are emitted only after the corresponding snapshot is visible.
A subscription has a bounded queue (positive Capacity; zero selects 64, negative
is invalid). On overflow the dispatcher discards queued deltas, guarantees one
event.Overflow carrying the highest discarded generation, then closes that
subscription after the marker; it never waits on control I/O. Resynchronize by
subscribing again first, then reading Metadata (or completing an explicit
refresh if freshness is required), and ignoring queued publication events at or
below the snapshot generation. This subscribe-before-snapshot rule also applies
to initial subscription. event.Subscription.Close is idempotent, cancels delivery
without requiring the caller to drain, and cannot block control work. Schema
refresh validates duplicate identities, key order, UDT cycles and type syntax;
an incomplete topology read cannot replace a complete valid snapshot. Bad
individual peers may be skipped with evidence; invalid tokens are never replaced
by random tokens. Recoverable keyspace errors preserve that keyspace's previous
schema and schedule retry, with newer drops winning. Refresh returns a
structured partial-refresh error when it preserves stale data. Fetches capture
change epochs; per-object drop and route-deletion tombstones fence out
superseded full/partial reads and recoverable-error fallback. Recreation
advances identity/epoch rather than allowing old results to resurrect an object.
Tombstones remain until no older fetch/feedback can publish.

AwaitSchemaAgreement checks all enabled hosts in one snapshot per check,
including zero-token hosts; filtered hosts are excluded. It queries each host
with a usable pool directly at ONE with idempotent typed queries. Missing/broken
peers prevent agreement and are retried until the minimum of caller/configured
agreement deadline; zero successes is never agreement.
Authorization/protocol/validation failures are terminal. Each retry takes a
fresh snapshot. Automatic handling of a successful DDL result additionally
requires that exact coordinator host identity to report successfully, even if a
later topology excludes or temporarily loses it; absence cannot count as
agreement. Manual agreement does not refresh metadata. Automatic
agreement/refresh runs within the original operation context; failure reports
that the DDL already succeeded and never retries the successful DDL.

Session.Close atomically rejects new session work, sets Closed()==true, cancels
active requests/pagers and background control/pool work, and closes connections
to interrupt blocked I/O. It waits for accepted writers and driver workers to
finish and releases only undelivered/prefetched pages. A delivered current page
keeps its independent output owner until the application's next NextInto or
Rows/Page Close, so Session.Close cannot recycle a still-valid current row.
Shutdown and query input drains do not wait for that output owner or
independently retained PageLease objects. A subsequent NextInto releases the current
page and reports cqlerr.ErrClosed unless an earlier terminal error already won;
Rows/Page Close remains idempotent. Concurrent/repeated Session.Close calls wait
for the same shutdown completion and return the same shutdown result. Shutdown
drains all admissions made by this session.

Package `config` holds the configuration API. A `config.Config` is an
opaque, immutable value made only by `ConfigBuilder.Build`; users never write a
`Config` literal or assign its fields. Each builder method sets one setting
and is named `With*` (`WithPort`, `WithTLS`, `WithConnectTimeout`), except `Build`,
`ConfigureDefaultProfile` and `NoAdvancedShardAwareness`. An on/off setting
takes a bool (`WithDiscovery(false)`), and a size or interval whose zero has no
other meaning turns its feature off at zero (`WithHeartbeat(0, 0)`,
`WithMetadataRefresh(0)`, `WithPreparedCacheSize(0)`), so no setting has a
separate `No*` twin; settings not chosen keep the driver's behavior, and when a setting is chosen
again the last call wins. Setters store the latest value without validation or
call history.
`Build` validates only the final configuration and returns its first typed error.
Correcting a setting clears the error on the next Build. Optional zero durations
disable their budget, and nil optional providers remove overrides. `WithIgnoreEvents` replaces its complete
list. `WithDefaultProfile(Profile)` replaces the complete execution profile;
`ConfigureDefaultProfile` composes callbacks, and a nil callback resets the profile. Slice inputs are copied; Build snapshots
configuration values while callback/provider objects remain shared and concurrency-safe.
These functions are the entry points:

```go
// package config
func New(contactPoints ...string) *ConfigBuilder
func (*ConfigBuilder) Build() (Config, error)
func (*ConfigBuilder) WithDefaultProfile(Profile) *ConfigBuilder
func (*ConfigBuilder) ConfigureDefaultProfile(func(*ProfileBuilder)) *ConfigBuilder
func (*ConfigBuilder) WithOnlyHost(id cql.UUID) *ConfigBuilder
func (*ConfigBuilder) WithHostFilter(keep func(routing.HostSnapshot) bool) *ConfigBuilder
func (*ConfigBuilder) WithConnectionsPerShard(n int) *ConfigBuilder
func (*ConfigBuilder) WithConnectionsPerHost(n int) *ConfigBuilder
func (*ConfigBuilder) WithTLS(*tls.Config) *ConfigBuilder
func (*ConfigBuilder) WithTLSSource(TLSSource) *ConfigBuilder
func (*ConfigBuilder) WithClientRoutes(ClientRoutes) *ConfigBuilder // replaces all; ClientRoutes{} removes
func (*ConfigBuilder) WithAuth(auth.Provider) *ConfigBuilder
func (*ConfigBuilder) WithResolver(endpoint.Resolver) *ConfigBuilder
func (*ConfigBuilder) WithAddressTranslator(endpoint.Translator) *ConfigBuilder
func (*ConfigBuilder) WithDialer(endpoint.Dialer) *ConfigBuilder
func (*ConfigBuilder) WithPartitioner(name string, routing.TokenPartitioner) *ConfigBuilder
func (*ConfigBuilder) WithMetadataMode(metadata.Mode) *ConfigBuilder
func (*ConfigBuilder) WithAdvancedShardAwareness(first, last uint16) *ConfigBuilder
func (*ConfigBuilder) WithDefaultAdvancedShardAwareness() *ConfigBuilder // ports 49152-65535
func NewProfile() *ProfileBuilder
func NewClientRoutes() *ClientRoutesBuilder
func (*ClientRoutesBuilder) WithRoute(connectionID, hostname string) *ClientRoutesBuilder
func (*ClientRoutesBuilder) Build() (ClientRoutes, error)
func (ClientRoutes) Routes() []ClientRoute // copy; ClientRoute{ConnectionID, Hostname}

// package gocql
func Connect[D Dialect](context.Context, config.Config) (*Session[D], error)
```

`Connect` rejects the zero `Config`. `WithOnlyHost` connects only to the node
with that host ID; it is a host filter and composes with `WithHostFilter`.

Configuration covers autodiscovery, timeouts, pools/shards/source ports,
DNS/dial hooks, TLS, auth-provider exchanges, LZ4/Snappy, coalescing/socket
options, address translation/filtering/client routes, startup identity and
configuration reporting, metadata scope/events/agreement,
profiles/retry/speculation, observers/logger and timestamp generation. The
native protocol version is not a setting: it comes from the session's dialect. TLS uses Go crypto/tls,
validates host/CA by default, and never downgrades after a TLS failure. Each
auth exchange is per connection; providers are concurrency-safe. One TLS
configuration serves every node; without an explicit ServerName each
connection verifies the node's IP address. `ConfigBuilder.WithTLS` copies a
fixed `*tls.Config`; `WithTLSSource` takes a `TLSSource`
(`TLSConfig() *tls.Config`) that the dialer asks for the config of every new
connection; open connections keep theirs, and a nil result
fails the dial. Build validates the source's config at that time. `TLSFileSource` (`NewTLSFiles(base, TLSFiles)`) loads the client
certificate/key and CA from files over base, fails if they cannot be loaded,
and rechecks them during a dial once `Interval` has passed, keeping the current
files when a check fails. Context-taking hooks honor cancellation; synchronous callbacks are
prompt/nonblocking and cannot reenter blocking Close/Wait. Client
routes supply per-connection-ID endpoint overrides and disable advanced shard
awareness (shard selection by source port through the shard-aware port), which
their proxy breaks, unless `WithAdvancedShardAwareness` or
`WithDefaultAdvancedShardAwareness` is called. `WithConnectionsPerShard(n)` (default 1) and
`WithConnectionsPerHost(n)` size the pool, the later call winning;
`WithConnectionsPerHost` dials the regular port only and keeps no per-shard
excess, so Build rejects it with `WithAdvancedShardAwareness` or
`WithMaxExcessPerShard`. Keepalive, orphan-stream budget, connection repair and
graceful close remain separate from user-request timeout.

Errors live in package `cqlerr` and preserve `errors.Is`/`errors.As` and
server error codes/details. A failed request returns an `*ExecutionError`,
which unwraps the server/context error, lists every failed attempt
(`AttemptFailure`) and reports an `ExecutionStatus` (`NotExecuted`,
`MaybeExecuted`, `PartiallyExecuted`; `ExecutionStatusOf(err)`). `KindOf(err)`
groups any error by `Kind` (`KindTimeout`, `KindUnavailable`, `KindOverloaded`,
...), and `Kind.Transient` reports whether trying again can help. Driver
conditions are sentinels tested with `errors.Is`: among them `ErrBusy`,
`ErrClosed`, `ErrReleased`, `ErrStaleRows`, `ErrNotFound`, `ErrNoKeyspace`,
`ErrNoTable`, `ErrNoRows`, `ErrNoHosts`, `ErrNoConnections`,
`ErrConditionalExec`, `ErrPagingNoProgress`, `ErrPagingComplete` and
`ErrPagingStateLimit`. Typed driver errors include `ValidationError`,
`UnsupportedSettingError`, `CodecError` (column/index/nested path and stage),
`ProtocolError` (offset/stage; one type for frame violations, which have stage
"frame" and offset -1, and for data that does not decode),
`SchemaChangedError`, `ServerMismatchError`, `ConnectError`, `HostError`,
`PartialRefreshError`, `NoHostsError`, `StreamCapabilityError` and
`StreamCapacityError`. `cqlerr` exports only sentinels the driver returns.

Every server error implements `ServerError` (`Code() ErrorCode`,
`Message() string`, `Warnings() []string`) and is a concrete `XxxError` type
per code with its details (`WriteTimeoutError`, `UnavailableError`, ...);
codes without details are `OtherServerError`. `ErrorCode` is a typed `uint16`
with one `CodeXxx` constant per native protocol code; it implements `error`,
and each server error matches its own code, so
`errors.Is(err, cqlerr.CodeOverloaded)` works through any wrapper.
Unrecognized future server errors are `OtherServerError` and retain their code
and message. Invalid lengths and metadata never panic or allocate based on
unbounded advertised counts.

### Transport setup details

A Unix maintenance target is the single contact point `unix:/absolute/path`;
a bare absolute path is accepted as a convenience. `Build` turns host discovery
off unless `WithDiscovery` is set explicitly; explicit discovery, TLS, and an
explicit `WithLocalAddress` on this transport fail with
cqlerr.UnsupportedSettingError, and mixing Unix and TCP contact points fails
validation. Default shard settings do not target a Unix socket. An
endpoint.Dialer receives an endpoint.DialTarget with Network `unix` and the
exact path as Address; an unknown bootstrap host address is invalid rather
than a fabricated TCP endpoint. A
socket represents only its reconciled local host, so remote strict targets fail.
For example:

```go
cfg, err := config.New("unix:/run/scylla/cql.m").Build()
session, err := gocql.Connect[gocql.ScyllaV4](ctx, cfg)
```

```go
// package endpoint
type DialTarget struct {
    Network, Address string
    LocalAddress netip.AddrPort
    Host routing.HostSnapshot
    ShardID cql.Option[uint32]
}
type Dialer interface { DialContext(context.Context, DialTarget) (net.Conn, error) }
type DialerFunc func(context.Context, DialTarget) (net.Conn, error)
type DefaultDialer struct { ConnectTimeout, SocketKeepalive time.Duration }
type Resolver interface {
    LookupNetIP(ctx context.Context, network, host string) ([]netip.Addr, error)
}
type Peer struct {
    RPCAddress, BroadcastAddress, ListenAddress, PreferredIP netip.Addr
    Datacenter, Rack string
    HostID cql.UUID
    Port, ShardAwarePort, ShardAwarePortTLS uint16
}
type Translator interface {
    Translate(ctx context.Context, peer Peer, addr netip.AddrPort) (netip.AddrPort, error)
}
type TranslatorFunc func(ctx context.Context, peer Peer, addr netip.AddrPort) (netip.AddrPort, error)
func IdentityTranslator() Translator

// package auth
type Provider interface {
    Start(context.Context, routing.HostSnapshot, string) (Exchange, cql.Nullable[[]byte], error)
}
type Exchange interface {
    Challenge(context.Context, cql.Nullable[cql.Bytes]) (cql.Nullable[[]byte], error)
    Success(context.Context, cql.Nullable[cql.Bytes]) error
    Close()
}
func Password(username, password string) Provider
```

The context-aware endpoint.Dialer contract applies to all new connections.
DefaultDialer implements the raw target contract; DialerFunc adapts a
function. A Translator returns addr unchanged when no translation applies;
an error fails that connection attempt like a dial error. TCP endpoints and source ranges are selected before hooks;
TLS containers,
trust-pool containers and certificate byte arrays are snapshotted. Private keys,
callbacks, parsed trust-certificate objects and session caches/providers retain
their immutable/shared concurrency obligations. A superseded endpoint, route or sharder opening is closed.

The per-connection setup deadline starts before its target resolution and is
not restarted for translation, raw dialing, TLS, authentication, initial USE,
REGISTER, or final target validation. DNS's local timeout only shortens lookup.
Each resolved contact/opening owns its deadline, even when aliases or ports
share an IP. A failed physical opening can be retried under a new attempt budget
and reconnect policy; it never extends the spent attempt or caller deadline.

## Struct mapping, documentation and stopping iteration

Public Statement, stream.Statement, exec.Options, exec.PrepareOptions, exec.StreamOptions and
Profile records expose their fields directly in package documentation.
exec.CompiledOptions.Values returns public exec.Options. The high-level codec and observer
interfaces expose their methods; lower-level plan contracts are documented in
docs/source/custom-codecs.md. Facade adapters copy settings only, never payloads
or codec plans, and introduce no serializer.

cmd/cqlgen generates a struct mapping as a `func (T) CQLFields()
cql.StructFields[T]` method, which ParamsOf and RowOf find in the method set
of T or *T, and a `func (T) CQLStreamColumns() []stream.OutputField[T]`
method, which stream.RowOf finds, so there is no registry and no dependence on
package initialization order. Generated struct mappings honor cql.WithCodec and
stream.WithCodec at construction.
Recognized built-in tags take precedence over type overrides; overrides replace
untagged and custom-expression defaults of the matching Go type. Unaffected
custom-expression defaults retain their generated implementations. Each mapping
is independent and generated defaults are never mutated. Option scopes:
WithCodec affects ParamsOf and RowOf, stream.WithCodec only stream.RowOf, and
ReuseBuffers only RowOf; the other functions ignore them. No per-row override resolution is permitted.

All Each callbacks may return gocql.StopIteration or a chain of single-error
wrappers to stop successfully. Joined errors remain failures. Only callback returns recognize this signal:
decoder/transport errors are never suppressed. Normalize successful stopping
before request metrics and observers. No later row callbacks or continuation
pages run. Ordinary callback errors remain failures. Result.Rows continues
counting fetched response rows, rather than callback invocations.

exec.Options and exec.CompiledOptions remain separate. Compile recurring settings once;
request-specific options may be compiled for one-shot calls. Query settings remain
immutable and request context deadlines need no new options compilation.
