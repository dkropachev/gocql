// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"encoding/binary"
	"fmt"
	"math"
	"strings"
)

// DataType is an immutable, complete CQL type descriptor. Its zero value is
// invalid: functions that take a DataType reject it with a ValidationError.
type DataType struct{ descriptor *dataTypeDescriptor }
type dataTypeDescriptor struct {
	Element, Key   DataType
	Keyspace, Name string
	Fields         Columns
	Kind           Type
	Dimension      uint16
	frozen         bool
	frozenKnown    bool
}

// ColumnSpec identifies one physical column and its complete type.
//
// The zero ColumnSpec names no column and has the invalid zero DataType.
type ColumnSpec struct {
	// Type is the column's complete type.
	Type DataType
	// Keyspace, Table and Name name the column. Each may be empty, as for a
	// tuple field or a bind marker without metadata.
	Keyspace, Table, Name string
}

// Columns is an immutable, ordered set of column specifications.
//
// The zero Columns is empty.
type Columns struct{ columns []ColumnSpec }

// scalarTypes holds one shared descriptor per scalar kind, indexed by Type.
var scalarTypes = func() (t [TypeDuration + 1]DataType) {
	for k := TypeASCII; k <= TypeDuration; k++ {
		t[k] = DataType{descriptor: &dataTypeDescriptor{Kind: k}}
	}
	return t
}()

// Predeclared scalar types. Each equals ScalarType of the matching Type
// constant. Treat them as constants: reassigning one changes only code that
// reads the variable, not ScalarType or decoded metadata.
var (
	ASCIIType     = scalarTypes[TypeASCII]
	BigIntType    = scalarTypes[TypeBigInt]
	BlobType      = scalarTypes[TypeBlob]
	BooleanType   = scalarTypes[TypeBoolean]
	CounterType   = scalarTypes[TypeCounter]
	DecimalType   = scalarTypes[TypeDecimal]
	DoubleType    = scalarTypes[TypeDouble]
	FloatType     = scalarTypes[TypeFloat]
	IntType       = scalarTypes[TypeInt]
	TextType      = scalarTypes[TypeText]
	TimestampType = scalarTypes[TypeTimestamp]
	UUIDType      = scalarTypes[TypeUUID]
	VarcharType   = scalarTypes[TypeVarchar]
	VarintType    = scalarTypes[TypeVarint]
	TimeUUIDType  = scalarTypes[TypeTimeUUID]
	InetType      = scalarTypes[TypeInet]
	DateType      = scalarTypes[TypeDate]
	TimeType      = scalarTypes[TypeTime]
	SmallIntType  = scalarTypes[TypeSmallInt]
	TinyIntType   = scalarTypes[TypeTinyInt]
	DurationType  = scalarTypes[TypeDuration]
)

// ScalarType returns the shared descriptor for a supported scalar kind. It
// fails only for non-scalar kinds; use the predeclared values such as TextType
// when the kind is known at compile time.
func ScalarType(kind Type) (DataType, error) {
	if err := validateScalarKind(kind); err != nil {
		return DataType{}, err
	}
	return scalarTypes[kind], nil
}

// ValidateScalarKind checks scalar support without constructing a descriptor.
func validateScalarKind(kind Type) error {
	if kind < TypeASCII || kind > TypeDuration {
		return typeValidation("kind", "unsupported scalar kind %v", kind)
	}
	return nil
}

// ListType constructs a CQL list type with the supplied element type.
func ListType(element DataType) (DataType, error) { return collectionDataType(TypeList, element) }

// SetType constructs a CQL set type with the supplied element type.
func SetType(element DataType) (DataType, error) { return collectionDataType(TypeSet, element) }
func collectionDataType(kind Type, element DataType) (DataType, error) {
	if err := validateDataType(element); err != nil {
		return DataType{}, err
	}
	return DataType{descriptor: &dataTypeDescriptor{Kind: kind, Element: element}}, nil
}

// MapType constructs a CQL Map type from its complete type description.
func MapType(key, value DataType) (DataType, error) {
	if err := validateDataType(key); err != nil {
		return DataType{}, err
	}
	if err := validateDataType(value); err != nil {
		return DataType{}, err
	}
	return DataType{descriptor: &dataTypeDescriptor{Kind: TypeMap, Key: key, Element: value}}, nil
}

// TupleType constructs a CQL Tuple type from its complete type description.
func TupleType(fields ...DataType) (DataType, error) {
	if len(fields) > math.MaxUint16 {
		return DataType{}, typeValidation("fields", "tuple requires 0..65535 fields")
	}
	columns := make([]ColumnSpec, len(fields))
	for i, t := range fields {
		if err := validateDataType(t); err != nil {
			return DataType{}, fmt.Errorf("tuple field %d: %w", i, err)
		}
		columns[i].Type = t
	}
	return DataType{descriptor: &dataTypeDescriptor{Kind: TypeTuple, Fields: Columns{columns: columns}}}, nil
}

// UDTType constructs a CQL UDT type from its complete type description.
func UDTType(keyspace, name string, fields ...ColumnSpec) (DataType, error) {
	if keyspace == "" || name == "" {
		return DataType{}, typeValidation("identity", "UDT keyspace and name must not be empty")
	}
	if len(fields) > math.MaxUint16 {
		return DataType{}, typeValidation("fields", "UDT has too many fields")
	}
	seen := make(map[string]struct{}, len(fields))
	for i, c := range fields {
		if c.Name == "" {
			return DataType{}, typeValidation("fields", "UDT field %d has no name", i)
		}
		if _, ok := seen[c.Name]; ok {
			return DataType{}, typeValidation("fields", "duplicate UDT field %q", c.Name)
		}
		seen[c.Name] = struct{}{}
		if err := validateDataType(c.Type); err != nil {
			return DataType{}, fmt.Errorf("UDT field %q: %w", c.Name, err)
		}
	}
	return DataType{descriptor: &dataTypeDescriptor{Kind: TypeUDT, Keyspace: keyspace, Name: name, Fields: NewColumns(fields...)}}, nil
}

// VectorType constructs a vector type with an element type and fixed nonzero dimension.
func VectorType(element DataType, dimension uint16) (DataType, error) {
	if err := validateDataType(element); err != nil {
		return DataType{}, err
	}
	if dimension == 0 {
		return DataType{}, typeValidation("dimension", "vector dimension must be positive")
	}
	return DataType{descriptor: &dataTypeDescriptor{Kind: TypeCustom, Element: element, Dimension: dimension}}, nil
}

// NewColumns snapshots column specifications into immutable metadata.
func NewColumns(columns ...ColumnSpec) Columns {
	return Columns{columns: append([]ColumnSpec(nil), columns...)}
}

// Kind returns the type's kind. A vector is TypeCustom, and so is the invalid
// zero DataType.
func (t DataType) Kind() Type {
	if t.descriptor == nil {
		return TypeCustom
	}
	return t.descriptor.Kind
}

// Frozen reports whether the type is annotated frozen. See FrozenKnown.
func (t DataType) Frozen() bool { return t.descriptor != nil && t.descriptor.frozen }

// FrozenKnown reports whether a schema or caller supplied the frozen annotation.
// Native protocol result metadata does not carry this annotation.
func (t DataType) FrozenKnown() bool {
	return t.descriptor != nil && t.descriptor.frozenKnown
}

// WithFrozen returns a copy of the type with the frozen annotation set to
// frozen, which FrozenKnown then reports. The zero DataType is returned
// unchanged.
func (t DataType) WithFrozen(frozen bool) DataType {
	if t.descriptor == nil {
		return t
	}
	d := *t.descriptor
	d.frozen = frozen
	d.frozenKnown = true
	return DataType{descriptor: &d}
}

// Element returns the element type of a list, set or vector, and false for
// any other type.
func (t DataType) Element() (DataType, bool) {
	if t.descriptor == nil {
		return DataType{}, false
	}
	if t.Kind() == TypeList || t.Kind() == TypeSet || t.descriptor.Dimension != 0 {
		return t.descriptor.Element, true
	}
	return DataType{}, false
}

// KeyValue returns the key and value types of a map, and false for any other
// type.
func (t DataType) KeyValue() (DataType, DataType, bool) {
	if t.descriptor == nil || t.Kind() != TypeMap {
		return DataType{}, DataType{}, false
	}
	return t.descriptor.Key, t.descriptor.Element, true
}

// Fields returns the fields of a tuple or UDT. A tuple's fields have only
// types; a UDT's also have names. It is empty for any other type.
func (t DataType) Fields() Columns {
	if t.descriptor == nil {
		return Columns{}
	}
	return NewColumns(t.descriptor.Fields.columns...)
}

// UDTName returns the keyspace and name of a UDT, and false for any other
// type.
func (t DataType) UDTName() (keyspace, name string, ok bool) {
	if t.descriptor == nil || t.Kind() != TypeUDT {
		return "", "", false
	}
	return t.descriptor.Keyspace, t.descriptor.Name, true
}

// Dimension returns the dimension of a vector, and false for any other type.
func (t DataType) Dimension() (uint16, bool) {
	if t.descriptor == nil || t.descriptor.Dimension == 0 {
		return 0, false
	}
	return t.descriptor.Dimension, true
}

// Len returns the number of columns.
func (c Columns) Len() int { return len(c.columns) }

// At returns the column at index, and false when index is out of range.
func (c Columns) At(index int) (ColumnSpec, bool) {
	if index < 0 || index >= len(c.columns) {
		return ColumnSpec{}, false
	}
	return c.columns[index], true
}

// same reports whether c and other are the same specifications, not only
// equal ones: the same backing, which Columns never modifies. The engine
// compares the same way in internal/protocol.sameColumns.
func (c Columns) same(other Columns) bool {
	return len(c.columns) == len(other.columns) && (len(c.columns) == 0 || &c.columns[0] == &other.columns[0])
}

// Equal reports whether c and other hold the same columns, in order, with the
// same keyspace, table, name and equal type.
func (c Columns) Equal(other Columns) bool {
	if c.same(other) {
		return true
	}
	if len(c.columns) != len(other.columns) {
		return false
	}
	for i, a := range c.columns {
		b := other.columns[i]
		if a.Keyspace != b.Keyspace || a.Table != b.Table || a.Name != b.Name || !a.Type.Equal(b.Type) {
			return false
		}
	}
	return true
}

// Equal reports whether t and other describe the same type, comparing kind,
// UDT identity, dimension and every nested type and field. The frozen
// annotation is not compared.
func (t DataType) Equal(other DataType) bool {
	if t.descriptor == other.descriptor {
		return true
	}
	if t.descriptor == nil || other.descriptor == nil {
		return false
	}
	return equalDataTypes(t, other, make(map[typeDescriptorPair]bool))
}

type typeDescriptorPair struct{ a, b *dataTypeDescriptor }

func equalDataTypes(t, other DataType, seen map[typeDescriptorPair]bool) bool {
	if t.descriptor == other.descriptor {
		return true
	}
	if t.descriptor == nil || other.descriptor == nil {
		return false
	}
	key := typeDescriptorPair{t.descriptor, other.descriptor}
	if seen[key] {
		return true
	}
	a, b := t.descriptor, other.descriptor
	if a.Kind != b.Kind || a.Dimension != b.Dimension || a.Keyspace != b.Keyspace || a.Name != b.Name || a.Fields.Len() != b.Fields.Len() {
		return false
	}
	seen[key] = true
	if !equalDataTypes(a.Element, b.Element, seen) || !equalDataTypes(a.Key, b.Key, seen) {
		return false
	}
	for i, x := range a.Fields.columns {
		y := b.Fields.columns[i]
		if x.Name != y.Name || x.Table != y.Table || x.Keyspace != y.Keyspace || !equalDataTypes(x.Type, y.Type, seen) {
			return false
		}
	}
	return true
}

// fingerprint is an unambiguous structural encoding, not a hash. Equality still
// compares descriptors directly and does not depend on this representation.
func (t DataType) fingerprint() string {
	var b strings.Builder
	var number [8]byte
	putNumber := func(n uint64) { binary.BigEndian.PutUint64(number[:], n); b.Write(number[:]) }
	putString := func(s string) { putNumber(uint64(len(s))); b.WriteString(s) }
	var appendType func(DataType)
	appendType = func(t DataType) {
		if t.descriptor == nil {
			b.WriteByte(0)
			return
		}
		b.WriteByte(1)
		d := t.descriptor
		putNumber(uint64(d.Kind))
		putNumber(uint64(d.Dimension))
		putString(d.Keyspace)
		putString(d.Name)
		appendType(d.Key)
		appendType(d.Element)
		putNumber(uint64(d.Fields.Len()))
		for _, c := range d.Fields.columns {
			putString(c.Keyspace)
			putString(c.Table)
			putString(c.Name)
			appendType(c.Type)
		}
	}
	appendType(t)
	return b.String()
}

// String returns the type in CQL syntax, such as "map<text,int>" or
// "frozen<ks.address>", or "<invalid>" for the zero DataType.
func (t DataType) String() string {
	if t.descriptor == nil {
		return "<invalid>"
	}
	d := t.descriptor
	var s string
	switch d.Kind {
	case TypeList, TypeSet:
		s = fmt.Sprintf("%s<%s>", d.Kind, d.Element)
	case TypeMap:
		s = fmt.Sprintf("map<%s,%s>", d.Key, d.Element)
	case TypeTuple:
		fields := make([]string, d.Fields.Len())
		for i, c := range d.Fields.columns {
			fields[i] = c.Type.String()
		}
		s = "tuple<" + strings.Join(fields, ",") + ">"
	case TypeUDT:
		s = d.Keyspace + "." + d.Name
	case TypeCustom:
		s = fmt.Sprintf("vector<%s,%d>", d.Element, d.Dimension)
	default:
		s = d.Kind.String()
	}
	if d.frozen {
		return "frozen<" + s + ">"
	}
	return s
}
func validateDataType(t DataType) error {
	if t.descriptor == nil {
		return typeValidation("type", "invalid zero DataType")
	}
	return nil
}
func typeValidation(field, format string, args ...any) error {
	return &ValidationError{Field: field, Err: fmt.Errorf(format, args...)}
}

// CellState is the state of a cell: present, NULL, UNSET or empty. The zero
// CellState is Present. Later releases may add values, so a switch over them
// needs a default case.
type CellState uint8

// Cell states.
const (
	// Present is a cell holding a value.
	Present CellState = iota

	// Null is a CQL NULL.
	Null

	// Unset is a bound parameter left unset: the server leaves its column
	// unchanged.
	Unset

	// EmptyCell is an empty value: present, with a zero-length body. Only some
	// types support it; see EmptyCodec.
	EmptyCell
)

// String returns the cell state name, or CellState(n) for a value it does
// not define.
func (s CellState) String() string {
	switch s {
	case Present:
		return "present"
	case Null:
		return "null"
	case Unset:
		return "unset"
	case EmptyCell:
		return "empty"
	default:
		return fmt.Sprintf("CellState(%d)", uint8(s))
	}
}

// Nullable represents a value or CQL NULL; its zero value is NULL.
// It supports result decoding and binding nullable application fields.
// Use BindValue for parameters that also need UNSET.
type Nullable[T any] struct {
	// Value is the value when Valid is true.
	Value T
	// Valid reports that the value is not NULL.
	Valid bool
}

// BindValue controls parameters using Present, Null, or Unset.
// Its zero value binds the ordinary zero value of T. Binding fails for any
// other State, EmptyCell included; to bind an empty value, wrap the base
// codec with EmptyCodec and bind a MaybeEmpty.
type BindValue[T any] struct {
	// Value is the value bound when State is Present.
	Value T
	// State is Present, Null or Unset.
	State CellState
}

// MaybeEmpty distinguishes an empty CQL value from an ordinary value. Its zero
// value encodes the ordinary zero value of T; Empty=true selects empty.
type MaybeEmpty[T any] struct {
	// Value is the value encoded when Empty is false.
	Value T
	// Empty selects the empty value instead of Value.
	Empty bool
}
