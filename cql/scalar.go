// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"net/netip"
	"unicode/utf8"

	"github.com/scylladb/gocql/v2/internal/wire"
)

// scalarCodec contains immutable operations shared by all calls to a codec.
type scalarCodec[T any] struct {
	encode      func(*ValueWriter, T) error
	decode      func(ValueReader) (T, error)
	decodeRow   func(*RowReader, Kind, Kind, bool) (T, error)
	clone       func(T) T
	kind        Kind
	alternative Kind
	compiled    bool
}

func checkScalarType(t DataType, kind, alternative Kind) error {
	if k := t.Kind(); k != kind && (alternative == 0 || k != alternative) {
		return scalarTypeError(kind, k)
	}
	return nil
}

//go:noinline
func scalarTypeError(kind, got Kind) error {
	return fmt.Errorf("gocql: codec for %s cannot use %s", kind, got)
}

func (c scalarCodec[T]) CheckType(t DataType) error {
	perfCheckType()

	return checkScalarType(t, c.kind, c.alternative)
}

func (c scalarCodec[T]) Encode(w *ValueWriter, v T) error {
	if err := c.typeGuard(w.Type()); err != nil {
		return err
	}
	return c.encode(w, v)
}
func (c scalarCodec[T]) Decode(r ValueReader) (T, error) {
	var zero T
	if err := c.typeGuard(r.Type()); err != nil {
		return zero, err
	}
	if err := requirePresent(r); err != nil {
		return zero, err
	}
	return c.decode(r)
}
func (c scalarCodec[T]) Clone(v T) T {
	if c.clone != nil {
		return c.clone(v)
	}
	return v
}

func requirePresent(r ValueReader) error {
	if r.err != nil {
		return r.err
	}
	if r.state != Present {
		return cellStateError(r)
	}
	if !r.lease.IsZero() && !r.lease.Alive() {
		return ErrReleased
	}
	return nil
}

//go:noinline
func cellStateError(r ValueReader) error {
	if r.state == Null {
		return nullCellError{kind: r.typ.Kind()}
	}
	return fmt.Errorf("gocql: nonnullable %s cannot decode cell state %v", r.typ.Kind(), r.state)
}

// fixedValue loads a contiguous fixed-width body without building a cursor.
// Callers run after requirePresent, which checked the lease.
func fixedValue(r ValueReader, width int) (uint64, bool) {
	data, ok := r.body.Contiguous()
	if !ok || len(data) != width {
		return 0, false
	}
	switch width {
	case 1:
		return uint64(data[0]), true
	case 2:
		return uint64(binary.BigEndian.Uint16(data)), true
	case 4:
		return uint64(binary.BigEndian.Uint32(data)), true
	default:
		return binary.BigEndian.Uint64(data), true
	}
}

func fixedCursor(r ValueReader, width int64) (c ValueCursor, err error) {
	if n := r.body.Len(); n != width {
		return ValueCursor{}, fixedWidthError(r, width, n)
	}
	c = cursorFor(&r)
	return c, nil
}

//go:noinline
func fixedWidthError(r ValueReader, width, got int64) error {
	return fmt.Errorf("gocql: %s requires %d bytes, got %d", r.typ.Kind(), width, got)
}

type signedScalar interface {
	~int8 | ~int16 | ~int32 | ~int64
}

// integerColumn decodes a signed integer cell of width bytes from the row,
// sign-extended. Row plans call it directly for built-in integer codecs.
func integerColumn(r *RowReader, kind, alternative Kind, compiled bool, width int64) (int64, error) {
	if _, err := r.scalarColumn(kind, alternative, compiled, width); err != nil {
		return 0, err
	}
	// scalarColumn checked the lease for this present cell; the body is
	// loaded without a second check.
	if body, ok := r.cursor.HeadBytes(int(width)); ok {
		r.cursor.AdvanceHead(int(width))
		switch width {
		case 1:
			return int64(int8(body[0])), nil
		case 2:
			return int64(int16(binary.BigEndian.Uint16(body))), nil
		case 4:
			return int64(int32(binary.BigEndian.Uint32(body))), nil
		default:
			return int64(binary.BigEndian.Uint64(body)), nil
		}
	}
	switch width {
	case 1:
		v, err := r.cursor.Uint8()
		return int64(int8(v)), err
	case 2:
		v, err := r.cursor.Uint16()
		return int64(int16(v)), err
	case 4:
		v, err := r.cursor.Uint32()
		return int64(int32(v)), err
	default:
		v, err := r.cursor.Uint64()
		return int64(v), err
	}
}

// blobColumn decodes a blob cell as a view borrowing the row's lease.
func blobColumn(r *RowReader, kind, alternative Kind, compiled bool) (Bytes, error) {
	length, err := r.scalarColumn(kind, alternative, compiled, 0)
	if err != nil {
		return Bytes{}, err
	}
	body, err := r.cursor.Take(length)
	if err != nil {
		return Bytes{}, err
	}
	return bytesView(body, r.lease), nil
}

// textColumn decodes a text or ascii cell like textCodec.Decode, validating
// the view it borrows from the response for the column's kind.
func textColumn(r *RowReader, compiled bool) (Text, error) {
	ascii := false
	if r != nil {
		if column, ok := r.columns.At(r.index); ok {
			ascii = column.Type.Kind() == KindASCII
		}
	}
	v, err := blobColumn(r, KindText, KindASCII, compiled)
	if err != nil {
		return Text{}, err
	}
	if err := validateText(v, ascii); err != nil {
		return Text{}, err
	}
	return textView(v), nil
}

// counterCodec is the int64 codec, which also accepts counter: a counter is a
// bigint the server only adds to.
func counterCodec() Codec[int64] {
	c := integerCodec[int64](KindBigInt, 8).(scalarCodec[int64])
	c.alternative = KindCounter
	return c
}

// intCodec is IntCodec: Int64Codec with a range check on decode.
func intCodec() Codec[int] {
	base := counterCodec().(scalarCodec[int64])
	return scalarCodec[int]{kind: base.kind, alternative: base.alternative,
		encode: func(w *ValueWriter, v int) error { return base.encode(w, int64(v)) },
		decode: func(r ValueReader) (int, error) {
			v, err := base.decode(r)
			if err != nil {
				return 0, err
			}
			if int64(int(v)) != v {
				return 0, fmt.Errorf("gocql: bigint %d overflows int", v)
			}
			return int(v), nil
		},
	}
}

func integerCodec[T signedScalar](kind Kind, width int64) Codec[T] {
	return scalarCodec[T]{kind: kind,
		decodeRow: func(r *RowReader, kind, alternative Kind, compiled bool) (T, error) {
			v, err := integerColumn(r, kind, alternative, compiled, width)
			return T(v), err
		},
		encode: func(w *ValueWriter, v T) error {
			switch width {
			case 1:
				return w.Uint8(uint8(v))
			case 2:
				return w.Uint16(uint16(v))
			case 4:
				return w.Uint32(uint32(v))
			default:
				return w.Uint64(uint64(v))
			}
		},
		decode: func(r ValueReader) (T, error) {
			if v, ok := fixedValue(r, int(width)); ok {
				switch width {
				case 1:
					return T(int8(v)), nil
				case 2:
					return T(int16(v)), nil
				case 4:
					return T(int32(v)), nil
				default:
					return T(int64(v)), nil
				}
			}
			c, err := fixedCursor(r, width)
			if err != nil {
				return 0, err
			}
			switch width {
			case 1:
				v, e := c.Uint8()
				return T(int8(v)), e
			case 2:
				v, e := c.Uint16()
				return T(int16(v)), e
			case 4:
				v, e := c.Uint32()
				return T(int32(v)), e
			default:
				v, e := c.Uint64()
				return T(int64(v)), e
			}
		},
	}
}

// Codecs of the scalar CQL types. Each accepts only the CQL types it names,
// and decodes a value without borrowing response storage unless its Go type
// is a view (Text, Bytes).
var (
	// BoolCodec maps boolean to bool.
	BoolCodec Codec[bool] = scalarCodec[bool]{kind: KindBoolean, encode: encodeBool, decode: decodeBool}

	// Int8Codec maps tinyint to int8.
	Int8Codec Codec[int8] = integerCodec[int8](KindTinyInt, 1)

	// Int16Codec maps smallint to int16.
	Int16Codec Codec[int16] = integerCodec[int16](KindSmallInt, 2)

	// Int32Codec maps int to int32.
	Int32Codec Codec[int32] = integerCodec[int32](KindInt, 4)

	// Int64Codec maps bigint and counter to int64.
	Int64Codec Codec[int64] = counterCodec()

	// IntCodec maps bigint and counter to int, as Int64Codec does to int64.
	// Decoding fails for a value that does not fit an int, which only a
	// 32-bit platform has.
	IntCodec Codec[int] = intCodec()

	// Float32Codec maps float to float32.
	Float32Codec Codec[float32] = scalarCodec[float32]{kind: KindFloat,
		encode: func(w *ValueWriter, v float32) error { return w.Uint32(math.Float32bits(v)) },
		decode: func(r ValueReader) (float32, error) {
			if v, ok := fixedValue(r, 4); ok {
				return math.Float32frombits(uint32(v)), nil
			}
			c, e := fixedCursor(r, 4)
			if e != nil {
				return 0, e
			}
			v, e := c.Uint32()
			return math.Float32frombits(v), e
		},
	}

	// Float64Codec maps double to float64.
	Float64Codec Codec[float64] = scalarCodec[float64]{kind: KindDouble,
		encode: func(w *ValueWriter, v float64) error { return w.Uint64(math.Float64bits(v)) },
		decode: func(r ValueReader) (float64, error) {
			if v, ok := fixedValue(r, 8); ok {
				return math.Float64frombits(v), nil
			}
			c, e := fixedCursor(r, 8)
			if e != nil {
				return 0, e
			}
			v, e := c.Uint64()
			return math.Float64frombits(v), e
		},
	}

	// UUIDCodec maps uuid and timeuuid to UUID. It rejects encoding a UUID
	// that is not version 1 into a timeuuid; decoding a timeuuid returns its
	// UUID as is.
	UUIDCodec Codec[UUID] = scalarCodec[UUID]{kind: KindUUID, alternative: KindTimeUUID, encode: encodeAnyUUID, decode: decodeUUID}

	// TimeUUIDCodec maps timeuuid to TimeUUID, rejecting a UUID that is not
	// version 1.
	TimeUUIDCodec Codec[TimeUUID] = scalarCodec[TimeUUID]{kind: KindTimeUUID, encode: encodeTimeUUID, decode: decodeTimeUUID}

	// InetCodec maps inet to netip.Addr, as a 4- or 16-byte address.
	InetCodec Codec[netip.Addr] = scalarCodec[netip.Addr]{kind: KindInet, encode: encodeInet, decode: decodeInet}

	// StringCodec maps text and ascii to string. It decodes into a newly
	// allocated string that stays valid after the row is released; use
	// TextCodec to decode without copying. A value must be valid UTF-8 for
	// text and ASCII for ascii, both when it is encoded and decoded.
	StringCodec Codec[string] = stringCodec{}

	// TextCodec maps text and ascii to Text, validating UTF-8 for text and
	// ASCII for ascii. A decoded Text borrows the response; Clone or
	// StringCodec detach it.
	TextCodec Codec[Text] = textCodec{}

	// BytesCodec maps blob to Bytes. A decoded Bytes borrows the response, and
	// encoding borrows the Bytes as ValueWriter.BorrowBytes does; Clone
	// detaches it.
	BytesCodec Codec[Bytes] = scalarCodec[Bytes]{kind: KindBlob,
		decodeRow: blobColumn,
		encode:    func(w *ValueWriter, v Bytes) error { return w.borrowView(v) },
		decode:    func(r ValueReader) (Bytes, error) { return r.Bytes(), nil },
		clone:     func(v Bytes) Bytes { return v.Clone() },
	}
)

func encodeBool(w *ValueWriter, v bool) error {
	if v {
		return w.Uint8(1)
	}
	return w.Uint8(0)
}
func decodeBool(r ValueReader) (bool, error) {
	v, ok := fixedValue(r, 1)
	if !ok {
		c, e := fixedCursor(r, 1)
		if e != nil {
			return false, e
		}
		b, e := c.Uint8()
		if e != nil {
			return false, e
		}
		v = uint64(b)
	}
	if v > 1 {
		return false, errors.New("gocql: boolean must be 0 or 1")
	}
	return v == 1, nil
}
func encodeUUID(w *ValueWriter, v UUID) error {
	p, e := w.scalarBytes(16)
	if e != nil {
		return e
	}
	copy(p, v[:])
	return nil
}
func decodeUUID(r ValueReader) (UUID, error) {
	var v UUID
	if data, ok := r.body.Contiguous(); ok && len(data) == 16 {
		copy(v[:], data)
		return v, nil
	}
	c, e := fixedCursor(r, 16)
	if e != nil {
		return v, e
	}
	hi, e := c.Uint64()
	if e != nil {
		return v, e
	}
	lo, e := c.Uint64()
	if e != nil {
		return v, e
	}
	binary.BigEndian.PutUint64(v[:8], hi)
	binary.BigEndian.PutUint64(v[8:], lo)
	return v, nil
}

// encodeAnyUUID writes a UUID into a uuid or a timeuuid, which must hold a
// version 1 UUID.
func encodeAnyUUID(w *ValueWriter, v UUID) error {
	if w.Type().Kind() == KindTimeUUID && v.Version() != 1 {
		return errors.New("gocql: timeuuid must have version 1")
	}
	return encodeUUID(w, v)
}
func encodeTimeUUID(w *ValueWriter, v TimeUUID) error {
	if UUID(v).Version() != 1 {
		return errors.New("gocql: timeuuid must have version 1")
	}
	return encodeUUID(w, UUID(v))
}
func decodeTimeUUID(r ValueReader) (TimeUUID, error) {
	v, e := decodeUUID(r)
	if e != nil {
		return TimeUUID{}, e
	}
	if v.Version() != 1 {
		return TimeUUID{}, errors.New("gocql: timeuuid must have version 1")
	}
	return TimeUUID(v), nil
}
func encodeInet(w *ValueWriter, v netip.Addr) error {
	if !v.IsValid() || v.Zone() != "" {
		return errors.New("gocql: inet requires a valid address without a zone")
	}
	if v.Is4() {
		a := v.As4()
		return w.Uint32(binary.BigEndian.Uint32(a[:]))
	}
	a := v.As16()
	p, e := w.scalarBytes(16)
	if e != nil {
		return e
	}
	copy(p, a[:])
	return nil
}
func decodeInet(r ValueReader) (netip.Addr, error) {
	c := r.cursorValue()
	switch r.Bytes().Len() {
	case 4:
		v, e := c.Uint32()
		if e != nil {
			return netip.Addr{}, e
		}
		var a [4]byte
		binary.BigEndian.PutUint32(a[:], v)
		return netip.AddrFrom4(a), nil
	case 16:
		hi, e := c.Uint64()
		if e != nil {
			return netip.Addr{}, e
		}
		lo, e := c.Uint64()
		if e != nil {
			return netip.Addr{}, e
		}
		var a [16]byte
		binary.BigEndian.PutUint64(a[:8], hi)
		binary.BigEndian.PutUint64(a[8:], lo)
		return netip.AddrFrom16(a), nil
	default:
		return netip.Addr{}, errors.New("gocql: inet requires 4 or 16 bytes")
	}
}

type stringCodec struct{ compiled bool }

func checkTextType(t DataType) error {
	if k := t.Kind(); k == KindText || k == KindASCII {
		return nil
	}
	return fmt.Errorf("gocql: incompatible text type %s", t.Kind())
}
func (c stringCodec) CheckType(t DataType) error {
	perfCheckType()
	return checkTextType(t)
}
func (c stringCodec) Encode(w *ValueWriter, v string) error {
	if e := c.typeGuard(w.Type()); e != nil {
		return e
	}
	if w.Type().Kind() == KindASCII {
		for i := 0; i < len(v); i++ {
			if v[i] >= utf8.RuneSelf {
				return errors.New("gocql: invalid ASCII")
			}
		}
	} else if !utf8.ValidString(v) {
		return errors.New("gocql: invalid UTF-8")
	}
	return w.BorrowText(v)
}

// Decode copies the text value into a new string.
func (c stringCodec) Decode(r ValueReader) (string, error) {
	t, err := textCodec(c).Decode(r)
	if err != nil {
		return "", err
	}
	return t.String(), nil
}
func (stringCodec) Clone(v string) string { return v }
func (c stringCodec) compileValueDecoder(t DataType) (Decoder[string], error) {
	if e := c.CheckType(t); e != nil {
		return nil, e
	}
	c.compiled = true
	return c, nil
}

type textCodec struct{ compiled bool }

func (c textCodec) CheckType(t DataType) error {
	perfCheckType()
	return checkTextType(t)
}
func (c textCodec) Encode(w *ValueWriter, v Text) error {
	if e := c.typeGuard(w.Type()); e != nil {
		return e
	}
	if e := validateText(v.Bytes(), w.Type().Kind() == KindASCII); e != nil {
		return e
	}
	return w.borrowView(v.Bytes())
}
func (c textCodec) Decode(r ValueReader) (Text, error) {
	if e := c.typeGuard(r.Type()); e != nil {
		return Text{}, e
	}
	if e := requirePresent(r); e != nil {
		return Text{}, e
	}
	if e := validateText(r.Bytes(), r.Type().Kind() == KindASCII); e != nil {
		return Text{}, e
	}
	return textView(r.Bytes()), nil
}
func (textCodec) Clone(v Text) Text { return v.Clone() }

// textValidation shares incremental UTF-8/ASCII rules between borrowed spans
// and final-storage streaming. It never constructs a string or retains a chunk.
type textValidation struct {
	remaining int
	ascii     bool
	low, high byte
}

func newTextValidation(ascii bool) textValidation {
	return textValidation{ascii: ascii, low: 0x80, high: 0xbf}
}
func (v *textValidation) write(b []byte) error {
	for _, c := range b {
		if v.ascii {
			if c >= utf8.RuneSelf {
				return errors.New("gocql: invalid ASCII")
			}
			continue
		}
		if v.remaining > 0 {
			if c < v.low || c > v.high {
				return errors.New("gocql: invalid UTF-8")
			}
			v.remaining--
			v.low, v.high = 0x80, 0xbf
			continue
		}
		switch {
		case c < 0x80:
		case c >= 0xc2 && c <= 0xdf:
			v.remaining = 1
		case c >= 0xe0 && c <= 0xef:
			v.remaining = 2
			if c == 0xe0 {
				v.low = 0xa0
			}
			if c == 0xed {
				v.high = 0x9f
			}
		case c >= 0xf0 && c <= 0xf4:
			v.remaining = 3
			if c == 0xf0 {
				v.low = 0x90
			}
			if c == 0xf4 {
				v.high = 0x8f
			}
		default:
			return errors.New("gocql: invalid UTF-8")
		}
	}
	return nil
}
func (v *textValidation) finish() error {
	if v.remaining != 0 {
		return errors.New("gocql: truncated UTF-8")
	}
	return nil
}
func validateText(b Bytes, ascii bool) error {
	if e := checkBytesAlive(b); e != nil {
		return e
	}
	if data, ok := bytesChain(b).Contiguous(); ok {
		if !ascii {
			if !utf8.Valid(data) {
				return errors.New("gocql: invalid UTF-8")
			}
			return nil
		}
		for _, c := range data {
			if c >= utf8.RuneSelf {
				return errors.New("gocql: invalid ASCII")
			}
		}
		return nil
	}
	v := newTextValidation(ascii)
	if err := eachBytes(b, func(s wire.Span) error { return v.write(s.Bytes()) }); err != nil {
		return err
	}
	return v.finish()
}

// ConvertCodec maps A through base, which maps B: to converts a value
// before base encodes it, and from converts what base decodes. Use it for an
// application type with no built-in codec, such as one that wraps or packs
// a built-in type. Clone clones through base.
func ConvertCodec[A, B any](base Codec[B], to func(A) B, from func(B) A) Codec[A] {
	return convertCodec[A, B]{base: base, to: to, from: from}
}

type convertCodec[A, B any] struct {
	base    Codec[B]
	to      func(A) B
	from    func(B) A
	encoder Encoder[B]
	decoder Decoder[B]
	typ     DataType
}

func (c convertCodec[A, B]) CheckType(t DataType) error {
	perfCheckType()

	if descriptorOf(c.typ) != nil {
		if c.typ.Equal(t) {
			return nil
		}
		return typeValidation("type", "compiled mapped scalar metadata changed")
	}
	if c.base == nil || c.to == nil || c.from == nil {
		return errors.New("gocql: incomplete named scalar adapter")
	}
	return c.base.CheckType(t)
}
func (c convertCodec[A, B]) Encode(w *ValueWriter, v A) error {
	if e := c.typeGuard(w.Type()); e != nil {
		return e
	}
	encoder := c.encoder
	if encoder == nil {
		encoder = c.base
	}
	return encoder.Encode(w, c.to(v))
}
func (c convertCodec[A, B]) Decode(r ValueReader) (A, error) {
	var zero A
	if e := c.typeGuard(r.Type()); e != nil {
		return zero, e
	}
	decoder := c.decoder
	if decoder == nil {
		decoder = c.base
	}
	v, e := decoder.Decode(r)
	if e != nil {
		return zero, e
	}
	return c.from(v), nil
}
func (c convertCodec[A, B]) Clone(v A) A { return c.from(c.base.Clone(c.to(v))) }

// NullableCodec adds NULL encoding and decoding to base.
func NullableCodec[T any](base Codec[T]) Codec[Nullable[T]] {
	return nullableCodec[T]{nullableEncoder[T]{base: base}, nullableDecoder[T]{base: base}}
}

// NullableEncoder encodes invalid Nullable values as NULL and valid values with base.
func NullableEncoder[T any](base Encoder[T]) Encoder[Nullable[T]] {
	return nullableEncoder[T]{base: base}
}

// NullableDecoder preserves NULL separately from the decoded zero value.
func NullableDecoder[T any](base Decoder[T]) Decoder[Nullable[T]] {
	return nullableDecoder[T]{base: base}
}

type nullableEncoder[T any] struct {
	base Encoder[T]
	typ  DataType
}

func (c nullableEncoder[T]) CheckType(t DataType) error {
	perfCheckType()

	if descriptorOf(c.typ) != nil {
		if c.typ.Equal(t) {
			return nil
		}
		return typeValidation("type", "compiled wrapper metadata changed")
	}
	if c.base == nil {
		return errors.New("gocql: nil nullable encoder")
	}
	return c.base.CheckType(t)
}
func (c nullableEncoder[T]) Encode(w *ValueWriter, v Nullable[T]) error {
	if e := c.typeGuard(w.Type()); e != nil {
		return e
	}
	if !v.Valid {
		return w.Null()
	}
	return c.base.Encode(w, v.Value)
}

type nullableDecoder[T any] struct {
	base Decoder[T]
	typ  DataType
}

func (c nullableDecoder[T]) CheckType(t DataType) error {
	perfCheckType()

	if descriptorOf(c.typ) != nil {
		if c.typ.Equal(t) {
			return nil
		}
		return typeValidation("type", "compiled wrapper metadata changed")
	}
	if c.base == nil {
		return errors.New("gocql: nil nullable decoder")
	}
	return c.base.CheckType(t)
}
func (c nullableDecoder[T]) Decode(r ValueReader) (Nullable[T], error) {
	if e := c.typeGuard(r.Type()); e != nil {
		return Nullable[T]{}, e
	}
	if r.err != nil {
		return Nullable[T]{}, r.err
	}
	if r.State() == Null {
		if e := checkBytesAlive(r.Bytes()); e != nil {
			return Nullable[T]{}, e
		}
		return Nullable[T]{}, nil
	}
	v, e := c.base.Decode(r)
	if e != nil {
		return Nullable[T]{}, e
	}
	return Nullable[T]{Value: v, Valid: true}, nil
}
func (c nullableDecoder[T]) Clone(v Nullable[T]) Nullable[T] {
	if v.Valid {
		v.Value = c.base.Clone(v.Value)
		return v
	}
	return Nullable[T]{}
}

type nullableCodec[T any] struct {
	nullableEncoder[T]
	nullableDecoder[T]
}

func (c nullableCodec[T]) CheckType(t DataType) error {
	perfCheckType()
	return c.nullableEncoder.CheckType(t)
}

// BindEncoder wraps base to encode Present, Null and Unset parameter states.
func BindEncoder[T any](base Encoder[T]) Encoder[BindValue[T]] {
	return bindEncoder[T]{base: base}
}

type bindEncoder[T any] struct {
	base Encoder[T]
	typ  DataType
}

func (c bindEncoder[T]) CheckType(t DataType) error {
	perfCheckType()

	if descriptorOf(c.typ) != nil {
		if c.typ.Equal(t) {
			return nil
		}
		return typeValidation("type", "compiled wrapper metadata changed")
	}
	if c.base == nil {
		return errors.New("gocql: nil bind encoder")
	}
	return c.base.CheckType(t)
}
func (c bindEncoder[T]) Encode(w *ValueWriter, v BindValue[T]) error {
	if e := c.typeGuard(w.Type()); e != nil {
		return e
	}
	switch v.State {
	case Present:
		return c.base.Encode(w, v.Value)
	case Null:
		return w.Null()
	case Unset:
		return w.Unset()
	default:
		return errors.New("gocql: invalid bind state")
	}
}

// EmptyCodec adds the explicit empty-cell state to base; it does not add NULL support.
func EmptyCodec[T any](base Codec[T]) Codec[MaybeEmpty[T]] {
	return emptyCodec[T]{base: base}
}

type emptyCodec[T any] struct {
	base    Codec[T]
	encoder Encoder[T]
	decoder Decoder[T]
	typ     DataType
}

func emptySupported(t DataType) bool {
	switch t.Kind() {
	case KindBoolean, KindTinyInt, KindSmallInt, KindInt, KindBigInt, KindFloat, KindDouble, KindDate, KindTime, KindTimestamp, KindUUID, KindTimeUUID, KindInet, KindVarint, KindDecimal, KindTuple:
		return true
	}
	return false
}
func (c emptyCodec[T]) CheckType(t DataType) error {
	perfCheckType()

	if descriptorOf(c.typ) != nil {
		if c.typ.Equal(t) {
			return nil
		}
		return typeValidation("type", "compiled wrapper metadata changed")
	}
	if c.base == nil {
		return errors.New("gocql: nil empty codec")
	}
	if e := c.base.CheckType(t); e != nil {
		return e
	}
	if !emptySupported(t) {
		return fmt.Errorf("gocql: %s does not support Empty", t.Kind())
	}
	return nil
}
func (c emptyCodec[T]) Encode(w *ValueWriter, v MaybeEmpty[T]) error {
	if e := c.typeGuard(w.Type()); e != nil {
		return e
	}
	if v.Empty {
		return w.emptyValue()
	}
	if c.encoder != nil {
		return c.encoder.Encode(w, v.Value)
	}
	return c.base.Encode(w, v.Value)
}
func (c emptyCodec[T]) Decode(r ValueReader) (MaybeEmpty[T], error) {
	if e := c.typeGuard(r.Type()); e != nil {
		return MaybeEmpty[T]{}, e
	}
	if r.err != nil {
		return MaybeEmpty[T]{}, r.err
	}
	if r.State() == Present && r.Bytes().Len() == 0 {
		if e := checkBytesAlive(r.Bytes()); e != nil {
			return MaybeEmpty[T]{}, e
		}
		return MaybeEmpty[T]{Empty: true}, nil
	}
	decoder := c.decoder
	if decoder == nil {
		decoder = c.base
	}
	v, e := decoder.Decode(r)
	if e != nil {
		return MaybeEmpty[T]{}, e
	}
	return MaybeEmpty[T]{Value: v}, nil
}
func (c emptyCodec[T]) Clone(v MaybeEmpty[T]) MaybeEmpty[T] {
	if !v.Empty {
		v.Value = c.base.Clone(v.Value)
		return v
	}
	return MaybeEmpty[T]{Empty: true}
}

func (c nullableEncoder[T]) compileValueEncoder(t DataType) (Encoder[Nullable[T]], error) {
	base, err := compileValueEncoder(c.base, t)
	if err != nil {
		return nil, err
	}
	c.base, c.typ = base, t
	return c, nil
}
func (c nullableDecoder[T]) compileValueDecoder(t DataType) (Decoder[Nullable[T]], error) {
	base, err := compileValueDecoder(c.base, t)
	if err != nil {
		return nil, err
	}
	c.base, c.typ = base, t
	return c, nil
}
func (c nullableDecoder[T]) DecodeInto(r ValueReader, dst *Nullable[T]) error {
	if dst == nil {
		return typeValidation("destination", "nil nullable destination")
	}
	*dst = Nullable[T]{}
	if err := c.typeGuard(r.Type()); err != nil {
		return err
	}
	if r.err != nil {
		return r.err
	}
	if r.State() == Null {
		return checkBytesAlive(r.Bytes())
	}
	if err := decodeValueInto(c.base, r, &dst.Value); err != nil {
		return err
	}
	dst.Valid = true
	return nil
}
func (c bindEncoder[T]) compileValueEncoder(t DataType) (Encoder[BindValue[T]], error) {
	base, err := compileValueEncoder(c.base, t)
	if err != nil {
		return nil, err
	}
	c.base, c.typ = base, t
	return c, nil
}
func (c emptyCodec[T]) compileValueEncoder(t DataType) (Encoder[MaybeEmpty[T]], error) {
	if err := c.CheckType(t); err != nil {
		return nil, err
	}
	base, err := compileValueEncoder(c.base, t)
	if err != nil {
		return nil, err
	}
	c.encoder, c.typ = base, t
	return c, nil
}
func (c emptyCodec[T]) compileValueDecoder(t DataType) (Decoder[MaybeEmpty[T]], error) {
	if err := c.CheckType(t); err != nil {
		return nil, err
	}
	base, err := compileValueDecoder(c.base, t)
	if err != nil {
		return nil, err
	}
	c.decoder, c.typ = base, t
	return c, nil
}
func (c emptyCodec[T]) DecodeInto(r ValueReader, dst *MaybeEmpty[T]) error {
	if dst == nil {
		return typeValidation("destination", "nil empty destination")
	}
	*dst = MaybeEmpty[T]{}
	if err := c.typeGuard(r.Type()); err != nil {
		return err
	}
	if r.err != nil {
		return r.err
	}
	if r.State() == Present && r.Bytes().Len() == 0 {
		if err := checkBytesAlive(r.Bytes()); err != nil {
			return err
		}
		dst.Empty = true
		return nil
	}
	decoder := c.decoder
	if decoder == nil {
		decoder = c.base
	}
	return decodeValueInto(decoder, r, &dst.Value)
}

func (c convertCodec[A, B]) compileValueEncoder(t DataType) (Encoder[A], error) {
	if c.to == nil || c.from == nil {
		return nil, typeValidation("codec", "nil scalar mapping")
	}
	base, err := compileValueEncoder(c.base, t)
	if err != nil {
		return nil, err
	}
	c.encoder, c.typ = base, t
	return c, nil
}
func (c convertCodec[A, B]) compileValueDecoder(t DataType) (Decoder[A], error) {
	if c.to == nil || c.from == nil {
		return nil, typeValidation("codec", "nil scalar mapping")
	}
	base, err := compileValueDecoder(c.base, t)
	if err != nil {
		return nil, err
	}
	c.decoder, c.typ = base, t
	return c, nil
}

// Compiled scalar codecs retain constant metadata safety guards while moving
// the CheckType contract to compilation. They still check state/lease/width and
// content exactly as the standalone public codecs do.
func (c scalarCodec[T]) typeGuard(t DataType) error {
	if c.compiled {
		return checkScalarType(t, c.kind, c.alternative)
	}
	return c.CheckType(t)
}
func (c scalarCodec[T]) compileValueDecoder(t DataType) (Decoder[T], error) {
	if e := c.CheckType(t); e != nil {
		return nil, e
	}
	c.compiled = true
	return c, nil
}
func (c scalarCodec[T]) compileValueEncoder(t DataType) (Encoder[T], error) {
	if e := c.CheckType(t); e != nil {
		return nil, e
	}
	c.compiled = true
	return c, nil
}
func (c textCodec) typeGuard(t DataType) error {
	if c.compiled {
		return checkTextType(t)
	}
	return c.CheckType(t)
}
func (c textCodec) compileValueDecoder(t DataType) (Decoder[Text], error) {
	if e := c.CheckType(t); e != nil {
		return nil, e
	}
	c.compiled = true
	return c, nil
}
func (c textCodec) compileValueEncoder(t DataType) (Encoder[Text], error) {
	if e := c.CheckType(t); e != nil {
		return nil, e
	}
	c.compiled = true
	return c, nil
}
func (c stringCodec) typeGuard(t DataType) error {
	if c.compiled {
		return checkTextType(t)
	}
	return c.CheckType(t)
}
func (c stringCodec) compileValueEncoder(t DataType) (Encoder[string], error) {
	if e := c.CheckType(t); e != nil {
		return nil, e
	}
	c.compiled = true
	return c, nil
}

func (c convertCodec[A, B]) typeGuard(t DataType) error {
	if descriptorOf(c.typ) == nil {
		return c.CheckType(t)
	}
	if !c.typ.Equal(t) {
		return typeValidation("type", "compiled wrapper metadata changed")
	}
	return nil
}

func (c nullableEncoder[T]) typeGuard(t DataType) error {
	if descriptorOf(c.typ) == nil {
		return c.CheckType(t)
	}
	if !c.typ.Equal(t) {
		return typeValidation("type", "compiled wrapper metadata changed")
	}
	return nil
}

func (c nullableDecoder[T]) typeGuard(t DataType) error {
	if descriptorOf(c.typ) == nil {
		return c.CheckType(t)
	}
	if !c.typ.Equal(t) {
		return typeValidation("type", "compiled wrapper metadata changed")
	}
	return nil
}

func (c bindEncoder[T]) typeGuard(t DataType) error {
	if descriptorOf(c.typ) == nil {
		return c.CheckType(t)
	}
	if !c.typ.Equal(t) {
		return typeValidation("type", "compiled wrapper metadata changed")
	}
	return nil
}

func (c emptyCodec[T]) typeGuard(t DataType) error {
	if descriptorOf(c.typ) == nil {
		return c.CheckType(t)
	}
	if !c.typ.Equal(t) {
		return typeValidation("type", "compiled wrapper metadata changed")
	}
	return nil
}

func (c scalarCodec[T]) decodeCompiled(r ValueReader) (T, error) {
	c.compiled = true
	return c.Decode(r)
}
func (c scalarCodec[T]) encodeCompiled(w *ValueWriter, v T) error {
	c.compiled = true
	return c.Encode(w, v)
}
func (c textCodec) decodeCompiled(r ValueReader) (Text, error) {
	c.compiled = true
	return c.Decode(r)
}
func (c textCodec) encodeCompiled(w *ValueWriter, v Text) error {
	c.compiled = true
	return c.Encode(w, v)
}
