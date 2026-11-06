//go:build integration

package integration

import (
	"bytes"
	"context"
	"fmt"

	gocql "github.com/scylladb/gocql/v2"

	inf "gopkg.in/inf.v0"

	fixtureDuration "github.com/scylladb/gocql/v2/internal/tests/valuecases"

	"math"
	"math/big"
	"net"
	"net/netip"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// These fixture helpers compose the public codecs; the integration values never
// pass through reflected parameter binding or result destination discovery.
func serializationExec[P any](s *gocql.Session, cql string, encoder gocql.ParamEncoder[P], value P) error {
	ctx := context.Background()
	p, err := gocql.Prepare(ctx, s, gocql.Statement[P, gocql.Unit]{CQL: cql, Params: encoder, Rows: gocql.NoRows}, gocql.PrepareOptions{})
	if err != nil {
		return err
	}
	b, err := BindOwned(p, value)
	if err != nil {
		return err
	}
	defer b.Close()
	_, err = Exec(ctx, s, b, gocql.Options{})
	return err
}

func serializationRows[R any](s *gocql.Session, cql string, decoder gocql.RowDecoder[R], values ...gocql.Value) (gocql.Rows[R], error) {
	ctx := context.Background()
	p, err := gocql.Prepare(ctx, s, gocql.Statement[[]gocql.Value, R]{CQL: cql, Params: gocql.DynamicParams, Rows: decoder}, gocql.PrepareOptions{})
	if err != nil {
		return gocql.Rows[R]{}, err
	}
	b, err := BindOwned(p, values)
	if err != nil {
		return gocql.Rows[R]{}, err
	}
	defer b.Close()
	return Run(ctx, s, b, gocql.Options{})
}

func serializationOne[R any](s *gocql.Session, cql string, decoder gocql.RowDecoder[R], values ...gocql.Value) (R, error) {
	var zero R
	rows, err := serializationRows(s, cql, decoder, values...)
	if err != nil {
		return zero, err
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return zero, err
		}
		return zero, gocql.ErrNoRows
	}
	row, err := rows.Clone()
	if err != nil {
		return zero, err
	}
	if rows.Next() {
		return zero, gocql.ErrMultipleRows
	}
	if err := rows.Err(); err != nil {
		return zero, err
	}
	return row, rows.Close()
}

func serializationDDL(t testing.TB, s *gocql.Session, cql string) {
	t.Helper()
	if err := v2CreateTable(context.Background(), s, cql); err != nil {
		t.Fatal(err)
	}
}
func serializationWrite(t testing.TB, s *gocql.Session, cql string, values ...gocql.Value) {
	t.Helper()
	if _, err := v2Exec(context.Background(), s, cql, values...); err != nil {
		t.Fatal(err)
	}
}
func serializationColumn(t testing.TB, s *gocql.Session, cql string, values ...gocql.Value) gocql.Value {
	t.Helper()
	r, err := v2ReadOne(context.Background(), s, cql, values...)
	if err != nil {
		t.Fatal(err)
	}
	if r.Columns().Len() != 1 {
		t.Fatalf("expected one physical column, got %d", r.Columns().Len())
	}
	v, err := r.At(0)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// Owned application representations are explicit, opt-in conversions in tests.
type serializationStringCodec struct{}

func (serializationStringCodec) CheckType(t gocql.DataType) error {
	return gocql.TextCodec.CheckType(t)
}
func (serializationStringCodec) Encode(w *gocql.ValueWriter, s string) error {
	return gocql.StringEncoder.Encode(w, s)
}
func (serializationStringCodec) Decode(r gocql.ValueReader) (string, error) {
	v, e := gocql.TextCodec.Decode(r)
	if e != nil {
		return "", e
	}
	return v.String(), nil
}
func (serializationStringCodec) Clone(s string) string { return s }

var serializationString gocql.Codec[string] = serializationStringCodec{}

type serializationSliceCodec[T any] struct {
	encoder gocql.Encoder[[]T]
	decoder gocql.Decoder[gocql.Sequence[T]]
	element gocql.Decoder[T]
}

func (c serializationSliceCodec[T]) CheckType(t gocql.DataType) error {
	if err := c.encoder.CheckType(t); err != nil {
		return err
	}
	return c.decoder.CheckType(t)
}
func (c serializationSliceCodec[T]) Encode(w *gocql.ValueWriter, v []T) error {
	return c.encoder.Encode(w, v)
}
func (c serializationSliceCodec[T]) Decode(r gocql.ValueReader) ([]T, error) {
	s, e := c.decoder.Decode(r)
	if e != nil {
		return nil, e
	}
	return gocql.Collect(s.Iterator())
}
func (c serializationSliceCodec[T]) Clone(v []T) []T {
	if v == nil {
		return nil
	}
	out := make([]T, len(v))
	for i := range v {
		out[i] = c.element.Clone(v[i])
	}
	return out
}
func serializationList[T any](c gocql.Codec[T]) gocql.Codec[[]T] {
	return serializationSliceCodec[T]{gocql.ListEncoder(c), gocql.ListDecoder(c), c}
}
func serializationSet[T any](c gocql.Codec[T]) gocql.Codec[[]T] {
	return serializationSliceCodec[T]{gocql.SetEncoder(c), gocql.SetDecoder(c), c}
}
func serializationVector[T any](c gocql.Codec[T]) gocql.Codec[[]T] {
	return serializationSliceCodec[T]{gocql.VectorEncoder(c), gocql.VectorDecoder(c), c}
}

type serializationCodec[T any] struct {
	gocql.Encoder[T]
	gocql.Decoder[T]
}

func (c serializationCodec[T]) CheckType(t gocql.DataType) error {
	if err := c.Encoder.CheckType(t); err != nil {
		return err
	}
	return c.Decoder.CheckType(t)
}
func serializationTuple[T any](p gocql.ParamEncoder[T], r gocql.RowDecoder[T]) gocql.Codec[T] {
	return serializationCodec[T]{gocql.TupleEncoder(p), gocql.TupleDecoder(r)}
}
func serializationUDT[T any](p gocql.ParamEncoder[T], r gocql.RowDecoder[T], opts gocql.UDTOptions) gocql.Codec[T] {
	return serializationCodec[T]{gocql.UDTEncoder(p, opts), gocql.UDTDecoder(r, opts)}
}

func serializationRoundTrip[T any](t testing.TB, s *gocql.Session, table, column string, id int32, codec gocql.Codec[T], input T) T {
	t.Helper()
	params := gocql.PairParams(1, gocql.OneParam(gocql.Int32), gocql.OneParam(codec))
	if err := serializationExec(s, fmt.Sprintf("INSERT INTO %s (id,%s) VALUES (?,?)", table, column), params, gocql.Pair[int32, T]{First: id, Second: input}); err != nil {
		t.Fatal(err)
	}
	output, err := serializationOne(s, fmt.Sprintf("SELECT %s FROM %s WHERE id=?", column, table), gocql.OneColumn(codec), gocql.IntValue(id))
	if err != nil {
		t.Fatal(err)
	}
	return output
}

type serializationMapCodec[K comparable, V any] struct {
	key   gocql.Codec[K]
	value gocql.Codec[V]
}

func (c serializationMapCodec[K, V]) CheckType(t gocql.DataType) error {
	if e := gocql.MapEncoder(c.key, c.value).CheckType(t); e != nil {
		return e
	}
	return gocql.MapDecoder(c.key, c.value).CheckType(t)
}
func (c serializationMapCodec[K, V]) Encode(w *gocql.ValueWriter, v map[K]V) error {
	entries := make([]gocql.Entry[K, V], 0, len(v))
	for k, x := range v {
		entries = append(entries, gocql.Entry[K, V]{Key: k, Value: x})
	}
	return gocql.MapEncoder(c.key, c.value).Encode(w, entries)
}
func (c serializationMapCodec[K, V]) Decode(r gocql.ValueReader) (map[K]V, error) {
	m, e := gocql.MapDecoder(c.key, c.value).Decode(r)
	if e != nil {
		return nil, e
	}
	out := make(map[K]V, m.Len())
	it := m.Iterator()
	for it.Next() {
		v := it.Value()
		out[v.Key] = v.Value
	}
	return out, it.Err()
}
func (c serializationMapCodec[K, V]) Clone(v map[K]V) map[K]V {
	if v == nil {
		return nil
	}
	o := make(map[K]V, len(v))
	for k, x := range v {
		o[c.key.Clone(k)] = c.value.Clone(x)
	}
	return o
}
func serializationMap[K comparable, V any](k gocql.Codec[K], v gocql.Codec[V]) gocql.Codec[map[K]V] {
	return serializationMapCodec[K, V]{k, v}
}

// assertSerializationValue compares public, decoded representations, including
// exact float bits and null state. It does not use encoded output as its oracle.
func assertSerializationValue(t testing.TB, want, got gocql.Value) {
	t.Helper()
	if want.State() != got.State() {
		t.Fatalf("state: expected %v, got %v", want.State(), got.State())
	}
	wk, gk := want.Type().Kind(), got.Type().Kind()
	textKinds := func(k gocql.Type) bool { return k == gocql.TypeText || k == gocql.TypeVarchar }
	if wk != gk && !(textKinds(wk) && textKinds(gk)) {
		t.Fatalf("type: expected %s, got %s", want.Type(), got.Type())
	}
	if want.State() != gocql.Present {
		return
	}
	var equal bool
	switch wk {
	case gocql.TypeBoolean:
		a, e := want.Bool()
		b, f := got.Bool()
		equal = e == nil && f == nil && a == b
	case gocql.TypeTinyInt, gocql.TypeSmallInt, gocql.TypeInt, gocql.TypeBigInt, gocql.TypeCounter, gocql.TypeDate, gocql.TypeTime, gocql.TypeTimestamp:
		a, e := want.Signed()
		b, f := got.Signed()
		equal = e == nil && f == nil && a == b
	case gocql.TypeFloat, gocql.TypeDouble:
		a, e := want.Float()
		b, f := got.Float()
		equal = e == nil && f == nil && math.Float64bits(a) == math.Float64bits(b)
	case gocql.TypeText, gocql.TypeVarchar, gocql.TypeAscii:
		a, e := want.Text()
		b, f := got.Text()
		equal = e == nil && f == nil && a.String() == b.String()
	case gocql.TypeBlob:
		a, e := want.Bytes()
		b, f := got.Bytes()
		equal = e == nil && f == nil && bytes.Equal(a.OwnedBytes(), b.OwnedBytes())
	case gocql.TypeUUID, gocql.TypeTimeUUID:
		a, e := want.UUID()
		b, f := got.UUID()
		equal = e == nil && f == nil && a == b
	case gocql.TypeInet:
		a, e := want.Inet()
		b, f := got.Inet()
		equal = e == nil && f == nil && a == b
	case gocql.TypeDuration:
		a, e := want.Duration()
		b, f := got.Duration()
		equal = e == nil && f == nil && a == b
	case gocql.TypeVarint:
		a, e := want.Varint()
		b, f := got.Varint()
		equal = e == nil && f == nil && a.BigInt().Cmp(b.BigInt()) == 0
	case gocql.TypeDecimal:
		a, e := want.Decimal()
		b, f := got.Decimal()
		equal = e == nil && f == nil && a.Scale == b.Scale && a.Unscaled.BigInt().Cmp(b.Unscaled.BigInt()) == 0
	case gocql.TypeMap:
		ai, e := want.Entries()
		bi, f := got.Entries()
		if e != nil || f != nil {
			t.Fatalf("map iterators: %v %v", e, f)
		}
		a, e := gocql.Collect(ai)
		b, f := gocql.Collect(bi)
		if e != nil || f != nil || len(a) != len(b) {
			t.Fatalf("map cardinality: %d %d %v %v", len(a), len(b), e, f)
		}
		key := func(v gocql.Value) string {
			encoded, e := gocql.EncodeValueOwned(v.Type(), gocql.DynamicCodec, v)
			if e != nil {
				t.Fatal(e)
			}
			return string(encoded.Bytes().OwnedBytes())
		}
		values := make(map[string]gocql.Value, len(b))
		for _, entry := range b {
			values[key(entry.Key)] = entry.Value
		}
		for _, entry := range a {
			v, ok := values[key(entry.Key)]
			if !ok {
				t.Fatal("map key missing")
			}
			assertSerializationValue(t, entry.Value, v)
		}
		return
	case gocql.TypeUDT:
		ai, e := want.Fields()
		bi, f := got.Fields()
		if e != nil || f != nil {
			t.Fatalf("UDT iterators: %v %v", e, f)
		}
		a, e := gocql.Collect(ai)
		b, f := gocql.Collect(bi)
		if e != nil || f != nil || len(a) != len(b) {
			t.Fatal("UDT field count mismatch")
		}
		for i := range a {
			if a[i].Name != b[i].Name {
				t.Fatal("UDT field name mismatch")
			}
			assertSerializationValue(t, a[i].Value, b[i].Value)
		}
		return
	default:
		ai, e := want.Elements()
		bi, f := got.Elements()
		if e != nil || f != nil {
			t.Fatalf("element iterators: %v %v", e, f)
		}
		a, e := gocql.Collect(ai)
		b, f := gocql.Collect(bi)
		if e != nil || f != nil || len(a) != len(b) {
			t.Fatalf("elements: %d %d %v %v", len(a), len(b), e, f)
		}
		if wk == gocql.TypeSet {
			key := func(v gocql.Value) string {
				x, e := gocql.EncodeValueOwned(v.Type(), gocql.DynamicCodec, v)
				if e != nil {
					t.Fatal(e)
				}
				return string(x.Bytes().OwnedBytes())
			}
			sort.Slice(a, func(i, j int) bool { return key(a[i]) < key(a[j]) })
			sort.Slice(b, func(i, j int) bool { return key(b[i]) < key(b[j]) })
		}
		for i := range a {
			assertSerializationValue(t, a[i], b[i])
		}
		return
	}
	if !equal {
		t.Fatalf("decoded %s value mismatch: expected %+v, got %+v", want.Type(), want, got)
	}
}

// serializationFixtureValue translates only the finite historical scalar corpus.
// Conversions are explicit fixture setup, outside driver binding and decoding.
func serializationFixtureValue(kind gocql.Type, input any) (gocql.Value, error) {
	typ, err := gocql.ScalarType(kind)
	if err != nil {
		return gocql.Value{}, err
	}
	null := func() (gocql.Value, error) { return gocql.NullValue(typ), nil }
	switch v := input.(type) {
	case *bool:
		if v == nil {
			return null()
		}
		return serializationFixtureValue(kind, *v)
	case *int8:
		if v == nil {
			return null()
		}
		return serializationFixtureValue(kind, *v)
	case *int16:
		if v == nil {
			return null()
		}
		return serializationFixtureValue(kind, *v)
	case *int32:
		if v == nil {
			return null()
		}
		return serializationFixtureValue(kind, *v)
	case *int64:
		if v == nil {
			return null()
		}
		return serializationFixtureValue(kind, *v)
	case *float32:
		if v == nil {
			return null()
		}
		return serializationFixtureValue(kind, *v)
	case *float64:
		if v == nil {
			return null()
		}
		return serializationFixtureValue(kind, *v)
	case *string:
		if v == nil {
			return null()
		}
		return serializationFixtureValue(kind, *v)
	case *time.Time:
		if v == nil {
			return null()
		}
		return serializationFixtureValue(kind, *v)
	case *time.Duration:
		if v == nil {
			return null()
		}
		return serializationFixtureValue(kind, *v)
	case *net.IP:
		if v == nil {
			return null()
		}
		return serializationFixtureValue(kind, *v)
	case *[16]byte:
		if v == nil {
			return null()
		}
		return serializationFixtureValue(kind, *v)
	case *inf.Dec:
		if v == nil {
			return null()
		}
		return serializationFixtureValue(kind, *v)
	case *fixtureDuration.Duration:
		if v == nil {
			return null()
		}
		return gocql.DurationValue(gocql.Duration{Months: v.Months, Days: v.Days, Nanoseconds: v.Nanoseconds})
	case fixtureDuration.Duration:
		return gocql.DurationValue(gocql.Duration{Months: v.Months, Days: v.Days, Nanoseconds: v.Nanoseconds})
	case *gocql.Duration:
		if v == nil {
			return null()
		}
		return gocql.DurationValue(*v)
	case *big.Int:
		if v == nil {
			return null()
		}
		switch kind {
		case gocql.TypeVarint:
			return gocql.VarintValue(gocql.VarintFromBigInt(v)), nil
		default:
			if !v.IsInt64() {
				return gocql.Value{}, fmt.Errorf("fixture integer overflow")
			}
			return serializationFixtureValue(kind, v.Int64())
		}
	case bool:
		return gocql.BoolValue(v), nil
	case int8:
		return serializationFixtureValue(kind, int64(v))
	case int16:
		return serializationFixtureValue(kind, int64(v))
	case int32:
		if kind == gocql.TypeDate {
			return gocql.DateValue(gocql.Date(int32(uint32(v) ^ uint32(1<<31)))), nil
		}
		return serializationFixtureValue(kind, int64(v))
	case uint32:
		if kind == gocql.TypeDate {
			return gocql.DateValue(gocql.Date(int32(v ^ uint32(1<<31)))), nil
		}
	case int64:
		switch kind {
		case gocql.TypeTinyInt:
			if v < math.MinInt8 || v > math.MaxInt8 {
				break
			}
			return gocql.TinyIntValue(int8(v)), nil
		case gocql.TypeSmallInt:
			if v < math.MinInt16 || v > math.MaxInt16 {
				break
			}
			return gocql.SmallIntValue(int16(v)), nil
		case gocql.TypeInt:
			if v < math.MinInt32 || v > math.MaxInt32 {
				break
			}
			return gocql.IntValue(int32(v)), nil
		case gocql.TypeBigInt:
			return gocql.BigIntValue(v), nil
		case gocql.TypeVarint:
			return gocql.VarintValue(gocql.VarintFromBigInt(big.NewInt(v))), nil
		case gocql.TypeTime:
			return gocql.TimeValue(gocql.TimeOfDay(v))
		case gocql.TypeTimestamp:
			return gocql.TimestampValue(gocql.Timestamp(v)), nil
		case gocql.TypeDuration:
			const day = int64(24 * time.Hour)
			return gocql.DurationValue(gocql.Duration{Days: int32(v / day), Nanoseconds: v % day})
		}
	case time.Duration:
		return serializationFixtureValue(kind, int64(v))
	case float32:
		return gocql.FloatValue(v), nil
	case float64:
		return gocql.DoubleValue(v), nil
	case inf.Dec:
		return gocql.DecimalValue(gocql.Decimal{Scale: int32(v.Scale()), Unscaled: gocql.VarintFromBigInt(v.UnscaledBig())}), nil
	case [16]byte:
		if kind == gocql.TypeTimeUUID {
			return gocql.TimeUUIDValue(gocql.TimeUUID(v))
		}
		return gocql.UUIDValue(gocql.UUID(v)), nil
	case net.IP:
		if v == nil {
			return null()
		}
		a, ok := netip.AddrFromSlice(v)
		if !ok {
			return gocql.Value{}, fmt.Errorf("invalid fixture IP")
		}
		return gocql.InetValue(a.Unmap())
	case time.Time:
		if kind == gocql.TypeDate {
			d, e := gocql.DateFromTime(v)
			if e != nil {
				return gocql.Value{}, e
			}
			return gocql.DateValue(d), nil
		}
		n, e := gocql.TimestampFromTime(v)
		if e != nil {
			return gocql.Value{}, e
		}
		return gocql.TimestampValue(n), nil
	case gocql.Duration:
		return gocql.DurationValue(v)
	case string:
		switch kind {
		case gocql.TypeText, gocql.TypeVarchar:
			return gocql.TextValue(v)
		case gocql.TypeAscii:
			return gocql.ASCIIValue(v)
		case gocql.TypeBlob:
			return gocql.BlobValue([]byte(v)), nil
		}
		if v == "" {
			return null()
		}
		switch kind {
		case gocql.TypeVarint:
			n, ok := new(big.Int).SetString(v, 10)
			if !ok {
				break
			}
			return gocql.VarintValue(gocql.VarintFromBigInt(n)), nil
		case gocql.TypeDecimal:
			parts := strings.Split(v, ";")
			if len(parts) != 2 {
				break
			}
			scale, e := strconv.ParseInt(parts[0], 10, 32)
			if e != nil {
				return gocql.Value{}, e
			}
			n, ok := new(big.Int).SetString(parts[1], 10)
			if !ok {
				break
			}
			return gocql.DecimalValue(gocql.Decimal{Scale: int32(scale), Unscaled: gocql.VarintFromBigInt(n)}), nil
		case gocql.TypeUUID, gocql.TypeTimeUUID:
			u, e := gocql.ParseUUID(v)
			if e != nil {
				return gocql.Value{}, e
			}
			if kind == gocql.TypeTimeUUID {
				return gocql.TimeUUIDValue(gocql.TimeUUID(u))
			}
			return gocql.UUIDValue(u), nil
		case gocql.TypeInet:
			a, e := netip.ParseAddr(v)
			if e != nil {
				return gocql.Value{}, e
			}
			return gocql.InetValue(a)
		case gocql.TypeDate:
			dayPos := strings.LastIndex(v, "-")
			monthPos := strings.LastIndex(v[:dayPos], "-")
			y, e := strconv.Atoi(v[:monthPos])
			if e != nil {
				return gocql.Value{}, e
			}
			m, e := strconv.Atoi(v[monthPos+1 : dayPos])
			if e != nil {
				return gocql.Value{}, e
			}
			d, e := strconv.Atoi(v[dayPos+1:])
			if e != nil {
				return gocql.Value{}, e
			}
			date, e := gocql.DateFromTime(time.Date(y, time.Month(m), d, 0, 0, 0, 0, time.UTC))
			if e != nil {
				return gocql.Value{}, e
			}
			return gocql.DateValue(date), nil
		case gocql.TypeDuration:
			d, e := serializationDurationLiteral(v)
			if e != nil {
				return gocql.Value{}, e
			}
			return gocql.DurationValue(d)
		}
	}
	return gocql.Value{}, fmt.Errorf("unsupported scalar fixture %s %T (%v)", kind, input, input)
}

func serializationDurationLiteral(s string) (gocql.Duration, error) {
	sign := int64(1)
	if strings.HasPrefix(s, "-") {
		sign = -1
		s = s[1:]
	}
	tokens := regexp.MustCompile(`([0-9]+(?:\.[0-9]+)?)(mo|ms|us|ns|y|w|d|h|m|s)`).FindAllStringSubmatch(s, -1)
	months, days, nanos := new(big.Rat), new(big.Rat), new(big.Rat)
	consumed := ""
	for _, token := range tokens {
		consumed += token[0]
		n, ok := new(big.Rat).SetString(token[1])
		if !ok {
			return gocql.Duration{}, fmt.Errorf("invalid duration")
		}
		target := nanos
		factor := int64(1)
		switch token[2] {
		case "y":
			target = months
			factor = 12
		case "mo":
			target = months
		case "w":
			target = days
			factor = 7
		case "d":
			target = days
		case "h":
			factor = int64(time.Hour)
		case "m":
			factor = int64(time.Minute)
		case "s":
			factor = int64(time.Second)
		case "ms":
			factor = int64(time.Millisecond)
		case "us":
			factor = int64(time.Microsecond)
		}
		target.Add(target, n.Mul(n, new(big.Rat).SetInt64(sign*factor)))
	}
	if consumed != s || !months.IsInt() || !days.IsInt() || !nanos.IsInt() || !months.Num().IsInt64() || !days.Num().IsInt64() || !nanos.Num().IsInt64() {
		return gocql.Duration{}, fmt.Errorf("invalid fixture duration %q", s)
	}
	m, d, n := months.Num().Int64(), days.Num().Int64(), nanos.Num().Int64()
	if m < math.MinInt32 || m > math.MaxInt32 || d < math.MinInt32 || d > math.MaxInt32 {
		return gocql.Duration{}, fmt.Errorf("duration overflow")
	}
	return gocql.Duration{Months: int32(m), Days: int32(d), Nanoseconds: n}, nil
}

type serializationRawDecoder struct{}

func (serializationRawDecoder) CheckType(t gocql.DataType) error {
	return gocql.DynamicCodec.CheckType(t)
}
func (serializationRawDecoder) Decode(r gocql.ValueReader) (gocql.EncodedValue, error) {
	return gocql.NewEncodedValue(r.Type(), r.State(), r.Bytes())
}
func (serializationRawDecoder) Clone(v gocql.EncodedValue) gocql.EncodedValue { return v.Clone() }

// The shared typed session intentionally preserves its keyspace. Names include
// a process nonce so repeated runs remain isolated from previous run data/schema.
var serializationFixtureNamespace = gocql.NewTimeUUID().String()

func serializationTableName(t testing.TB, parts ...string) string {
	parts = append(parts, "v2", serializationFixtureNamespace)
	return testTableName(t, parts...)
}
func serializationTypeName(t testing.TB, parts ...string) string {
	return serializationTableName(t, parts...)
}

func serializationUDTOptions(name string) gocql.UDTOptions {
	return gocql.UDTOptions{Keyspace: fixtureKeyspace(""), Name: name}
}
