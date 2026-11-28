//go:build integration

package integration

import (
	"bytes"
	"context"
	"fmt"

	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/execopt"

	inf "gopkg.in/inf.v0"

	"github.com/scylladb/gocql/v2/cql"
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
func serializationExec[P any](s *gocql.Session, stmt string, encoder cql.ParamEncoder[P], value P) error {
	ctx := context.Background()
	p, err := s.Prepare(ctx, cql.Statement[P, cql.EmptyRow]{CQL: stmt, Params: encoder}, execopt.PrepareOptions{})
	if err != nil {
		return err
	}
	_, err = p.Exec(ctx, value)
	return err
}

func serializationRows[R any](s *gocql.Session, stmt string, decoder cql.RowDecoder[R], values ...cql.Value) (gocql.Rows[R], error) {
	ctx := context.Background()
	p, err := s.Prepare(ctx, cql.Statement[[]cql.Value, R]{CQL: stmt, Params: cql.DynamicParams, Rows: decoder}, execopt.PrepareOptions{})
	if err != nil {
		return gocql.Rows[R]{}, err
	}
	return p.Run(ctx, values)
}

func serializationOne[R any](s *gocql.Session, stmt string, decoder cql.RowDecoder[R], values ...cql.Value) (R, error) {
	var zero R
	rows, err := serializationRows(s, stmt, decoder, values...)
	if err != nil {
		return zero, err
	}
	defer rows.Close()
	var current R
	if !rows.NextInto(&current) {
		if err := rows.Err(); err != nil {
			return zero, err
		}
		return zero, errNoRows
	}
	row, err := rows.Clone()
	if err != nil {
		return zero, err
	}
	if rows.NextInto(&current) {
		return zero, errMultipleRows
	}
	if err := rows.Err(); err != nil {
		return zero, err
	}
	return row, rows.Close()
}

func serializationDDL(t testing.TB, s *gocql.Session, stmt string) {
	t.Helper()
	if err := v2CreateTable(context.Background(), s, stmt); err != nil {
		t.Fatal(err)
	}
}
func serializationWrite(t testing.TB, s *gocql.Session, stmt string, values ...cql.Value) {
	t.Helper()
	if _, err := v2Exec(context.Background(), s, stmt, values...); err != nil {
		t.Fatal(err)
	}
}
func serializationColumn(t testing.TB, s *gocql.Session, stmt string, values ...cql.Value) cql.Value {
	t.Helper()
	r, err := v2ReadOne(context.Background(), s, stmt, values...)
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

func (serializationStringCodec) CheckType(t cql.DataType) error {
	return cql.TextViewCodec.CheckType(t)
}
func (serializationStringCodec) Encode(w *cql.ValueWriter, s string) error {
	return cql.StringCodec.Encode(w, s)
}
func (serializationStringCodec) Decode(r cql.ValueReader) (string, error) {
	v, e := cql.TextViewCodec.Decode(r)
	if e != nil {
		return "", e
	}
	return fmt.Sprint(v), nil
}
func (serializationStringCodec) Clone(s string) string { return s }

var serializationString cql.Codec[string] = serializationStringCodec{}

func serializationList[T any](c cql.Codec[T]) cql.Codec[[]T] { return cql.ListCodec(c) }

func serializationSet[T any](c cql.Codec[T]) cql.Codec[[]T] { return cql.SetCodec(c) }

func serializationVector[T any](c cql.Codec[T]) cql.Codec[[]T] { return cql.VectorCodec(c) }

type serializationCodec[T any] struct {
	cql.Encoder[T]
	cql.Decoder[T]
}

func (c serializationCodec[T]) CheckType(t cql.DataType) error {
	if err := c.Encoder.CheckType(t); err != nil {
		return err
	}
	return c.Decoder.CheckType(t)
}
func serializationTuple[T any](p cql.ParamEncoder[T], r cql.RowDecoder[T]) cql.Codec[T] {
	return serializationCodec[T]{cql.TupleEncoder(p), cql.TupleDecoder(r)}
}
func serializationUDT[T any](p cql.ParamEncoder[T], r cql.RowDecoder[T], opts ...cql.StructOption) cql.Codec[T] {
	return serializationCodec[T]{cql.UDTEncoder(p, opts...), cql.UDTDecoder(r, opts...)}
}

func serializationRoundTrip[T any](t testing.TB, s *gocql.Session, table, column string, id int32, codec cql.Codec[T], input T) T {
	t.Helper()
	params := cql.PairParams(cql.OneParam(cql.Int32Codec), cql.OneParam(codec))
	if err := serializationExec(s, fmt.Sprintf("INSERT INTO %s (id,%s) VALUES (?,?)", table, column), params, cql.Pair[int32, T]{First: id, Second: input}); err != nil {
		t.Fatal(err)
	}
	output, err := serializationOne(s, fmt.Sprintf("SELECT %s FROM %s WHERE id=?", column, table), cql.OneColumn(codec), cql.IntValue(id))
	if err != nil {
		t.Fatal(err)
	}
	return output
}

type serializationMapCodec[K comparable, V any] struct {
	key   cql.Codec[K]
	value cql.Codec[V]
}

func (c serializationMapCodec[K, V]) CheckType(t cql.DataType) error {
	if e := cql.MapEncoder(c.key, c.value).CheckType(t); e != nil {
		return e
	}
	return cql.MapDecoder(c.key, c.value).CheckType(t)
}
func (c serializationMapCodec[K, V]) Encode(w *cql.ValueWriter, v map[K]V) error {
	entries := make([]cql.Entry[K, V], 0, len(v))
	for k, x := range v {
		entries = append(entries, cql.Entry[K, V]{Key: k, Value: x})
	}
	return cql.MapEncoder(c.key, c.value).Encode(w, entries)
}
func (c serializationMapCodec[K, V]) Decode(r cql.ValueReader) (map[K]V, error) {
	m, e := cql.MapDecoder(c.key, c.value).Decode(r)
	if e != nil {
		return nil, e
	}
	out := make(map[K]V, len(m))
	for _, v := range m {
		out[v.Key] = v.Value
	}
	return out, nil
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
func serializationMap[K comparable, V any](k cql.Codec[K], v cql.Codec[V]) cql.Codec[map[K]V] {
	return serializationMapCodec[K, V]{k, v}
}

// assertSerializationValue compares public, decoded representations, including
// exact float bits and null state. It does not use encoded output as its oracle.
func assertSerializationValue(t testing.TB, want, got cql.Value) {
	t.Helper()
	if want.State() != got.State() {
		t.Fatalf("state: expected %v, got %v", want.State(), got.State())
	}
	wk, gk := want.Type().Kind(), got.Type().Kind()
	textKinds := func(k cql.Kind) bool { return k == cql.KindText }
	if wk != gk && !(textKinds(wk) && textKinds(gk)) {
		t.Fatalf("type: expected %s, got %s", want.Type(), got.Type())
	}
	if want.State() != cql.Present {
		return
	}
	var equal bool
	switch wk {
	case cql.KindBoolean:
		a, e := want.Bool()
		b, f := got.Bool()
		equal = e == nil && f == nil && a == b
	case cql.KindTinyInt, cql.KindSmallInt, cql.KindInt, cql.KindBigInt, cql.KindCounter, cql.KindDate, cql.KindTime, cql.KindTimestamp:
		a, e := want.Int64()
		b, f := got.Int64()
		equal = e == nil && f == nil && a == b
	case cql.KindFloat, cql.KindDouble:
		a, e := want.Float64()
		b, f := got.Float64()
		equal = e == nil && f == nil && math.Float64bits(a) == math.Float64bits(b)
	case cql.KindText, cql.KindASCII:
		a, e := want.TextView()
		b, f := got.TextView()
		equal = e == nil && f == nil && fmt.Sprint(a) == fmt.Sprint(b)
	case cql.KindBlob:
		a, e := want.Bytes()
		b, f := got.Bytes()
		equal = e == nil && f == nil && bytes.Equal(ownedBytesForTest(a), ownedBytesForTest(b))
	case cql.KindUUID, cql.KindTimeUUID:
		a, e := want.UUID()
		b, f := got.UUID()
		equal = e == nil && f == nil && a == b
	case cql.KindInet:
		a, e := want.Inet()
		b, f := got.Inet()
		equal = e == nil && f == nil && a == b
	case cql.KindDuration:
		a, e := want.Duration()
		b, f := got.Duration()
		equal = e == nil && f == nil && a == b
	case cql.KindVarint:
		a, e := want.Varint()
		b, f := got.Varint()
		equal = e == nil && f == nil && a.BigInt().Cmp(b.BigInt()) == 0
	case cql.KindDecimal:
		a, e := want.Decimal()
		b, f := got.Decimal()
		equal = e == nil && f == nil && a.Scale == b.Scale && a.Unscaled.BigInt().Cmp(b.Unscaled.BigInt()) == 0
	case cql.KindMap:
		a, e := collectSeq(want.Entries())
		b, f := collectSeq(got.Entries())
		if e != nil || f != nil || len(a) != len(b) {
			t.Fatalf("map cardinality: %d %d %v %v", len(a), len(b), e, f)
		}
		key := func(v cql.Value) string {
			encoded, e := cql.EncodeValue(v.Type(), cql.DynamicCodec, v)
			if e != nil {
				t.Fatal(e)
			}
			return string(ownedBytesForTest(encoded.Bytes()))
		}
		values := make(map[string]cql.Value, len(b))
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
	case cql.KindUDT:
		a, e := collectSeq(want.Fields())
		b, f := collectSeq(got.Fields())
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
		a, e := collectSeq(want.Elements())
		b, f := collectSeq(got.Elements())
		if e != nil || f != nil || len(a) != len(b) {
			t.Fatalf("elements: %d %d %v %v", len(a), len(b), e, f)
		}
		if wk == cql.KindSet {
			key := func(v cql.Value) string {
				x, e := cql.EncodeValue(v.Type(), cql.DynamicCodec, v)
				if e != nil {
					t.Fatal(e)
				}
				return string(ownedBytesForTest(x.Bytes()))
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
func serializationFixtureValue(kind cql.Kind, input any) (cql.Value, error) {
	typ, err := cql.ScalarType(kind)
	if err != nil {
		return cql.Value{}, err
	}
	// The corpus still names text by its pre-v3 wire ID 0x000A, which
	// ScalarType maps to KindText; switch on the resolved kind.
	kind = typ.Kind()
	null := func() (cql.Value, error) { return cql.NullValue(), nil }
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
		return cql.DurationValue(cql.Duration{Months: v.Months, Days: v.Days, Nanoseconds: v.Nanoseconds}), nil
	case fixtureDuration.Duration:
		return cql.DurationValue(cql.Duration{Months: v.Months, Days: v.Days, Nanoseconds: v.Nanoseconds}), nil
	case *cql.Duration:
		if v == nil {
			return null()
		}
		return cql.DurationValue(*v), nil
	case *big.Int:
		if v == nil {
			return null()
		}
		switch kind {
		case cql.KindVarint:
			return cql.VarintValue(cql.VarintFromBigInt(v)), nil
		default:
			if !v.IsInt64() {
				return cql.Value{}, fmt.Errorf("fixture integer overflow")
			}
			return serializationFixtureValue(kind, v.Int64())
		}
	case bool:
		return cql.BooleanValue(v), nil
	case int8:
		return serializationFixtureValue(kind, int64(v))
	case int16:
		return serializationFixtureValue(kind, int64(v))
	case int32:
		if kind == cql.KindDate {
			return cql.DateValue(cql.Date(int32(uint32(v) ^ uint32(1<<31)))), nil
		}
		return serializationFixtureValue(kind, int64(v))
	case uint32:
		if kind == cql.KindDate {
			return cql.DateValue(cql.Date(int32(v ^ uint32(1<<31)))), nil
		}
	case int64:
		switch kind {
		case cql.KindTinyInt:
			if v < math.MinInt8 || v > math.MaxInt8 {
				break
			}
			return cql.TinyIntValue(int8(v)), nil
		case cql.KindSmallInt:
			if v < math.MinInt16 || v > math.MaxInt16 {
				break
			}
			return cql.SmallIntValue(int16(v)), nil
		case cql.KindInt:
			if v < math.MinInt32 || v > math.MaxInt32 {
				break
			}
			return cql.IntValue(int32(v)), nil
		case cql.KindBigInt:
			return cql.BigIntValue(v), nil
		case cql.KindVarint:
			return cql.VarintValue(cql.VarintFromBigInt(big.NewInt(v))), nil
		case cql.KindTime:
			return cql.TimeValue(cql.TimeOfDay(v)), nil
		case cql.KindTimestamp:
			return cql.TimestampValue(cql.Timestamp(v)), nil
		case cql.KindDuration:
			const day = int64(24 * time.Hour)
			return cql.DurationValue(cql.Duration{Days: int32(v / day), Nanoseconds: v % day}), nil
		}
	case time.Duration:
		return serializationFixtureValue(kind, int64(v))
	case float32:
		return cql.FloatValue(v), nil
	case float64:
		return cql.DoubleValue(v), nil
	case inf.Dec:
		return cql.DecimalValue(cql.Decimal{Scale: int32(v.Scale()), Unscaled: cql.VarintFromBigInt(v.UnscaledBig())}), nil
	case [16]byte:
		if kind == cql.KindTimeUUID {
			return cql.TimeUUIDValue(cql.TimeUUID(v)), nil
		}
		return cql.UUIDValue(cql.UUID(v)), nil
	case net.IP:
		if v == nil {
			return null()
		}
		a, ok := netip.AddrFromSlice(v)
		if !ok {
			return cql.Value{}, fmt.Errorf("invalid fixture IP")
		}
		return cql.InetValue(a.Unmap()), nil
	case time.Time:
		if kind == cql.KindDate {
			d, e := cql.DateFromTime(v)
			if e != nil {
				return cql.Value{}, e
			}
			return cql.DateValue(d), nil
		}
		n, e := cql.TimestampFromTime(v)
		if e != nil {
			return cql.Value{}, e
		}
		return cql.TimestampValue(n), nil
	case cql.Duration:
		return cql.DurationValue(v), nil
	case string:
		switch kind {
		case cql.KindText:
			return cql.TextValue(v), nil
		case cql.KindASCII:
			return cql.ASCIIValue(v), nil
		case cql.KindBlob:
			return cql.BlobValue([]byte(v)), nil
		}
		if v == "" {
			return null()
		}
		switch kind {
		case cql.KindVarint:
			n, ok := new(big.Int).SetString(v, 10)
			if !ok {
				break
			}
			return cql.VarintValue(cql.VarintFromBigInt(n)), nil
		case cql.KindDecimal:
			parts := strings.Split(v, ";")
			if len(parts) != 2 {
				break
			}
			scale, e := strconv.ParseInt(parts[0], 10, 32)
			if e != nil {
				return cql.Value{}, e
			}
			n, ok := new(big.Int).SetString(parts[1], 10)
			if !ok {
				break
			}
			return cql.DecimalValue(cql.Decimal{Scale: int32(scale), Unscaled: cql.VarintFromBigInt(n)}), nil
		case cql.KindUUID, cql.KindTimeUUID:
			u, e := cql.ParseUUID(v)
			if e != nil {
				return cql.Value{}, e
			}
			if kind == cql.KindTimeUUID {
				return cql.TimeUUIDValue(cql.TimeUUID(u)), nil
			}
			return cql.UUIDValue(u), nil
		case cql.KindInet:
			a, e := netip.ParseAddr(v)
			if e != nil {
				return cql.Value{}, e
			}
			return cql.InetValue(a), nil
		case cql.KindDate:
			dayPos := strings.LastIndex(v, "-")
			monthPos := strings.LastIndex(v[:dayPos], "-")
			y, e := strconv.Atoi(v[:monthPos])
			if e != nil {
				return cql.Value{}, e
			}
			m, e := strconv.Atoi(v[monthPos+1 : dayPos])
			if e != nil {
				return cql.Value{}, e
			}
			d, e := strconv.Atoi(v[dayPos+1:])
			if e != nil {
				return cql.Value{}, e
			}
			date, e := cql.DateFromTime(time.Date(y, time.Month(m), d, 0, 0, 0, 0, time.UTC))
			if e != nil {
				return cql.Value{}, e
			}
			return cql.DateValue(date), nil
		case cql.KindDuration:
			d, e := serializationDurationLiteral(v)
			if e != nil {
				return cql.Value{}, e
			}
			return cql.DurationValue(d), nil
		}
	}
	return cql.Value{}, fmt.Errorf("unsupported scalar fixture %s %T (%v)", kind, input, input)
}

func serializationDurationLiteral(s string) (cql.Duration, error) {
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
			return cql.Duration{}, fmt.Errorf("invalid duration")
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
		return cql.Duration{}, fmt.Errorf("invalid fixture duration %q", s)
	}
	m, d, n := months.Num().Int64(), days.Num().Int64(), nanos.Num().Int64()
	if m < math.MinInt32 || m > math.MaxInt32 || d < math.MinInt32 || d > math.MaxInt32 {
		return cql.Duration{}, fmt.Errorf("duration overflow")
	}
	return cql.Duration{Months: int32(m), Days: int32(d), Nanoseconds: n}, nil
}

type serializationRawDecoder struct{}

func (serializationRawDecoder) CheckType(t cql.DataType) error {
	return cql.DynamicCodec.CheckType(t)
}
func (serializationRawDecoder) Decode(r cql.ValueReader) (cql.EncodedValue, error) {
	body, err := r.Bytes().Copy()
	if err != nil {
		return cql.EncodedValue{}, err
	}
	return cql.NewEncodedValue(r.Type(), r.State(), body)
}
func (serializationRawDecoder) Clone(v cql.EncodedValue) cql.EncodedValue { return v.Clone() }

// The shared typed session intentionally preserves its keyspace. Names include
// a process nonce so repeated runs remain isolated from previous run data/schema.
var serializationFixtureNamespace = cql.NewTimeUUID().String()

func serializationTableName(t testing.TB, parts ...string) string {
	parts = append(parts, "v2", serializationFixtureNamespace)
	return testTableName(t, parts...)
}
func serializationTypeName(t testing.TB, parts ...string) string {
	return serializationTableName(t, parts...)
}

func serializationUDTOptions(name string) cql.StructOption {
	return cql.UDTName(fixtureKeyspace(""), name)
}
