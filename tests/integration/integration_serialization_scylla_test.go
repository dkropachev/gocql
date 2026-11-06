//go:build integration

package integration

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	gocql "github.com/scylladb/gocql/v2"

	valcases "github.com/scylladb/gocql/v2/internal/tests/valuecases"

	"math/big"
	"net/netip"
	"strings"
	"testing"
	"time"
)

// truncateTable executes a TRUNCATE on the given fully-qualified table, retrying
// transient ScyllaDB topology contention ("Another global topology request is
// ongoing" / "Another TRUNCATE TABLE is ongoing"). TRUNCATE is a global topology
// operation, so when several parallel tests issue DDL/TRUNCATE concurrently
// Scylla rejects the overlapping request, and SimpleRetryPolicy does not retry
// it. See scylladb/gocql#895.
func truncateTable(t *testing.T, session *gocql.Session, fqTable string) error {
	t.Helper()
	return retryOnTopologyBusy(func() error {
		_, err := v2Exec(context.Background(), session, "TRUNCATE "+fqTable)
		return err
	})
}

// retryOnTopologyBusy runs exec, retrying (with a short backoff) only while it
// returns the transient ScyllaDB "Another global topology request is ongoing"
// error. Any other error (or success) is returned immediately.
func retryOnTopologyBusy(exec func() error) error {
	return retryOnTopologyBusyWithSleep(exec, time.Sleep)
}

// retryOnTopologyBusyWithSleep is retryOnTopologyBusy with an injectable sleep
// function so tests can avoid real backoff delays.
func retryOnTopologyBusyWithSleep(exec func() error, sleep func(time.Duration)) error {
	const maxAttempts = 10
	var err error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		err = exec()
		if err == nil || !isTopologyBusyErr(err) {
			return err
		}
		// Small backoff capped at ~0.5s; topology requests are short.
		sleep(time.Duration(50*(attempt+1)) * time.Millisecond)
	}
	return err
}

// topologyBusyMessages lists the distinct ScyllaDB error message variants seen
// for transient topology/TRUNCATE contention. Different Scylla versions/error
// paths (invalid_request_exception vs. std::runtime_error) phrase this
// differently, so isTopologyBusyErr matches on any of them.
var topologyBusyMessages = []string{
	"Another global topology request is ongoing",
	"Another TRUNCATE TABLE is ongoing",
}

// isTopologyBusyErr reports whether err is a transient ScyllaDB error returned
// when another global topology/TRUNCATE request is already in progress.
func isTopologyBusyErr(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	for _, m := range topologyBusyMessages {
		if strings.Contains(msg, m) {
			return true
		}
	}
	return false
}

// TestRetryOnTopologyBusy verifies the retry logic deterministically (no
// cluster required): topology-busy errors are retried until success, other
// errors are returned immediately, and retries are bounded.
func TestRetryOnTopologyBusy(t *testing.T) {
	t.Parallel()

	busy := errors.New("Error during truncate: exceptions::invalid_request_exception (Another global topology request is ongoing, please retry.)")
	busyTruncate := errors.New("Error during truncate: std::runtime_error (Another TRUNCATE TABLE is ongoing, please retry.)")
	other := errors.New("some other error")

	// noSleep avoids real backoff so these subtests stay fast and deterministic.
	noSleep := func(time.Duration) {}

	t.Run("retries busy then succeeds", func(t *testing.T) {
		calls := 0
		err := retryOnTopologyBusyWithSleep(func() error {
			calls++
			if calls < 3 {
				return busy
			}
			return nil
		}, noSleep)
		if err != nil {
			t.Fatalf("expected success, got %v", err)
		}
		if calls != 3 {
			t.Fatalf("expected 3 calls, got %d", calls)
		}
	})

	t.Run("does not retry non-busy errors", func(t *testing.T) {
		calls := 0
		err := retryOnTopologyBusyWithSleep(func() error {
			calls++
			return other
		}, noSleep)
		if err != other {
			t.Fatalf("expected the original error, got %v", err)
		}
		if calls != 1 {
			t.Fatalf("expected 1 call (no retry), got %d", calls)
		}
	})

	t.Run("gives up after max attempts", func(t *testing.T) {
		calls := 0
		err := retryOnTopologyBusyWithSleep(func() error {
			calls++
			return busy
		}, noSleep)
		if !isTopologyBusyErr(err) {
			t.Fatalf("expected the busy error after exhausting retries, got %v", err)
		}
		if calls != 10 {
			t.Fatalf("expected 10 attempts, got %d", calls)
		}
	})

	t.Run("isTopologyBusyErr classification", func(t *testing.T) {
		if !isTopologyBusyErr(busy) {
			t.Error("busy error should be classified as topology-busy")
		}
		if !isTopologyBusyErr(busyTruncate) {
			t.Error("busyTruncate (runtime_error TRUNCATE variant) should be classified as topology-busy")
		}
		if isTopologyBusyErr(other) {
			t.Error("other error should not be classified as topology-busy")
		}
		if isTopologyBusyErr(nil) {
			t.Error("nil should not be classified as topology-busy")
		}
	})

	t.Run("retries the TRUNCATE-variant busy error", func(t *testing.T) {
		calls := 0
		err := retryOnTopologyBusyWithSleep(func() error {
			calls++
			if calls < 2 {
				return busyTruncate
			}
			return nil
		}, noSleep)
		if err != nil {
			t.Fatalf("expected success, got %v", err)
		}
		if calls != 2 {
			t.Fatalf("expected 2 calls, got %d", calls)
		}
	})
}

func TestSerializationSimpleTypesCassandra(t *testing.T) {
	t.Parallel()
	types := valcases.GetSimple()
	// Preserve every one of the 96 binary fixtures and every language adapter.
	t.Run("Marshal", func(t *testing.T) {
		for _, tc := range types {
			t.Run(tc.CQLName, func(t *testing.T) {
				for _, vc := range tc.Cases {
					for _, lc := range vc.LangCases {
						t.Run(vc.Name+"/"+lc.LangType, func(t *testing.T) {
							typ, err := gocql.ScalarType(gocql.Type(tc.CQLType))
							if err != nil {
								t.Fatal(err)
							}
							value, err := serializationFixtureValue(gocql.Type(tc.CQLType), lc.Value)
							if err != nil {
								t.Fatal(err)
							}
							for _, owned := range []bool{false, true} {
								var encoded gocql.EncodedValue
								if owned {
									encoded, err = gocql.EncodeValueOwned(typ, gocql.DynamicCodec, value)
								} else {
									encoded, err = gocql.EncodeValueBorrowed(typ, gocql.DynamicCodec, value)
								}
								if (err != nil) != lc.ErrInsert {
									t.Fatalf("encode error: %v (expected error %t)", err, lc.ErrInsert)
								}
								if err != nil {
									continue
								}
								if !bytes.Equal(vc.Data, encoded.Bytes().OwnedBytes()) {
									t.Fatalf("binary fixture: expected %x, got %x", vc.Data, encoded.Bytes().OwnedBytes())
								}
								if (vc.Data == nil) != (encoded.State() == gocql.Null) {
									t.Fatalf("null/present distinction lost: %s", vc.Name)
								}
							}
						})
					}
				}
			})
		}
	})
	t.Run("Unmarshal", func(t *testing.T) {
		for _, tc := range types {
			t.Run(tc.CQLName, func(t *testing.T) {
				for _, vc := range tc.Cases {
					for _, lc := range vc.LangCases {
						t.Run(vc.Name+"/"+lc.LangType, func(t *testing.T) {
							typ, err := gocql.ScalarType(gocql.Type(tc.CQLType))
							if err != nil {
								t.Fatal(err)
							}
							state := gocql.Present
							if vc.Data == nil {
								state = gocql.Null
							}
							encoded, err := gocql.NewEncodedValue(typ, state, gocql.BorrowBytes(vc.Data))
							if err != nil {
								t.Fatal(err)
							}
							actual, err := gocql.DecodeValue(gocql.DynamicCodec, encoded)
							if (err != nil) != lc.ErrSelect {
								t.Fatalf("decode error: %v (expected error %t)", err, lc.ErrSelect)
							}
							if err != nil {
								return
							}
							expected, err := serializationFixtureValue(gocql.Type(tc.CQLType), lc.Value)
							if err != nil {
								t.Fatal(err)
							}
							assertSerializationValue(t, expected, actual)
						})
					}
				}
			})
		}
	})
	s := createV2Session(t)
	defer s.Close()
	tables := make([]string, len(types))
	for i, tc := range types {
		tables[i] = serializationTableName(t, tc.CQLName)
		serializationDDL(t, s, fmt.Sprintf("CREATE TABLE %s(test_id text PRIMARY KEY,test_col %s)", tables[i], tc.CQLName))
	}
	t.Run("InsertSelect", func(t *testing.T) {
		for i, tc := range types {
			t.Run(tc.CQLName, func(t *testing.T) {
				for _, vc := range tc.Cases {
					for _, lc := range vc.LangCases {
						t.Run(vc.Name+"/"+lc.LangType, func(t *testing.T) {
							input, err := serializationFixtureValue(gocql.Type(tc.CQLType), lc.Value)
							if err != nil {
								t.Fatal(err)
							}
							id := vc.Name + "/" + lc.LangType
							_, err = v2Exec(t.Context(), s, fmt.Sprintf("INSERT INTO %s(test_id,test_col) VALUES(?,?)", tables[i]), codecTestMust(gocql.TextValue(id)), input)
							if (err != nil) != lc.ErrInsert {
								t.Fatalf("insert error: %v (expected error %t)", err, lc.ErrInsert)
							}
							if err != nil {
								return
							}
							cql := fmt.Sprintf("SELECT test_col FROM %s WHERE test_id=?", tables[i])
							got := serializationColumn(t, s, cql, codecTestMust(gocql.TextValue(id)))
							assertSerializationValue(t, input, got)
							raw, err := serializationOne(s, cql, gocql.OneColumn(serializationRawDecoder{}), codecTestMust(gocql.TextValue(id)))
							if err != nil {
								t.Fatal(err)
							}
							if !bytes.Equal(vc.Data, raw.Bytes().OwnedBytes()) {
								t.Fatalf("database binary fixture: expected %x, got %x", vc.Data, raw.Bytes().OwnedBytes())
							}
							if (vc.Data == nil) != (raw.State() == gocql.Null) {
								t.Fatal("database null/present distinction lost")
							}
						})
					}
				}
			})
		}
	})
}

func mustParseUUID(s string) gocql.UUID {
	u, e := gocql.ParseUUID(s)
	if e != nil {
		panic(e)
	}
	return u
}
func mustParseBigInt(s string) *big.Int {
	n, ok := new(big.Int).SetString(s, 10)
	if !ok {
		panic(s)
	}
	return n
}

func mustCreateDuration(m, d int32, n time.Duration) gocql.Duration {
	return gocql.Duration{Months: m, Days: d, Nanoseconds: int64(n)}
}
func serializationMustValue(v gocql.Value, err error) gocql.Value {
	if err != nil {
		panic(err)
	}
	return v
}
func serializationMustType(v gocql.DataType, err error) gocql.DataType {
	if err != nil {
		panic(err)
	}
	return v
}

// Historical method subcase names identify the preserved coverage: collection
// of cloned rows and single-row access with an explicitly retained page.
func dynamicSerializationResult(t *testing.T, s *gocql.Session, cql, method string, args ...gocql.Value) gocql.Value {
	t.Helper()
	rows, err := v2Query(t.Context(), s, cql, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatalf("expected one row: %v", rows.Err())
	}
	var row gocql.DynamicRow
	if method == "SliceMap" {
		row, err = rows.Clone()
		if err != nil {
			t.Fatal(err)
		}
	} else {
		lease, e := rows.Retain()
		if e != nil {
			t.Fatal(e)
		}
		defer lease.Close()
		row = rows.Row()
		if e = rows.Close(); e != nil {
			t.Fatal(e)
		}
		row = row.Clone()
	}
	if row.Columns().Len() != 1 {
		t.Fatalf("expected one physical column, got %d", row.Columns().Len())
	}
	v, err := row.At(0)
	if err != nil {
		t.Fatal(err)
	}
	if method == "SliceMap" {
		if rows.Next() {
			t.Fatal("more than one row")
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
	}
	return v.Clone()
}

type serializationDynamicCase struct {
	name, cql string
	expected  gocql.Value
}

func checkDynamicSerializationCases(t *testing.T, s *gocql.Session, table string, cases []serializationDynamicCase) {
	t.Helper()
	for i, tc := range cases {
		run := func(t *testing.T) {
			for _, present := range []bool{true, false} {
				name, literal := "NonNull", tc.cql
				expected := tc.expected
				if !present {
					name, literal = "Null", "NULL"
					expected = gocql.NullValue(tc.expected.Type())
				}
				t.Run(name, func(t *testing.T) {
					id := int32(i * 2)
					if !present {
						id++
					}
					serializationWrite(t, s, fmt.Sprintf("INSERT INTO %s(id,%s_col) VALUES(?,%s)", table, tc.name, literal), gocql.IntValue(id))
					for _, method := range []string{"SliceMap", "MapScan"} {
						t.Run(method, func(t *testing.T) {
							got := dynamicSerializationResult(t, s, fmt.Sprintf("SELECT %s_col FROM %s WHERE id=?", tc.name, table), method, gocql.IntValue(id))
							assertSerializationValue(t, expected, got)
						})
					}
				})
			}
		}
		if tc.name == "tuple" {
			run(t)
			continue
		}
		label := tc.name
		if strings.HasPrefix(label, "vector_") || label == "list" || label == "set" || label == "map" {
			label += "_col"
		}
		t.Run(label, run)
	}
}
func TestSliceMapMapScanTypes(t *testing.T) {
	t.Parallel()
	s := createV2Session(t)
	defer s.Close()
	table := serializationTableName(t)
	date, err := gocql.DateFromTime(time.Date(2015, 5, 3, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	cases := []serializationDynamicCase{
		{"tinyint", "42", gocql.TinyIntValue(42)}, {"smallint", "1234", gocql.SmallIntValue(1234)}, {"int", "123456", gocql.IntValue(123456)}, {"bigint", "1234567890", gocql.BigIntValue(1234567890)},
		{"float", "3.14", gocql.FloatValue(3.14)}, {"double", "2.718281828", gocql.DoubleValue(2.718281828)}, {"boolean", "true", gocql.BoolValue(true)},
		{"text", "'hello world'", codecTestMust(gocql.TextValue("hello world"))}, {"ascii", "'hello ascii'", serializationMustValue(gocql.ASCIIValue("hello ascii"))}, {"varchar", "'hello varchar'", codecTestMust(gocql.TextValue("hello varchar"))},
		{"timestamp", "1388534400000", gocql.TimestampValue(1388534400000)}, {"uuid", "550e8400-e29b-41d4-a716-446655440000", gocql.UUIDValue(mustParseUUID("550e8400-e29b-41d4-a716-446655440000"))},
		{"timeuuid", "60d79c23-5793-11f0-8afe-bcfce78b517a", serializationMustValue(gocql.TimeUUIDValue(gocql.TimeUUID(mustParseUUID("60d79c23-5793-11f0-8afe-bcfce78b517a"))))},
		{"inet", "'127.0.0.1'", serializationMustValue(gocql.InetValue(netip.MustParseAddr("127.0.0.1")))}, {"blob", "0x48656c6c6f", gocql.BlobValue([]byte("Hello"))},
		{"varint", "123456789012345678901234567890", gocql.VarintValue(gocql.VarintFromBigInt(mustParseBigInt("123456789012345678901234567890")))},
		{"decimal", "123.45", gocql.DecimalValue(gocql.Decimal{Scale: 2, Unscaled: gocql.VarintFromBigInt(big.NewInt(12345))})}, {"date", "'2015-05-03'", gocql.DateValue(date)},
		{"time", "'13:30:54.234'", serializationMustValue(gocql.TimeValue(gocql.TimeOfDay(13*time.Hour + 30*time.Minute + 54*time.Second + 234*time.Millisecond)))},
		{"duration", "1y2mo3d4h5m6s789ms", serializationMustValue(gocql.DurationValue(mustCreateDuration(14, 3, 4*time.Hour+5*time.Minute+6*time.Second+789*time.Millisecond)))},
	}
	cols := ""
	for _, tc := range cases {
		cols += fmt.Sprintf(",%s_col %s", tc.name, tc.name)
	}
	serializationDDL(t, s, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY%s)", table, cols))
	if err := truncateTable(t, s, table); err != nil {
		t.Fatal(err)
	}
	checkDynamicSerializationCases(t, s, table, cases)
}
func TestSliceMapMapScanCounterTypes(t *testing.T) {
	t.Parallel()
	s := createV2SessionFromConfig(createV2Cluster(), t, true)
	defer s.Close()
	table := serializationTableName(t)
	serializationDDL(t, s, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s(id int PRIMARY KEY,counter_col counter)", table))
	if err := truncateTable(t, s, table); err != nil {
		t.Fatal(err)
	}
	serializationWrite(t, s, fmt.Sprintf("UPDATE %s SET counter_col=counter_col+42 WHERE id=?", table), gocql.IntValue(1))
	for _, method := range []string{"SliceMap", "MapScan"} {
		t.Run(method, func(t *testing.T) {
			got := dynamicSerializationResult(t, s, fmt.Sprintf("SELECT counter_col FROM %s WHERE id=?", table), method, gocql.IntValue(1))
			assertSerializationValue(t, gocql.CounterValue(42), got)
		})
	}
}
func TestSliceMapMapScanTupleTypes(t *testing.T) {
	t.Parallel()
	s := createV2Session(t)
	defer s.Close()
	table := serializationTableName(t)
	serializationDDL(t, s, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,tuple_col tuple<int,text>)", table))
	if err := truncateTable(t, s, table); err != nil {
		t.Fatal(err)
	}
	typ := serializationMustType(gocql.TupleType(serializationMustType(gocql.ScalarType(gocql.TypeInt)), serializationMustType(gocql.ScalarType(gocql.TypeText))))
	checkDynamicSerializationCases(t, s, table, []serializationDynamicCase{{"tuple", "(42,'hello')", serializationMustValue(gocql.TupleValue(typ, []gocql.Value{gocql.IntValue(42), codecTestMust(gocql.TextValue("hello"))}))}})
}
func TestSliceMapMapScanVectorTypes(t *testing.T) {
	t.Parallel()
	serializationVectorGate(t, false)
	s := createV2Session(t)
	defer s.Close()
	table := serializationTableName(t)
	serializationDDL(t, s, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,vector_float_col vector<float,3>,vector_text_col vector<text,2>)", table))
	if err := truncateTable(t, s, table); err != nil {
		t.Fatal(err)
	}
	fl := serializationMustType(gocql.VectorDataType(serializationMustType(gocql.ScalarType(gocql.TypeFloat)), 3))
	tx := serializationMustType(gocql.VectorDataType(serializationMustType(gocql.ScalarType(gocql.TypeText)), 2))
	checkDynamicSerializationCases(t, s, table, []serializationDynamicCase{{"vector_float", "[1.0,2.5,-3.0]", serializationMustValue(gocql.VectorValue(fl, []gocql.Value{gocql.FloatValue(1), gocql.FloatValue(2.5), gocql.FloatValue(-3)}))}, {"vector_text", "['hello','world']", serializationMustValue(gocql.VectorValue(tx, []gocql.Value{codecTestMust(gocql.TextValue("hello")), codecTestMust(gocql.TextValue("world"))}))}})
}
func TestSliceMapMapScanCollectionTypes(t *testing.T) {
	t.Parallel()
	s := createV2Session(t)
	defer s.Close()
	table := serializationTableName(t)
	serializationDDL(t, s, fmt.Sprintf("CREATE TABLE %s(id int PRIMARY KEY,list_col list<text>,set_col set<int>,map_col map<text,int>)", table))
	if err := truncateTable(t, s, table); err != nil {
		t.Fatal(err)
	}
	tx, in := serializationMustType(gocql.ScalarType(gocql.TypeText)), serializationMustType(gocql.ScalarType(gocql.TypeInt))
	list, set, mp := serializationMustType(gocql.ListType(tx)), serializationMustType(gocql.SetType(in)), serializationMustType(gocql.MapType(tx, in))
	checkDynamicSerializationCases(t, s, table, []serializationDynamicCase{
		{"list", "['a','b','c']", serializationMustValue(gocql.ListValue(list, []gocql.Value{codecTestMust(gocql.TextValue("a")), codecTestMust(gocql.TextValue("b")), codecTestMust(gocql.TextValue("c"))}))},
		{"set", "{1,2,3}", serializationMustValue(gocql.SetValue(set, []gocql.Value{gocql.IntValue(1), gocql.IntValue(2), gocql.IntValue(3)}))},
		{"map", "{'key1':1,'key2':2}", serializationMustValue(gocql.MapValue(mp, []gocql.ValueEntry{{Key: codecTestMust(gocql.TextValue("key1")), Value: gocql.IntValue(1)}, {Key: codecTestMust(gocql.TextValue("key2")), Value: gocql.IntValue(2)}}))},
	})
}
