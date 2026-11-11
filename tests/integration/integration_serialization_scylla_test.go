//go:build integration

package integration

import (
	"context"
	"fmt"

	gocql "github.com/scylladb/gocql/v2"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cql/dynamic"

	"math/big"
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
func truncateTable(t *testing.T, session *gocql.Session[testDialect], fqTable string) error {
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

func mustParseUUID(s string) cql.UUID {
	u, e := cql.ParseUUID(s)
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

func mustCreateDuration(m, d int32, n time.Duration) cql.Duration {
	return cql.Duration{Months: m, Days: d, Nanoseconds: int64(n)}
}
func serializationMustValue(v dynamic.Value, err error) dynamic.Value {
	if err != nil {
		panic(err)
	}
	return v
}
func serializationMustType(v cql.DataType, err error) cql.DataType {
	if err != nil {
		panic(err)
	}
	return v
}

// Historical method subcase names identify the preserved coverage: collection
// of cloned rows and single-row access with an explicitly retained page.
func dynamicSerializationResult(t *testing.T, s *gocql.Session[testDialect], stmt, method string, args ...dynamic.Value) dynamic.Value {
	t.Helper()
	rows, err := v2Query(t.Context(), s, stmt, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var row dynamic.Row
	if !rows.NextInto(&row) {
		t.Fatalf("expected one row: %v", rows.Err())
	}
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
		var extra dynamic.Row
		if rows.NextInto(&extra) {
			t.Fatal("more than one row")
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
	}
	return v.Clone()
}

type serializationDynamicCase struct {
	name, stmt string
	expected   dynamic.Value
}

func checkDynamicSerializationCases(t *testing.T, s *gocql.Session[testDialect], table string, cases []serializationDynamicCase) {
	t.Helper()
	for i, tc := range cases {
		run := func(t *testing.T) {
			for _, present := range []bool{true, false} {
				name, literal := "NonNull", tc.stmt
				expected := tc.expected
				if !present {
					name, literal = "Null", "NULL"
					expected = dynamic.Null(tc.expected.Type())
				}
				t.Run(name, func(t *testing.T) {
					id := int32(i * 2)
					if !present {
						id++
					}
					serializationWrite(t, s, fmt.Sprintf("INSERT INTO %s(id,%s_col) VALUES(?,%s)", table, tc.name, literal), dynamic.Int(id))
					for _, method := range []string{"SliceMap", "MapScan"} {
						t.Run(method, func(t *testing.T) {
							got := dynamicSerializationResult(t, s, fmt.Sprintf("SELECT %s_col FROM %s WHERE id=?", tc.name, table), method, dynamic.Int(id))
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
