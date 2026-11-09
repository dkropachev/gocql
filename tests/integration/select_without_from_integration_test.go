//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	gocql "github.com/scylladb/gocql/v2"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
)

// Scylla's no-FROM SELECT resolves selectors against virtual system.one_row.
// Capability is probed, since development servers may implement it before a release.
var selectWithoutFromOnce sync.Once
var selectWithoutFromSupported bool
var selectWithoutFromProbeErr error

func skipUnlessSelectWithoutFrom(tb testing.TB) {
	tb.Helper()
	if *flagDistribution == "cassandra" {
		tb.Skip("SELECT without FROM is a ScyllaDB feature")
	}
	selectWithoutFromOnce.Do(probeSelectWithoutFrom)
	if selectWithoutFromProbeErr != nil {
		tb.Fatalf("probe SELECT without FROM: %v", selectWithoutFromProbeErr)
	}
	if !selectWithoutFromSupported {
		tb.Skip("server does not support SELECT without FROM")
	}
}
func probeSelectWithoutFrom() {
	config, err := createV2Cluster().Build()
	if err != nil {
		selectWithoutFromProbeErr = err
		return
	}
	session, err := gocql.Connect(context.Background(), config)
	if err != nil {
		selectWithoutFromProbeErr = err
		return
	}
	defer session.Close()
	row, err := v2ReadOne(context.Background(), session, "SELECT 1")
	if err == nil {
		value, e := row.At(0)
		if e != nil {
			selectWithoutFromProbeErr = e
			return
		}
		number, e := value.Signed()
		if e != nil {
			selectWithoutFromProbeErr = e
			return
		}
		if number != 1 {
			selectWithoutFromProbeErr = fmt.Errorf("SELECT 1 returned %d", number)
			return
		}
		selectWithoutFromSupported = true
		return
	}
	if serverErrorCode(err) != cqlerr.ErrCodeSyntax {
		selectWithoutFromProbeErr = err
	}
}
func serverErrorCode(err error) int {
	if server, ok := errors.AsType[cqlerr.RequestError](err); ok {
		return server.GetCode()
	}
	return 0
}
func isRejectedByServer(err error) bool {
	code := serverErrorCode(err)
	return code == cqlerr.ErrCodeSyntax || code == cqlerr.ErrCodeInvalid
}
func createSessionWithoutKeyspace(t testing.TB) *gocql.Session {
	t.Helper()
	// createV2Cluster never sets a keyspace; only createV2SessionFromConfig does.
	session, err := gocql.Connect(context.Background(), mustBuild(t, createV2Cluster()))
	if err != nil {
		t.Fatal(err)
	}
	return session
}
func noFromOne(t testing.TB, s *gocql.Session, stmt string, values ...cql.Value) cql.DynamicRow {
	t.Helper()
	row, err := v2ReadOne(context.Background(), s, stmt, values...)
	if err != nil {
		t.Fatal(err)
	}
	return row
}
func noFromValue(t testing.TB, row cql.DynamicRow, index int) cql.Value {
	t.Helper()
	value, err := row.At(index)
	if err != nil {
		t.Fatal(err)
	}
	return value
}
func noFromInt(t testing.TB, row cql.DynamicRow, index int) int64 {
	t.Helper()
	n, err := noFromValue(t, row, index).Signed()
	if err != nil {
		t.Fatal(err)
	}
	return n
}
func noFromText(t testing.TB, row cql.DynamicRow, index int) string {
	t.Helper()
	value, err := noFromValue(t, row, index).Text()
	if err != nil {
		t.Fatal(err)
	}
	return value.String()
}
func noFromRows(t testing.TB, s *gocql.Session, stmt string, values ...cql.Value) gocql.Rows[cql.DynamicRow] {
	t.Helper()
	rows, err := v2Query(context.Background(), s, stmt, values...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := rows.Close(); err != nil {
			t.Error(err)
		}
	})
	return rows
}
func noFromColumn(t testing.TB, columns cql.Columns, index int) cql.ColumnSpec {
	t.Helper()
	column, ok := columns.At(index)
	if !ok {
		t.Fatalf("column %d absent", index)
	}
	return column
}
func logColumns(t testing.TB, stmt string, columns cql.Columns) {
	t.Helper()
	for i := 0; i < columns.Len(); i++ {
		column, _ := columns.At(i)
		t.Logf("%s: %d = %s.%s.%s (%s)", stmt, i, column.Keyspace, column.Table, column.Name, column.Type.Kind())
	}
}
