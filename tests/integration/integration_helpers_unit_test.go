//go:build integration

package integration

import (
	"bytes"
	"context"
	"errors"
	"testing"

	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/cql"
)

// This verifies the helpers' ownership and cardinality contract against a real
// server, including the second page after the helper closes its bound handle.
func TestV2IntegrationHelpers(t *testing.T) {
	session := createV2Session(t, func(config *gocql.Config) {
		profile := *config.DefaultProfile
		profile.PageSize = 1
		config.DefaultProfile = &profile
	})
	defer session.Close()
	ctx := context.Background()
	table := (fixtureKeyspace("") + ".") + testTableName(t, "ownership")
	if err := v2CreateTable(ctx, session, "CREATE TABLE IF NOT EXISTS "+table+" (bucket int, id int, payload blob, PRIMARY KEY (bucket, id))"); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := v2CreateTable(ctx, session, "DROP TABLE IF EXISTS "+table); err != nil {
			t.Error(err)
		}
	}()
	payload := []byte("owned helper result")
	for id := int32(1); id <= 2; id++ {
		if _, err := v2Exec(ctx, session, "INSERT INTO "+table+" (bucket, id, payload) VALUES (?, ?, ?)", cql.IntValue(1), cql.IntValue(id), cql.BlobValue(payload)); err != nil {
			t.Fatal(err)
		}
	}
	row, err := v2ReadOne(ctx, session, "SELECT payload FROM "+table+" WHERE bucket = ? AND id = ?", cql.IntValue(1), cql.IntValue(1))
	if err != nil {
		t.Fatal(err)
	}
	value, err := row.Get("payload")
	if err != nil {
		t.Fatal(err)
	}
	got, err := value.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.OwnedBytes(), payload) {
		t.Fatal("single-row helper did not retain an owned payload")
	}
	if _, err := v2ReadOne(ctx, session, "SELECT payload FROM "+table+" WHERE bucket = ?", cql.IntValue(2)); !errors.Is(err, gocql.ErrNoRows) {
		t.Fatalf("empty result: got %v, want ErrNoRows", err)
	}
	if _, err := v2ReadOne(ctx, session, "SELECT payload FROM "+table+" WHERE bucket = ?", cql.IntValue(1)); !errors.Is(err, gocql.ErrMultipleRows) {
		t.Fatalf("multiple result pages: got %v, want ErrMultipleRows", err)
	}
	rows, err := v2Query(ctx, session, "SELECT id FROM "+table+" WHERE bucket = ?", cql.IntValue(1))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("paged result has %d rows, want 2", count)
	}
}
