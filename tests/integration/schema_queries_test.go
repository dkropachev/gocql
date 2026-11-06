//go:build integration

package integration

import (
	"testing"

	"github.com/scylladb/gocql/v2/internal/tests"
	"github.com/scylladb/gocql/v2/routing"
)

func TestSchemaQueries(t *testing.T) {
	t.Parallel()

	cluster := createV2Cluster()

	v2ConfigureRouting(t, &cluster, routing.Options{TokenAware: true})

	session := createV2SessionFromConfig(cluster, t, false)
	defer session.Close()

	keyspaceMetadata, ok := session.Metadata().Keyspace(fixtureKeyspace(""))
	if !ok {
		t.Fatal("keyspace missing from typed metadata snapshot")
	}
	tests.AssertTrue(t, "keyspace present in metadata snapshot", keyspaceMetadata.Name == fixtureKeyspace(""))
}
