package execution

import (
	"testing"

	"github.com/scylladb/gocql/v2/internal/cluster"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
)

// refreshAllSchemaSession returns a session whose schema mock knows ks1 with
// tables t1 and t2, and ks2 with t3; all three are loaded into the cache and
// have tablets.
func refreshAllSchemaSession(t *testing.T) (*cluster.Core, *schemaDataMock) {
	t.Helper()

	col := func(name string) []columnInfo { return []columnInfo{{name: name, kind: "partition_key"}} }
	ctrl := &schemaDataMock{knownKeyspaces: map[string][]tableInfo{
		"ks1": {{name: "t1", columns: col("id")}, {name: "t2", columns: col("id")}},
		"ks2": {{name: "t3", columns: col("id")}},
	}}
	s := newSchemaEventTestSessionWithMock(ctrl)
	t.Cleanup(func() { _ = s.Close() })

	for _, ks := range []string{"ks1", "ks2"} {
		if _, err := implCluster.SessionData(s).InternalMetadataDescriber.GetKeyspace(ks); err != nil {
			t.Fatalf("load %s: %v", ks, err)
		}
	}
	addTestTablets(t, s, "ks1", "t1")
	addTestTablets(t, s, "ks1", "t2")
	addTestTablets(t, s, "ks2", "t3")
	return s, ctrl
}
