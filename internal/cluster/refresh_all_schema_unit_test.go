package cluster

import (
	"testing"
)

// refreshAllSchemaSession returns a session whose schema mock knows ks1 with
// tables t1 and t2, and ks2 with t3; all three are loaded into the cache and
// have tablets.
func refreshAllSchemaSession(t *testing.T) (*Core, *schemaDataMock) {
	t.Helper()

	col := func(name string) []columnInfo { return []columnInfo{{name: name, kind: "partition_key"}} }
	ctrl := &schemaDataMock{knownKeyspaces: map[string][]tableInfo{
		"ks1": {{name: "t1", columns: col("id")}, {name: "t2", columns: col("id")}},
		"ks2": {{name: "t3", columns: col("id")}},
	}}
	s := newSchemaEventTestSessionWithMock(ctrl)
	t.Cleanup(func() { _ = s.Close() })

	for _, ks := range []string{"ks1", "ks2"} {
		if _, err := SessionData(s).InternalMetadataDescriber.GetKeyspace(ks); err != nil {
			t.Fatalf("load %s: %v", ks, err)
		}
	}
	addTestTablets(t, s, "ks1", "t1")
	addTestTablets(t, s, "ks1", "t2")
	addTestTablets(t, s, "ks2", "t3")
	return s, ctrl
}

func refreshAllSchemaNow(t *testing.T, s *Core) {
	t.Helper()
	if err := SessionData(s).InternalMetadataDescriber.refreshAllSchema(); err != nil {
		t.Fatalf("refreshAllSchema: %v", err)
	}
	MetadataData(SessionData(s).InternalMetadataDescriber.InternalMetadata).InternalTabletsMetadata.Flush()
}

func hasTablets(s *Core, keyspace, table string) bool {
	return len(MetadataData(SessionData(s).InternalMetadataDescriber.InternalMetadata).InternalTabletsMetadata.GetTableTablets(keyspace, table)) > 0
}

// requireTablets checks which of the three tables still have tablets.
func requireTablets(t *testing.T, s *Core, want map[string]bool) {
	t.Helper()
	for _, kt := range [][2]string{{"ks1", "t1"}, {"ks1", "t2"}, {"ks2", "t3"}} {
		key := kt[0] + "." + kt[1]
		if got := hasTablets(s, kt[0], kt[1]); got != want[key] {
			t.Errorf("%s has tablets = %v, want %v", key, got, want[key])
		}
	}
}
