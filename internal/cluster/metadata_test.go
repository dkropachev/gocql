package cluster

import (
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/metadata"
	"github.com/scylladb/gocql/v2/internal/model"
	implRouting "github.com/scylladb/gocql/v2/internal/routing"
	routing "github.com/scylladb/gocql/v2/internal/routing"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/tablets"
)

func TestV2MetadataSnapshotsDetachNestedSchema(t *testing.T) {
	column := &metadata.ColumnMetadata{Name: "id", Type: "int", Index: metadata.ColumnIndexMetadata{Options: map[string]any{"nested": map[string]any{"bytes": []byte{1}}}}}
	ks := &metadata.KeyspaceMetadata{Name: "ks", StrategyOptions: map[string]any{"replication_factor": "1"}, Tables: map[string]*metadata.TableMetadata{"t": {Columns: map[string]*metadata.ColumnMetadata{"id": column}, PartitionKey: []*metadata.ColumnMetadata{column}, Extensions: map[string]any{"bytes": []byte{2}}, Options: metadata.TableMetadataOptions{CDC: map[string]string{"enabled": "true"}}}}, Types: map[string]*metadata.TypeMetadata{"u": {FieldNames: []string{"a"}, FieldTypes: []string{"int"}}}, Functions: map[string]*metadata.FunctionMetadata{"f": {ArgumentNames: []string{"a"}, ArgumentTypes: []string{"int"}}}}
	m := routing.NewMetadataSnapshot(routing.SnapshotState{InternalGeneration: 8, InternalKeyspaces: map[string]*metadata.KeyspaceMetadata{"ks": metadata.InternalCloneV2Keyspace(ks)}, InternalHosts: []model.HostSnapshot{{Tokens: []int64{1}}}})
	ks.Tables["t"].Columns["id"].Name = "mutated"
	ks.Tables["t"].Extensions["bytes"].([]byte)[0] = 9
	first, _ := m.Keyspace("ks")
	if first.Tables["t"].Columns["id"].Name != "id" || first.Tables["t"].Extensions["bytes"].([]byte)[0] != 2 {
		t.Fatal("snapshot retained mutable schema")
	}
	first.Types["u"].FieldNames[0] = "changed"
	first.Functions["f"].ArgumentNames[0] = "changed"
	first.Tables["t"].PartitionKey[0].Index.Options["nested"].(map[string]any)["bytes"].([]byte)[0] = 9
	second, _ := m.Keyspace("ks")
	if second.Types["u"].FieldNames[0] != "a" || second.Functions["f"].ArgumentNames[0] != "a" || second.Tables["t"].Columns["id"].Index.Options["nested"].(map[string]any)["bytes"].([]byte)[0] != 1 {
		t.Fatal("getter leaked mutable snapshot")
	}
	hosts := m.Hosts()
	hosts[0].Tokens[0] = 9
	if m.Hosts()[0].Tokens[0] != 1 {
		t.Fatal("host tokens alias")
	}
}
func TestV2MetadataRetainedAcrossPublicationAndClose(t *testing.T) {
	s := func() *Session {
		opaqueStorageValue := new(Session)
		return opaqueStorageValue
	}()

	first := s.Metadata()
	InternalSessionPublishV2Metadata(s)
	second := s.Metadata()
	if first == second || second.Generation() <= first.Generation() {
		t.Fatal("publication did not advance")
	}
	InternalSessionCloseV2Peripheral(s)
	if first.Generation() == 0 {
		t.Fatal("retained snapshot invalidated")
	}
}

type panicLifecycle struct{}

func (panicLifecycle) ObserveLifecycle(settings.LifecycleEvent) { panic("observer failure") }
func TestV2LifecyclePanicContained(t *testing.T) {
	s := func() *Session {
		opaqueStorageValuex := new(Session)
		return opaqueStorageValuex
	}()

	p := InternalSessionPeripheral(s)
	p.InternalConfig.LifecycleObserver = panicLifecycle{}
	InternalSessionObserveV2Lifecycle(s, settings.LifecycleEvent{Kind: "test", At: time.Now()})
	InternalSessionCloseV2Peripheral(s)
}

func TestV2SchemaValidationRejectsBrokenIdentityAndOrdering(t *testing.T) {
	ks := &metadata.KeyspaceMetadata{Name: "ks"}
	tables := []metadata.TableMetadata{{Name: "t"}}
	columns := []metadata.ColumnMetadata{{Table: "t", Name: "p", Type: "int", Kind: metadata.ColumnPartitionKey, ComponentIndex: 0}}
	check := func(cols []metadata.ColumnMetadata, types []metadata.TypeMetadata) error {
		return metadata.InternalValidateV2SchemaParts(ks, tables, cols, types, nil, nil, nil, nil)
	}
	if err := check(columns, nil); err != nil {
		t.Fatal(err)
	}
	malformed := append([]metadata.ColumnMetadata(nil), columns...)
	malformed[0].ComponentIndex = 2
	if err := check(malformed, nil); err == nil {
		t.Fatal("key gap accepted")
	}
	malformed = append(columns, columns[0])
	if err := check(malformed, nil); err == nil {
		t.Fatal("duplicate identity accepted")
	}
	types := []metadata.TypeMetadata{{Name: "a", FieldNames: []string{"b"}, FieldTypes: []string{"frozen<b>"}}, {Name: "b", FieldNames: []string{"a"}, FieldTypes: []string{"frozen<a>"}}}
	if err := check(columns, types); err == nil {
		t.Fatal("UDT cycle accepted")
	}
	types = []metadata.TypeMetadata{{Name: "a", FieldNames: []string{"x", "x"}, FieldTypes: []string{"int", "int"}}}
	if err := check(columns, types); err == nil {
		t.Fatal("duplicate UDT field accepted")
	}
}
func TestV2SchemaTypeSyntax(t *testing.T) {
	for _, typ := range []string{"int", "empty", "map<text, frozen<tuple<int, text>>>", "vector<float, 3>", `frozen<"quoted-type">`} {
		if _, err := metadata.InternalValidateV2CQLType("ks", typ, map[string]bool{"quoted-type": true}); err != nil {
			t.Fatal(typ, err)
		}
	}
	for _, typ := range []string{"map<int>", "list<>", "tuple<int", "vector<float, 0>", "vector<float, 65536>", "int garbage", "frozen<missing>", "map<int,text>>"} {
		if _, err := metadata.InternalValidateV2CQLType("ks", typ, nil); err == nil {
			t.Fatal("invalid type accepted", typ)
		}
	}
}
func TestV2MetricsPercentilesAndSchemaGeneration(t *testing.T) {
	s := func() *Session {
		opaqueStorageValuexx := new(Session)
		return opaqueStorageValuexx
	}()

	for i := 1; i <= 100; i++ {
		InternalSessionRecordV2Latency(s, time.Duration(i)*time.Millisecond)
	}
	InternalSessionRecordV2Orphan(s)
	InternalSessionPeripheral(s).InternalPublishLatencySummary()
	m := s.Metrics()
	if m.LatencyP50 != 50*time.Millisecond || m.LatencyP95 != 95*time.Millisecond || m.LatencyP99 != 99*time.Millisecond || m.Orphans != 1 {
		t.Fatal(m)
	}
	generation := InternalSessionTypedSchemaGeneration(s)
	InternalSessionPublishV2Metadata(s)
	if InternalSessionTypedSchemaGeneration(s) != generation {
		t.Fatal("topology invalidated prepared schema")
	}
}

func TestV2TabletFeedbackOverflowResyncAndEpoch(t *testing.T) {
	s := func() *Session {
		opaqueStorageValuexxx := new(Session)
		return opaqueStorageValuexxx
	}()
	SessionData(s).InternalMetadataDescriber = InternalNewMetadataDescriber(s)
	defer MetadataData(SessionData(s).InternalMetadataDescriber.InternalMetadata).InternalTabletsMetadata.Close()
	defer InternalSessionCloseV2Peripheral(s)
	MetadataData(SessionData(s).InternalMetadataDescriber.InternalMetadata).InternalKeyspaceMetadata.InternalSet("ks", &metadata.KeyspaceMetadata{Name: "ks", Tables: map[string]*metadata.TableMetadata{"t": {Name: "t"}}})
	p := InternalSessionPeripheral(s)
	p.InternalFeedbackOnce.Do(func() {}) // Hold the consumer while filling the bounded queue.
	sub, err := s.Subscribe(EventFilter{Kinds: []EventKind{OverflowEvent}, Capacity: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	for i := 0; i < 1025; i++ {
		hint, err := tablets.NewTabletInfo("ks", "t", int64(i), int64(i+1), []tablets.ReplicaInfo{tablets.NewReplicaInfo(tablets.HostUUID{1}, 0)})
		if err != nil {
			t.Fatal(err)
		}
		InternalSessionApplyV2TabletHint(s, hint)
	}
	p.InternalFeedbackMu.Lock()
	if len(p.InternalFeedback) > 1024 || !p.InternalFeedbackResync {
		t.Fatal("feedback did not bound and mark overflow")
	}
	p.InternalFeedbackMu.Unlock()
	// A newer schema event must win over every already queued hint.
	p.InternalMu.Lock()
	p.InternalBumpSchemaEpochLocked("ks")
	p.InternalMu.Unlock()
	go InternalSessionRunV2TabletFeedback(s, p)
	select {
	case event := <-sub.Events():
		if event.Kind != OverflowEvent || event.Change != "TABLET_RESYNC" {
			t.Fatal(event)
		}
	case <-time.After(time.Second):
		t.Fatal("feedback resync was not published")
	}
	if got := MetadataData(SessionData(s).InternalMetadataDescriber.InternalMetadata).InternalTabletsMetadata.GetTableTablets("ks", "t"); len(got) != 0 {
		t.Fatal("stale hint restored tablet", got)
	}
	snapshot := s.Metadata()
	if _, known := routing.InternalMetadataSnapshotTabletRoot(snapshot, implRouting.InternalMetadataTable{InternalKeyspace: "ks", InternalTable: "t"}); !known {
		t.Fatal("resync lost tablet cache-miss marker")
	}
	if replicas, err := snapshot.Replicas("ks", "t", 4); err != nil || len(replicas) != 0 {
		t.Fatal("tablet cache miss invented vnode replicas", replicas, err)
	}
}
