package execution

import (
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/metadata"
	"github.com/scylladb/gocql/v2/internal/model"
	implRouting "github.com/scylladb/gocql/v2/internal/routing"
	routing "github.com/scylladb/gocql/v2/internal/routing"
)

func v2TestHost(id byte, dc, rack string) model.HostSnapshot {
	var uuid cqltype.UUID
	uuid[15] = id
	return model.HostSnapshot{ID: uuid, Datacenter: dc, Rack: rack, Enabled: true, Up: true, Connected: true, Shards: 4}
}
func v2TestMetadata() *implRouting.MetadataSnapshot {
	hosts := []model.HostSnapshot{v2TestHost(1, "a", "r1"), v2TestHost(2, "a", "r2"), v2TestHost(3, "b", "r3"), v2TestHost(4, "a", "r1")}
	return routing.NewMetadataSnapshot(routing.SnapshotState{InternalHostIndex: implRouting.InternalBuildRoutingHostIndex(hosts), InternalHosts: hosts, InternalKeyspaces: map[string]*metadata.KeyspaceMetadata{"ks": {Tables: map[string]*metadata.TableMetadata{"table": {Name: "table"}}}}, InternalRanges: map[string][]implRouting.TokenRange{"ks": {{Start: 0, End: 0, Replicas: []model.Target{{HostID: hosts[2].ID}, {HostID: hosts[1].ID}, {HostID: hosts[0].ID}}}}}, InternalTablets: map[implRouting.InternalMetadataTable][]implRouting.TokenRange{}})
}

func v2PlanIDs(p implRouting.QueryPlan) []byte {
	var result []byte
	for {
		v, ok := implRouting.InternalNextTarget(p)
		if !ok {
			return result
		}
		result = append(result, v.HostID[15])
	}
}
