package session

import (
	"github.com/scylladb/gocql/v2/cql"
	implRouting "github.com/scylladb/gocql/v2/internal/routing"
	routing "github.com/scylladb/gocql/v2/internal/routing"
	metadata2 "github.com/scylladb/gocql/v2/metadata"
)

func v2TestHost(id byte, dc, rack string) metadata2.HostSnapshot {
	var uuid cql.UUID
	uuid[15] = id
	return metadata2.HostSnapshot{HostID: uuid, Datacenter: dc, Rack: rack, Enabled: true, Up: true, Connected: true, Shards: 4}
}
func v2TestMetadata() *implRouting.MetadataSnapshot {
	hosts := []metadata2.HostSnapshot{v2TestHost(1, "a", "r1"), v2TestHost(2, "a", "r2"), v2TestHost(3, "b", "r3"), v2TestHost(4, "a", "r1")}
	return routing.NewMetadataSnapshot(routing.SnapshotState{InternalHostIndex: implRouting.InternalBuildRoutingHostIndex(hosts), InternalHosts: hosts, InternalKeyspaces: map[string]*metadata2.Keyspace{"ks": {Tables: map[string]*metadata2.Table{"table": {Name: "table"}}}}, InternalRanges: map[string][]implRouting.Span{"ks": {{Start: 0, End: 0, Replicas: []metadata2.Target{{HostID: hosts[2].HostID}, {HostID: hosts[1].HostID}, {HostID: hosts[0].HostID}}}}}, InternalTablets: map[implRouting.InternalMetadataTable][]implRouting.Span{}})
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
