package gocql

import (
	routing "github.com/scylladb/gocql/v2/internal/routing"
)

func v2TestHost(id byte, dc, rack string) HostSnapshot {
	var uuid UUID
	uuid[15] = id
	return HostSnapshot{ID: uuid, Datacenter: dc, Rack: rack, Enabled: true, Up: true, Connected: true, Shards: 4}
}
func v2TestMetadata() *MetadataSnapshot {
	hosts := []HostSnapshot{v2TestHost(1, "a", "r1"), v2TestHost(2, "a", "r2"), v2TestHost(3, "b", "r3"), v2TestHost(4, "a", "r1")}
	return routing.NewMetadataSnapshot(routing.SnapshotState{InternalHostIndex: buildRoutingHostIndex(hosts), InternalHosts: hosts, InternalKeyspaces: map[string]*KeyspaceMetadata{"ks": {Tables: map[string]*TableMetadata{"table": {Name: "table"}}}}, InternalRanges: map[string][]TokenRange{"ks": {{Start: 0, End: 0, Replicas: []Target{{HostID: hosts[2].ID}, {HostID: hosts[1].ID}, {HostID: hosts[0].ID}}}}}, InternalTablets: map[metadataTable][]TokenRange{}})
}
