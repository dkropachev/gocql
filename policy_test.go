package gocql

import (
	"github.com/scylladb/gocql/v2/cql"
	implRouting "github.com/scylladb/gocql/v2/internal/routing"
	"github.com/scylladb/gocql/v2/metadata"
)

func v2TestHost(id byte, dc, rack string) metadata.HostSnapshot {
	var uuid cql.UUID
	uuid[15] = id
	return metadata.HostSnapshot{HostID: uuid, Datacenter: dc, Rack: rack, Enabled: true, Up: true, Connected: true, Shards: 4}
}
func v2TestMetadata() *metadata.Snapshot {
	hosts := []metadata.HostSnapshot{v2TestHost(1, "a", "r1"), v2TestHost(2, "a", "r2"), v2TestHost(3, "b", "r3"), v2TestHost(4, "a", "r1")}
	return implRouting.NewMetadataSnapshot(implRouting.SnapshotState{InternalHostIndex: buildRoutingHostIndex(hosts), InternalHosts: hosts, InternalKeyspaces: map[string]*metadata.Keyspace{"ks": {Tables: map[string]*metadata.Table{"table": {Name: "table"}}}}, InternalRanges: map[string][]metadata.TokenRange{"ks": {{Start: 0, End: 0, Replicas: []metadata.Target{{HostID: hosts[2].HostID}, {HostID: hosts[1].HostID}, {HostID: hosts[0].HostID}}}}}, InternalTablets: map[metadataTable][]metadata.TokenRange{}}).Public()
}
