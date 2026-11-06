package gocql

import (
	"github.com/scylladb/gocql/v2/cql"
	implRouting "github.com/scylladb/gocql/v2/internal/routing"
	"github.com/scylladb/gocql/v2/metadata"
	routing "github.com/scylladb/gocql/v2/routing"
)

func v2TestHost(id byte, dc, rack string) routing.HostSnapshot {
	var uuid cql.UUID
	uuid[15] = id
	return routing.HostSnapshot{ID: uuid, Datacenter: dc, Rack: rack, Enabled: true, Up: true, Connected: true, Shards: 4}
}
func v2TestMetadata() *metadata.Snapshot {
	hosts := []routing.HostSnapshot{v2TestHost(1, "a", "r1"), v2TestHost(2, "a", "r2"), v2TestHost(3, "b", "r3"), v2TestHost(4, "a", "r1")}
	return implRouting.NewMetadataSnapshot(implRouting.SnapshotState{InternalHostIndex: buildRoutingHostIndex(hosts), InternalHosts: hosts, InternalKeyspaces: map[string]*metadata.Keyspace{"ks": {Tables: map[string]*metadata.Table{"table": {Name: "table"}}}}, InternalRanges: map[string][]routing.TokenRange{"ks": {{Start: 0, End: 0, Replicas: []routing.Target{{HostID: hosts[2].ID}, {HostID: hosts[1].ID}, {HostID: hosts[0].ID}}}}}, InternalTablets: map[metadataTable][]routing.TokenRange{}})
}
