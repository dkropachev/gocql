package routing

import (
	"github.com/scylladb/gocql/v2/cql"
	metadata2 "github.com/scylladb/gocql/v2/metadata"
	"github.com/scylladb/gocql/v2/routing"
)

func v2TestHost(id byte, dc, rack string) metadata2.HostSnapshot {
	var uuid cql.UUID
	uuid[15] = id
	return metadata2.HostSnapshot{HostID: uuid, Datacenter: dc, Rack: rack, Enabled: true, Up: true, Connected: true, Shards: 4}
}
func v2TestMetadata() *MetadataSnapshot {
	hosts := []metadata2.HostSnapshot{v2TestHost(1, "a", "r1"), v2TestHost(2, "a", "r2"), v2TestHost(3, "b", "r3"), v2TestHost(4, "a", "r1")}
	return NewMetadataSnapshot(SnapshotState{InternalHostIndex: InternalBuildRoutingHostIndex(hosts), InternalHosts: hosts, InternalKeyspaces: map[string]*metadata2.Keyspace{"ks": {Tables: map[string]*metadata2.Table{"table": {Name: "table"}}}}, InternalRanges: map[string][]metadata2.TokenRange{"ks": {{Start: 0, End: 0, Replicas: []metadata2.Target{{HostID: hosts[2].HostID}, {HostID: hosts[1].HostID}, {HostID: hosts[0].HostID}}}}}, InternalTablets: map[InternalMetadataTable][]metadata2.TokenRange{}})
}

// nextTarget is the target of a plan's next route, without the route to any
// shard of the same host that may follow it.
var nextTarget = InternalNextTarget

func v2PlanIDs(p QueryPlan) []byte {
	var result []byte
	for {
		v, ok := nextTarget(p)
		if !ok {
			return result
		}
		result = append(result, v.HostID[15])
	}
}

// planHosts is the last byte of each host a plan routes to, in order.
func planHosts(plan QueryPlan) []byte {
	var hosts []byte
	for {
		target, ok := nextTarget(plan)
		if !ok {
			return hosts
		}
		hosts = append(hosts, target.HostID[15])
	}
}

// pickFallbackPolicy picks pick, when set, and falls back to fallback,
// counting Fallback calls.
type pickFallbackPolicy struct {
	pick      cql.Option[Route]
	fallback  []Route
	fallbacks int
}

func (p *pickFallbackPolicy) Validate(*MetadataSnapshot) error { return nil }
func (p *pickFallbackPolicy) Pick(routing.Info, *MetadataSnapshot) (Route, bool) {
	return p.pick.Value, p.pick.Set
}
func (p *pickFallbackPolicy) Fallback(routing.Info, *MetadataSnapshot) QueryPlan {
	p.fallbacks++
	return &routeList{routes: p.fallback}
}
func (p *pickFallbackPolicy) Learn(routing.AttemptEvent) {}

type routeList struct{ routes []Route }

func (l *routeList) Next() (Route, bool) {
	if len(l.routes) == 0 {
		return Route{}, false
	}
	r := l.routes[0]
	l.routes = l.routes[1:]
	return r, true
}
