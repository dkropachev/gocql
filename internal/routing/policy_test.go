package routing

import (
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/metadata"
	"github.com/scylladb/gocql/v2/internal/model"
)

func v2TestHost(id byte, dc, rack string) model.HostSnapshot {
	var uuid cqltype.UUID
	uuid[15] = id
	return model.HostSnapshot{ID: uuid, Datacenter: dc, Rack: rack, Enabled: true, Up: true, Connected: true, Shards: 4}
}
func v2TestMetadata() *MetadataSnapshot {
	hosts := []model.HostSnapshot{v2TestHost(1, "a", "r1"), v2TestHost(2, "a", "r2"), v2TestHost(3, "b", "r3"), v2TestHost(4, "a", "r1")}
	return NewMetadataSnapshot(SnapshotState{InternalHostIndex: InternalBuildRoutingHostIndex(hosts), InternalHosts: hosts, InternalKeyspaces: map[string]*metadata.KeyspaceMetadata{"ks": {Tables: map[string]*metadata.TableMetadata{"table": {Name: "table"}}}}, InternalRanges: map[string][]TokenRange{"ks": {{Start: 0, End: 0, Replicas: []model.Target{{HostID: hosts[2].ID}, {HostID: hosts[1].ID}, {HostID: hosts[0].ID}}}}}, InternalTablets: map[InternalMetadataTable][]TokenRange{}})
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
	pick      cqltype.Option[Route]
	fallback  []Route
	fallbacks int
}

func (p *pickFallbackPolicy) Validate(*MetadataSnapshot) error { return nil }
func (p *pickFallbackPolicy) Pick(model.RoutingInfo, *MetadataSnapshot) (Route, bool) {
	return p.pick.Value, p.pick.Set
}
func (p *pickFallbackPolicy) Fallback(model.RoutingInfo, *MetadataSnapshot) QueryPlan {
	p.fallbacks++
	return &routeList{routes: p.fallback}
}
func (p *pickFallbackPolicy) Observe(AttemptEvent) {}

type routeList struct{ routes []Route }

func (l *routeList) Next() (Route, bool) {
	if len(l.routes) == 0 {
		return Route{}, false
	}
	r := l.routes[0]
	l.routes = l.routes[1:]
	return r, true
}
