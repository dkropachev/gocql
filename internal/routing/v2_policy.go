// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package routing

import (
	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"

	"context"
	"errors"
	"io"
	"math"
	"math/rand/v2"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/scylladb/gocql/v2/internal/murmur"
	pool "github.com/scylladb/gocql/v2/internal/pool"
	runtimeerrors "github.com/scylladb/gocql/v2/internal/runtimeerrors"
	"github.com/scylladb/gocql/v2/internal/wire"
	// PartitionKey is the canonical encoded component stream. It never owns a
	// second serialization of the bound parameters.
)

type PartitionKey struct{ state PartitionKeyState }

func (k PartitionKey) Len() int64 { return PartitionKeyData(&k).InternalChain.Len() }
func (k PartitionKey) ForEach(fn func(Bytes) error) error {
	if fn == nil {
		return typeValidation("partition key", "nil visitor")
	}
	return PartitionKeyData(&k).InternalChain.Each(func(s wire.Span) error {
		c, e := wire.NewChain(s)
		if e != nil {
			return e
		}
		return fn(cqltype.BytesView(c, wire.Ref{}))
	})
}

type TokenPartitioner interface {
	Token(PartitionKey) (int64, error)
}

var Murmur3TokenPartitioner TokenPartitioner = murmur3StreamingPartitioner{}
var CDCTokenPartitioner TokenPartitioner = cdcStreamingPartitioner{}

// InternalValidatePartitionKey applies InternalNewPartitionKey's checks
// without building the key.
func InternalValidatePartitionKey(cells []encodedCell, indexes []int) error {
	for _, idx := range indexes {
		if idx < 0 || idx >= len(cells) {
			return typeValidation("partition key", "invalid column index")
		}
		c := cells[idx]
		if c.State != Null && c.State != Unset && c.State != Present {
			return typeValidation("partition key", "invalid cell state")
		}
		if len(indexes) > 1 && c.Body.Len() > 65535 {
			return typeValidation("partition key", "composite component exceeds 65535 bytes")
		}
	}
	return nil
}

func InternalNewPartitionKey(cells []encodedCell, indexes []int) (PartitionKey, bool, error) {
	if len(indexes) == 0 {
		return *NewPartitionKey(PartitionKeyState{}), false, nil
	}
	missing := false
	for _, idx := range indexes {
		if idx < 0 || idx >= len(cells) {
			return *NewPartitionKey(PartitionKeyState{}), false, typeValidation("partition key", "invalid column index")
		}
		c := cells[idx]
		if c.State == Null || c.State == Unset {
			missing = true
		} else if c.State != Present {
			return *NewPartitionKey(PartitionKeyState{}), false, typeValidation("partition key", "invalid cell state")
		}
		if len(indexes) > 1 && c.Body.Len() > 65535 {
			return *NewPartitionKey(PartitionKeyState{}), false, typeValidation("partition key", "composite component exceeds 65535 bytes")
		}
	}
	if missing {
		return *NewPartitionKey(PartitionKeyState{}), false, nil
	}
	b := wire.NewBuilder(256)
	for _, idx := range indexes {
		if idx < 0 || idx >= len(cells) {
			return *NewPartitionKey(PartitionKeyState{}), false, typeValidation("partition key", "invalid column index")
		}
		c := cells[idx]
		if c.State == Null || c.State == Unset {
			return *NewPartitionKey(PartitionKeyState{}), false, nil
		}
		if c.State != Present {
			return *NewPartitionKey(PartitionKeyState{}), false, typeValidation("partition key", "invalid cell state")
		}
		if len(indexes) > 1 {
			if c.Body.Len() > 65535 {
				return *NewPartitionKey(PartitionKeyState{}), false, typeValidation("partition key", "composite component exceeds 65535 bytes")
			}
			if e := b.Uint16(uint16(c.Body.Len())); e != nil {
				return *NewPartitionKey(PartitionKeyState{}), false, e
			}
		}
		if e := c.Body.Each(b.Borrow); e != nil {
			return *NewPartitionKey(PartitionKeyState{}), false, e
		}
		if len(indexes) > 1 {
			if e := b.Uint8(0); e != nil {
				return *NewPartitionKey(PartitionKeyState{}), false, e
			}
		}
	}
	c, e := b.Freeze()
	return *NewPartitionKey(PartitionKeyState{InternalChain: c}), e == nil, e
}
func InternalComputeRoutingToken(cells []encodedCell, indexes []int, p TokenPartitioner) (Option[int64], error) {
	if _, builtin := p.(murmur3StreamingPartitioner); builtin {
		for _, index := range indexes {
			if index >= 0 && index < len(cells) && cells[index].State != Present && cells[index].State != Null && cells[index].State != Unset {
				return Option[int64]{}, typeValidation("partition key", "invalid cell state")
			}
		}
		token, err := InternalRoutingToken(cells, indexes)
		if token.Set && token.Value == math.MinInt64 {
			token.Value = math.MaxInt64
		}
		return token, err
	}
	if p == nil {
		// No partitioner, no token: only validate, without building the key.
		return Option[int64]{}, InternalValidatePartitionKey(cells, indexes)
	}
	k, ok, e := InternalNewPartitionKey(cells, indexes)
	if e != nil || !ok {
		return Option[int64]{}, e
	}
	v, e := p.Token(k)
	return Option[int64]{Value: v, Set: e == nil}, e
}

type cdcStreamingPartitioner struct{}

func (cdcStreamingPartitioner) Token(k PartitionKey) (int64, error) {
	if k.Len() < 8 {
		return math.MinInt64, nil
	}
	var n uint64
	remaining := 8
	e := PartitionKeyData(&k).InternalChain.Each(func(s wire.Span) error {
		for _, b := range s.Bytes() {
			if remaining == 0 {
				break
			}
			n = n<<8 | uint64(b)
			remaining--
		}
		return nil
	})
	v := int64(n)
	if v == math.MinInt64 {
		v = math.MaxInt64
	}
	return v, e
}

type murmur3StreamingPartitioner struct{}

func (murmur3StreamingPartitioner) Token(k PartitionKey) (int64, error) {
	var hash murmur.Stream
	err := PartitionKeyData(&k).InternalChain.Each(func(s wire.Span) error { _, err := hash.Write(s.Bytes()); return err })
	token := hash.Sum64()
	if token == math.MinInt64 {
		token = math.MaxInt64
	}
	return token, err
}

type RoutingOptions struct {
	Datacenter, Rack                          string
	Latency                                   LatencyOptions
	TokenAware                                bool
	AllowRemote, ShuffleReplicas, LeaderAware bool
}
type LatencyOptions struct {
	Enabled                            bool
	ExclusionThreshold                 float64
	MinimumSamples                     int
	Scale, RetryPeriod, UpdateInterval time.Duration
}
type InternalRoutingLatency struct {
	nextProbe       time.Time // protected by mu, including selection reservations
	InternalUpdated time.Time
	InternalValue   float64
	InternalSamples int
	mu              sync.Mutex
}
type InternalRoutingDecisions struct{ InternalSlow map[UUID]time.Time }
type InternalRoutingPolicy struct {
	InternalDecisions atomic.Pointer[InternalRoutingDecisions]
	updateAfter       atomic.Pointer[time.Time]
	// fixed holds, for each replica count up to sixteen, the permutation
	// a plan applies when replicas are not shuffled per request, which
	// depends only on seed and the count.
	fixed             *[17][16]uint8
	InternalLatencies sync.Map
	InternalOptions   RoutingOptions
	seed              uint64
	round             atomic.Uint64
	minimumBits       atomic.Uint64
}

func NewRoutingPolicy(o RoutingOptions) (RoutingPolicy, error) {
	if o.Rack != "" && o.Datacenter == "" {
		return nil, typeValidation("routing rack", "requires a datacenter")
	}
	if o.Latency.Enabled {
		if o.Latency.ExclusionThreshold == 0 {
			o.Latency.ExclusionThreshold = 2
		}
		if o.Latency.MinimumSamples == 0 {
			o.Latency.MinimumSamples = 50
		}
		if o.Latency.Scale == 0 {
			o.Latency.Scale = 100 * time.Millisecond
		}
		if o.Latency.RetryPeriod == 0 {
			o.Latency.RetryPeriod = 10 * time.Second
		}
		if o.Latency.UpdateInterval == 0 {
			o.Latency.UpdateInterval = time.Second
		}
		if o.Latency.ExclusionThreshold < 1 || math.IsNaN(o.Latency.ExclusionThreshold) || math.IsInf(o.Latency.ExclusionThreshold, 0) || o.Latency.MinimumSamples < 1 || o.Latency.Scale <= 0 || o.Latency.RetryPeriod <= 0 || o.Latency.UpdateInterval <= 0 {
			return nil, typeValidation("latency routing", "invalid configuration")
		}
	}
	p := &InternalRoutingPolicy{InternalOptions: o, seed: rand.Uint64()}
	if !o.ShuffleReplicas {
		p.fixed = new([17][16]uint8)
		for n := range p.fixed {
			p.fixed[n] = smallPermutation(p.seed, n)
		}
	}
	return p, nil
}
func (p *InternalRoutingPolicy) Validate(m *MetadataSnapshot) error {
	if m == nil {
		return typeValidation("routing", "missing metadata")
	}
	if p.InternalOptions.Datacenter == "" {
		return nil
	}
	idx := m.routingIndex()
	dc := idx.enabledDC[p.InternalOptions.Datacenter] > 0
	rack := idx.enabledRack[routingLocality{p.InternalOptions.Datacenter, p.InternalOptions.Rack}] > 0
	if !dc {
		return typeValidation("routing datacenter", "%q has no eligible hosts", p.InternalOptions.Datacenter)
	}
	if p.InternalOptions.Rack != "" && !rack {
		return typeValidation("routing rack", "%q has no eligible hosts", p.InternalOptions.Rack)
	}
	return nil
}

// InternalTargetPlan routes to its targets in order. A target with a shard
// that is not strict is followed by a route to any shard of its host.
// Targets resolve their hosts through hosts when set, else by ID.
type InternalTargetPlan struct {
	InternalTargets []Target
	hosts           []*pool.HostRoute
	next            int
	fallback        bool
}

func (p *InternalTargetPlan) Next() (Route, bool) {
	if p == nil {
		return Route{}, false
	}
	if p.fallback {
		p.fallback = false
		return p.route(p.next - 1).WithShard(AnyShard), true
	}
	if p.next >= len(p.InternalTargets) {
		return Route{}, false
	}
	t := p.InternalTargets[p.next]
	r := p.route(p.next)
	p.next++
	p.fallback = t.ShardID.Set && !t.StrictShard
	return r, true
}
func (p *InternalTargetPlan) route(i int) Route {
	if i < len(p.hosts) && p.hosts[i] != nil {
		r := InternalRouteOf(p.InternalTargets[i])
		r.host = p.hosts[i]
		return r
	}
	return InternalRouteOf(p.InternalTargets[i])
}

// Host indices and locality slices are built once on topology publication.
type routingLocality struct{ dc, rack string }
type InternalRoutingHostIndex struct {
	enabledDC   map[string]int
	enabledRack map[routingLocality]int
	byID        map[UUID]int
	dc          map[string][]int
	rack        map[routingLocality][]int
	all         []int
	// routes holds each host's route, in host order.
	routes []*pool.HostRoute
	// ids holds the hosts' IDs in host order when there are few enough
	// that scanning them beats hashing an ID.
	ids []UUID
}

// smallHostIndex is the most hosts whose IDs a lookup scans.
const smallHostIndex = 16

// host returns the position of the host with id.
func (x *InternalRoutingHostIndex) host(id UUID) (int, bool) {
	if x.ids != nil {
		for i := range x.ids {
			if x.ids[i] == id {
				return i, true
			}
		}
		return 0, false
	}
	i, ok := x.byID[id]
	return i, ok
}

// InternalBuildRoutingHostIndex indexes hosts with routes that belong to no
// session.
func InternalBuildRoutingHostIndex(hosts []HostSnapshot) *InternalRoutingHostIndex {
	return InternalBuildRoutingHostIndexWith(hosts, pool.NewHostRoute)
}

// InternalBuildRoutingHostIndexWith indexes hosts with the routes route
// returns for their IDs.
func InternalBuildRoutingHostIndexWith(hosts []HostSnapshot, route func(UUID) *pool.HostRoute) *InternalRoutingHostIndex {
	x := &InternalRoutingHostIndex{enabledDC: make(map[string]int), enabledRack: make(map[routingLocality]int), byID: make(map[UUID]int, len(hosts)), dc: make(map[string][]int), rack: make(map[routingLocality][]int), routes: make([]*pool.HostRoute, len(hosts))}
	for i, h := range hosts {
		perfAdd(2, 1)
		x.byID[h.ID] = i
		x.routes[i] = route(h.ID)
		if len(hosts) <= smallHostIndex {
			x.ids = append(x.ids, h.ID)
		}
		x.all = append(x.all, i)
		x.dc[h.Datacenter] = append(x.dc[h.Datacenter], i)
		k := routingLocality{h.Datacenter, h.Rack}
		if h.Enabled {
			x.enabledDC[h.Datacenter]++
			x.enabledRack[k]++
		}
		x.rack[k] = append(x.rack[k], i)
	}
	return x
}
func (m *MetadataSnapshot) routingIndex() *InternalRoutingHostIndex {
	if SnapshotData(m).InternalHostIndex != nil {
		return SnapshotData(m).InternalHostIndex
	}
	return InternalBuildRoutingHostIndex(SnapshotData(m).InternalHosts)
}

type InternalLazyRoutingPlan struct {
	policy       *InternalRoutingPolicy
	metadata     *MetadataSnapshot
	index        *InternalRoutingHostIndex
	InternalSlow *InternalRoutingDecisions
	InternalNow  time.Time
	// fallback is the route to any shard of the host just routed to a
	// shard, which the plan returns next when set.
	fallback                   Route
	smallEmitted               [8]Route
	emitted                    []Route
	replicas                   []Target
	permutation                []int // used only when replicas exceed the inline capacity
	groups                     [3][]int
	leader                     Option[Target]
	stage, cursor, latencyPass int
	rotation                   int
	token                      int64
	smallPermutation           [16]uint8
	shard                      Option[uint32] // the request's own shard, for every host
	pinned                     bool
	tokenSet                   bool
	// rotate says that the fallback tiers start at a rotating offset, taken
	// from the policy's shared counter only when a plan reaches them.
	rotate bool
	// strict plans route to a host's shard only: no fallback to any shard.
	strict bool
}

func (p *InternalRoutingPolicy) Plan(info RoutingInfo, m *MetadataSnapshot) QueryPlan {
	return p.InternalPlanInto(info, m, new(InternalLazyRoutingPlan))
}

// InternalPlanInto initializes a worker-owned cursor over immutable placement
// data. The preceding traversal must be finished before its storage is reused.
func (p *InternalRoutingPolicy) InternalPlanInto(info RoutingInfo, m *MetadataSnapshot, plan *InternalLazyRoutingPlan) QueryPlan {
	return p.InternalPlanWith(info, m, plan, nil)
}

// InternalPlanWith is InternalPlanInto for a request whose tablet was looked
// up in m, or that looks it up itself when tablet is nil or did not run.
func (p *InternalRoutingPolicy) InternalPlanWith(info RoutingInfo, m *MetadataSnapshot, plan *InternalLazyRoutingPlan, tablet *InternalTabletLookup) QueryPlan {
	info.Conditional = info.Conditional || info.Kind != OrdinaryStatement
	if m == nil {
		return &InternalTargetPlan{}
	}
	*plan = InternalLazyRoutingPlan{policy: p, metadata: m, index: m.routingIndex(), InternalSlow: p.InternalDecisions.Load()}
	if plan.InternalSlow != nil && len(plan.InternalSlow.InternalSlow) > 0 {
		// Only a slow host's penalty reads the plan's time; published
		// decisions are not modified, so a plan without one never does.
		plan.InternalNow = time.Now()
	}
	plan.emitted = plan.smallEmitted[:0]
	plan.token, plan.tokenSet = info.Token.Value, info.Token.Set
	plan.shard = info.ShardID
	plan.strict = info.Conditional || info.ShardID.Set
	if info.HostID.Set {
		plan.pinned = true
		// Pinned plans never traverse replica leaders. Keep only the captured
		// destination in that slot instead of retaining all routing inputs.
		plan.leader = Option[Target]{Value: Target{HostID: info.HostID.Value}, Set: true}
		return plan
	}
	o := &p.InternalOptions
	var lookup InternalTabletLookup
	if tablet != nil && tablet.done {
		lookup = *tablet
	} else if o.TokenAware && info.Token.Set {
		lookup = InternalLookupTablet(m, info)
	}
	if o.TokenAware && info.Token.Set && info.Keyspace != "" && info.Table != "" {
		plan.replicas, _ = m.lookupReplicas(&lookup, info.Keyspace, info.Table, info.Token.Value)
	}
	conditional := info.Conditional || info.Consistency == Serial || info.Consistency == LocalSerial
	if len(plan.replicas) > len(plan.smallPermutation) {
		plan.permutation = make([]int, len(plan.replicas))
		for i := range plan.permutation {
			plan.permutation[i] = i
		}
	} else if !conditional && len(plan.replicas) > 1 && p.fixed != nil {
		plan.smallPermutation = p.fixed[len(plan.replicas)]
	} else {
		// These are replica-relative positions, not global host indices. The
		// inline path has at most sixteen entries; larger sets keep full ints.
		for i := range plan.replicas {
			plan.smallPermutation[i] = uint8(i)
		}
	}
	if !conditional && len(plan.replicas) > 1 && (plan.permutation != nil || p.fixed == nil) {
		seed := p.seed
		if o.ShuffleReplicas {
			seed = rand.Uint64()
		}
		rng := rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)
		for i := len(plan.replicas) - 1; i > 0; i-- {
			bound := uint64(i + 1)
			threshold := -bound % bound
			n := rng.Uint64()
			for n < threshold {
				n = rng.Uint64()
			}
			j := int(n % bound)
			if plan.permutation != nil {
				plan.permutation[i], plan.permutation[j] = plan.permutation[j], plan.permutation[i]
			} else {
				plan.smallPermutation[i], plan.smallPermutation[j] = plan.smallPermutation[j], plan.smallPermutation[i]
			}
		}
	}
	if o.LeaderAware && o.TokenAware && info.Token.Set && info.Consistency != One && info.Consistency != LocalOne {
		if ks := SnapshotData(m).InternalKeyspaces[info.Keyspace]; ks != nil && ks.ConsistencyMode == "global" {
			_, r := m.lookupReplicas(&lookup, info.Keyspace, info.Table, info.Token.Value)
			if r != nil && r.InternalVersion.Set && len(r.InternalSpan.Replicas) > 0 {
				plan.leader = Option[Target]{Value: r.InternalSpan.Replicas[0], Set: true}
			}
		}
	}
	if o.Datacenter == "" {
		plan.groups[1] = plan.index.all
	} else {
		plan.groups[1] = plan.index.dc[o.Datacenter]
		if o.Rack != "" {
			plan.groups[0] = plan.index.rack[routingLocality{o.Datacenter, o.Rack}]
		}
		if o.AllowRemote {
			plan.groups[2] = plan.index.all
		}
	}
	plan.rotate = !conditional
	return plan
}

// smallPermutation is the permutation of n replica positions that a plan
// with seed applies; it shuffles exactly as InternalPlanWith does.
func smallPermutation(seed uint64, n int) [16]uint8 {
	var perm [16]uint8
	for i := range n {
		perm[i] = uint8(i)
	}
	rng := rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)
	for i := n - 1; i > 0; i-- {
		bound := uint64(i + 1)
		threshold := -bound % bound
		v := rng.Uint64()
		for v < threshold {
			v = rng.Uint64()
		}
		j := int(v % bound)
		perm[i], perm[j] = perm[j], perm[i]
	}
	return perm
}
func (p *InternalLazyRoutingPlan) host(t Target) (int, *HostSnapshot) {
	perfAdd(0, 1)
	i, ok := p.index.host(t.HostID)
	if !ok {
		return -1, nil
	}
	return i, &SnapshotData(p.metadata).InternalHosts[i]
}
func (p *InternalLazyRoutingPlan) eligible(h *HostSnapshot) bool {
	return h.Enabled && (p.policy.InternalOptions.Datacenter == "" || h.Datacenter == p.policy.InternalOptions.Datacenter || p.policy.InternalOptions.AllowRemote)
}
func (p *InternalLazyRoutingPlan) InternalPenalty(t Target) bool {
	if p.InternalSlow == nil {
		return false
	}
	until, ok := p.InternalSlow.InternalSlow[t.HostID]
	if !ok {
		return false
	}
	if p.InternalNow.Before(until) {
		return true
	}
	if p.policy == nil {
		return false
	}
	value, ok := p.policy.InternalLatencies.Load(t.HostID)
	if !ok {
		return false
	}
	// Expiration permits a new measurement before the next periodic decision
	// publication. Reserve one probe across concurrent plans; other requests
	// still keep this host behind healthy candidates. Penalized fallback is
	// retained when the ordinary plan has no usable alternative.
	sample := value.(*InternalRoutingLatency)
	// time.Time retains the full configured duration and monotonic clock. An
	// absolute UnixNano deadline can wrap even for a valid positive duration.
	// Observe already uses this per-host lock; no policy-wide lock is involved.
	sample.mu.Lock()
	defer sample.mu.Unlock()
	if p.InternalNow.Before(sample.nextProbe) {
		return true
	}
	sample.nextProbe = p.InternalNow.Add(p.policy.InternalOptions.Latency.RetryPeriod)
	return false
}

// duplicate reports whether the plan already routed to r: to its host and
// shard, or to any shard of its host, or, for r to any shard, to its host.
func (p *InternalLazyRoutingPlan) duplicate(r Route) bool {
	for _, e := range p.emitted {
		if e.host == r.host && (e.Shard == r.Shard || e.Shard == AnyShard || r.Shard == AnyShard) {
			return true
		}
	}
	return false
}

// accept returns the route to target t of host i, h, unless the latency
// pass excludes it or the plan already routed to it. A replica's
// route goes to the shard that owns the plan's token, unless placement or
// the request names one; another host's goes to any shard. A route to a
// shard is followed by one to any shard of the host unless the plan or
// target is strict.
func (p *InternalLazyRoutingPlan) accept(t Target, i int, h *HostSnapshot, replica bool) (Route, bool) {
	if p.InternalPenalty(t) != (p.latencyPass == 1) {
		return Route{}, false
	}
	host := p.index.routes[i]
	r := Route{host: host, Shard: AnyShard}
	switch {
	case p.shard.Set:
		r.Shard = int32(min(p.shard.Value, math.MaxInt32))
	case t.ShardID.Set:
		r.Shard = int32(min(t.ShardID.Value, math.MaxInt32))
	case replica && p.tokenSet && h.Shards > 0:
		r.Shard = shardOf(p.token, h.Shards, h.ShardingIgnoreMSB)
	}
	if p.duplicate(r) {
		return Route{}, false
	}
	p.emitted = append(p.emitted, r)
	if r.Shard != AnyShard && !p.strict && !t.StrictShard {
		p.fallback = Route{host: host, Shard: AnyShard}
	}
	return r, true
}
func (p *InternalLazyRoutingPlan) Next() (Route, bool) {
	if r := p.fallback; r.host != nil {
		// The fallback follows its host's route to a shard; it repeats
		// only a route to any shard of the host.
		p.fallback = Route{}
		repeated := false
		for _, e := range p.emitted {
			repeated = repeated || e.host == r.host && e.Shard == AnyShard
		}
		if !repeated {
			p.emitted = append(p.emitted, r)
			return r, true
		}
	}
	if !p.pinned && p.metadata != nil && SnapshotData(p.metadata).InternalPlacementSignal != nil && SnapshotData(p.metadata).InternalPlacementEpoch != SnapshotData(p.metadata).InternalPlacementSignal.Load() {
		p.replicas = nil
		p.permutation = nil
		p.leader = Option[Target]{}
		if p.stage <= 3 {
			p.stage = 4
			p.cursor = 0
		}
	}
	if p.pinned {
		if p.stage > 0 {
			return Route{}, false
		}
		p.stage++
		t := p.leader.Value
		if i, h := p.host(t); h != nil && h.Enabled {
			p.latencyPass = 0
			p.InternalSlow = nil // a pinned host is never skipped for latency
			return p.accept(t, i, h, false)
		}
		return Route{}, false
	}
	for {
		if p.stage == 0 {
			p.stage++
			if p.leader.Set {
				t := p.leader.Value
				if i, h := p.host(t); h != nil && p.eligible(h) && h.Connected && h.Up && (!t.ShardID.Set || h.Shards > 0 && t.ShardID.Value < h.Shards) {
					t.StrictShard = t.ShardID.Set
					if r, ok := p.accept(t, i, h, true); ok {
						return r, true
					}
				}
			}
		}
		// Connected replicas: rack, DC, then permitted remote replicas. Remote
		// replicas precede local nonreplicas.
		for p.stage >= 1 && p.stage <= 3 {
			// A tier no replica can be in is skipped without looking its
			// replicas up: the rack tier without a rack, the remote tier
			// without a datacenter or without remote replicas allowed.
			if p.policy != nil {
				if o := &p.policy.InternalOptions; p.stage == 1 && o.Rack == "" || p.stage == 3 && (o.Datacenter == "" || !o.AllowRemote) {
					p.cursor = 0
					p.stage++
					continue
				}
			}
			for p.cursor < len(p.replicas) {
				var k int
				if p.permutation != nil {
					k = p.permutation[p.cursor]
				} else {
					k = int(p.smallPermutation[p.cursor])
				}
				t := p.replicas[k]
				p.cursor++
				i, h := p.host(t)
				if h == nil || !p.eligible(h) || !h.Connected || !h.Up {
					continue
				}
				o := &p.policy.InternalOptions
				local := o.Datacenter == "" || h.Datacenter == o.Datacenter
				tier := 3
				if local {
					tier = 2
					if o.Rack != "" && h.Rack == o.Rack {
						tier = 1
					}
				}
				if tier != p.stage {
					continue
				}
				if r, ok := p.accept(t, i, h, true); ok {
					return r, true
				}
			}
			p.cursor = 0
			p.stage++
		}
		// Shared immutable fallback indices are traversed only on demand.
		if p.rotate && p.stage >= 4 {
			p.rotate = false
			p.rotation = int(p.policy.round.Add(1) & uint64(^uint(0)>>1))
		}
		for p.stage >= 4 && p.stage <= 8 {
			group := p.stage - 4
			connected := true
			if p.stage >= 7 {
				group = p.stage - 6
				connected = false
			}
			ids := p.groups[group]
			for p.cursor < len(ids) {
				k := (p.cursor + p.rotation) % len(ids)
				p.cursor++
				perfAdd(1, 1)
				h := &SnapshotData(p.metadata).InternalHosts[ids[k]]
				if !p.eligible(h) || connected && (!h.Connected || !h.Up) {
					continue
				}
				if group == 2 && p.policy.InternalOptions.Datacenter != "" && h.Datacenter == p.policy.InternalOptions.Datacenter {
					continue
				}
				if r, ok := p.accept(Target{HostID: h.ID}, ids[k], h, false); ok {
					return r, true
				}
			}
			p.cursor = 0
			p.stage++
		}
		if p.latencyPass == 0 && p.InternalSlow != nil {
			p.latencyPass = 1
			p.stage = 0
			p.cursor = 0
			continue
		}
		return Route{}, false
	}
}
func (p *InternalRoutingPolicy) Observe(e AttemptEvent) {
	if !p.InternalOptions.Latency.Enabled || e.Cancelled || !InternalRoutingLatencySample(e.Err) || e.Latency <= 0 {
		return
	}
	actual, found := p.InternalLatencies.Load(e.Target.HostID)
	if !found {
		actual, _ = p.InternalLatencies.LoadOrStore(e.Target.HostID, &InternalRoutingLatency{})
	}
	v := actual.(*InternalRoutingLatency)
	now := e.Started.Add(e.Latency)
	if e.Started.IsZero() {
		now = time.Now()
	}
	v.mu.Lock()
	weight := 1.0
	if v.InternalSamples > 0 {
		elapsed := max(now.Sub(v.InternalUpdated), 0)
		weight = 1 - math.Exp(-float64(elapsed)/float64(p.InternalOptions.Latency.Scale))
	}
	v.InternalValue += weight * (float64(e.Latency) - v.InternalValue)
	v.InternalSamples++
	v.InternalUpdated = now
	v.nextProbe = now.Add(p.InternalOptions.Latency.RetryPeriod)
	value, samples := v.InternalValue, v.InternalSamples
	v.mu.Unlock()
	if samples >= p.InternalOptions.Latency.MinimumSamples {
		for {
			bits := p.minimumBits.Load()
			if bits != 0 && value >= math.Float64frombits(bits) {
				break
			}
			if p.minimumBits.CompareAndSwap(bits, math.Float64bits(value)) {
				p.updateAfter.Store(nil)
				break
			}
		}
	}
	clock := time.Now()
	if !p.claimLatencyUpdate(clock) {
		return
	}
	minimum := math.Inf(1)
	p.InternalLatencies.Range(func(_, value any) bool {
		v := value.(*InternalRoutingLatency)
		v.mu.Lock()
		if v.InternalSamples >= p.InternalOptions.Latency.MinimumSamples && v.InternalValue > 0 {
			minimum = min(minimum, v.InternalValue)
		}
		v.mu.Unlock()
		return true
	})
	decisions := &InternalRoutingDecisions{InternalSlow: make(map[UUID]time.Time)}
	p.InternalLatencies.Range(func(key, value any) bool {
		v := value.(*InternalRoutingLatency)
		v.mu.Lock()
		if v.InternalSamples >= p.InternalOptions.Latency.MinimumSamples && v.InternalValue > minimum*p.InternalOptions.Latency.ExclusionThreshold {
			decisions.InternalSlow[key.(UUID)] = v.InternalUpdated.Add(p.InternalOptions.Latency.RetryPeriod)
		}
		v.mu.Unlock()
		return true
	})
	p.InternalDecisions.Store(decisions)
}

// A publication claim allocates its immutable deadline only at a due boundary.
// Contending claimants may allocate losing candidates there; ordinary enabled
// observations before the deadline and disabled latency paths allocate nothing.
func (p *InternalRoutingPolicy) claimLatencyUpdate(clock time.Time) bool {
	due := p.updateAfter.Load()
	if due != nil && clock.Before(*due) {
		return false
	}
	next := clock.Add(p.InternalOptions.Latency.UpdateInterval)
	return p.updateAfter.CompareAndSwap(due, &next)
}

type singleTargetRouting struct {
	target Target
	strict bool
}

func SingleTargetRouting(t Target, strictShard bool) RoutingPolicy {
	return &singleTargetRouting{target: t, strict: strictShard}
}
func (p *singleTargetRouting) Validate(m *MetadataSnapshot) error {
	if (p.strict || p.target.StrictShard) && !p.target.ShardID.Set {
		return typeValidation("routing shard", "strict target requires a shard ID")
	}
	if m != nil {
		if i, ok := m.routingIndex().host(p.target.HostID); ok {
			h := SnapshotData(m).InternalHosts[i]
			if h.Enabled {
				if (p.strict || p.target.StrictShard) && p.target.ShardID.Set && (h.Shards == 0 || p.target.ShardID.Value >= h.Shards) {
					return typeValidation("routing shard", "unavailable shard")
				}
				return nil
			}
		}
	}
	return typeValidation("routing host", "host is not eligible")
}
func (p *singleTargetRouting) Plan(_ RoutingInfo, m *MetadataSnapshot) QueryPlan {
	if m != nil {
		if i, ok := m.routingIndex().host(p.target.HostID); ok {
			h := SnapshotData(m).InternalHosts[i]
			if h.Enabled {
				t := p.target
				t.StrictShard = p.strict || t.StrictShard
				if t.ShardID.Set && (h.Shards == 0 || t.ShardID.Value >= h.Shards) {
					if t.StrictShard {
						return &InternalTargetPlan{}
					}
					t.ShardID = Option[uint32]{}
				}
				return &InternalTargetPlan{InternalTargets: []Target{t}, hosts: []*pool.HostRoute{m.routingIndex().routes[i]}}
			}
		}
	}
	return &InternalTargetPlan{}
}
func (*singleTargetRouting) Observe(AttemptEvent) {}

func InternalRoutingLatencySample(err error) bool {
	if err == nil {
		return true
	}
	var server RequestError
	if errors.As(err, &server) {
		switch server.GetCode() {
		case ErrCodeUnavailable, ErrCodeOverloaded, ErrCodeBootstrapping, ErrCodeUnprepared:
			return false
		}
		var rate *RequestErrRateLimitReached
		return !errors.As(err, &rate)
	}
	// Local validation/serialization and broken transport are not response latency.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, runtimeerrors.ErrNoConnections) || errors.Is(err, runtimeerrors.ErrConnectionClosed) {
		return false
	}
	var validation *ValidationError
	var network net.Error
	if errors.As(err, &validation) || errors.As(err, &network) {
		return false
	}
	return true
}

type PartitionKeyState struct {
	InternalChain wire.Chain
}
