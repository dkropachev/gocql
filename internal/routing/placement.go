// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package routing

import (
	"cmp"
	"iter"
	"maps"
	"math/rand/v2"
	"strings"
	"sync/atomic"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/tablets"
	// Immutable AVL indices share every unaffected subtree. Neither an observation
	// nor a routing plan may mutate a published node or replica slice.
)

// placementOrder orders the keys of a placement tree. Implementations are
// empty structs, so a tree carries its order in its type, not in each call.
type placementOrder[K any] interface {
	compare(a, b K) int
}

// tokenOrder orders tablet ranges by their last token.
type tokenOrder struct{}

func (tokenOrder) compare(a, b int64) int { return cmp.Compare(a, b) }

// tableOrder orders placements by keyspace, then table.
type tableOrder struct{}

func (tableOrder) compare(a, b InternalMetadataTable) int {
	// Names are ordered by length first: a lookup mostly meets names of
	// other lengths, or its own keyspace, and decides those without
	// comparing bytes in order.
	if c := comparePlacementName(a.InternalKeyspace, b.InternalKeyspace); c != 0 {
		return c
	}
	return comparePlacementName(a.InternalTable, b.InternalTable)
}

// InternalPlacementTree is an immutable AVL tree ordered by O. A nil tree is
// empty; Put and Delete return a new root sharing unchanged subtrees.
type InternalPlacementTree[K, V any, O placementOrder[K]] struct {
	key           K
	InternalValue V
	left, right   *InternalPlacementTree[K, V, O]
	height        int
}

func (n *InternalPlacementTree[K, V, O]) treeHeight() int {
	if n == nil {
		return 0
	}
	return n.height
}

func placementNode[K, V any, O placementOrder[K]](key K, value V, l, r *InternalPlacementTree[K, V, O]) *InternalPlacementTree[K, V, O] {
	return &InternalPlacementTree[K, V, O]{key: key, InternalValue: value, left: l, right: r, height: 1 + max(l.treeHeight(), r.treeHeight())}
}

func (n *InternalPlacementTree[K, V, O]) balance() *InternalPlacementTree[K, V, O] {
	if n.left.treeHeight() > n.right.treeHeight()+1 {
		l := n.left
		if l.right.treeHeight() > l.left.treeHeight() {
			r := l.right
			l = placementNode(r.key, r.InternalValue, placementNode(l.key, l.InternalValue, l.left, r.left), r.right)
		}
		return placementNode(l.key, l.InternalValue, l.left, placementNode(n.key, n.InternalValue, l.right, n.right))
	}
	if n.right.treeHeight() > n.left.treeHeight()+1 {
		r := n.right
		if r.left.treeHeight() > r.right.treeHeight() {
			l := r.left
			r = placementNode(l.key, l.InternalValue, l.left, placementNode(r.key, r.InternalValue, l.right, r.right))
		}
		return placementNode(r.key, r.InternalValue, placementNode(n.key, n.InternalValue, n.left, r.left), r.right)
	}
	return n
}

// Get returns the node holding key, or nil.
func (n *InternalPlacementTree[K, V, O]) Get(key K) *InternalPlacementTree[K, V, O] {
	var order O
	for n != nil {
		c := order.compare(key, n.key)
		if c == 0 {
			return n
		}
		if c < 0 {
			n = n.left
		} else {
			n = n.right
		}
	}
	return nil
}

// Put returns a tree that maps key to value.
func (n *InternalPlacementTree[K, V, O]) Put(key K, value V) *InternalPlacementTree[K, V, O] {
	if n == nil {
		return placementNode[K, V, O](key, value, nil, nil)
	}
	var order O
	c := order.compare(key, n.key)
	if c == 0 {
		return placementNode(key, value, n.left, n.right)
	}
	if c < 0 {
		return placementNode(n.key, n.InternalValue, n.left.Put(key, value), n.right).balance()
	}
	return placementNode(n.key, n.InternalValue, n.left, n.right.Put(key, value)).balance()
}

// Delete returns a tree without key.
func (n *InternalPlacementTree[K, V, O]) Delete(key K) *InternalPlacementTree[K, V, O] {
	if n == nil {
		return nil
	}
	var order O
	c := order.compare(key, n.key)
	if c < 0 {
		return placementNode(n.key, n.InternalValue, n.left.Delete(key), n.right).balance()
	}
	if c > 0 {
		return placementNode(n.key, n.InternalValue, n.left, n.right.Delete(key)).balance()
	}
	if n.left == nil {
		return n.right
	}
	if n.right == nil {
		return n.left
	}
	next := n.right
	for next.left != nil {
		next = next.left
	}
	return placementNode(next.key, next.InternalValue, n.left, n.right.Delete(next.key)).balance()
}

// All yields the tree's entries in key order.
func (n *InternalPlacementTree[K, V, O]) All() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) { n.walk(yield) }
}

func (n *InternalPlacementTree[K, V, O]) walk(yield func(K, V) bool) bool {
	return n == nil || n.left.walk(yield) && yield(n.key, n.InternalValue) && n.right.walk(yield)
}

func comparePlacementName(a, b string) int {
	if len(a) != len(b) {
		if len(a) < len(b) {
			return -1
		}
		return 1
	}
	if a == b {
		return 0
	}
	return strings.Compare(a, b)
}

var tabletObservationID atomic.Uint64

type InternalTabletRange struct {
	InternalSpan    TokenRange
	InternalVersion cqltype.Option[uint64]
	observation     uint64
}
type InternalTabletRangeTree = InternalPlacementTree[int64, *InternalTabletRange, tokenOrder]
type InternalTabletPlacement struct {
	InternalRanges   *InternalTabletRangeTree
	InternalIdentity string
	// index lists InternalRanges in chunks for lookups; it is used only
	// while its root is still InternalRanges.
	index *tabletRangeIndex
}
type tabletPlacementTree = InternalPlacementTree[InternalMetadataTable, InternalTabletPlacement, tableOrder]

func InternalTabletRangeAfter(n *InternalTabletRangeTree, token int64) *InternalTabletRangeTree {
	var result *InternalTabletRangeTree
	for n != nil {
		if n.key > token {
			result = n
			n = n.left
		} else {
			n = n.right
		}
	}
	return result
}
func InternalTabletRangeFor(n *InternalTabletRangeTree, token int64) *InternalTabletRangeTree {
	var result *InternalTabletRangeTree
	for n != nil {
		if n.key >= token {
			result = n
			n = n.left
		} else {
			n = n.right
		}
	}
	if result != nil && token > result.InternalValue.InternalSpan.Start {
		return result
	}
	return nil
}
func (a *InternalTabletRange) SameTabletObservation(h tablets.TabletInfo) bool {
	v, ok := h.Version()
	if a.InternalVersion.Set != ok || ok && a.InternalVersion.Value != v || a.InternalSpan.Start != h.FirstToken() || a.InternalSpan.End != h.LastToken() {
		return false
	}
	r := h.ReplicasUnsafe()
	if len(r) != len(a.InternalSpan.Replicas) {
		return false
	}
	for i, t := range a.InternalSpan.Replicas {
		if t.HostID != cqltype.UUID(r[i].HostUUIDValue()) || !t.ShardID.Set || t.ShardID.Value != uint32(r[i].ShardID()) {
			return false
		}
	}
	return true
}
func InternalUpdateTabletRange(root *InternalTabletRangeTree, h tablets.TabletInfo) (*InternalTabletRangeTree, bool) {
	first, last := h.FirstToken(), h.LastToken()
	n := InternalTabletRangeAfter(root, first)
	if n != nil && n.InternalValue.SameTabletObservation(h) {
		return root, false
	}
	for n != nil && n.InternalValue.InternalSpan.Start < last {
		root = root.Delete(n.key)
		n = InternalTabletRangeAfter(root, first)
	}
	r := &InternalTabletRange{observation: tabletObservationID.Add(1), InternalSpan: TokenRange{Start: first, End: last, Replicas: make([]Target, len(h.ReplicasUnsafe()))}}
	for i, replica := range h.ReplicasUnsafe() {
		r.InternalSpan.Replicas[i] = Target{HostID: cqltype.UUID(replica.HostUUIDValue()), ShardID: cqltype.Option[uint32]{Value: uint32(replica.ShardID()), Set: true}}
	}
	r.InternalVersion.Value, r.InternalVersion.Set = h.Version()
	return root.Put(last, r), true
}

func InternalMetadataSnapshotTableIdentity(m *MetadataSnapshot, k InternalMetadataTable) string {
	if m == nil {
		return ""
	}
	ks := SnapshotData(m).InternalKeyspaces[k.InternalKeyspace]
	if ks == nil {
		return ""
	}
	if t := ks.Tables[k.InternalTable]; t != nil {
		return t.ID
	}
	if v := ks.Views[k.InternalTable]; v != nil {
		return v.ID
	}
	return ""
}
func InternalMetadataSnapshotTabletRoot(m *MetadataSnapshot, k InternalMetadataTable) (*InternalTabletRangeTree, bool) {
	if m == nil {
		return nil, false
	}
	if n := SnapshotData(m).InternalPlacements.Get(k); n != nil {
		if SnapshotData(m).InternalPlacementSignal != nil && SnapshotData(m).InternalPlacementEpoch != SnapshotData(m).InternalPlacementSignal.Load() {
			return nil, true
		}
		return n.InternalValue.InternalRanges, true
	}
	return nil, false
}

// InternalTabletLookup is the tablet holding a request's token in a
// snapshot, found once for an execution and shared by its tablet version
// block and its routing plan.
type InternalTabletLookup struct {
	// node is the tablet holding the token, or nil.
	node *InternalTabletRange
	// tracked reports that the snapshot tracks the table's tablets, so
	// a missing node means no replicas rather than vnode placement.
	tracked bool
	// done reports that the lookup ran; the zero lookup did not.
	done bool
}

// InternalLookupTablet finds the tablet holding info's token in m.
func InternalLookupTablet(m *MetadataSnapshot, info model.RoutingInfo) InternalTabletLookup {
	l := InternalTabletLookup{done: true}
	if m == nil || info.Table == "" || info.Keyspace == "" || !info.Token.Set {
		return l
	}
	if SnapshotData(m).InternalPlacementSignal != nil && SnapshotData(m).InternalPlacementEpoch != SnapshotData(m).InternalPlacementSignal.Load() {
		// A stale snapshot routes to no replicas, as placementReplicas does.
		l.tracked = true
		return l
	}
	if n := SnapshotData(m).InternalPlacements.Get(InternalMetadataTable{InternalKeyspace: info.Keyspace, InternalTable: info.Table}); n != nil {
		l.tracked = true
		l.node = n.InternalValue.tabletRangeFor(info.Token.Value)
	}
	return l
}

// InternalPlacementCache remembers the placement of one statement's table
// in the placement tree it was last found in, so that requests of the
// statement skip the tree walk until a tablet update replaces the tree.
type InternalPlacementCache struct {
	entry atomic.Pointer[placementCacheEntry]
}

type placementCacheEntry struct {
	root *tabletPlacementTree
	// placement is the table's placement in root, or nil without one.
	placement *InternalTabletPlacement
}

// InternalLookupTabletCached is InternalLookupTablet for a statement whose
// keyspace and table are fixed, finding their placement through c.
func InternalLookupTabletCached(m *MetadataSnapshot, info model.RoutingInfo, c *InternalPlacementCache) InternalTabletLookup {
	l := InternalTabletLookup{done: true}
	if m == nil || info.Table == "" || info.Keyspace == "" || !info.Token.Set {
		return l
	}
	if SnapshotData(m).InternalPlacementSignal != nil && SnapshotData(m).InternalPlacementEpoch != SnapshotData(m).InternalPlacementSignal.Load() {
		l.tracked = true
		return l
	}
	root := SnapshotData(m).InternalPlacements
	e := c.entry.Load()
	if e == nil || e.root != root {
		e = &placementCacheEntry{root: root}
		if n := root.Get(InternalMetadataTable{InternalKeyspace: info.Keyspace, InternalTable: info.Table}); n != nil {
			e.placement = &n.InternalValue
		}
		c.entry.Store(e)
	}
	if e.placement != nil {
		l.tracked = true
		l.node = e.placement.tabletRangeFor(info.Token.Value)
	}
	return l
}

// VersionBlock returns the tablet version block a request with lookup l
// sends: a random nibble of the tablet's version and its index, or a random
// byte when the tablet or its version is unknown.
func (l *InternalTabletLookup) VersionBlock(info model.RoutingInfo) byte {
	if info.Table == "" || info.Keyspace == "" || !info.Token.Set {
		return 0
	}
	if n := l.node; n != nil && n.InternalVersion.Set {
		index := rand.Uint32N(16)
		return byte(index<<4) | byte(n.InternalVersion.Value>>(index*4)&15)
	}
	return byte(rand.Uint32())
}

func InternalMetadataSnapshotTabletVersionBlock(m *MetadataSnapshot, info model.RoutingInfo) byte {
	l := InternalLookupTablet(m, info)
	return l.VersionBlock(info)
}

// placementReplicas is a private borrowed view; public accessors detach it.
func (m *MetadataSnapshot) placementReplicas(keyspace, table string, token int64) ([]Target, *InternalTabletRange) {
	l := InternalLookupTablet(m, model.RoutingInfo{Keyspace: keyspace, Table: table, Token: cqltype.Option[int64]{Value: token, Set: true}})
	return m.lookupReplicas(&l, keyspace, table, token)
}

// lookupReplicas is placementReplicas for a request whose tablet l was
// looked up in m.
func (m *MetadataSnapshot) lookupReplicas(l *InternalTabletLookup, keyspace, table string, token int64) ([]Target, *InternalTabletRange) {
	if m == nil {
		return nil, nil
	}
	if l.tracked {
		if n := l.node; n != nil {
			return n.InternalSpan.Replicas, n
		}
		return nil, nil
	}
	ranges, tablet, err := InternalMetadataSnapshotLookupRanges(m, keyspace, table)
	if err != nil || len(ranges) == 0 {
		return nil, nil
	}
	return m.legacyPlacementReplicas(ranges, tablet, token), nil
}
func (m *MetadataSnapshot) legacyPlacementReplicas(ranges []TokenRange, tablet bool, token int64) []Target {
	lo, hi := 0, len(ranges)
	for lo < hi {
		mid := (lo + hi) / 2
		if ranges[mid].End < token {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if tablet {
		if lo == len(ranges) || token <= ranges[lo].Start {
			return nil
		}
	} else if lo == len(ranges) {
		lo = 0
	}
	return ranges[lo].Replicas
}

// InternalTabletFeedbackStamp is what a request captured before sending to
// check a tablet hint in its response against. A capture takes only
// published copies; the schema values it would read from them are read on
// receipt of a hint, which most responses do not carry, when
// InternalSchemaEpochs is set.
type InternalTabletFeedbackStamp struct {
	InternalSnapshot *MetadataSnapshot
	// InternalSchemaEpochs and InternalTableRevisions are the published
	// copies InternalSchema and InternalTable are read from on receipt.
	InternalSchemaEpochs   *map[string]uint64
	InternalTableRevisions *map[InternalMetadataTable]uint64
	// InternalIdentity is the table's schema source identity until receipt,
	// then the snapshot's identity it matched.
	InternalIdentity                             string
	InternalEpoch, InternalSchema, InternalTable uint64
	InternalValid                                bool
}

func InternalClonePlacementSnapshot(m *MetadataSnapshot) *MetadataSnapshot {
	return NewMetadataSnapshot(SnapshotState{InternalGeneration: SnapshotData(m).InternalGeneration + 1, InternalHosts: SnapshotData(m).InternalHosts, InternalHostIndex: SnapshotData(m).InternalHostIndex, InternalKeyspaces: SnapshotData(m).InternalKeyspaces, InternalSchemaSources: SnapshotData(m).InternalSchemaSources, InternalRanges: SnapshotData(m).InternalRanges, InternalTablets: SnapshotData(m).InternalTablets, InternalPartitioner: SnapshotData(m).InternalPartitioner, InternalDiagnostics: SnapshotData(m).InternalDiagnostics, InternalPlacements: SnapshotData(m).InternalPlacements, InternalPlacementEpoch: SnapshotData(m).InternalPlacementEpoch, InternalPlacementSignal: SnapshotData(m).InternalPlacementSignal})
}

// Schema/topology refresh is a cold path. It reconciles object identities and
// removed replicas without constructing a new schema graph for tablet feedback.
func InternalReconcileTabletPlacement(old, current *MetadataSnapshot) *tabletPlacementTree {
	var root *tabletPlacementTree
	if old != nil {
		root = SnapshotData(old).InternalPlacements
	}
	known := make(map[cqltype.UUID]bool, len(SnapshotData(current).InternalHosts))
	for _, h := range SnapshotData(current).InternalHosts {
		known[h.ID] = true
	}
	for k, t := range root.All() {
		if _, _, err := InternalMetadataSnapshotLookupRanges(current, k.InternalKeyspace, k.InternalTable); err != nil || InternalMetadataSnapshotTableIdentity(current, k) != t.InternalIdentity {
			root = root.Delete(k)
			continue
		}
		ranges := t.InternalRanges
		previousKS := SnapshotData(old).InternalKeyspaces[k.InternalKeyspace]
		currentKS := SnapshotData(current).InternalKeyspaces[k.InternalKeyspace]
		if previousKS != nil && currentKS != nil {
			if previousKS.StrategyClass != currentKS.StrategyClass || !maps.Equal(previousKS.StrategyOptions, currentKS.StrategyOptions) || previousKS.ConsistencyMode != currentKS.ConsistencyMode {
				ranges = nil
			}
			if a, b := previousKS.Tables[k.InternalTable], currentKS.Tables[k.InternalTable]; a != nil && b != nil && !a.Equals(b) {
				ranges = nil
			}
		}
		if SnapshotData(old).InternalPlacementEpoch != SnapshotData(current).InternalPlacementEpoch {
			ranges = nil
		} else {
			for end, r := range ranges.All() {
				for _, target := range r.InternalSpan.Replicas {
					if !known[target.HostID] {
						ranges = ranges.Delete(end)
						break
					}
				}
			}
		}
		if ranges != t.InternalRanges {
			t.InternalRanges = ranges
			root = root.Put(k, InternalIndexTabletPlacement(t))
		}
	}
	for k, ranges := range SnapshotData(current).InternalTablets {
		if root.Get(k) != nil {
			continue
		}
		if _, _, err := InternalMetadataSnapshotLookupRanges(current, k.InternalKeyspace, k.InternalTable); err != nil {
			continue
		}
		t := InternalTabletPlacement{InternalIdentity: InternalMetadataSnapshotTableIdentity(current, k)}
		for _, r := range ranges {
			if r.Start < r.End {
				v := &InternalTabletRange{observation: tabletObservationID.Add(1), InternalSpan: r}
				t.InternalRanges = t.InternalRanges.Put(r.End, v)
			}
		}
		root = root.Put(k, t)
	}
	for name, ks := range SnapshotData(current).InternalKeyspaces {
		if ks.TabletBased {
			for table := range ks.Tables {
				k := InternalMetadataTable{InternalKeyspace: name, InternalTable: table}
				if root.Get(k) == nil {
					root = root.Put(k, InternalTabletPlacement{InternalIdentity: InternalMetadataSnapshotTableIdentity(current, k)})
				}
			}
			for table := range ks.Views {
				k := InternalMetadataTable{InternalKeyspace: name, InternalTable: table}
				if root.Get(k) == nil {
					root = root.Put(k, InternalTabletPlacement{InternalIdentity: InternalMetadataSnapshotTableIdentity(current, k)})
				}
			}
		}
	}
	return root
}

// Monotonically unique observation IDs provide a bounded exact interval fence:
// every inserted overlapping record raises maxID; removals without an insertion
// reduce count. Published records never change IDs. Unchanged AVL ancestors can
// be rebuilt without changing a range's identity.
type InternalTabletRangeObservation struct {
	maxID uint64
	count uint64
}

func InternalObserveTabletRanges(root *InternalTabletRangeTree, first, last int64) InternalTabletRangeObservation {
	var result InternalTabletRangeObservation
	var visit func(*InternalTabletRangeTree)
	visit = func(n *InternalTabletRangeTree) {
		if n == nil {
			return
		}
		if n.key <= first {
			visit(n.right)
			return
		}
		if n.InternalValue.InternalSpan.Start >= last {
			visit(n.left)
			return
		}
		visit(n.left)
		result.maxID = max(result.maxID, n.InternalValue.observation)
		result.count++
		visit(n.right)
	}
	visit(root)
	return result
}
