package session

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/model"
)

// TestPreparedViewConcurrentUpdate publishes a new host with new result metadata
// while 32 readers load the view; every load must be a consistent triple.
func TestPreparedViewConcurrentUpdate(t *testing.T) {
	first := cqltype.UUID{13: 1}
	r0 := &model.ResultSchemaIdentity{}
	state := newTestPreparedState(&InternalPreparedState{}, map[cqltype.UUID]*InternalHostPrepared{first: {InternalResult: r0}}, r0, first)
	var stop atomic.Bool
	var bad atomic.Int64
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				v := state.loadView()
				entry := v.hosts[v.resultHost]
				if entry == nil || entry.InternalResult != v.result {
					bad.Add(1)
				}
			}
		}()
	}
	for i := 1; i <= 2000; i++ {
		id := cqltype.UUID{15: byte(i), 14: byte(i >> 8)}
		result := &model.ResultSchemaIdentity{}
		state.InternalMu.Lock()
		state.publishHost(id, &InternalHostPrepared{InternalResult: result}, true, result)
		state.InternalMu.Unlock()
	}
	stop.Store(true)
	wg.Wait()
	if n := bad.Load(); n != 0 {
		t.Fatalf("%d inconsistent views", n)
	}
	if got := len(state.loadView().hosts); got != 2001 {
		t.Fatalf("hosts = %d, want 2001", got)
	}
}

func TestPreparedViewEmpty(t *testing.T) {
	var state InternalPreparedState
	if state.host(cqltype.UUID{1: 1}) != nil || state.loadView().result != nil {
		t.Fatal("zero state must expose an empty view")
	}
}

// newTestPreparedState seeds a view for tests.
func newTestPreparedState(state *InternalPreparedState, hosts map[cqltype.UUID]*InternalHostPrepared, result *model.ResultSchemaIdentity, resultHost cqltype.UUID) *InternalPreparedState {
	state.view.Store(&preparedView{hosts: hosts, result: result, resultHost: resultHost})
	return state
}
