// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package pool

import (
	"context"
	"errors"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	endpoint2 "github.com/scylladb/gocql/v2/endpoint"
	"github.com/scylladb/gocql/v2/internal/connectivity"
	"github.com/scylladb/gocql/v2/internal/endpoint"
	logging "github.com/scylladb/gocql/v2/internal/logging"
	"github.com/scylladb/gocql/v2/internal/transport"
)

func TestHostConnPool(t *testing.T) {
	t.Run("FillingStopped", func(t *testing.T) {
		t.Run("Conviction", func(t *testing.T) {
			for _, convict := range []bool{false, true} {
				t.Run(map[bool]string{false: "keep", true: "convict"}[convict], func(t *testing.T) {
					id := cql.MustParseUUID("9f3b8cab-d382-4c16-9c4c-07283f206bbc")
					host := (endpoint.HostInfoBuilder{HostId: id.String(), ConnectAddress: net.ParseIP("127.0.0.7"), Port: 9042, DataCenter: "dc1", Rack: "r2", Tokens: []string{"-12", "64"}}).Build()
					endpoint.InternalHostInfoSetState(&host, endpoint.NodeUp)
					features := endpoint.ScyllaHostFeatures{}
					endpoint.ScyllaHostFeaturesData(&features).InternalNrShards = 4
					endpoint.InternalHostInfoSetScyllaFeatures(&host, features)
					cause := errors.New("actual pool failure completion")
					calls, downs := 0, 0
					p := &InternalHostConnPool{InternalHost: &host, InternalConnPicker: convictionPicker{}, InternalLogger: logging.Nop()}
					policy := &convictionRecorder{callback: func(e endpoint2.ConvictionEvent) bool {
						err, snapshot := e.Err, e.Host
						if !p.InternalFilling {
							t.Fatal("fill ended before the policy saw its failure")
						}
						if e.ShardID.Set || e.Source != endpoint2.ConvictionPool {
							t.Fatalf("whole-host pool failure reported as %+v", e)
						}
						if !p.InternalMu.TryLock() {
							t.Fatal("conviction callback holds pool lock")
						}
						p.InternalMu.Unlock()
						calls++
						if !errors.Is(err, cause) {
							t.Fatal("failure replaced", err)
						}
						if snapshot.ID != id || snapshot.Address.String() != "127.0.0.7:9042" || snapshot.Datacenter != "dc1" || snapshot.Rack != "r2" || !reflect.DeepEqual(snapshot.Tokens, []int64{-12, 64}) || !snapshot.Up || snapshot.Shards != 4 || !snapshot.Enabled || snapshot.Connected {
							t.Fatalf("lost host context: %+v", snapshot)
						}
						snapshot.Tokens[0] = 999 // Detached callback storage cannot mutate HostInfo.
						return convict
					}}
					p.InternalServices = InternalPoolServices{Config: &connectivity.Config{}, Conviction: policy, Down: func(down *endpoint.HostInfo) {
						downs++
						if down != &host {
							t.Fatal("wrong down host")
						}
					}}
					p.InternalFilling = true
					p.fillingStopped(cause)
					if p.InternalFilling {
						t.Fatal("fill not ended")
					}
					if calls != 1 || downs != map[bool]int{false: 0, true: 1}[convict] {
						t.Fatal("conviction decision changed", calls, downs)
					}
					fresh, err := endpoint.SnapshotHost(&host)
					if err != nil || fresh.Tokens[0] != -12 {
						t.Fatal("callback mutated host storage", fresh, err)
					}
					var hook endpoint2.ConvictionPolicy = policy
					hook.Reset(endpoint2.ConvictionEvent{Host: fresh})
					if len(policy.resets) != 1 || policy.resets[0].Host.ID != id {
						t.Fatal("reset lost host context")
					}
					// A successful completion or a still-connected pool must not invoke policy.
					p.fillingStopped(nil)
					p.InternalConnPicker = convictionPicker{count: 1}
					p.fillingStopped(cause)
					if calls != 1 {
						t.Fatal("conviction invoked without an empty failed pool", calls)
					}
				})
			}
		})
		t.Run("ClosedPool", func(t *testing.T) {
			// Close cancels a fill in progress, which then fails with the
			// cancellation and an empty pool. That failure is the pool's own
			// doing: the policy must not see it, and the host, which a newer
			// pool may serve by now, must not be marked down.
			host := (endpoint.HostInfoBuilder{HostId: "9f3b8cab-d382-4c16-9c4c-07283f206bbc", ConnectAddress: net.ParseIP("127.0.0.7"), Port: 9042}).Build()
			calls, downs := 0, 0
			policy := &convictionRecorder{callback: func(endpoint2.ConvictionEvent) bool { calls++; return true }}
			p := &InternalHostConnPool{InternalHost: &host, InternalConnPicker: convictionPicker{}, InternalLogger: logging.Nop(), InternalClosed: true, InternalFilling: true}
			p.InternalServices = InternalPoolServices{Config: &connectivity.Config{}, Conviction: policy, Down: func(*endpoint.HostInfo) { downs++ }}
			p.fillingStopped(context.Canceled)
			if calls != 0 || downs != 0 {
				t.Fatalf("closed pool reported its cancelled fill: %d policy calls, %d downs", calls, downs)
			}
			if p.InternalFilling {
				t.Fatal("fill not ended")
			}
		})
		t.Run("Shard", func(t *testing.T) {
			id := cql.MustParseUUID("9f3b8cab-d382-4c16-9c4c-07283f206bbc")
			host := (endpoint.HostInfoBuilder{HostId: id.String(), ConnectAddress: net.ParseIP("127.0.0.7"), Port: 9042}).Build()
			cause := errors.New("shard dial failure")
			newPool := func(policy endpoint2.ConvictionPolicy, downs *int) *InternalHostConnPool {
				p := &InternalHostConnPool{InternalHost: &host, InternalConnPicker: convictionPicker{count: 1}, InternalLogger: logging.Nop(), InternalRepairCtx: context.Background()}
				p.InternalServices = InternalPoolServices{Config: &connectivity.Config{}, Conviction: policy, Down: func(*endpoint.HostInfo) { *downs++ }, Connected: func(*InternalHostConnPool) {}}
				return p
			}
			t.Run("ConvictMarksDown", func(t *testing.T) {
				var events []endpoint2.ConvictionEvent
				policy := &convictionRecorder{callback: func(e endpoint2.ConvictionEvent) bool {
					events = append(events, e)
					return e.ShardID.Set
				}}
				downs := 0
				p := newPool(policy, &downs)
				p.shardFailed(cause, 3, 8)
				if len(events) != 1 || !events[0].ShardID.Set || events[0].ShardID.Value != 3 || !errors.Is(events[0].Err, cause) || events[0].Host.ID != id || events[0].Source != endpoint2.ConvictionPool {
					t.Fatalf("shard failure reported as %+v", events)
				}
				if downs != 0 {
					t.Fatal("shard verdict marked the host down before filling stopped")
				}
				// The pool keeps a connection, so only the shard verdict convicts.
				p.fillingStopped(cause)
				if downs != 1 || len(events) != 1 {
					t.Fatal("shard verdict not applied once filling stopped", downs, len(events))
				}
				p.fillingStopped(cause)
				if downs != 1 {
					t.Fatal("shard verdict applied twice", downs)
				}
			})
			t.Run("ClosedPoolKeepsHost", func(t *testing.T) {
				policy := &convictionRecorder{callback: func(endpoint2.ConvictionEvent) bool { return true }}
				downs := 0
				p := newPool(policy, &downs)
				p.shardFailed(cause, 3, 8)
				p.InternalClosed = true
				p.fillingStopped(cause)
				if downs != 0 {
					t.Fatal("closed pool marked its host down on a shard verdict")
				}
			})
			t.Run("Connect", func(t *testing.T) {
				// InternalConnect reports a failed dial to the shard the picker chose.
				for _, tc := range []struct {
					name          string
					shard, shards int
					want          bool
				}{{"ChosenShard", 5, 8, true}, {"UnchosenShard", 0, 0, false}} {
					t.Run(tc.name, func(t *testing.T) {
						var events []endpoint2.ConvictionEvent
						policy := &convictionRecorder{callback: func(e endpoint2.ConvictionEvent) bool {
							events = append(events, e)
							return false
						}}
						downs := 0
						p := newPool(policy, &downs)
						p.InternalConnPicker = convictionPicker{count: 1, shard: tc.shard, shards: tc.shards}
						p.InternalServices.Reconnection = &NoReconnectionPolicy{}
						var dialed [2]int
						p.InternalServices.Dial = func(_ context.Context, _ *endpoint.HostInfo, _ transport.ConnErrorHandler, shard, shards int) (*transport.Conn, error) {
							dialed = [2]int{shard, shards}
							return nil, cause
						}
						if err := p.InternalConnect(); !errors.Is(err, cause) {
							t.Fatal("dial failure replaced", err)
						}
						if dialed != [2]int{tc.shard, tc.shards} {
							t.Fatal("dialed another shard", dialed)
						}
						if !tc.want {
							if len(events) != 0 {
								t.Fatalf("failure without a chosen shard reported as %+v", events)
							}
							return
						}
						if len(events) != 1 || !events[0].ShardID.Set || events[0].ShardID.Value != uint32(tc.shard) || !errors.Is(events[0].Err, cause) {
							t.Fatalf("chosen shard failure reported as %+v", events)
						}
					})
				}
			})
			t.Run("UnknownShard", func(t *testing.T) {
				policy := &convictionRecorder{callback: func(endpoint2.ConvictionEvent) bool {
					t.Fatal("failure without a chosen shard reported as a shard failure")
					return false
				}}
				downs := 0
				newPool(policy, &downs).shardFailed(cause, 0, 0)
			})
			t.Run("Reset", func(t *testing.T) {
				policy := &convictionRecorder{callback: func(endpoint2.ConvictionEvent) bool { return false }}
				downs := 0
				p := newPool(policy, &downs)
				p.shardConnected(2, 8)
				if len(policy.resets) != 0 {
					t.Fatal("reset a shard that never failed")
				}
				p.shardFailed(cause, 2, 8)
				p.shardConnected(2, 8)
				p.shardConnected(2, 8)
				if len(policy.resets) != 1 || !policy.resets[0].ShardID.Set || policy.resets[0].ShardID.Value != 2 || policy.resets[0].Err != nil {
					t.Fatalf("shard reset reported as %+v", policy.resets)
				}
				p.shardFailed(cause, 5, 8)
				p.resetConviction()
				if len(policy.resets) != 2 || policy.resets[1].ShardID.Set || policy.resets[1].Host.ID != id {
					t.Fatalf("host reset reported as %+v", policy.resets)
				}
				p.shardConnected(5, 8)
				if len(policy.resets) != 2 {
					t.Fatal("host reset left a failed shard behind")
				}
			})
		})
	})
}

type convictionRecorder struct {
	callback func(endpoint2.ConvictionEvent) bool
	resets   []endpoint2.ConvictionEvent
}

func (r *convictionRecorder) AddFailure(e endpoint2.ConvictionEvent) bool {
	return r.callback(e)
}
func (r *convictionRecorder) Reset(e endpoint2.ConvictionEvent) { r.resets = append(r.resets, e) }

type convictionPicker struct {
	InternalNopConnPicker
	count, shard, shards int
}

func (p convictionPicker) NextShard() (int, int) { return p.shard, p.shards }

func (p convictionPicker) Size() (int, int) { return p.count, 0 }

// A host pool requested after the policy pool closed (by the session's
// reconnect loop or a topology event racing Session.Close) is closed at
// once and kept out of the map, where nothing would close it.
func TestPolicyConnPoolAfterClose(t *testing.T) {
	id := cql.MustParseUUID("9f3b8cab-d382-4c16-9c4c-07283f206bbc")
	host := (endpoint.HostInfoBuilder{HostId: id.String(), ConnectAddress: net.ParseIP("127.0.0.7"), Port: 9042, DataCenter: "dc1", Rack: "r1"}).Build()
	p := &InternalPolicyConnPool{
		InternalServices:      InternalPoolServices{Config: &connectivity.Config{}, Context: context.Background()},
		InternalHostConnPools: map[cql.UUID]*InternalHostConnPool{},
		InternalNumConns:      1,
	}
	p.Close()
	hp := p.InternalEnsureHost(&host)
	if !hp.InternalClosed {
		t.Fatal("host pool made after Close is open")
	}
	p.SetHosts([]*endpoint.HostInfo{&host})
	if len(p.InternalHostConnPools) != 0 {
		t.Fatalf("closed policy pool holds %d host pools", len(p.InternalHostConnPools))
	}
}

// Closing a pool cancels its repair context; InternalConnect must stop
// retrying a failed dial then, rather than sleep out the reconnection
// policy's remaining intervals.
func TestHostConnPoolConnectStopsRetriesWhenRepairEnds(t *testing.T) {
	id := cql.MustParseUUID("9f3b8cab-d382-4c16-9c4c-07283f206bbc")
	host := (endpoint.HostInfoBuilder{HostId: id.String(), ConnectAddress: net.ParseIP("127.0.0.7"), Port: 9042, DataCenter: "dc1", Rack: "r1"}).Build()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := &InternalHostConnPool{InternalHost: &host, InternalConnPicker: convictionPicker{count: 1}, InternalLogger: logging.Nop(), InternalRepairCtx: ctx}
	p.InternalServices = InternalPoolServices{
		Config:       &connectivity.Config{},
		Conviction:   &convictionRecorder{callback: func(endpoint2.ConvictionEvent) bool { return false }},
		Down:         func(*endpoint.HostInfo) {},
		Connected:    func(*InternalHostConnPool) {},
		Reconnection: &ConstantReconnectionPolicy{MaxRetries: 5, Interval: time.Hour},
	}
	cause := errors.New("dial failed")
	dials := 0
	p.InternalServices.Dial = func(context.Context, *endpoint.HostInfo, transport.ConnErrorHandler, int, int) (*transport.Conn, error) {
		dials++
		cancel()
		return nil, cause
	}
	done := make(chan error, 1)
	go func() { done <- p.InternalConnect() }()
	select {
	case err := <-done:
		if !errors.Is(err, cause) || dials != 1 {
			t.Fatalf("InternalConnect = %v after %d dials, want the dial error after one", err, dials)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the retry loop kept sleeping after its repair context ended")
	}
}
