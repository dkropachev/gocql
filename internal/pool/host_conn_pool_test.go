// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package pool

import (
	"context"
	"errors"
	"net"
	"reflect"
	"testing"

	"github.com/scylladb/gocql/v2/internal/connectivity"
	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/endpoint"
	logging "github.com/scylladb/gocql/v2/internal/logging"
)

func TestHostConnPool(t *testing.T) {
	t.Run("FillingStopped", func(t *testing.T) {
		t.Run("Conviction", func(t *testing.T) {
			for _, convict := range []bool{false, true} {
				t.Run(map[bool]string{false: "keep", true: "convict"}[convict], func(t *testing.T) {
					id := cqltype.MustParseUUID("9f3b8cab-d382-4c16-9c4c-07283f206bbc")
					host := (endpoint.HostInfoBuilder{HostId: id.String(), ConnectAddress: net.ParseIP("127.0.0.7"), Port: 9042, DataCenter: "dc1", Rack: "r2", Tokens: []string{"-12", "64"}}).Build()
					endpoint.InternalHostInfoSetState(&host, endpoint.NodeUp)
					features := endpoint.ScyllaHostFeatures{}
					endpoint.ScyllaHostFeaturesData(&features).InternalNrShards = 4
					endpoint.InternalHostInfoSetScyllaFeatures(&host, features)
					cause := errors.New("actual pool failure completion")
					calls, downs := 0, 0
					p := &InternalHostConnPool{InternalHost: &host, InternalConnPicker: convictionPicker{}, InternalLogger: logging.Nop()}
					policy := &convictionRecorder{callback: func(e connectivity.ConvictionEvent) bool {
						err, snapshot := e.Err, e.Host
						if e.Shard.Set || e.Source != connectivity.ConvictionPool {
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
					p.InternalServices = InternalPoolServices{Config: &connectivity.Config{}, Conviction: policy, Down: func(address net.IP, port int) {
						downs++
						if !address.Equal(net.ParseIP("127.0.0.7")) || port != 9042 {
							t.Fatal("wrong down endpoint")
						}
					}}
					p.fillingStopped(cause)
					if calls != 1 || downs != map[bool]int{false: 0, true: 1}[convict] {
						t.Fatal("conviction decision changed", calls, downs)
					}
					fresh, err := endpoint.SnapshotHost(&host)
					if err != nil || fresh.Tokens[0] != -12 {
						t.Fatal("callback mutated host storage", fresh, err)
					}
					var hook connectivity.ConvictionPolicy = policy
					hook.Reset(connectivity.ConvictionEvent{Host: fresh})
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
		t.Run("Shard", func(t *testing.T) {
			id := cqltype.MustParseUUID("9f3b8cab-d382-4c16-9c4c-07283f206bbc")
			host := (endpoint.HostInfoBuilder{HostId: id.String(), ConnectAddress: net.ParseIP("127.0.0.7"), Port: 9042}).Build()
			cause := errors.New("shard dial failure")
			newPool := func(policy connectivity.ConvictionPolicy, downs *int) *InternalHostConnPool {
				p := &InternalHostConnPool{InternalHost: &host, InternalConnPicker: convictionPicker{count: 1}, InternalLogger: logging.Nop(), InternalRepairCtx: context.Background()}
				p.InternalServices = InternalPoolServices{Config: &connectivity.Config{}, Conviction: policy, Down: func(net.IP, int) { *downs++ }, Connected: func(*InternalHostConnPool) {}}
				return p
			}
			t.Run("ConvictMarksDown", func(t *testing.T) {
				var events []connectivity.ConvictionEvent
				policy := &convictionRecorder{callback: func(e connectivity.ConvictionEvent) bool {
					events = append(events, e)
					return e.Shard.Set
				}}
				downs := 0
				p := newPool(policy, &downs)
				p.shardFailed(cause, 3, 8)
				if len(events) != 1 || !events[0].Shard.Set || events[0].Shard.Value != 3 || !errors.Is(events[0].Err, cause) || events[0].Host.ID != id || events[0].Source != connectivity.ConvictionPool {
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
			t.Run("UnknownShard", func(t *testing.T) {
				policy := &convictionRecorder{callback: func(connectivity.ConvictionEvent) bool {
					t.Fatal("failure without a chosen shard reported as a shard failure")
					return false
				}}
				downs := 0
				newPool(policy, &downs).shardFailed(cause, 0, 0)
			})
			t.Run("Reset", func(t *testing.T) {
				policy := &convictionRecorder{callback: func(connectivity.ConvictionEvent) bool { return false }}
				downs := 0
				p := newPool(policy, &downs)
				p.shardConnected(2, 8)
				if len(policy.resets) != 0 {
					t.Fatal("reset a shard that never failed")
				}
				p.shardFailed(cause, 2, 8)
				p.shardConnected(2, 8)
				p.shardConnected(2, 8)
				if len(policy.resets) != 1 || !policy.resets[0].Shard.Set || policy.resets[0].Shard.Value != 2 || policy.resets[0].Err != nil {
					t.Fatalf("shard reset reported as %+v", policy.resets)
				}
				p.shardFailed(cause, 5, 8)
				p.hostConnected()
				if len(policy.resets) != 2 || policy.resets[1].Shard.Set || policy.resets[1].Host.ID != id {
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
	callback func(connectivity.ConvictionEvent) bool
	resets   []connectivity.ConvictionEvent
}

func (r *convictionRecorder) AddFailure(e connectivity.ConvictionEvent) bool {
	return r.callback(e)
}
func (r *convictionRecorder) Reset(e connectivity.ConvictionEvent) { r.resets = append(r.resets, e) }

type convictionPicker struct {
	InternalNopConnPicker
	count int
}

func (p convictionPicker) Size() (int, int) { return p.count, 0 }
