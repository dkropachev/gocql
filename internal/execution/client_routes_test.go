// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	oldevents "github.com/scylladb/gocql/v2/events"
	"github.com/scylladb/gocql/v2/internal/cluster"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/endpoint"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/logging"
	"github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/settings"
)

func TestClientRoutes(t *testing.T) {
	t.Run("Config", func(t *testing.T) {
		t.Run("SnapshotsAndValidation", func(t *testing.T) {
			cfg := settings.DefaultConfig("localhost")
			cfg.Pool.ShardAwarePort = false
			cfg.ClientRoutes = &settings.ClientRoutesOptions{ConnectionIDs: []string{"one", "two"}, HostnameOverrides: map[string]string{"one": "proxy.example"}}
			if err := settings.InternalValidateConfig(cfg); err != nil {
				t.Fatal(err)
			}
			snapshot := settings.InternalSnapshotConfig(cfg)
			cfg.ClientRoutes.ConnectionIDs[0] = "changed"
			cfg.ClientRoutes.HostnameOverrides["one"] = "changed"
			if snapshot.ClientRoutes.ConnectionIDs[0] != "one" || snapshot.ClientRoutes.HostnameOverrides["one"] != "proxy.example" {
				t.Fatal("route config aliases input")
			}
			edits := []func(*settings.Config){func(c *settings.Config) { c.Pool.ShardAwarePort = true }, func(c *settings.Config) { c.ClientRoutes.ConnectionIDs = []string{"one", "one"} }, func(c *settings.Config) { c.ClientRoutes.HostnameOverrides["unknown"] = "host" }, func(c *settings.Config) { c.ClientRoutes.HostnameOverrides["one"] = "" }, func(c *settings.Config) { c.ClientRoutes.TLS = true }, func(c *settings.Config) { c.ClientRoutes.TLS = true; c.TLS = &tls.Config{InsecureSkipVerify: true} }}
			for _, edit := range edits {
				c := settings.InternalSnapshotConfig(snapshot)
				edit(&c)
				if err := settings.InternalValidateConfig(c); err == nil {
					t.Fatal("incompatible route setting accepted")
				}
			}
		})
	})

	t.Run("Connect", func(t *testing.T) {
		t.Run("DNSAndFallbackShareDeadline", func(t *testing.T) {
			ids := []cqltype.UUID{cqltype.MustRandomUUID(), cqltype.MustRandomUUID(), cqltype.MustRandomUUID()}
			srv := newControlTestServer(t, func(srv *controlTestServer) {
				srv.queries = func(query string, stream int, resp *protocol.Framer) bool {
					if !strings.Contains(query, "system.client_routes") {
						return false
					}
					_, port, _ := net.SplitHostPort(srv.Address)
					var goodPort int
					fmt.Sscan(port, &goodPort)
					columns := []rowsCol{{"connection_id", optNative(cqltype.TypeUUID)}, {"host_id", optNative(cqltype.TypeUUID)}, {"address", optNative(cqltype.TypeVarchar)}, {"port", optNative(cqltype.TypeInt)}, {"tls_port", optNative(cqltype.TypeInt)}}
					var rows [][][]byte
					for i, id := range ids {
						port := 1
						if i == 2 {
							port = goodPort
						}
						address := []string{"bad-dns", "bad-connect", "good"}[i]
						rows = append(rows, [][]byte{id[:], testLocalHostID[:], []byte(address), binary.BigEndian.AppendUint32(nil, uint32(port)), nil})
					}
					writeRows(resp, stream, columns, rows)
					return true
				}
			})
			config := settings.DefaultConfig(srv.Address)
			config.Protocol = 4
			config.Metadata.Mode = settings.MetadataDisabled
			config.Metadata.InitialDiscovery = false
			config.Pool.ShardAwarePort = false
			config.Pool.ConnectionsPerHost = 1
			config.Logger = logging.InternalNopLogger{}
			config.ClientRoutes = &settings.ClientRoutesOptions{ConnectionIDs: []string{ids[0].String(), ids[1].String(), ids[2].String()}}
			config.DNSResolver = dnsResolverFunc(func(name string) ([]net.IP, error) {
				if name == "bad-dns" {
					return nil, errors.New("DNS unavailable")
				}
				return []net.IP{net.IPv4(127, 0, 0, 1)}, nil
			})
			var mu sync.Mutex
			var deadlines []time.Time
			var targets []string
			config.DialHost = func(ctx context.Context, target endpoint.DialTarget) (net.Conn, error) {
				if !target.Host.ID.IsEmpty() {
					deadline, _ := ctx.Deadline()
					mu.Lock()
					deadlines = append(deadlines, deadline)
					targets = append(targets, target.Address)
					mu.Unlock()
				}
				return (endpoint.DefaultDialer{}).DialContext(ctx, target)
			}
			s, err := Connect(context.Background(), config)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			route, ok := implCluster.ClientRoutesHandlerData(implCluster.SessionData(s).InternalClientRoutesHandler).InternalRouteCache.PreferredRoute(testLocalHostID.String())
			if !ok || route.InternalConnectionID != ids[2].String() {
				t.Fatal("did not bind successful configured alternative", route)
			}
			mu.Lock()
			defer mu.Unlock()
			if len(deadlines) < 2 || !deadlines[0].Equal(deadlines[1]) {
				t.Fatal("route alternatives restarted setup budget", deadlines, targets)
			}
		})
	})

	t.Run("Update", func(t *testing.T) {
		t.Run("DeletionAdvancesInsertionEpoch", func(t *testing.T) {
			p := cluster.NewClientRoutesAddressTranslator(cluster.ClientRoutesConfig{}, nil, false, logging.InternalNopLogger{})
			host := func() *endpoint.HostInfo {
				opaqueStorageValuexxxxxxxxxxx := new(endpoint.HostInfo)
				implEndpoint.HostInfoData(opaqueStorageValuexxxxxxxxxxx).InternalHostId = cqltype.MustRandomUUID()
				implEndpoint.HostInfoData(opaqueStorageValuexxxxxxxxxxx).InternalConnectAddress = net.IPv4(127, 0, 0, 1)
				implEndpoint.HostInfoData(opaqueStorageValuexxxxxxxxxxx).InternalPort = 9042
				return opaqueStorageValuexxxxxxxxxxx
			}()
			s := func() *cluster.Core {
				opaqueStorageValuexxxxxxxxxxxx := new(cluster.Core)
				implCluster.SessionData(opaqueStorageValuexxxxxxxxxxxx).InternalHostSource = &cluster.InternalRingDescriber{InternalHosts: map[string]*endpoint.HostInfo{host.HostID(): host}}
				return opaqueStorageValuexxxxxxxxxxxx
			}()
			implCluster.ClientRoutesHandlerData(p).InternalSession = s
			route := cluster.InternalClientRoute{InternalConnectionID: "connection", InternalHostID: host.HostID(), InternalAddress: "proxy", InternalPort: 9042}
			implCluster.ClientRoutesHandlerData(p).InternalRouteCache.ReplaceByConnectionIDs([]string{"connection"}, []cluster.InternalClientRoute{route})
			before := implEndpoint.InternalHostInfoTransportEpoch(host)
			implCluster.InternalClientRoutesHandlerInvalidateRouteTask(p, cluster.InternalUpdateTask{InternalPairs: []cluster.InternalPair{{InternalConnectionID: "connection", InternalHostID: host.HostID()}}})
			if implEndpoint.InternalHostInfoTransportEpoch(host) == before {
				t.Fatal("deletion left late validated transport insertion eligible")
			}
		})

		t.Run("EmptyFullReadDetectsNewerPair", func(t *testing.T) {
			p := cluster.NewClientRoutesAddressTranslator(cluster.ClientRoutesConfig{}, nil, false, logging.InternalNopLogger{})
			before := implCluster.InternalClientRoutesHandlerCaptureRouteEpochs(p)
			implCluster.InternalClientRoutesHandlerInvalidateRouteTask(p, cluster.InternalUpdateTask{InternalPairs: []cluster.InternalPair{{InternalConnectionID: "connection", InternalHostID: "new-host"}}})
			if implCluster.InternalClientRoutesHandlerApplyRouteRead(p, cluster.InternalUpdateTask{InternalConnectionIDs: []string{"connection"}}, before, nil) {
				t.Fatal("empty stale full read falsely reported accepted publication")
			}
		})

		t.Run("EventExpansionFallsBackToFullRefresh", func(t *testing.T) {
			p := cluster.NewClientRoutesAddressTranslator(cluster.ClientRoutesConfig{Endpoints: cluster.ClientRoutesEndpointList{{ConnectionID: "allowed"}}}, nil, false, logging.InternalNopLogger{})
			// More than the bounded pair capacity must schedule a full connection scope,
			// without allocating the Cartesian product or dropping required recovery.
			hosts := make([]string, 1025)
			for i := range hosts {
				hosts[i] = fmt.Sprint(i)
			}
			var freed atomic.Int32
			if !implCluster.InternalClientRoutesHandlerHandleRouteEventOwned(p, &oldevents.ClientRoutesChangedEvent{ChangeType: "UPDATED", ConnectionIDs: []string{"allowed"}, HostIDs: hosts}, func() { freed.Add(1) }) {
				t.Fatal("full-refresh fallback not admitted")
			}
			task := <-implCluster.ClientRoutesHandlerData(p).InternalUpdateTasks
			if task.InternalPairs != nil || !reflect.DeepEqual(task.InternalConnectionIDs, []string{"allowed"}) {
				t.Fatal("oversized event was not coalesced into full refresh")
			}
			if freed.Load() != 0 {
				t.Fatal("route event released before downstream consumption")
			}
			task.InternalRelease()
			if freed.Load() != 1 {
				t.Fatal("route ownership lost")
			}
			// Queue drain owns every accepted event release during terminal shutdown.
			for i := 0; i < 3; i++ {
				if !implCluster.InternalClientRoutesHandlerUpdateHostPortMappingAsync(p, cluster.InternalUpdateTask{InternalConnectionIDs: []string{"allowed"}, InternalRelease: func() { freed.Add(1) }}) {
					t.Fatal("owned task rejected")
				}
			}
			close(implCluster.ClientRoutesHandlerData(p).InternalCloseChan)
			implCluster.InternalClientRoutesHandlerStartUpdateWorker(p)
			implCluster.ClientRoutesHandlerData(p).InternalWorkers.Wait()
			if freed.Load() != 4 {
				t.Fatal("route worker terminal drain lost/duplicated ownership", freed.Load())
			}
		})

		t.Run("FencePerPairAndPreserveDisjoint", func(t *testing.T) {
			p := cluster.NewClientRoutesAddressTranslator(cluster.ClientRoutesConfig{Endpoints: cluster.ClientRoutesEndpointList{{ConnectionID: "a"}, {ConnectionID: "b"}}}, nil, false, logging.InternalNopLogger{})
			a := cluster.InternalClientRoute{InternalConnectionID: "a", InternalHostID: "host", InternalAddress: "old", InternalPort: 9042}
			b := cluster.InternalClientRoute{InternalConnectionID: "b", InternalHostID: "host", InternalAddress: "b", InternalPort: 9042}
			implCluster.ClientRoutesHandlerData(p).InternalRouteCache.ReplaceByConnectionIDs([]string{"a", "b"}, []cluster.InternalClientRoute{b, a})
			if route := implCluster.InternalClientRoutesHandlerOrderedRoutes(p, "host")[0]; route.InternalConnectionID != "a" {
				t.Fatal("server row order overrode config order", route)
			}
			old := implCluster.InternalClientRoutesHandlerCaptureRouteEpochs(p)
			changed := cluster.InternalUpdateTask{InternalPairs: []cluster.InternalPair{{InternalConnectionID: "a", InternalHostID: "host"}}}
			implCluster.InternalClientRoutesHandlerInvalidateRouteTask(p, changed)
			implCluster.InternalClientRoutesHandlerApplyRouteRead(p, changed, implCluster.InternalClientRoutesHandlerCaptureRouteEpochs(p), nil)
			newer := b
			newer.InternalAddress = "new-b"
			implCluster.InternalClientRoutesHandlerInvalidateRouteTask(p, cluster.InternalUpdateTask{InternalPairs: []cluster.InternalPair{{InternalConnectionID: "b", InternalHostID: "host"}}})
			implCluster.InternalClientRoutesHandlerApplyRouteRead(p, cluster.InternalUpdateTask{InternalPairs: []cluster.InternalPair{{InternalConnectionID: "b", InternalHostID: "host"}}}, implCluster.InternalClientRoutesHandlerCaptureRouteEpochs(p), []cluster.InternalClientRoute{newer})
			if implCluster.InternalClientRoutesHandlerApplyRouteRead(p, cluster.InternalUpdateTask{InternalConnectionIDs: []string{"a", "b"}}, old, []cluster.InternalClientRoute{a, b}) {
				t.Fatal("late full read accepted changed pairs")
			}
			routes := implCluster.InternalClientRoutesHandlerOrderedRoutes(p, "host")
			if len(routes) != 1 || routes[0] != newer {
				t.Fatal("stale full read restored deletion or erased disjoint new route", routes)
			}
			implCluster.InternalClientRoutesHandlerBindRoute(p, newer)
			if route := implCluster.InternalClientRoutesHandlerOrderedRoutes(p, "host")[0]; route != newer {
				t.Fatal("sticky route lost")
			}
		})
	})
}
