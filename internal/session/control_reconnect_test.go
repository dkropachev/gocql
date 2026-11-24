// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/event"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/transport"
)

// TestControlReconnect stops the only node after the control connection's
// first heartbeat, when the next is 30s away, and starts it again. The control
// connection returns within seconds, not at that heartbeat, and its return is
// one Control event.
func TestControlReconnect(t *testing.T) {
	// A node without peers. The topology refresh reads them on the control
	// connection.
	node := func(addr string) *controlTestServer {
		return newControlTestServerAt(t, addr, func(srv *controlTestServer) {
			srv.queries = func(query string, stream int, resp *protocol.Framer) bool {
				if !strings.Contains(query, "system.peers") {
					return false
				}
				writeRows(resp, stream, systemPeersCols, nil)
				return true
			}
		})
	}
	srv := node("127.0.0.1:0")
	addr := srv.Address
	s, err := srv.cluster().Open()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	sub, err := s.Subscribe(event.Filter{Kinds: []event.Kind{event.KindControl}})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	// Past the first heartbeat, which sets the next one 30s away.
	time.Sleep(1500 * time.Millisecond)

	// The node goes away: it takes no connections, and the control
	// connection breaks. The test server keeps the connections it has.
	srv.Stop()
	control := implCluster.SessionData(s).InternalControl.InternalGetConn().InternalConn.(*transport.Conn)
	control.InternalCloseWithError(errors.New("node stopped"))
	// The control connection fails to reconnect while the node is away.
	time.Sleep(1500 * time.Millisecond)
	node(addr)

	start := time.Now()
	deadline := start.Add(10 * time.Second)
	for {
		err := s.RefreshRingNow()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("control connection did not return:", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Logf("control connection returned %v after the node", time.Since(start))
	// Let a duplicate arrive.
	time.Sleep(200 * time.Millisecond)
	reconnects := 0
	for {
		select {
		case e := <-sub.Events():
			if e.Kind == event.KindControl {
				reconnects++
			}
			continue
		default:
		}
		break
	}
	if reconnects != 1 {
		t.Fatalf("%d Control events for one reconnect, want 1", reconnects)
	}

	// A wake for a connection that broke while the loop was reconnecting
	// arrives when the loop has a healthy one, which it keeps.
	current := implCluster.SessionData(s).InternalControl.InternalGetConn().InternalConn.(*transport.Conn)
	implCluster.SessionData(s).InternalControl.(*implCluster.InternalControlConn).HandleError(current, errors.New("stale"), true)
	time.Sleep(200 * time.Millisecond)
	if now := implCluster.SessionData(s).InternalControl.InternalGetConn().InternalConn; now != current || current.Closed() {
		t.Fatal("a stale wake replaced a healthy control connection")
	}
	select {
	case e := <-sub.Events():
		t.Fatalf("a stale wake published %+v", e)
	default:
	}
}
