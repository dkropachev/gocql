// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"errors"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/event"
	"github.com/scylladb/gocql/v2/internal/endpoint"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/protocol"
)

// TestDebouncedStatusEvents checks that of several status changes of one
// node handled together, subscribers get only the one the session applied,
// the latest.
func TestDebouncedStatusEvents(t *testing.T) {
	t.Parallel()
	// The filter keeps the UP from filling a pool; the state is still set.
	f := newNodeEventFixture(t, func(cfg *InternalClusterSettings) {
		cfg.HostFilter = HostFilterFunc(func(*endpoint.HostInfo) bool { return false })
	})
	f.addHost(t, "127.0.10.1")
	f.addHost(t, "127.0.10.2")
	defer f.session.CloseV2Peripheral()
	sub, err := f.session.Subscribe(event.Filter{Kinds: []event.Kind{event.KindStatus}})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	f.session.HandleNodeEvent([]protocol.Frame{
		statusChange("UP", "127.0.10.1"),
		statusChange("DOWN", "127.0.10.2"),
		statusChange("DOWN", "127.0.10.1"),
		statusChange("UP", "127.0.10.1"),
	})
	var got []string
	for len(got) < 2 {
		select {
		case e := <-sub.Events():
			got = append(got, e.Address.Addr().String()+" "+string(e.Change))
		case <-time.After(time.Second):
			t.Fatalf("events %v, want two", got)
		}
	}
	if got[0] != "127.0.10.2 DOWN" || got[1] != "127.0.10.1 UP" {
		t.Fatalf("events %v, want the DOWN of .2 and the last change of .1, UP", got)
	}
	select {
	case e := <-sub.Events():
		t.Fatalf("a superseded status change was delivered: %+v", e)
	case <-time.After(20 * time.Millisecond):
	}
}

// TestIncompleteSchemaEvent checks that a schema event whose refresh failed
// is delivered marked Incomplete, and that the retry that publishes the
// schema delivers a ChangePublicationResync.
func TestIncompleteSchemaEvent(t *testing.T) {
	t.Parallel()
	ctrl := &schemaDataMock{knownKeyspaces: map[string][]tableInfo{"test_ks": {{name: "tbl", columns: []columnInfo{{name: "id", kind: "partition_key"}}}}}}
	s := newSchemaEventTestSessionWithMock(ctrl)
	defer s.Close()
	populateKeyspace(s, "test_ks", "tbl")
	sub, err := s.Subscribe(event.Filter{})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	ctrl.mu.Lock()
	ctrl.queryError = errors.New("schema read failed")
	ctrl.mu.Unlock()
	s.handleSchemaEventAndPublish([]protocol.Frame{&frm.SchemaChangeTable{Change: "UPDATED", Keyspace: "test_ks", Object: "tbl"}})
	e := nextEvent(t, sub, event.KindSchema)
	if !e.Incomplete || e.Object != "tbl" {
		t.Fatalf("event %+v, want an Incomplete Schema event of tbl", e)
	}
	ctrl.mu.Lock()
	ctrl.queryError = nil
	ctrl.mu.Unlock()
	if e := nextEvent(t, sub, event.KindOverflow); e.Change != event.ChangePublicationResync {
		t.Fatalf("event %+v, want ChangePublicationResync once the retry published", e)
	}
	if s.Peripheral().incompleteEvents.Load() {
		t.Fatal("the resync left the incomplete mark set")
	}
}

// nextEvent returns the next event of sub of kind, skipping others.
func nextEvent(t *testing.T, sub *Subscription, kind event.Kind) event.Event {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		select {
		case e := <-sub.Events():
			if e.Kind == kind {
				return e
			}
		case <-deadline:
			t.Fatalf("no %v event", kind)
		}
	}
}
