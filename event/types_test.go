// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package event_test

import (
	"net/netip"
	"testing"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/event"
)

func TestEventString(t *testing.T) {
	t.Parallel()
	host := cql.MustParseUUID("9f3b8cab-d382-4c16-9c4c-07283f206bbc")
	for want, e := range map[string]event.Event{
		"unknown gen=0":                                                                                {},
		"schema UPDATED TABLE app.events gen=42":                                                       {Kind: event.KindSchema, Change: event.ChangeUpdated, ObjectKind: event.ObjectTable, Keyspace: "app", Object: "events", Generation: 42},
		"schema CREATED KEYSPACE app gen=1 incomplete":                                                 {Kind: event.KindSchema, Change: event.ChangeCreated, ObjectKind: event.ObjectKeyspace, Keyspace: "app", Generation: 1, Incomplete: true},
		"schema DROPPED FUNCTION app.f(int,text) gen=3":                                                {Kind: event.KindSchema, Change: event.ChangeDropped, ObjectKind: event.ObjectFunction, Keyspace: "app", Object: "f", ArgumentTypes: []string{"int", "text"}, Generation: 3},
		"schema CREATED FUNCTION app.now() gen=3":                                                      {Kind: event.KindSchema, Change: event.ChangeCreated, ObjectKind: event.ObjectFunction, Keyspace: "app", Object: "now", Generation: 3},
		"pool POOL_DOWN 10.0.0.1:9042 host=9f3b8cab-d382-4c16-9c4c-07283f206bbc gen=7":                 {Kind: event.KindPool, Change: event.ChangePoolDown, Address: netip.MustParseAddrPort("10.0.0.1:9042"), HostID: host, Generation: 7},
		"client route ROUTES_UPDATED hosts=9f3b8cab-d382-4c16-9c4c-07283f206bbc connections=a,b gen=2": {Kind: event.KindClientRoute, Change: event.ChangeRoutesUpdated, HostIDs: []cql.UUID{host}, ConnectionIDs: []string{"a", "b"}, Generation: 2},
		"overflow RESYNC gen=9":                                                                        {Kind: event.KindOverflow, Change: event.ChangeResync, Generation: 9},
	} {
		if got := e.String(); got != want {
			t.Errorf("String() = %q, want %q", got, want)
		}
	}
}
