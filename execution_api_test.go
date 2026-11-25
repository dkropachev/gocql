// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"errors"
	"testing"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
	"github.com/scylladb/gocql/v2/exec"
)

// TestPreparedRouting reads a statement's routing from the Prepared itself,
// with no Worker: the statement's table and kind, and the consistency and
// host pin of the options passed, or of the session's defaults without
// them. It sends nothing.
func TestPreparedRouting(t *testing.T) {
	f := newLifetimeFixture(t)
	info, err := f.p.Routing(gridKey)
	if err != nil {
		t.Fatal(err)
	}
	if info.Keyspace != "ks" || info.Table != "tbl" || info.Kind != cql.OrdinaryStatement || info.HostID.Set {
		t.Fatalf("Routing = %+v, want ks.tbl, ordinary, no host pin", info)
	}
	host := cql.UUID{15: 1}
	options, err := f.s.CompileOptions(exec.Options{Consistency: cql.Some(cql.LocalOne), HostID: cql.Some(host)})
	if err != nil {
		t.Fatal(err)
	}
	info, err = f.p.Routing(gridKey, options)
	if err != nil {
		t.Fatal(err)
	}
	if info.Consistency != cql.LocalOne || info.HostID != cql.Some(host) {
		t.Fatalf("Routing with options = %+v, want LOCAL_ONE pinned to %s", info, host)
	}
	serial, err := f.s.CompileOptions(exec.Options{Consistency: cql.Some(cql.Serial)})
	if err != nil {
		t.Fatal(err)
	}
	if info, err := f.p.Routing(gridKey, serial); err != nil || info.Kind != cql.ConditionalStatement {
		t.Fatalf("Routing of a SERIAL read = %+v, %v; want it routed as conditional", info, err)
	}
	if _, err := f.p.Routing(gridKey, options, options); err == nil {
		t.Fatal("Routing took two CompiledOptions")
	}
	if _, err := new(Prepared[int32, []byte]).Routing(gridKey); !errors.As(err, new(*cql.ValidationError)) {
		t.Fatalf("zero Prepared.Routing = %v, want a cql.ValidationError", err)
	}
	if err := f.s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := f.p.Routing(gridKey); !errors.Is(err, cqlerr.ErrClosed) {
		t.Fatalf("Routing after Session.Close = %v, want cqlerr.ErrClosed", err)
	}
}
