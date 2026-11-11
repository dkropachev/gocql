// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"errors"
	"testing"

	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/runtimeerrors"
	implSettings "github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/internal/transport"
)

// A session opened for one database fails on reaching the other: the
// dialect fixes how results that differ between them are read.
func TestCheckServer(t *testing.T) {
	scylla := &transport.Conn{}
	implEndpoint.ScyllaHostFeaturesData(&scylla.InternalScyllaSupported.ScyllaHostFeatures).InternalIsScylla = true
	cassandra := &transport.Conn{}
	for _, tc := range []struct {
		want     implSettings.Server
		conn     *transport.Conn
		mismatch bool
	}{
		{implSettings.ServerAny, scylla, false},
		{implSettings.ServerAny, cassandra, false},
		{implSettings.ServerScylla, scylla, false},
		{implSettings.ServerCassandra, cassandra, false},
		{implSettings.ServerScylla, cassandra, true},
		{implSettings.ServerCassandra, scylla, true},
	} {
		err := checkServer(tc.want, tc.conn)
		_, mismatch := errors.AsType[*runtimeerrors.ServerMismatchError](err)
		if mismatch != tc.mismatch || !tc.mismatch && err != nil {
			t.Errorf("want %v, scylla %t: %v", tc.want, tc.conn.InternalIsScyllaConn(), err)
		}
		if mismatch && runtimeerrors.KindOf(err) != runtimeerrors.KindInvalid {
			t.Errorf("mismatch kind %v", runtimeerrors.KindOf(err))
		}
	}
}
