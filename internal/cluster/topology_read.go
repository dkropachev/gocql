// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"context"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/binding"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/metadata"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/transport"
)

func readV2Topology(ctx context.Context, conn transport.ConnInterface, stmt string, port int) ([]*implEndpoint.HostInfo, error) {
	rows, err := readV2TopologyRows(ctx, conn, stmt)
	if err != nil {
		return nil, err
	}
	hosts := make([]*implEndpoint.HostInfo, 0, len(rows))
	for _, row := range rows {
		h, err := row.InternalHost(port)
		if err != nil {
			return nil, err
		}
		hosts = append(hosts, h)
	}
	return hosts, nil
}

// readV2TopologyRows reads the rows of the topology query stmt.
func readV2TopologyRows(ctx context.Context, conn transport.ConnInterface, stmt string) ([]metadata.InternalV2TopologyRow, error) {
	backend, ok := conn.(interface {
		InternalExecuteTyped(context.Context, *model.AttemptRequest) (*transport.InternalResponsePage, error)
	})
	if !ok {
		return nil, cqltype.TypeValidation("topology query", "connection does not support typed requests")
	}
	timeout := timeForTopology(conn)
	execute := backend.InternalExecuteTyped
	if c, ok := conn.(*transport.Conn); ok {
		execute = c.InternalExecuteV2System
	}
	return readV2SystemRowsVia(ctx, 4, timeout, execute, binding.Statement[cql.EmptyRow, metadata.InternalV2TopologyRow]{CQL: stmt, Params: cql.NoParams, Rows: metadata.InternalV2DecodeTopology()}, cql.NewColumns(), cql.EmptyRow{})
}

func timeForTopology(conn transport.ConnInterface) time.Duration {
	if c, ok := conn.(*transport.Conn); ok {
		return c.InternalGetSystemRequestState().InternalTimeout
	}
	return time.Minute
}

// Resolve each explicitly configured endpoint through its own system.local.
// This establishes real identities without discovering additional peers.
func (s *Core) v2ContactIdentities(ctx context.Context, contacts []*implEndpoint.HostInfo) ([]*implEndpoint.HostInfo, error) {
	var hosts []*implEndpoint.HostInfo
	current := s.privateStorage.InternalControl.InternalGetConn()
	seen := make(map[cql.UUID]bool)
	for _, contact := range contacts {
		var conn *transport.Conn
		if current != nil {
			if c, ok := current.InternalConn.(*transport.Conn); ok && contact.ConnectAddress().Equal(c.InternalHost.ConnectAddress()) && contact.Port() == c.InternalHost.Port() {
				conn = c
			}
		}
		temporary := conn == nil
		var err error
		if temporary {
			conn, err = sessionDial(s, ctx, contact, sessionControlConnConfig(s), transport.InternalConnErrorHandlerFn(func(c *transport.Conn, _ error, _ bool) { c.Close() }))
			if err != nil {
				return nil, err
			}
		}
		rows, err := readV2Topology(ctx, conn, transport.InternalQrySystemLocal, s.privateStorage.InternalCfg.Port)
		if temporary {
			conn.Close()
			conn.InternalWaitEventDispatch()
		}
		if err != nil {
			return nil, err
		}
		if len(rows) != 1 || implEndpoint.InternalHostInfoHostUUID(rows[0]).IsZero() {
			return nil, cqltype.TypeValidation("contact identity", "expected one actual local UUID")
		}
		implEndpoint.InternalHostInfoUpdate(contact, rows[0])
		if !seen[implEndpoint.InternalHostInfoHostUUID(contact)] {
			hosts = append(hosts, contact)
			seen[implEndpoint.InternalHostInfoHostUUID(contact)] = true
		}
	}
	return hosts, nil
}
