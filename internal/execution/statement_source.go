// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"bytes"
	"context"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/routing"
	"github.com/scylladb/gocql/v2/internal/transport"
)

// statementSource is what a request executes: a statement prepared on the
// server, or CQL text sent with every request. It holds the per-attempt
// differences between the two, so the execution paths do not branch on
// which one they run.
type statementSource interface {
	// prepared returns the shared prepared state, or nil for CQL text.
	prepared() *InternalPreparedState
	// token computes the routing token of the bound values.
	token(cells []model.EncodedCell) (cqltype.Option[int64], error)
	// onConn returns the statement's prepared entry on conn, preparing it
	// first when needed or when force is set; failed names the entry an
	// UNPREPARED answer rejected. CQL text has no entry and returns nil.
	onConn(ctx context.Context, rt *InternalSessionRuntime, conn *transport.Conn, values model.EncodedValues, force bool, failed *InternalHostPrepared) (*InternalHostPrepared, error)
	// rejected reports whether an UNPREPARED answer naming id rejected the
	// statement sent with the prepared ID sent, so that re-preparing it can
	// repair the attempt. It takes the ID, not the request: an interface
	// call's arguments escape, and the request must stay on the stack.
	rejected(id, sent []byte) bool
	// observe starts a schema-dependency observation for one response.
	observe() uint64
}

// cqlText is the source of a statement sent as CQL text.
type cqlText struct{}

func (cqlText) prepared() *InternalPreparedState { return nil }
func (cqlText) token([]model.EncodedCell) (cqltype.Option[int64], error) {
	return cqltype.Option[int64]{}, nil
}
func (cqlText) onConn(context.Context, *InternalSessionRuntime, *transport.Conn, model.EncodedValues, bool, *InternalHostPrepared) (*InternalHostPrepared, error) {
	return nil, nil
}
func (cqlText) rejected(_, _ []byte) bool { return false }
func (cqlText) observe() uint64           { return 0 }

// preparedSource is the source of a server-prepared statement. It is a single
// pointer, so storing it in a statementSource does not allocate.
type preparedSource struct{ state *InternalPreparedState }

// InternalPreparedSource returns the source executing state.
func InternalPreparedSource(state *InternalPreparedState) statementSource {
	if state == nil {
		return cqlText{}
	}
	return preparedSource{state: state}
}

func (p preparedSource) prepared() *InternalPreparedState { return p.state }

// token reads the statement's partition key indexes and partitioner, which
// are fixed when the prepared state is built.
func (p preparedSource) token(cells []model.EncodedCell) (cqltype.Option[int64], error) {
	state := p.state
	return routing.InternalComputeRoutingToken(cells, state.InternalInfo.PartitionKeyIndexes, state.InternalSession.CapturedRoutingPartitioner(state.partitioner))
}
func (p preparedSource) onConn(ctx context.Context, rt *InternalSessionRuntime, conn *transport.Conn, values model.EncodedValues, force bool, failed *InternalHostPrepared) (*InternalHostPrepared, error) {
	return rt.prepareOnConnExpected(ctx, conn, p.state, values, force, failed)
}
func (preparedSource) rejected(id, sent []byte) bool {
	return bytes.Equal(id, sent)
}
func (p preparedSource) observe() uint64 {
	return p.state.InternalDependencies.InternalTracker.InternalStart()
}

// source returns the request's statement source; a request built without one
// sends CQL text.
func (r *InternalExecutionRequest) source() statementSource {
	if r.InternalSource == nil {
		return cqlText{}
	}
	return r.InternalSource
}

// InternalPrepared returns the request's prepared state, or nil for CQL text.
func (r *InternalExecutionRequest) InternalPrepared() *InternalPreparedState {
	return r.source().prepared()
}

// setPrepared fills w's statement ID fields from the host's prepared entry.
// An EXECUTE sends no keyspace: the statement was prepared in its keyspace,
// and the ID names it.
func setPrepared(w *model.AttemptRequest, prepared *InternalHostPrepared, conn *transport.Conn, includeMetadata bool) {
	if prepared == nil {
		return
	}
	w.Keyspace = ""
	w.PreparedID = prepared.InternalId
	w.ResultMetadataID = prepared.InternalMetadataID
	w.SkipMetadata = !includeMetadata && transport.InternalMetadataIDTracked(conn.InternalTracksResultMetadataID(), prepared.InternalMetadataID)
}
