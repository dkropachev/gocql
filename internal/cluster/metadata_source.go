// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"context"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	implMetadata "github.com/scylladb/gocql/v2/internal/metadata"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/runtimeerrors"
	"github.com/scylladb/gocql/v2/internal/transport"
)

type schemaSource struct{ session *Core }

func (s *Core) NewSchemaSource() implMetadata.Source { return schemaSource{s} }
func (source schemaSource) InternalSchemaContext() context.Context {
	s := source.session
	if s != nil && s.privateStorage.InternalCtx != nil {
		return s.privateStorage.InternalCtx
	}
	return context.Background()
}
func (source schemaSource) InternalSchemaSettings() implMetadata.QuerySettings {
	s := source.session
	v := implMetadata.QuerySettings{SystemSchema: s.privateStorage.InternalUseSystemSchema, Available: s.privateStorage.InternalControl != nil, Aggregates: s.privateStorage.InternalHasAggregatesAndFunctions, Timeout: s.privateStorage.InternalCfg.MetadataSchemaRequestTimeout, Depth: s.v2MetadataDepthLimit()}
	if s.privateStorage.InternalHostSource != nil && s.privateStorage.InternalPool != nil {
		if c := s.GetConn(); c != nil {
			v.Scylla = c.InternalIsScyllaConn()
			v.TabletsV2 = c.InternalFramers.InternalDefaults.InternalTabletsRoutingV2
		}
	}
	return v
}
func (source schemaSource) InternalQuerySystem(ctx context.Context, request *model.AttemptRequest) (*model.ResponseData, error) {
	s := source.session
	if s.privateStorage.InternalControl == nil {
		return nil, implMetadata.InternalErrNoControl
	}
	var response *transport.InternalResponsePage
	var err error
	if backend, ok := s.privateStorage.InternalControl.(v2SystemQueryer); ok {
		response, err = backend.InternalQueryV2System(ctx, request)
	} else {
		h := s.privateStorage.InternalControl.InternalGetConn()
		if h == nil || h.InternalConn == nil {
			return nil, runtimeerrors.ErrNoConnections
		}
		c, ok := h.InternalConn.(*transport.Conn)
		if !ok {
			return nil, cqltype.TypeValidation("schema query", "control connection does not support typed requests")
		}
		response, err = c.InternalExecuteV2System(ctx, request)
	}
	if response == nil {
		return nil, err
	}
	return &response.ResponseData, err
}
