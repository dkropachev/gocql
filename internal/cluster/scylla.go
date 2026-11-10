// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/logging"
)

var _ endpoint.InternalCqlProtocolExtension = &endpoint.InternalTabletsRoutingV1Ext{}

var _ endpoint.InternalCqlProtocolExtension = &endpoint.InternalRateLimitExt{}

var _ endpoint.InternalCqlProtocolExtension = &endpoint.InternalLwtAddMetadataMarkExt{}

var _ endpoint.InternalCqlProtocolExtension = &endpoint.InternalScyllaUseMetadataIDExt{}

// Returns a partitioner specific to the table, or "nil"
// if the cluster-global partitioner should be used
func InternalScyllaGetTablePartitioner(session *Core, keyspaceName, tableName string) (endpoint.Partitioner, error) {
	isCdc, err := InternalScyllaIsCdcTable(session, keyspaceName, tableName)
	if err != nil {
		return nil, err
	}
	if isCdc {
		return endpoint.InternalScyllaCDCPartitioner{InternalLogger: &logging.InternalDefaultLogger{}}, nil
	}

	return nil, nil
}
