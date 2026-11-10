// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"strings"

	"github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/logging"
)

var _ endpoint.Partitioner = endpoint.InternalScyllaCDCPartitioner{InternalLogger: &logging.InternalDefaultLogger{}}

var _ endpoint.InternalInt64Hasher = endpoint.InternalScyllaCDCPartitioner{}

func scyllaIsCdcTable(session *Core, keyspaceName, tableName string) (bool, error) {
	if !strings.HasSuffix(tableName, endpoint.InternalScyllaCDCLogTableNameSuffix) {
		// Not a CDC table, use the default partitioner
		return false, nil
	}

	// Check if the table has the CDC partitioner set.
	tableMeta, err := sessionTableMetadata(session, keyspaceName, tableName)
	if err != nil {
		return false, err
	}

	return tableMeta.Options.Partitioner == endpoint.InternalScyllaCDCPartitionerFullName, nil
}
