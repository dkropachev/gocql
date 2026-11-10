// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package endpoint

import (
	"encoding/binary"
	"math"

	"github.com/scylladb/gocql/v2/internal/debug"
	"github.com/scylladb/gocql/v2/internal/logging"
)

// cdc partitioner

const (
	scyllaCDCPartitionerName             = "CDCPartitioner"
	InternalScyllaCDCPartitionerFullName = "com.scylladb.dht.CDCPartitioner"

	scyllaCDCPartitionKeyLength  = 16
	scyllaCDCVersionMask         = 0x0F
	scyllaCDCMinSupportedVersion = 1
	scyllaCDCMaxSupportedVersion = 1

	scyllaCDCMinToken                   = InternalInt64Token(math.MinInt64)
	InternalScyllaCDCLogTableNameSuffix = "_scylla_cdc_log"
	scyllaCDCExtensionName              = "cdc"
)

type InternalScyllaCDCPartitioner struct {
	InternalLogger logging.StdLogger
}

func (p InternalScyllaCDCPartitioner) Name() string {
	return scyllaCDCPartitionerName
}

func (p InternalScyllaCDCPartitioner) Hash(partitionKey []byte) Token {
	return InternalInt64Token(p.InternalHashInt64(partitionKey))
}

func (p InternalScyllaCDCPartitioner) InternalHashInt64(partitionKey []byte) int64 {
	if len(partitionKey) < 8 {
		// The key is too short to extract any sensible token,
		// so return the min token instead
		if debug.Enabled {
			p.InternalLogger.Printf("scylla: cdc partition key too short: %d < 8", len(partitionKey))
		}
		return int64(scyllaCDCMinToken)
	}

	upperQword := binary.BigEndian.Uint64(partitionKey[0:])

	if debug.Enabled {
		// In debug mode, do some more checks

		if len(partitionKey) != scyllaCDCPartitionKeyLength {
			// The token has unrecognized format, but the first quadword
			// should be the token value that we want
			p.InternalLogger.Printf("scylla: wrong size of cdc partition key: %d", len(partitionKey))
		}

		lowerQword := binary.BigEndian.Uint64(partitionKey[8:])
		version := lowerQword & scyllaCDCVersionMask
		if version < scyllaCDCMinSupportedVersion || version > scyllaCDCMaxSupportedVersion {
			// We don't support this version yet,
			// the token may be wrong
			p.InternalLogger.Printf(
				"scylla: unsupported version: %d is not in range [%d, %d]",
				version,
				scyllaCDCMinSupportedVersion,
				scyllaCDCMaxSupportedVersion,
			)
		}
	}

	return int64(upperQword)
}

func (p InternalScyllaCDCPartitioner) ParseString(str string) Token {
	return InternalParseInt64Token(str)
}
