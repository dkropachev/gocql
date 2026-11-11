// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package endpoint

import (
	"context"
	"encoding/binary"
	"math"

	"log/slog"
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
	InternalLogger *slog.Logger
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
		if l := p.InternalLogger; l != nil {
			l.Debug("cdc partition key too short; using min token", "len", len(partitionKey))
		}
		return int64(scyllaCDCMinToken)
	}

	upperQword := binary.BigEndian.Uint64(partitionKey[0:])

	if l := p.InternalLogger; l != nil && l.Enabled(context.Background(), slog.LevelDebug) {
		// With Debug enabled, do some more checks

		if len(partitionKey) != scyllaCDCPartitionKeyLength {
			// The token has unrecognized format, but the first quadword
			// should be the token value that we want
			l.Debug("unexpected cdc partition key length", "len", len(partitionKey), "want", scyllaCDCPartitionKeyLength)
		}

		lowerQword := binary.BigEndian.Uint64(partitionKey[8:])
		version := lowerQword & scyllaCDCVersionMask
		if version < scyllaCDCMinSupportedVersion || version > scyllaCDCMaxSupportedVersion {
			// We don't support this version yet,
			// the token may be wrong
			l.Debug("unsupported cdc stream version; token may be wrong", "version", version,
				"min", scyllaCDCMinSupportedVersion, "max", scyllaCDCMaxSupportedVersion)
		}
	}

	return int64(upperQword)
}

func (p InternalScyllaCDCPartitioner) ParseString(str string) Token {
	return InternalParseInt64Token(str)
}
