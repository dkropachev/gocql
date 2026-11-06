// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
)

type Compression = cqltype.Compression

const LZ4Compression = cqltype.LZ4Compression

const NoCompression = cqltype.NoCompression

const SnappyCompression = cqltype.SnappyCompression
