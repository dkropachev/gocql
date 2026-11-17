// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package config

import (
	"github.com/scylladb/gocql/v2/cql"
)

type Compression = cql.Compression

const LZ4Compression = cql.LZ4Compression

const NoCompression = cql.NoCompression

const SnappyCompression = cql.SnappyCompression
