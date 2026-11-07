// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	implTransport "github.com/scylladb/gocql/v2/internal/transport"
)

// Deprecated: use cqlerr.ErrSchemaMismatch.
type ErrSchemaMismatch = implTransport.ErrSchemaMismatch

type StreamLimits = implTransport.StreamLimits
