// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	implMetadata "github.com/scylladb/gocql/v2/internal/metadata"
)

// Deprecated: use cqlerr.NoHostsError.
type NoHostsError = implMetadata.NoHostsError

// Deprecated: use cqlerr.PartialRefreshError.
type PartialRefreshError = implMetadata.PartialRefreshError
