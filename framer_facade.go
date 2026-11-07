// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	protocol "github.com/scylladb/gocql/v2/internal/protocol"
)

// Deprecated: use cqlerr.ErrProtocol.
type ErrProtocol = protocol.ErrProtocol

// Deprecated: use cqlerr.ErrReadHeaderTimeout.
var ErrReadHeaderTimeout = protocol.ErrReadHeaderTimeout

func NewErrProtocol(a0 string, a1 ...any) error { return protocol.NewErrProtocol(a0, a1...) }
