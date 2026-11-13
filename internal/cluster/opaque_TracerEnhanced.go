// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"sync"
)

type TracerEnhancedStorage struct {
	session  *Core
	traceIDs [][]byte
	mu       sync.Mutex
}
