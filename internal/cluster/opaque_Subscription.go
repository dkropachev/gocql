// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"sync"
)

type SubscriptionStorage struct {
	events chan Event
	parent *InternalV2PeripheralState
	filter EventFilter
	mu     sync.Mutex
	closed bool
}
