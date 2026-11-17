// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"sync"

	"github.com/scylladb/gocql/v2/event"
)

type SubscriptionStorage struct {
	events chan event.Event
	parent *InternalV2PeripheralState
	filter event.Filter
	mu     sync.Mutex
	closed bool
}
