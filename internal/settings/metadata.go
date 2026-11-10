// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package settings

import (
	"time"

	"github.com/scylladb/gocql/v2/internal/cqltype"
)

type LifecycleObserver interface{ ObserveLifecycle(LifecycleEvent) }
type LifecycleEvent struct {
	At          time.Time
	Err         error
	Kind, Stage string
	Stream      int16
	HostID      cqltype.UUID
}
