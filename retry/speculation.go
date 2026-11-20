// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package retry

import (
	"time"
)

// SpeculationPolicy schedules additional attempts of eligible executions.
type SpeculationPolicy interface {
	// Delay returns how long to wait before starting another attempt, given the
	// number of attempts started so far, and false to start no more.
	Delay(attempt int) (time.Duration, bool)
}
