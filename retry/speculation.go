// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package retry

import (
	"time"
)

// SpeculationPolicy schedules additional attempts of eligible executions.
// Delay receives the number of attempts started so far and returns how long
// to wait before starting another, and whether to start one at all.
type SpeculationPolicy interface {
	Delay(attempt int) (time.Duration, bool)
}
