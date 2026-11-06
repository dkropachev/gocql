// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package routing

import (
	"time"
)

func deadlineCases() []struct {
	name   string
	now    time.Time
	period time.Duration
} {
	return []struct {
		name   string
		now    time.Time
		period time.Duration
	}{
		{"ordinary-monotonic", time.Now(), time.Minute},
		{"cross-UnixNano-2262", time.Date(2262, 4, 11, 23, 47, 16, 800000000, time.UTC), time.Minute},
		{"maximum-positive-duration", time.Now(), time.Duration(1<<63 - 1)},
		{"maximum-after-2262", time.Date(2262, 4, 11, 23, 47, 16, 800000000, time.UTC), time.Duration(1<<63 - 1)},
	}
}
