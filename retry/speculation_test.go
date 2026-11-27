// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package retry_test

import (
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/retry"
)

// TestFixedSpeculation checks that FixedSpeculation waits After before each
// additional attempt until Max attempts have started, and that its zero
// value never speculates.
func TestFixedSpeculation(t *testing.T) {
	t.Parallel()
	policy := retry.FixedSpeculation{After: 5 * time.Millisecond, Max: 3}
	for started, want := range []bool{true, true, true, false, false} {
		delay, ok := policy.Delay(retry.Speculation{Started: started})
		if ok != want || ok && delay != policy.After {
			t.Errorf("Delay(started %d) = %v, %t; want %v, %t", started, delay, ok, policy.After, want)
		}
	}
	if _, ok := (retry.FixedSpeculation{}).Delay(retry.Speculation{Started: 1}); ok {
		t.Error("the zero FixedSpeculation speculates")
	}
}
