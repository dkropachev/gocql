// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cqlerr

import (
	"fmt"
	"testing"
)

// Cassandra over protocol 5 reports an LWT with an unknown outcome as
// CAS_WRITE_UNKNOWN; ScyllaDB and Cassandra over protocol 4 as a write timeout
// of write type CAS. One check covers both.
func TestIsCASWriteUnknown(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want bool
	}{
		{&CASWriteUnknownError{}, true},
		{fmt.Errorf("wrapped: %w", &CASWriteUnknownError{}), true},
		{&WriteTimeoutError{WriteType: "CAS"}, true},
		{&WriteTimeoutError{WriteType: "SIMPLE"}, false},
		{&ReadTimeoutError{}, false},
		{nil, false},
	} {
		if got := IsCASWriteUnknown(tc.err); got != tc.want {
			t.Errorf("%v: %t, want %t", tc.err, got, tc.want)
		}
		if tc.want && ExecutionStatusOf(tc.err) != MaybeExecuted {
			t.Errorf("%v: status %v, want MaybeExecuted", tc.err, ExecutionStatusOf(tc.err))
		}
	}
}
