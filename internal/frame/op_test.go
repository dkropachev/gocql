// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package frame

import "testing"

func TestOp(t *testing.T) {
	t.Parallel()

	t.Run("String", func(t *testing.T) {
		t.Parallel()

		if got := OpAuthSuccess.String(); got != "AUTH_SUCCESS" {
			t.Fatalf("OpAuthSuccess.String() = %q", got)
		}
		// An opcode outside the protocol must still print something identifying
		// rather than collide with a known one.
		if got := Op(0x42).String(); got != "UNKNOWN_OP_66" {
			t.Fatalf("Op(0x42).String() = %q, want UNKNOWN_OP_66", got)
		}
	})
}
