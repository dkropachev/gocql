//go:build !race

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package transport

import (
	"bytes"
	"testing"
)

// This file builds without -race: sync.Pool drops items under it.

func TestRecvV5Allocs(t *testing.T) {
	l := newRecvLoop(t, 5, recvWire(t, 5, invFrame(5, 0, typedTestRowsBody(bytes.Repeat([]byte{'x'}, 8)))))
	recv := func() { l.one(t).Owner.Release() }
	recv()
	if got := testing.AllocsPerRun(200, recv); got > 1 {
		t.Fatalf("v5 receive allocs = %v, want <= 1", got)
	}
}
