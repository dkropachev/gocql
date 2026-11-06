//go:build unit
// +build unit

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	implProtocol "github.com/scylladb/gocql/v2/internal/protocol"

	"testing"
)

// readStartupOptions decodes the string map carried by a STARTUP frame body.
// The framer read helpers panic on a truncated buffer and this runs on the fake
// server's read goroutine, so a malformed frame is turned into a test failure
// instead of taking the whole test binary down.
func readStartupOptions(t *testing.T, f *implProtocol.Framer) (opts map[string]string) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("malformed STARTUP frame: %v", r)
			opts = nil
		}
	}()

	opts = make(map[string]string)
	for n := f.ReadShort(); n > 0; n-- {
		key := f.ReadString()
		opts[key] = f.ReadString()
	}
	return opts
}
