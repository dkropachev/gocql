// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package tests

import (
	"bytes"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"
)

// DriverGoroutines returns the stacks of the live goroutines the driver
// started, keyed by goroutine ID. A goroutine counts as the driver's when
// its creator is in a non-test file of this module, or when the standard
// library created it (time.AfterFunc, context.AfterFunc) to run code of a
// non-test file of this module. A fake server's goroutines and the tests'
// own are created from test files or by package testing, so they do not
// count.
func DriverGoroutines() map[string]string {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	out := make(map[string]string)
	for _, g := range bytes.Split(buf, []byte("\n\n")) {
		stack := string(g)
		lines := strings.Split(stack, "\n")
		// The header is "goroutine N [state, wait]:"; only N is stable.
		id, _, _ := strings.Cut(lines[0], " [")
		runsDriverCode := false
		for i, line := range lines {
			if i+1 >= len(lines) {
				break
			}
			file := strings.TrimSpace(lines[i+1])
			driverFrame := strings.HasPrefix(line, "github.com/scylladb/gocql/v2") && !strings.Contains(file, "_test.go:")
			if !strings.HasPrefix(line, "created by ") {
				runsDriverCode = runsDriverCode || driverFrame
				continue
			}
			creator := strings.TrimPrefix(line, "created by ")
			switch {
			case strings.HasPrefix(creator, "github.com/scylladb/gocql/v2"):
				if !strings.Contains(file, "_test.go:") {
					out[id] = stack
				}
			case strings.HasPrefix(creator, "testing."):
			default:
				if runsDriverCode {
					out[id] = stack
				}
			}
		}
	}
	return out
}

// CheckGoroutinesJoined fails t when a driver goroutine that was not in
// before is still alive after wait. wait bounds how long a goroutine that
// is stopped but not joined may take to notice.
func CheckGoroutinesJoined(t testing.TB, before map[string]string, wait time.Duration) {
	t.Helper()
	deadline := time.Now().Add(wait)
	for {
		var leaked []string
		for id, stack := range DriverGoroutines() {
			if _, ok := before[id]; !ok {
				leaked = append(leaked, stack)
			}
		}
		if len(leaked) == 0 {
			return
		}
		if time.Now().After(deadline) {
			slices.Sort(leaked)
			t.Fatalf("%d driver goroutines outlived the session:\n\n%s", len(leaked), strings.Join(leaked, "\n\n"))
		}
		time.Sleep(10 * time.Millisecond)
	}
}
