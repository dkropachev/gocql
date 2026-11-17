// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cqlerr

import (
	"errors"
	"fmt"
	"net"
	"testing"

	"github.com/scylladb/gocql/v2/cql"
)

type benchTimeoutError struct{}

func (benchTimeoutError) Error() string   { return "i/o timeout" }
func (benchTimeoutError) Timeout() bool   { return true }
func (benchTimeoutError) Temporary() bool { return true }

var benchKindSink Kind

// BenchmarkKindOf measures classifying the errors a failed attempt carries.
func BenchmarkKindOf(b *testing.B) {
	unavailable := &UnavailableError{}
	for _, bench := range []struct {
		name string
		err  error
	}{
		{"server", fmt.Errorf("attempt: %w", unavailable)},
		{"execution", newExecutionError(unavailable, cql.Result{}, 0, nil)},
		{"net_timeout", &net.OpError{Op: "read", Err: benchTimeoutError{}}},
		{"unknown", fmt.Errorf("wrap: %w", errors.New("x"))},
	} {
		b.Run(bench.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				benchKindSink = KindOf(bench.err)
			}
		})
	}
}
