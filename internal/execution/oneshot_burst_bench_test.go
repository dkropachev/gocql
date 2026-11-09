// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"context"
	"fmt"
	"testing"
)

// BenchmarkPreparedOneShotBurst runs n one-shot calls that are all in flight
// at once, then answers them, so that the pool creates n queries and closes
// all but its idle cap as they finish.
func BenchmarkPreparedOneShotBurst(b *testing.B) {
	for _, n := range []int{256, 2048} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			s, _, peers := runtimeTestSession(b, 1)
			defer s.Close()
			defer peers[0].Close()
			requests := make(chan oneShotRequest, n)
			p := oneShotPrepare(b, s, peers[0], requests)
			row := runtimeRowsBody(nil, "row")
			errs := make(chan error, n)
			pending := make([]oneShotRequest, 0, n)
			b.ResetTimer()
			for b.Loop() {
				for i := range n {
					go func() {
						var dst []byte
						_, err := p.One(context.Background(), int32(i), &dst, Options{})
						errs <- err
					}()
				}
				pending = pending[:0]
				for range n {
					pending = append(pending, <-requests)
				}
				for _, r := range pending {
					if _, err := peers[0].Write(typedTestResponse(b, 4, r.stream, row)); err != nil {
						b.Fatal(err)
					}
				}
				for range n {
					if err := <-errs; err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}
