// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package routing

import (
	"testing"

	"github.com/scylladb/gocql/v2/routing"
)

func BenchmarkDCAwareValidate(b *testing.B) {
	for _, rack := range []string{"", "r1"} {
		b.Run("rack="+rack, func(b *testing.B) {
			p := codecTestMust(NewRoutingPolicy(routing.Options{Datacenter: "a", Rack: rack}))
			m := v2TestMetadata()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if err := p.Validate(m); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
