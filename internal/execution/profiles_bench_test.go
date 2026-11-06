// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"testing"
)

// BenchmarkNewOptionTemplates encodes the templates of resolved options, as
// NewOptions and a query created with options for another session do.
func BenchmarkNewOptionTemplates(b *testing.B) {
	profile := defaultProfile()
	profile.PageSize = 5000
	for _, c := range []struct {
		name   string
		values OptionValues
	}{{"plain", OptionValues{}}, {"timestamp", OptionValues{Timestamp: Option[int64]{Value: 1, Set: true}}}} {
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if newOptionTemplates(&profile, &c.values, "ks") == nil {
					b.Fatal("no templates")
				}
			}
		})
	}
}
