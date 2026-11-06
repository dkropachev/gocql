// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package protocol

import (
	"testing"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/wire"
)

func BenchmarkTypedFrame(b *testing.B) {
	// Builds a prepared point write's frame with its
	// options encoded field by field and from a template.
	b.Run("ParamsTemplate", func(b *testing.B) {
		value := []byte{0, 0, 0, 4, 0, 0, 0, 1, 0, 0, 4, 0}
		values, _ := wire.NewChain(wire.Borrow(value), wire.Borrow(make([]byte, 1024)))
		settings := protocolSettings(4)
		scratch := wire.NewBuilderScratch(256, 16)
		for _, c := range []struct {
			name   string
			paging []byte
		}{{"point", nil}, {"page", []byte("paging-state-token-0123456789")}} {
			r := &model.AttemptRequest{PreparedID: []byte("0123456789abcdef"), Values: values, ValueCount: 2, Consistency: cqltype.LocalQuorum, PageSize: 5000, SerialConsistency: cqltype.LocalSerial, DefaultTimestamp: true, Timestamp: 1, PagingState: c.paging}
			template, _ := NewParamsTemplate(r, 4)
			for _, mode := range []string{"fields", "template"} {
				b.Run(c.name+"/"+mode, func(b *testing.B) {
					r.Params = nil
					if mode == "template" {
						r.Params = template
					}
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						scratch.Reset()
						r.Timestamp = int64(i)
						if _, _, err := settings.BuildTypedRequestInto(r, 9, scratch); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		}
	})
}
