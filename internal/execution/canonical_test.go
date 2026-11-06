// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"bytes"
	"context"
	"fmt"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/scylladb/gocql/v2/internal/wire"
)

type testWriter interface {
	InternalWriteContext(context.Context, []byte) (int, error)
	InternalSetWriteTimeout(time.Duration)
}
type canonicalWriter struct {
	testWriter
	payload []byte
	bytes   atomic.Int64
}

func (w *canonicalWriter) InternalWriteSpans(ctx context.Context, chain wire.Chain) (int64, error) {
	if e := chain.Each(func(s wire.Span) error {
		p := s.Bytes()
		if len(p) == len(w.payload) && bytes.Equal(p, w.payload) {
			if unsafe.SliceData(p) != unsafe.SliceData(w.payload) {
				return fmt.Errorf("canonical payload was copied")
			}
			w.bytes.Add(int64(len(p)))
		}
		return nil
	}); e != nil {
		return 0, e
	}
	return w.testWriter.(interface {
		InternalWriteSpans(context.Context, wire.Chain) (int64, error)
	}).InternalWriteSpans(ctx, chain)
}
