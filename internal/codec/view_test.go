// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package codec

import (
	"errors"
	"testing"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/wire"
)

func FoundationChain(t *testing.T, parts ...[]byte) wire.Chain {
	t.Helper()
	spans := make([]wire.Span, len(parts))
	for i, p := range parts {
		spans[i] = wire.Borrow(p)
	}
	chain, err := wire.NewChain(spans...)
	if err != nil {
		t.Fatal(err)
	}
	return chain
}
func foundationPanicReleased(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		v := recover()
		err, ok := v.(error)
		if !ok || !errors.Is(err, cqltype.ErrReleased) {
			t.Fatalf("expected ErrReleased panic, got %v", v)
		}
	}()
	fn()
}
