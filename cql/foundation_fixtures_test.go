package cql

import (
	"testing"

	"github.com/scylladb/gocql/v2/internal/wire"
)

func foundationScalar(t testing.TB, kind Kind) DataType {
	t.Helper()
	d, err := ScalarType(kind)
	if err != nil {
		t.Fatal(err)
	}
	return d
}
func foundationChain(t *testing.T, parts ...[]byte) wire.Chain {
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
