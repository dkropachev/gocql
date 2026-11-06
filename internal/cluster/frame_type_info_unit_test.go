package cluster

import (
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/protocol"
)

// typeOption writes a CQL [option] type description the way a server does.
type typeOption func(f *protocol.Framer)

func optNative(t cqltype.Type) typeOption {
	return func(f *protocol.Framer) { f.WriteShort(uint16(t)) }
}

func optCollection(t cqltype.Type, elems ...typeOption) typeOption {
	return func(f *protocol.Framer) {
		f.WriteShort(uint16(t))
		for _, e := range elems {
			e(f)
		}
	}
}
