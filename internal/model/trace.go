package model

import (
	"net/netip"

	"github.com/scylladb/gocql/v2/internal/cqltype"
)

type TraceSessionRow struct {
	InternalCoordinator cqltype.Nullable[netip.Addr]
	InternalStarted     cqltype.Nullable[cqltype.Timestamp]
	InternalDuration    cqltype.Nullable[int32]
}
type TraceActivityRow struct {
	InternalSource   netip.Addr
	InternalActivity cqltype.Text
	InternalThread   cqltype.Text
	InternalElapsed  cqltype.Nullable[int32]
	InternalId       cqltype.TimeUUID
}
