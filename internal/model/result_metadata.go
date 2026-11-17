package model

import (
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/schemafence"
)

// resultPlanCache is scoped to one application decoder/handle, not CQL. Only
// its current shape is retained; live pages own previous selected plans.
type ResultSchemaIdentity struct {
	InternalTracker *schemafence.InternalPreparedSchemaTracker
	InternalColumns cql.Columns
	InternalEpoch   schemafence.InternalPreparedSchemaStamp
}
