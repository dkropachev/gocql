//go:build integration

package integration

import (
	"context"

	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/binding"
	implSession "github.com/scylladb/gocql/v2/internal/session"
)

type fixtureOptions interface {
	gocql.Options | implSession.Options
}
type fixturePrepareOptions interface {
	gocql.PrepareOptions | implSession.PrepareOptions
}

func codecTestMust[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

// bindOptions binds v to session, failing loudly on values a test meant to
// be valid.
func bindOptions[O fixtureOptions](session *gocql.Session[testDialect], v O) gocql.CompiledOptions {
	switch o := any(v).(type) {
	case gocql.Options:
		return codecTestMust(session.CompileOptions(o))
	case implSession.Options:
		return gocql.CompiledOptions(codecTestMust(implSession.InternalCompileOptions((*implSession.Session)(session).Core(), o)))
	}
	panic("unreachable fixture options type")
}

// prepareFixture preserves connection injection only inside these historical
// protocol fixtures. Public preparation remains free of transport hooks.
func prepareFixture[P, R any, O fixturePrepareOptions](ctx context.Context, session *gocql.Session[testDialect], statement cql.Statement[P, R], options O) (*gocql.Prepared[P, R], error) {
	var internal implSession.PrepareOptions
	switch o := any(options).(type) {
	case gocql.PrepareOptions:
		internal = implSession.PrepareOptions{Keyspace: o.Keyspace, HostID: o.HostID, Trace: o.Trace}
	case implSession.PrepareOptions:
		internal = o
	}
	p, err := implSession.Prepare(ctx, (*implSession.Session)(session).Core(), fixtureStatement(statement), internal)
	return (*gocql.Prepared[P, R])(p), err
}

func fixtureStatement[P, R any](s cql.Statement[P, R]) binding.Statement[P, R] {
	return binding.Statement[P, R]{CQL: s.CQL, Params: s.Params, Rows: s.Rows, Kind: s.Kind, BindMarkers: s.BindMarkers}
}

// preparedData exposes a prepared statement's per-host cache to the protocol
// and cache fixtures, which inspect or corrupt cached prepared IDs.
func preparedData[P, R any](p *gocql.Prepared[P, R]) *implSession.PreparedStorage[P, R] {
	return implSession.PreparedData((*implSession.Prepared[P, R])(p))
}
