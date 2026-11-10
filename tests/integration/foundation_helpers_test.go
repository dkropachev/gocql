//go:build integration

package integration

import (
	"context"

	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/binding"
	implExecution "github.com/scylladb/gocql/v2/internal/execution"
)

type fixtureOptions interface {
	gocql.Options | implExecution.Options
}
type fixturePrepareOptions interface {
	gocql.PrepareOptions | implExecution.PrepareOptions
}

func codecTestMust[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

// bindOptions binds v to session, failing loudly on values a test meant to
// be valid.
func bindOptions[O fixtureOptions](session *gocql.Session, v O) gocql.CompiledOptions {
	switch o := any(v).(type) {
	case gocql.Options:
		return codecTestMust(session.BindOptions(o))
	case implExecution.Options:
		return gocql.CompiledOptions(codecTestMust(implExecution.InternalBindOptions((*implExecution.Session)(session).Core(), o)))
	}
	panic("unreachable fixture options type")
}

// prepareFixture preserves connection injection only inside these historical
// protocol fixtures. Public preparation remains free of transport hooks.
func prepareFixture[P, R any, O fixturePrepareOptions](ctx context.Context, session *gocql.Session, statement cql.Statement[P, R], options O) (*gocql.Prepared[P, R], error) {
	var internal implExecution.PrepareOptions
	switch o := any(options).(type) {
	case gocql.PrepareOptions:
		internal = implExecution.PrepareOptions{Keyspace: o.Keyspace, HostID: o.HostID, Trace: o.Trace}
	case implExecution.PrepareOptions:
		internal = o
	}
	p, err := implExecution.Prepare(ctx, (*implExecution.Session)(session).Core(), fixtureStatement(statement), internal)
	return (*gocql.Prepared[P, R])(p), err
}

func fixtureStatement[P, R any](s cql.Statement[P, R]) binding.Statement[P, R] {
	return binding.Statement[P, R]{CQL: s.CQL, Params: s.Params, Rows: s.Rows, Kind: s.Kind, BindMarkers: s.BindMarkers}
}
