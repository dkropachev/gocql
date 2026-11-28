// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"iter"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/binding"
	"github.com/scylladb/gocql/v2/internal/cqltype"
)

// UnpreparedExec executes one CQL text command and owns its query's cleanup.
func (s *Session) UnpreparedExec[P any](ctx context.Context, statement binding.Statement[P, cql.EmptyRow], value P, options ...Options) (cql.Result, error) {
	q, err := unpreparedCall(s, statement, value, options)
	if err != nil {
		return InternalExecutionErrorResult(err), err
	}
	defer q.state.close()
	return q.Exec(ctx, value)
}

// UnpreparedOne reads one owned row and owns its query's cleanup.
func (s *Session) UnpreparedOne[P, R any](ctx context.Context, statement binding.Statement[P, R], value P, dst *R, options ...Options) (bool, cql.Result, error) {
	q, err := unpreparedCall(s, statement, value, options)
	if err != nil {
		return false, InternalExecutionErrorResult(err), err
	}
	defer q.state.close()
	return q.One(ctx, value, dst)
}

// UnpreparedEach visits rows synchronously and owns its query's cleanup.
func (s *Session) UnpreparedEach[P, R any](ctx context.Context, statement binding.Statement[P, R], value P, dst *R, fn func(*R) error, options ...Options) (cql.Result, error) {
	if fn == nil {
		return nilEachCallback()
	}
	q, err := unpreparedCall(s, statement, value, options)
	if err != nil {
		return InternalExecutionErrorResult(err), err
	}
	defer q.state.close()
	return q.Each(ctx, value, dst, fn)
}

// UnpreparedAll iterates rows as Query.All does and owns its query's cleanup:
// the query is closed when the iteration ends.
func (s *Session) UnpreparedAll[P, R any](ctx context.Context, statement binding.Statement[P, R], value P, dst *R, options ...Options) iter.Seq2[*R, error] {
	return func(yield func(*R, error) bool) {
		q, err := unpreparedCall(s, statement, value, options)
		if err != nil {
			yield(nil, err)
			return
		}
		defer q.state.close()
		if _, err := q.Each(ctx, value, dst, yieldRow(yield)); err != nil {
			yield(nil, err)
		}
	}
}

// unpreparedCall validates and resolves the call's options, a throwaway call
// having no setup step to do it in, and builds the query that runs it.
func unpreparedCall[P, R any](s *Session, statement binding.Statement[P, R], value P, options []Options) (*Query[P, R], error) {
	if err := unpreparedMarkers(&statement, value); err != nil {
		return nil, notSent(err)
	}
	var values Options
	switch len(options) {
	case 0:
	case 1:
		values = options[0]
	default:
		return nil, notSent(cqltype.TypeValidation("options", "at most one CompiledOptions"))
	}
	o, err := queryOptions(s.Core(), values)
	if err != nil {
		return nil, notSent(err)
	}
	q, err := newUnpreparedQuery(s.Core(), statement, o)
	return q, notSent(err)
}

// unpreparedMarkers completes statement's BindMarkers for an unprepared
// execution with value. Without them, cql.DynamicParams takes its markers'
// types from the values; any other statement with parameters, P other than
// cql.EmptyParams, is rejected, since its encoder would bind nothing.
func unpreparedMarkers[P, R any](statement *binding.Statement[P, R], value P) error {
	if statement.BindMarkers.Len() != 0 {
		return nil
	}
	switch v := any(value).(type) {
	case cql.EmptyParams:
		return nil
	case []cql.Value:
		if any(statement.Params) == any(cql.DynamicParams) {
			specs := make([]cql.ColumnSpec, len(v))
			for i := range v {
				typ := v[i].Type()
				if typ.Equal(cql.DataType{}) {
					// cql.NullValue and cql.UnsetValue have no type; a
					// QUERY sends no marker types, and either is written
					// the same for any type, so blob stands in.
					typ = cql.BlobType
				}
				specs[i] = cql.ColumnSpec{Type: typ}
			}
			statement.BindMarkers = cql.NewColumns(specs...)
			return nil
		}
	}
	return cqltype.TypeValidation("bind markers", "a statement with parameters executed without preparation needs BindMarkers")
}
