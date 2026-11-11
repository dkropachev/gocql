// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"

	"github.com/scylladb/gocql/v2/internal/binding"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/model"
)

// UnpreparedExec executes one CQL text command and owns its query's cleanup.
func (s *Session) UnpreparedExec[P any](ctx context.Context, statement binding.Statement[P, codec.EmptyRecord], value P, options ...Options) (model.Result, error) {
	q, err := unpreparedCall(s, statement, options)
	if err != nil {
		return InternalExecutionErrorResult(err), err
	}
	defer q.state.close()
	return q.Exec(ctx, value)
}

// UnpreparedOne reads one owned row and owns its query's cleanup.
func (s *Session) UnpreparedOne[P, R any](ctx context.Context, statement binding.Statement[P, R], value P, dst *R, options ...Options) (bool, model.Result, error) {
	q, err := unpreparedCall(s, statement, options)
	if err != nil {
		return false, InternalExecutionErrorResult(err), err
	}
	defer q.state.close()
	return q.One(ctx, value, dst)
}

// UnpreparedEach visits rows synchronously and owns its query's cleanup.
func (s *Session) UnpreparedEach[P, R any](ctx context.Context, statement binding.Statement[P, R], value P, dst *R, fn func(*R) error, options ...Options) (model.Result, error) {
	if fn == nil {
		return nilEachCallback()
	}
	q, err := unpreparedCall(s, statement, options)
	if err != nil {
		return InternalExecutionErrorResult(err), err
	}
	defer q.state.close()
	return q.Each(ctx, value, dst, fn)
}

// unpreparedCall validates and resolves the call's options, a throwaway call
// having no setup step to do it in, and builds the query that runs it.
func unpreparedCall[P, R any](s *Session, statement binding.Statement[P, R], options []Options) (*Query[P, R], error) {
	var values Options
	switch len(options) {
	case 0:
	case 1:
		values = options[0]
	default:
		return nil, cqltype.TypeValidation("options", "at most one Options")
	}
	o, err := queryOptions(s.Core(), values)
	if err != nil {
		return nil, err
	}
	return newUnpreparedQuery(s.Core(), statement, o)
}
