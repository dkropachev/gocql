// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import "context"

// UnpreparedExec executes one CQL text command and owns its query's cleanup.
func (s *Session) UnpreparedExec[P any](ctx context.Context, statement Statement[P, EmptyRecord], value P, options ...BoundOptions) (Result, error) {
	q, err := unpreparedCall(s, statement, options)
	if err != nil {
		return InternalExecutionErrorResult(err), err
	}
	defer q.state.close()
	return q.Exec(ctx, value)
}

// UnpreparedOne reads one owned row and owns its query's cleanup.
func (s *Session) UnpreparedOne[P, R any](ctx context.Context, statement Statement[P, R], value P, dst *R, options ...BoundOptions) (bool, Result, error) {
	q, err := unpreparedCall(s, statement, options)
	if err != nil {
		return false, InternalExecutionErrorResult(err), err
	}
	defer q.state.close()
	return q.One(ctx, value, dst)
}

// UnpreparedEach visits rows synchronously and owns its query's cleanup.
func (s *Session) UnpreparedEach[P, R any](ctx context.Context, statement Statement[P, R], value P, dst *R, fn func(*R) error, options ...BoundOptions) (Result, error) {
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

func unpreparedCall[P, R any](s *Session, statement Statement[P, R], options []BoundOptions) (*Query[P, R], error) {
	o, err := oneShotOptions(options)
	if err != nil {
		return nil, err
	}
	return newUnpreparedQuery(InternalCore(s), statement, o)
}
