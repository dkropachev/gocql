// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import "sync"

type PreparedStorage[P, R any] struct {
	InternalParams ParamPlan[P]
	InternalRows   RowDecoder[R]
	poolErr        error
	InternalState  *InternalPreparedState
	InternalPlans  *InternalResultPlanCache[R]
	pool           *queryPool[P, R]
	poolOnce       sync.Once
	InternalKind   StatementKind
}

func PreparedData[P any, R any](v *Prepared[P, R]) *PreparedStorage[P, R] { return &v.privateStorage }
