// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import "github.com/scylladb/gocql/v2/internal/wire"

// The writer and reader a compiled plan runs on are unexported. These let
// the external tests drive a plan, so that their generic instantiations
// compile with package cql_test instead of adding to package cql's
// compile, which is already the largest of the module.

// EncodeParamsForTest encodes value with plan and freezes the parameters.
func EncodeParamsForTest[P any](columns Columns, plan CompiledParams[P], value P, allowUnset bool) (wire.Chain, error) {
	builder := wire.NewBuilder(8)
	w := newParamWriter(builder, columns, allowUnset)
	if err := plan.Encode(w, value); err != nil {
		return wire.Chain{}, err
	}
	if err := finishParamWriter(w); err != nil {
		return wire.Chain{}, err
	}
	return builder.Freeze()
}

// DecodeRowForTest decodes one row of chain with plan and checks that the
// plan read every column.
func DecodeRowForTest[R any](columns Columns, plan CompiledRow[R], chain wire.Chain) (R, error) {
	reader := newRowReader(columns, chain, wire.NewLease(chain, nil))
	row, err := decodeRow(plan, reader)
	if err == nil {
		err = finishRowReader(reader)
	}
	return row, err
}
