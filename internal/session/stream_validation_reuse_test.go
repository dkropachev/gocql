// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"github.com/scylladb/gocql/v2/cql"
)

type pageValidationDecoder struct{ compiles, resets, decodes int }
type pageValidationPlan struct {
	decoder *pageValidationDecoder
	plan    cql.StreamRowPlan[[]byte]
}

func (d *pageValidationDecoder) Compile(columns cql.Columns) (cql.StreamRowPlan[[]byte], error) {
	d.compiles++
	plan, err := cql.StreamOneColumn(cql.StreamBlob).Compile(columns)
	return pageValidationPlan{decoder: d, plan: plan}, err
}
func (p pageValidationPlan) Reset(dst *[]byte) {
	p.decoder.resets++
	p.plan.Reset(dst)
}
func (p pageValidationPlan) DecodeInto(reader *cql.StreamRowReader, dst *[]byte) error {
	p.decoder.decodes++
	return p.plan.DecodeInto(reader, dst)
}
