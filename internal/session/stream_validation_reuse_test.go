// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
)

type pageValidationDecoder struct{ compiles, resets, decodes int }
type pageValidationPlan struct {
	decoder *pageValidationDecoder
	plan    codec.StreamRowPlan[[]byte]
}

func (d *pageValidationDecoder) Compile(columns cqltype.Columns) (codec.StreamRowPlan[[]byte], error) {
	d.compiles++
	plan, err := codec.StreamOneColumn(codec.StreamBlob).Compile(columns)
	return pageValidationPlan{decoder: d, plan: plan}, err
}
func (p pageValidationPlan) Reset(dst *[]byte) {
	p.decoder.resets++
	p.plan.Reset(dst)
}
func (p pageValidationPlan) DecodeInto(reader *codec.StreamRowReader, dst *[]byte) error {
	p.decoder.decodes++
	return p.plan.DecodeInto(reader, dst)
}
