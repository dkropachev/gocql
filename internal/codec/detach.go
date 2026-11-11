// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package codec

import "github.com/scylladb/gocql/v2/internal/cqltype"

// DetachRow makes decoded fields independent of the response before its page
// is released. Built-in plans preserve owned destination buffers and detach
// only borrowed fields. Custom plans use their recursive Clone contract.
func DetachRow[R any](plan RowPlan[R], dst *R) {
	if p, ok := plan.(interface{ detachRow(*R) }); ok {
		p.detachRow(dst)
	} else {
		*dst = plan.Clone(*dst)
	}
}

func detachValue[T any](decoder Decoder[T], dst *T) {
	if d, ok := decoder.(interface{ detachValue(*T) }); ok {
		d.detachValue(dst)
	} else {
		*dst = decoder.Clone(*dst)
	}
}

func (p rowPlan[R]) detachRow(dst *R) {
	for _, field := range p.ordered {
		field.detachField(dst)
	}
}

func (f outputField[R, V]) detachField(dst *R) { detachValue(f.codec, f.field(dst)) }
func (f ignoredField[R]) detachField(*R)       {}

func (p singleColumnPlan[T]) detachRow(dst *T) { detachValue(p.decoder, dst) }

func (p pairedRowPlan[A, B]) detachRow(dst *Pair[A, B]) {
	DetachRow(p.a, &dst.First)
	DetachRow(p.b, &dst.Second)
}

func (c scalarCodec[T]) detachValue(dst *T) {
	// Blob, varint and decimal retain page views and provide clone callbacks.
	// The other scalar decoders materialize their result directly.
	if c.clone != nil {
		*dst = c.Clone(*dst)
	}
}

func (stringCodec) detachValue(*string)           {}
func (byteSliceCodec) detachValue(*[]byte)        {}
func (c textCodec) detachValue(dst *cqltype.Text) { *dst = c.Clone(*dst) }

func (c nullableDecoder[T]) detachValue(dst *cqltype.Nullable[T]) {
	if dst.Valid {
		detachValue(c.base, &dst.Value)
	}
}

func (c emptyCodec[T]) detachValue(dst *cqltype.EmptyValueOf[T]) {
	if !dst.Empty {
		decoder := c.decoder
		if decoder == nil {
			decoder = c.base
		}
		detachValue(decoder, &dst.Value)
	}
}

func (c sliceCodec[T]) detachValue(dst *[]T) { c.dec.detachValue(dst) }
func (c sequenceDecoder[T]) detachValue(dst *[]T) {
	for i := range *dst {
		detachValue(c.element, &(*dst)[i])
	}
}

func (c mapSliceCodec[K, V]) detachValue(dst *[]Entry[K, V]) { c.dec.detachValue(dst) }
func (c mapDecoder[K, V]) detachValue(dst *[]Entry[K, V]) {
	for i := range *dst {
		detachValue(c.key, &(*dst)[i].Key)
		detachValue(c.value, &(*dst)[i].Value)
	}
}

func (c recordDecoder[T]) detachValue(dst *T) { DetachRow(c.plan, dst) }
