// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"github.com/scylladb/gocql/v2/internal/wire"
)

// These accessors give the codecs of this package the representation of the
// value types. Constructors take ownership of descriptor slices; consumers
// never mutate them. Descriptors are shared: every ScalarType call and
// predeclared scalar such as IntType returns the same descriptor for a kind,
// so writing through descriptorOf changes that type process-wide. Build a
// private descriptor with typeFromDescriptor instead. The engine reads the same
// representation through the mirrors in internal/cqltype.
func descriptorOf(t DataType) *dataTypeDescriptor { return t.descriptor }
func typeFromDescriptor(d *dataTypeDescriptor) DataType {
	return DataType{descriptor: d}
}
func columnSlice(c Columns) []ColumnSpec               { return c.columns }
func bytesChain(b Bytes) wire.Chain                    { return b.chain }
func bytesLease(b Bytes) wire.Ref                      { return b.lease }
func bytesView(c wire.Chain, l wire.Ref) Bytes         { return Bytes{chain: c, lease: l} }
func textView(b Bytes) Text                            { return Text{bytes: b} }
func textBytes(t Text) Bytes                           { return t.bytes }
func checkBytesAlive(b Bytes) error                    { return b.checkAlive() }
func eachBytes(b Bytes, f func(wire.Span) error) error { return b.each(f) }
func typeFingerprint(t DataType) string                { return t.fingerprint() }
