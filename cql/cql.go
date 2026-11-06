// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Package cql holds the CQL data vocabulary: statements, types, values,
// codecs, consistency levels and server errors. It does no I/O.
package cql

import cqltype "github.com/scylladb/gocql/v2/internal/cqltype"

type Consistency = cqltype.Consistency

const (
	Any         = cqltype.Any
	One         = cqltype.One
	Two         = cqltype.Two
	Three       = cqltype.Three
	Quorum      = cqltype.Quorum
	All         = cqltype.All
	LocalQuorum = cqltype.LocalQuorum
	EachQuorum  = cqltype.EachQuorum
	Serial      = cqltype.Serial
	LocalSerial = cqltype.LocalSerial
	LocalOne    = cqltype.LocalOne
)

// Option distinguishes an explicit zero value from an inherited setting.
type Option[T any] = cqltype.Option[T]

// Some returns an Option set to v.
func Some[T any](v T) Option[T] { return cqltype.Some(v) }
