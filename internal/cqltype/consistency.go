// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cqltype

import "github.com/scylladb/gocql/v2/cql"

type Consistency = cql.Consistency

const (
	Any         = cql.Any
	One         = cql.One
	Two         = cql.Two
	Three       = cql.Three
	Quorum      = cql.Quorum
	All         = cql.All
	LocalQuorum = cql.LocalQuorum
	EachQuorum  = cql.EachQuorum
	Serial      = cql.Serial
	LocalSerial = cql.LocalSerial
	LocalOne    = cql.LocalOne
)

type Option[T any] = cql.Option[T]
