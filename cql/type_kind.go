// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"fmt"
)

// Kind is the kind of a CQL data type, as the native protocol numbers it, so
// the zero Kind is KindCustom. KindVector is the driver's own: the protocol
// sends a vector as a custom type. CQL's text and varchar are one type, so
// both are KindText: the protocol's text ID 0x000A and the schema names text
// and varchar all decode to it. Later releases may add values, so a switch
// over them needs a default case.
type Kind uint16

// The kinds of CQL data types.
const (
	KindCustom    Kind = 0x0000
	KindASCII     Kind = 0x0001
	KindBigInt    Kind = 0x0002
	KindBlob      Kind = 0x0003
	KindBoolean   Kind = 0x0004
	KindCounter   Kind = 0x0005
	KindDecimal   Kind = 0x0006
	KindDouble    Kind = 0x0007
	KindFloat     Kind = 0x0008
	KindInt       Kind = 0x0009
	KindTimestamp Kind = 0x000B
	KindUUID      Kind = 0x000C
	KindText      Kind = 0x000D
	KindVarint    Kind = 0x000E
	KindTimeUUID  Kind = 0x000F
	KindInet      Kind = 0x0010
	KindDate      Kind = 0x0011
	KindTime      Kind = 0x0012
	KindSmallInt  Kind = 0x0013
	KindTinyInt   Kind = 0x0014
	KindDuration  Kind = 0x0015
	KindList      Kind = 0x0020
	KindMap       Kind = 0x0021
	KindSet       Kind = 0x0022
	KindUDT       Kind = 0x0030
	KindTuple     Kind = 0x0031

	// KindVector is the kind of a vector<element, dimension>. It is not a
	// native protocol type ID.
	KindVector Kind = 0x0100
)

// String returns the kind's CQL name, such as "bigint", or Kind(n) for a
// value it does not define.
func (t Kind) String() string {
	switch t {
	case KindCustom:
		return "custom"
	case KindASCII:
		return "ascii"
	case KindBigInt:
		return "bigint"
	case KindBlob:
		return "blob"
	case KindBoolean:
		return "boolean"
	case KindCounter:
		return "counter"
	case KindDecimal:
		return "decimal"
	case KindDouble:
		return "double"
	case KindFloat:
		return "float"
	case KindInt:
		return "int"
	case KindText:
		return "text"
	case KindTimestamp:
		return "timestamp"
	case KindUUID:
		return "uuid"
	case KindTimeUUID:
		return "timeuuid"
	case KindInet:
		return "inet"
	case KindDate:
		return "date"
	case KindDuration:
		return "duration"
	case KindTime:
		return "time"
	case KindSmallInt:
		return "smallint"
	case KindTinyInt:
		return "tinyint"
	case KindList:
		return "list"
	case KindMap:
		return "map"
	case KindSet:
		return "set"
	case KindVarint:
		return "varint"
	case KindTuple:
		return "tuple"
	case KindUDT:
		return "udt"
	case KindVector:
		return "vector"
	default:
		return fmt.Sprintf("Kind(%d)", int(t))
	}
}
