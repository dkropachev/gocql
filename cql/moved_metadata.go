// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"fmt"
)

// String returns a human readable name for the Cassandra datatype
// described by t.
// Type is the identifier of a Cassandra internal datatype.
type Type int

const TypeCustom Type = 0x0000

const TypeAscii Type = 0x0001

const TypeBigInt Type = 0x0002

const TypeBlob Type = 0x0003

const TypeBoolean Type = 0x0004

const TypeCounter Type = 0x0005

const TypeDecimal Type = 0x0006

const TypeDouble Type = 0x0007

const TypeFloat Type = 0x0008

const TypeInt Type = 0x0009

const TypeText Type = 0x000A

const TypeTimestamp Type = 0x000B

const TypeUUID Type = 0x000C

const TypeVarchar Type = 0x000D

const TypeVarint Type = 0x000E

const TypeTimeUUID Type = 0x000F

const TypeInet Type = 0x0010

const TypeDate Type = 0x0011

const TypeTime Type = 0x0012

const TypeSmallInt Type = 0x0013

const TypeTinyInt Type = 0x0014

const TypeDuration Type = 0x0015

const TypeList Type = 0x0020

const TypeMap Type = 0x0021

const TypeSet Type = 0x0022

const TypeUDT Type = 0x0030

const TypeTuple Type = 0x0031

// String returns the name of the identifier.
func (t Type) String() string {
	switch t {
	case TypeCustom:
		return "custom"
	case TypeAscii:
		return "ascii"
	case TypeBigInt:
		return "bigint"
	case TypeBlob:
		return "blob"
	case TypeBoolean:
		return "boolean"
	case TypeCounter:
		return "counter"
	case TypeDecimal:
		return "decimal"
	case TypeDouble:
		return "double"
	case TypeFloat:
		return "float"
	case TypeInt:
		return "int"
	case TypeText:
		return "text"
	case TypeTimestamp:
		return "timestamp"
	case TypeUUID:
		return "uuid"
	case TypeVarchar:
		return "varchar"
	case TypeTimeUUID:
		return "timeuuid"
	case TypeInet:
		return "inet"
	case TypeDate:
		return "date"
	case TypeDuration:
		return "duration"
	case TypeTime:
		return "time"
	case TypeSmallInt:
		return "smallint"
	case TypeTinyInt:
		return "tinyint"
	case TypeList:
		return "list"
	case TypeMap:
		return "map"
	case TypeSet:
		return "set"
	case TypeVarint:
		return "varint"
	case TypeTuple:
		return "tuple"
	default:
		return fmt.Sprintf("unknown_type_%d", t)
	}
}
