// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cqltype

import (
	"fmt"
	"math"

	cql "github.com/scylladb/gocql/v2/cql"
)

// dataTypeFromInfo adapts protocol metadata only; it never constructs values or
// calls the legacy reflection-based serialization methods.
func DataTypeFromInfo(info TypeInfo) (cql.DataType, error) { return dataTypeFromInfoDepth(info, 0) }

func dataTypeFromInfoDepth(info TypeInfo, depth int) (cql.DataType, error) {
	if info == nil {
		return cql.DataType{}, TypeValidation("type", "nil protocol descriptor")
	}
	if depth > 128 {
		return cql.DataType{}, TypeValidation("type", "protocol descriptor nesting exceeds 128")
	}
	child := func(t TypeInfo) (cql.DataType, error) { return dataTypeFromInfoDepth(t, depth+1) }
	switch t := info.(type) {
	case *NativeType:
		if t == nil {
			return cql.DataType{}, TypeValidation("type", "nil protocol descriptor")
		}
		return child(*t)
	case *CollectionType:
		if t == nil {
			return cql.DataType{}, TypeValidation("type", "nil protocol descriptor")
		}
		return child(*t)
	case *TupleTypeInfo:
		if t == nil {
			return cql.DataType{}, TypeValidation("type", "nil protocol descriptor")
		}
		return child(*t)
	case *UDTTypeInfo:
		if t == nil {
			return cql.DataType{}, TypeValidation("type", "nil protocol descriptor")
		}
		return child(*t)
	case *VectorType:
		if t == nil {
			return cql.DataType{}, TypeValidation("type", "nil protocol descriptor")
		}
		return child(*t)
	case CollectionType:
		e, err := child(t.Elem)
		if err != nil {
			return cql.DataType{}, err
		}
		switch t.Type() {
		case cql.TypeList:
			return cql.ListType(e)
		case cql.TypeSet:
			return cql.SetType(e)
		case cql.TypeMap:
			k, err := child(t.Key)
			if err != nil {
				return cql.DataType{}, err
			}
			return cql.MapType(k, e)
		}
		return cql.DataType{}, TypeValidation("type", "invalid collection kind %v", t.Type())
	case TupleTypeInfo:
		if len(t.Elems) > math.MaxUint16 {
			return cql.DataType{}, TypeValidation("fields", "too many tuple fields")
		}
		fields := make([]cql.DataType, len(t.Elems))
		for i, f := range t.Elems {
			v, err := child(f)
			if err != nil {
				return cql.DataType{}, err
			}
			fields[i] = v
		}
		return cql.TupleType(fields...)
	case UDTTypeInfo:
		if len(t.Elements) > math.MaxUint16 {
			return cql.DataType{}, TypeValidation("fields", "too many UDT fields")
		}
		fields := make([]cql.ColumnSpec, len(t.Elements))
		for i, f := range t.Elements {
			v, err := child(f.Type)
			if err != nil {
				return cql.DataType{}, err
			}
			fields[i] = cql.ColumnSpec{Name: f.Name, Type: v}
		}
		return cql.UDTType(t.KeySpace, t.Name, fields...)
	case VectorType:
		if t.Dimensions <= 0 || t.Dimensions > math.MaxUint16 {
			return cql.DataType{}, TypeValidation("dimension", "invalid vector dimension %d", t.Dimensions)
		}
		e, err := child(t.SubType)
		if err != nil {
			return cql.DataType{}, err
		}
		return cql.VectorType(e, uint16(t.Dimensions))
	default:
		return cql.ScalarType(info.Type())
	}
}

func ColumnsFromInfo(info []ColumnInfo) (cql.Columns, error) {
	columns := make([]cql.ColumnSpec, len(info))
	for i, c := range info {
		t, err := DataTypeFromInfo(c.TypeInfo)
		if err != nil {
			return cql.Columns{}, fmt.Errorf("column %d (%s): %w", i, c.Name, err)
		}
		columns[i] = cql.ColumnSpec{Keyspace: c.Keyspace, Table: c.Table, Name: c.Name, Type: t}
	}
	return ColumnsOwned(columns), nil
}
