// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package metadata

import (
	"strconv"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
)

var schemaScalarTypes = map[string]cqltype.Type{
	"ascii": cqltype.TypeAscii, "bigint": cqltype.TypeBigInt, "blob": cqltype.TypeBlob,
	"boolean": cqltype.TypeBoolean, "counter": cqltype.TypeCounter, "date": cqltype.TypeDate,
	"decimal": cqltype.TypeDecimal, "double": cqltype.TypeDouble, "duration": cqltype.TypeDuration,
	"float": cqltype.TypeFloat, "inet": cqltype.TypeInet, "int": cqltype.TypeInt,
	"smallint": cqltype.TypeSmallInt, "text": cqltype.TypeText, "time": cqltype.TypeTime,
	"timestamp": cqltype.TypeTimestamp, "timeuuid": cqltype.TypeTimeUUID, "tinyint": cqltype.TypeTinyInt,
	"uuid": cqltype.TypeUUID, "varchar": cqltype.TypeVarchar, "varint": cqltype.TypeVarint,
}

// InternalSchemaDataType builds the DataType of a schema type string, such as
// the type of a system_schema.columns row, resolving user types from ks.
// Custom (quoted class name) types are rejected.
func InternalSchemaDataType(ks *KeyspaceMetadata, source string) (cqltype.DataType, error) {
	if ks == nil {
		return cqltype.DataType{}, cqltype.TypeValidation("schema type", "nil keyspace")
	}
	p := schemaTypeBuilder{v2CQLTypeParser: v2CQLTypeParser{source: source, keyspace: ks.Name, maxDepth: 64}, ks: ks}
	t, err := p.build(0)
	if err != nil {
		return cqltype.DataType{}, err
	}
	p.space()
	if p.offset != len(source) {
		return cqltype.DataType{}, cqltype.TypeValidation("schema type", "trailing syntax in %q", source)
	}
	return t, nil
}

type schemaTypeBuilder struct {
	ks *KeyspaceMetadata
	v2CQLTypeParser
}

func (p *schemaTypeBuilder) build(depth int) (cqltype.DataType, error) {
	if depth >= p.maxDepth {
		return cqltype.DataType{}, cqltype.TypeValidation("schema type", "maximum nesting depth exceeded")
	}
	p.space()
	if p.offset < len(p.source) && p.source[p.offset] == '\'' {
		return cqltype.DataType{}, cqltype.TypeValidation("schema type", "custom type %q is not supported", p.source)
	}
	name, quoted, err := p.identifier()
	if err != nil {
		return cqltype.DataType{}, err
	}
	p.space()
	if p.offset < len(p.source) && p.source[p.offset] == '.' {
		if name != p.keyspace {
			return cqltype.DataType{}, cqltype.TypeValidation("schema type", "foreign keyspace %q", name)
		}
		p.offset++
		if name, _, err = p.identifier(); err != nil {
			return cqltype.DataType{}, err
		}
		quoted = true
		p.space()
	}
	if p.offset < len(p.source) && p.source[p.offset] == '<' {
		if quoted {
			return cqltype.DataType{}, cqltype.TypeValidation("schema type", "UDT cannot have parameters")
		}
		return p.constructor(name, depth)
	}
	if !quoted {
		if kind, ok := schemaScalarTypes[name]; ok {
			return cqltype.ScalarType(kind)
		}
	}
	return p.userType(name, depth)
}

func (p *schemaTypeBuilder) constructor(name string, depth int) (cqltype.DataType, error) {
	switch name {
	case "list", "set", "map", "tuple", "frozen", "vector":
	default:
		return cqltype.DataType{}, cqltype.TypeValidation("schema type", "unknown constructor %q", name)
	}
	p.offset++ // '<'
	var params []cqltype.DataType
	var dimension uint64
	for {
		if name == "vector" && len(params) == 1 {
			p.space()
			start := p.offset
			for p.offset < len(p.source) && p.source[p.offset] >= '0' && p.source[p.offset] <= '9' {
				p.offset++
			}
			d, err := strconv.ParseUint(p.source[start:p.offset], 10, 16)
			if err != nil || d == 0 {
				return cqltype.DataType{}, cqltype.TypeValidation("schema type", "invalid vector dimension")
			}
			dimension = d
		} else {
			t, err := p.build(depth + 1)
			if err != nil {
				return cqltype.DataType{}, err
			}
			params = append(params, t)
		}
		p.space()
		if p.offset >= len(p.source) {
			return cqltype.DataType{}, cqltype.TypeValidation("schema type", "unterminated parameters")
		}
		c := p.source[p.offset]
		p.offset++
		if c == '>' {
			break
		}
		if c != ',' {
			return cqltype.DataType{}, cqltype.TypeValidation("schema type", "invalid parameter separator")
		}
	}
	want := 1
	switch name {
	case "map":
		want = 2
	case "vector":
		want = 1 // the dimension is not a type parameter
	}
	if name != "tuple" && len(params) != want {
		return cqltype.DataType{}, cqltype.TypeValidation("schema type", "wrong arity for %q", name)
	}
	switch name {
	case "list":
		return cqltype.ListType(params[0])
	case "set":
		return cqltype.SetType(params[0])
	case "map":
		return cqltype.MapType(params[0], params[1])
	case "tuple":
		return cqltype.TupleType(params...)
	case "vector":
		return cqltype.VectorDataType(params[0], uint16(dimension))
	default: // frozen
		return params[0].WithFrozen(true), nil
	}
}

func (p *schemaTypeBuilder) userType(name string, depth int) (cqltype.DataType, error) {
	udt := p.ks.Types[name]
	if udt == nil {
		return cqltype.DataType{}, cqltype.TypeValidation("schema type", "unknown UDT %q", name)
	}
	if len(udt.FieldNames) != len(udt.FieldTypes) {
		return cqltype.DataType{}, cqltype.TypeValidation("schema type", "UDT %q has mismatched fields", name)
	}
	fields := make([]cqltype.ColumnSpec, len(udt.FieldNames))
	for i, fieldType := range udt.FieldTypes {
		field := schemaTypeBuilder{v2CQLTypeParser: v2CQLTypeParser{source: fieldType, keyspace: p.keyspace, maxDepth: p.maxDepth}, ks: p.ks}
		t, err := field.build(depth + 1)
		if err != nil {
			return cqltype.DataType{}, err
		}
		field.space()
		if field.offset != len(fieldType) {
			return cqltype.DataType{}, cqltype.TypeValidation("schema type", "trailing syntax in %q", fieldType)
		}
		fields[i] = cqltype.ColumnSpec{Name: udt.FieldNames[i], Type: t}
	}
	return cqltype.UDTType(p.keyspace, name, fields...)
}
