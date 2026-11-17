// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package metadata

import (
	"strconv"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/metadata"
)

var schemaScalarTypes = map[string]cql.Type{
	"ascii": cql.TypeAscii, "bigint": cql.TypeBigInt, "blob": cql.TypeBlob,
	"boolean": cql.TypeBoolean, "counter": cql.TypeCounter, "date": cql.TypeDate,
	"decimal": cql.TypeDecimal, "double": cql.TypeDouble, "duration": cql.TypeDuration,
	"float": cql.TypeFloat, "inet": cql.TypeInet, "int": cql.TypeInt,
	"smallint": cql.TypeSmallInt, "text": cql.TypeText, "time": cql.TypeTime,
	"timestamp": cql.TypeTimestamp, "timeuuid": cql.TypeTimeUUID, "tinyint": cql.TypeTinyInt,
	"uuid": cql.TypeUUID, "varchar": cql.TypeVarchar, "varint": cql.TypeVarint,
}

// InternalSchemaDataType builds the DataType of a schema type string, such as
// the type of a system_schema.columns row, resolving user types from ks.
// Custom (quoted class name) types are rejected.
func InternalSchemaDataType(ks *metadata.Keyspace, source string) (cql.DataType, error) {
	if ks == nil {
		return cql.DataType{}, cqltype.TypeValidation("schema type", "nil keyspace")
	}
	p := schemaTypeBuilder{v2CQLTypeParser: v2CQLTypeParser{source: source, keyspace: ks.Name, maxDepth: 64}, ks: ks}
	t, err := p.build(0)
	if err != nil {
		return cql.DataType{}, err
	}
	p.space()
	if p.offset != len(source) {
		return cql.DataType{}, cqltype.TypeValidation("schema type", "trailing syntax in %q", source)
	}
	return t, nil
}

type schemaTypeBuilder struct {
	ks *metadata.Keyspace
	v2CQLTypeParser
}

func (p *schemaTypeBuilder) build(depth int) (cql.DataType, error) {
	if depth >= p.maxDepth {
		return cql.DataType{}, cqltype.TypeValidation("schema type", "maximum nesting depth exceeded")
	}
	p.space()
	if p.offset < len(p.source) && p.source[p.offset] == '\'' {
		return cql.DataType{}, cqltype.TypeValidation("schema type", "custom type %q is not supported", p.source)
	}
	name, quoted, err := p.identifier()
	if err != nil {
		return cql.DataType{}, err
	}
	p.space()
	if p.offset < len(p.source) && p.source[p.offset] == '.' {
		if name != p.keyspace {
			return cql.DataType{}, cqltype.TypeValidation("schema type", "foreign keyspace %q", name)
		}
		p.offset++
		if name, _, err = p.identifier(); err != nil {
			return cql.DataType{}, err
		}
		quoted = true
		p.space()
	}
	if p.offset < len(p.source) && p.source[p.offset] == '<' {
		if quoted {
			return cql.DataType{}, cqltype.TypeValidation("schema type", "UDT cannot have parameters")
		}
		return p.constructor(name, depth)
	}
	if !quoted {
		if kind, ok := schemaScalarTypes[name]; ok {
			return cql.ScalarType(kind)
		}
	}
	return p.userType(name, depth)
}

func (p *schemaTypeBuilder) constructor(name string, depth int) (cql.DataType, error) {
	switch name {
	case "list", "set", "map", "tuple", "frozen", "vector":
	default:
		return cql.DataType{}, cqltype.TypeValidation("schema type", "unknown constructor %q", name)
	}
	p.offset++ // '<'
	var params []cql.DataType
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
				return cql.DataType{}, cqltype.TypeValidation("schema type", "invalid vector dimension")
			}
			dimension = d
		} else {
			t, err := p.build(depth + 1)
			if err != nil {
				return cql.DataType{}, err
			}
			params = append(params, t)
		}
		p.space()
		if p.offset >= len(p.source) {
			return cql.DataType{}, cqltype.TypeValidation("schema type", "unterminated parameters")
		}
		c := p.source[p.offset]
		p.offset++
		if c == '>' {
			break
		}
		if c != ',' {
			return cql.DataType{}, cqltype.TypeValidation("schema type", "invalid parameter separator")
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
		return cql.DataType{}, cqltype.TypeValidation("schema type", "wrong arity for %q", name)
	}
	switch name {
	case "list":
		return cql.ListType(params[0])
	case "set":
		return cql.SetType(params[0])
	case "map":
		return cql.MapType(params[0], params[1])
	case "tuple":
		return cql.TupleType(params...)
	case "vector":
		return cql.VectorType(params[0], uint16(dimension))
	default: // frozen
		return params[0].WithFrozen(true), nil
	}
}

func (p *schemaTypeBuilder) userType(name string, depth int) (cql.DataType, error) {
	udt := p.ks.Types[name]
	if udt == nil {
		return cql.DataType{}, cqltype.TypeValidation("schema type", "unknown UDT %q", name)
	}
	if len(udt.FieldNames) != len(udt.FieldTypes) {
		return cql.DataType{}, cqltype.TypeValidation("schema type", "UDT %q has mismatched fields", name)
	}
	fields := make([]cql.ColumnSpec, len(udt.FieldNames))
	for i, fieldType := range udt.FieldTypes {
		field := schemaTypeBuilder{v2CQLTypeParser: v2CQLTypeParser{source: fieldType, keyspace: p.keyspace, maxDepth: p.maxDepth}, ks: p.ks}
		t, err := field.build(depth + 1)
		if err != nil {
			return cql.DataType{}, err
		}
		field.space()
		if field.offset != len(fieldType) {
			return cql.DataType{}, cqltype.TypeValidation("schema type", "trailing syntax in %q", fieldType)
		}
		fields[i] = cql.ColumnSpec{Name: udt.FieldNames[i], Type: t}
	}
	return cql.UDTType(p.keyspace, name, fields...)
}
