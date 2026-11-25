// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import (
	"strconv"
	"strings"
)

// maxParsedTypeDepth bounds the nesting ParseDataType accepts.
const maxParsedTypeDepth = 64

// ParseDataType parses a CQL type as a schema or a CREATE TABLE statement
// writes it, such as "int", "map<text, frozen<list<int>>>",
// "tuple<int, text>" or "vector<float, 3>". Names are not case-sensitive,
// varchar is text, and frozen<...> sets the frozen annotation. A user-defined
// type needs its fields, which a name does not give, so ParseDataType
// rejects one; build it with UDTType or read it from metadata. It fails with
// a ValidationError.
func ParseDataType(s string) (DataType, error) {
	p := typeParser{source: s}
	t, err := p.parse(0)
	if err != nil {
		return DataType{}, err
	}
	p.space()
	if p.offset != len(p.source) {
		return DataType{}, p.fail("trailing text")
	}
	return t, nil
}

// MustParseDataType is ParseDataType that panics on error, for
// package-level variables.
func MustParseDataType(s string) DataType { return must(ParseDataType(s)) }

var parsedScalarKinds = map[string]Type{
	"ascii": TypeASCII, "bigint": TypeBigInt, "blob": TypeBlob, "boolean": TypeBoolean,
	"counter": TypeCounter, "date": TypeDate, "decimal": TypeDecimal, "double": TypeDouble,
	"duration": TypeDuration, "float": TypeFloat, "inet": TypeInet, "int": TypeInt,
	"smallint": TypeSmallInt, "text": TypeText, "varchar": TypeText, "time": TypeTime,
	"timestamp": TypeTimestamp, "timeuuid": TypeTimeUUID, "tinyint": TypeTinyInt,
	"uuid": TypeUUID, "varint": TypeVarint,
}

type typeParser struct {
	source string
	offset int
}

func (p *typeParser) fail(format string, args ...any) error {
	return typeValidation("type", "%q at offset %d: "+format, append([]any{p.source, p.offset}, args...)...)
}

func (p *typeParser) space() {
	for p.offset < len(p.source) && strings.IndexByte(" \t\r\n", p.source[p.offset]) >= 0 {
		p.offset++
	}
}

func (p *typeParser) name() string {
	p.space()
	start := p.offset
	for p.offset < len(p.source) {
		c := p.source[p.offset]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' {
			p.offset++
			continue
		}
		break
	}
	return strings.ToLower(p.source[start:p.offset])
}

// expect consumes c after optional space and reports whether it was there.
func (p *typeParser) expect(c byte) bool {
	p.space()
	if p.offset < len(p.source) && p.source[p.offset] == c {
		p.offset++
		return true
	}
	return false
}

func (p *typeParser) parse(depth int) (DataType, error) {
	if depth > maxParsedTypeDepth {
		return DataType{}, p.fail("nesting deeper than %d", maxParsedTypeDepth)
	}
	name := p.name()
	if name == "" {
		return DataType{}, p.fail("expected a type")
	}
	if kind, ok := parsedScalarKinds[name]; ok {
		return scalarTypes[kind], nil
	}
	switch name {
	case "list", "set", "map", "tuple", "frozen", "vector":
	default:
		return DataType{}, p.fail("unknown type %q; a user-defined type needs UDTType", name)
	}
	if !p.expect('<') {
		return DataType{}, p.fail("expected < after %s", name)
	}
	var params []DataType
	var dimension uint64
	for {
		if name == "vector" && dimension != 0 {
			return DataType{}, p.fail("vector takes a type and a dimension")
		}
		if name == "vector" && len(params) == 1 {
			p.space()
			start := p.offset
			for p.offset < len(p.source) && p.source[p.offset] >= '0' && p.source[p.offset] <= '9' {
				p.offset++
			}
			d, err := strconv.ParseUint(p.source[start:p.offset], 10, 16)
			if err != nil || d == 0 {
				return DataType{}, p.fail("invalid vector dimension")
			}
			dimension = d
		} else {
			t, err := p.parse(depth + 1)
			if err != nil {
				return DataType{}, err
			}
			params = append(params, t)
		}
		if p.expect('>') {
			break
		}
		if !p.expect(',') {
			return DataType{}, p.fail("expected , or >")
		}
	}
	arity := func(n int) error {
		if len(params) != n {
			return p.fail("%s takes %d types, got %d", name, n, len(params))
		}
		return nil
	}
	switch name {
	case "list", "set", "frozen":
		if err := arity(1); err != nil {
			return DataType{}, err
		}
		switch name {
		case "list":
			return ListType(params[0])
		case "set":
			return SetType(params[0])
		}
		return params[0].WithFrozen(true), nil
	case "map":
		if err := arity(2); err != nil {
			return DataType{}, err
		}
		return MapType(params[0], params[1])
	case "vector":
		if dimension == 0 || len(params) != 1 {
			return DataType{}, p.fail("vector takes a type and a dimension")
		}
		return VectorType(params[0], uint16(dimension))
	default:
		return TupleType(params...)
	}
}
