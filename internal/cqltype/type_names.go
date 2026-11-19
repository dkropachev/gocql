// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cqltype

import (
	"encoding/hex"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/logging"
)

func AsVectorType(t TypeInfo) (VectorType, bool) { return asVectorType(t, 0) }

func asVectorType(t TypeInfo, depth int) (VectorType, bool) {
	if v, ok := t.(VectorType); ok {
		return v, true
	}
	n, ok := t.(NativeType)
	if !ok || n.Type() != cql.TypeCustom {
		return VectorType{}, false
	}
	const vectorTypePrefix = ApacheCassandraTypePrefix + "VectorType"
	if !strings.HasPrefix(n.Custom(), vectorTypePrefix+"(") {
		return VectorType{}, false
	}

	spec := strings.TrimPrefix(n.Custom(), vectorTypePrefix)
	spec = strings.Trim(spec, "()")
	// split last comma -> subtype spec , dimensions
	idx := strings.LastIndex(spec, ",")
	if idx <= 0 {
		return VectorType{}, false
	}
	subStr := strings.TrimSpace(spec[:idx])
	dimStr := strings.TrimSpace(spec[idx+1:])
	dim, err := strconv.Atoi(dimStr)
	if err != nil || dim < 1 {
		return VectorType{}, false
	}
	subType := GetCassandraLongType(subStr, n.Version(), logging.Nop())
	// recurse if subtype itself is still a custom vector
	// A subtype nested too deeply is left custom instead of being parsed
	// again, which would repeat the work once per MaxLongTypeDepth levels.
	if depth < MaxLongTypeDepth {
		if innerVec, ok := asVectorType(subType, depth+1); ok {
			subType = innerVec
		}
	}
	return VectorType{
		NativeType: NewCustomType(n.Version(), cql.TypeCustom, vectorTypePrefix),
		SubType:    subType,
		Dimensions: dim,
	}, true
}

// MaxLongTypeDepth bounds how deeply GetCassandraLongType nests composite
// types, matching the default metadata depth limit. The names come from the
// server, so without it a hostile name nests until the stack overflows, and
// every level rescans the rest of the name.
const MaxLongTypeDepth = 64

// GetCassandraLongType parses a Java marshal class name, such as
// org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type).
// A name it cannot parse, or one nested deeper than MaxLongTypeDepth, is
// returned as a custom type and logged.
func GetCassandraLongType(name string, protoVer byte, logger *slog.Logger) TypeInfo {
	return getCassandraLongType(name, protoVer, logging.Or(logger), 0)
}

func getCassandraLongType(name string, protoVer byte, logger *slog.Logger, depth int) TypeInfo {
	const prefix = ApacheCassandraTypePrefix
	if depth > MaxLongTypeDepth {
		logger.Warn("unparsable type definition", "detail", fmt.Sprintf("type nesting exceeds %d", MaxLongTypeDepth))
		return NewCustomType(protoVer, cql.TypeCustom, name)
	}
	depth++
	if strings.HasPrefix(name, prefix+"SetType") {
		return CollectionType{
			NativeType: NewNativeType(protoVer, cql.TypeSet),
			Elem:       getCassandraLongType(UnwrapCompositeTypeDefinition(name, prefix+"SetType", '('), protoVer, logger, depth),
		}
	} else if strings.HasPrefix(name, prefix+"ListType") {
		return CollectionType{
			NativeType: NewNativeType(protoVer, cql.TypeList),
			Elem:       getCassandraLongType(UnwrapCompositeTypeDefinition(name, prefix+"ListType", '('), protoVer, logger, depth),
		}
	} else if strings.HasPrefix(name, prefix+"MapType") {
		names := SplitJavaCompositeTypes(name, prefix+"MapType")
		if len(names) != 2 {
			logger.Warn("unparsable type definition", "type", name, "detail", fmt.Sprintf("map type, it has %d subelements, expecting 2", len(names)))
			return NewCustomType(protoVer, cql.TypeCustom, name)
		}
		return CollectionType{
			NativeType: NewNativeType(protoVer, cql.TypeMap),
			Key:        getCassandraLongType(names[0], protoVer, logger, depth),
			Elem:       getCassandraLongType(names[1], protoVer, logger, depth),
		}
	} else if strings.HasPrefix(name, prefix+"TupleType") {
		names := SplitJavaCompositeTypes(name, prefix+"TupleType")
		types := make([]TypeInfo, len(names))

		for i, name := range names {
			types[i] = getCassandraLongType(name, protoVer, logger, depth)
		}

		return TupleTypeInfo{
			NativeType: NewNativeType(protoVer, cql.TypeTuple),
			Elems:      types,
		}
	} else if strings.HasPrefix(name, prefix+"UserType") {
		names := SplitJavaCompositeTypes(name, prefix+"UserType")
		// Without keyspace and type name, make() below gets a negative length.
		if len(names) < 2 {
			logger.Warn("unparsable type definition", "type", name, "detail", fmt.Sprintf("udt type, it has %d subelements, expecting at least 2", len(names)))
			return NewCustomType(protoVer, cql.TypeCustom, name)
		}
		fields := make([]UDTField, len(names)-2)

		for i := 2; i < len(names); i++ {
			spec := strings.Split(names[i], ":")
			if len(spec) < 2 {
				logger.Warn("unparsable type definition", "type", name, "detail", fmt.Sprintf("udt field %q, expecting <name>:<type>", names[i]))
				return NewCustomType(protoVer, cql.TypeCustom, name)
			}
			fieldName, _ := hex.DecodeString(spec[0])
			fields[i-2] = UDTField{
				Name: string(fieldName),
				Type: getCassandraLongType(spec[1], protoVer, logger, depth),
			}
		}

		udtName, _ := hex.DecodeString(names[1])
		return UDTTypeInfo{
			NativeType: NewNativeType(protoVer, cql.TypeUDT),
			KeySpace:   names[0],
			Name:       string(udtName),
			Elements:   fields,
		}
	} else if strings.HasPrefix(name, prefix+"VectorType") {
		names := SplitJavaCompositeTypes(name, prefix+"VectorType")
		// Without the element type and the dimensions, indexing names below panics.
		if len(names) != 2 {
			logger.Warn("unparsable type definition", "type", name, "detail", fmt.Sprintf("vector type, it has %d subelements, expecting 2", len(names)))
			return NewCustomType(protoVer, cql.TypeCustom, name)
		}
		// A nested vector arrives here, not readVectorTypeInfo, which validates only
		// the outer spec, so both of its guards are repeated below.
		elem := strings.TrimSpace(names[0])
		if elem == "" {
			// Without this the element resolves to a custom type with no name at all,
			// while the other two parsers of this grammar both reject it.
			logger.Warn("unparsable type definition", "type", name, "detail", fmt.Sprintf("vector type %q: missing element type", name))
			return NewCustomType(protoVer, cql.TypeCustom, name)
		}
		subType := getCassandraLongType(elem, protoVer, logger, depth)
		// A negative dimension reaches unmarshalVector's MakeSlice.
		dim, err := strconv.Atoi(strings.TrimSpace(names[1]))
		if err != nil {
			logger.Warn("unparsable type definition", "type", name, "detail", fmt.Sprintf("vector dimensions %q: %v", names[1], err))
			return NewCustomType(protoVer, cql.TypeCustom, name)
		}
		if dim < 1 {
			// Separate arm: err is nil here, so %v would log "<nil>".
			logger.Warn("unparsable type definition", "type", name, "detail", fmt.Sprintf("vector dimensions %q: expecting a positive value", names[1]))
			return NewCustomType(protoVer, cql.TypeCustom, name)
		}

		return VectorType{
			NativeType: NewCustomType(protoVer, cql.TypeCustom, prefix+"VectorType"),
			SubType:    subType,
			Dimensions: dim,
		}
	} else if strings.HasPrefix(name, prefix+"FrozenType") {
		names := SplitJavaCompositeTypes(name, prefix+"FrozenType")
		return getCassandraLongType(strings.TrimSpace(names[0]), protoVer, logger, depth)
	} else {
		// basic type
		typ := GetApacheCassandraType(name)
		if typ == cql.TypeCustom {
			// Keep the name so an error can say what the server sent, not "custom()".
			return NewCustomType(protoVer, cql.TypeCustom, name)
		}
		return NativeType{
			proto: protoVer,
			typ:   typ,
		}
	}
}

func SplitJavaCompositeTypes(name string, typeName string) []string {
	return SplitCompositeTypes(name, typeName, '(', ')')
}

func UnwrapCompositeTypeDefinition(name string, typeName string, typeOpen int32) string {
	return strings.TrimPrefix(name[:len(name)-1], typeName+string(typeOpen))
}

func SplitCompositeTypes(name string, typeName string, typeOpen int32, typeClose int32) []string {
	def := UnwrapCompositeTypeDefinition(name, typeName, typeOpen)
	if !strings.Contains(def, string(typeOpen)) {
		parts := strings.Split(def, ",")
		for i := range parts {
			parts[i] = strings.TrimSpace(parts[i])
		}
		return parts
	}
	// Segments are slices of def, so splitting is linear in its length: the
	// name comes from the server, and copying a segment one character at a
	// time made a long name quadratic.
	var parts []string
	lessCount := 0
	start := 0
	for i, char := range def {
		switch {
		case char == ',' && lessCount == 0:
			if i > start {
				parts = append(parts, strings.TrimSpace(def[start:i]))
			}
			start = i + 1
		case char == typeOpen:
			lessCount++
		case char == typeClose:
			lessCount--
		}
	}
	if start < len(def) {
		parts = append(parts, strings.TrimSpace(def[start:]))
	}
	return parts
}

func GetApacheCassandraType(class string) cql.Type {
	switch strings.TrimPrefix(class, ApacheCassandraTypePrefix) {
	case "AsciiType":
		return cql.TypeAscii
	case "LongType":
		return cql.TypeBigInt
	case "BytesType":
		return cql.TypeBlob
	case "BooleanType":
		return cql.TypeBoolean
	case "CounterColumnType":
		return cql.TypeCounter
	case "DecimalType":
		return cql.TypeDecimal
	case "DoubleType":
		return cql.TypeDouble
	case "FloatType":
		return cql.TypeFloat
	case "Int32Type":
		return cql.TypeInt
	case "ShortType":
		return cql.TypeSmallInt
	case "ByteType":
		return cql.TypeTinyInt
	case "TimeType":
		return cql.TypeTime
	case "DateType", "TimestampType":
		return cql.TypeTimestamp
	case "UUIDType", "LexicalUUIDType":
		return cql.TypeUUID
	case "UTF8Type":
		return cql.TypeVarchar
	case "IntegerType":
		return cql.TypeVarint
	case "TimeUUIDType":
		return cql.TypeTimeUUID
	case "InetAddressType":
		return cql.TypeInet
	case "MapType":
		return cql.TypeMap
	case "ListType":
		return cql.TypeList
	case "SetType":
		return cql.TypeSet
	case "TupleType":
		return cql.TypeTuple
	case "DurationType":
		return cql.TypeDuration
	case "SimpleDateType":
		return cql.TypeDate
	case "UserType":
		return cql.TypeUDT
	default:
		return cql.TypeCustom
	}
}

const ApacheCassandraTypePrefix = "org.apache.cassandra.db.marshal."
