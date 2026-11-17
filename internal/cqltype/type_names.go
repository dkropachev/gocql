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

func AsVectorType(t TypeInfo) (VectorType, bool) {
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
	if innerVec, ok := AsVectorType(subType); ok {
		subType = innerVec
	}
	return VectorType{
		NativeType: NewCustomType(n.Version(), cql.TypeCustom, vectorTypePrefix),
		SubType:    subType,
		Dimensions: dim,
	}, true
}

func GetCassandraLongType(name string, protoVer byte, logger *slog.Logger) TypeInfo {
	logger = logging.Or(logger)
	const prefix = ApacheCassandraTypePrefix
	if strings.HasPrefix(name, prefix+"SetType") {
		return CollectionType{
			NativeType: NewNativeType(protoVer, cql.TypeSet),
			Elem:       GetCassandraLongType(UnwrapCompositeTypeDefinition(name, prefix+"SetType", '('), protoVer, logger),
		}
	} else if strings.HasPrefix(name, prefix+"ListType") {
		return CollectionType{
			NativeType: NewNativeType(protoVer, cql.TypeList),
			Elem:       GetCassandraLongType(UnwrapCompositeTypeDefinition(name, prefix+"ListType", '('), protoVer, logger),
		}
	} else if strings.HasPrefix(name, prefix+"MapType") {
		names := SplitJavaCompositeTypes(name, prefix+"MapType")
		if len(names) != 2 {
			logger.Warn("unparsable type definition", "type", name, "detail", fmt.Sprintf("map type, it has %d subelements, expecting 2", len(names)))
			return NewCustomType(protoVer, cql.TypeCustom, name)
		}
		return CollectionType{
			NativeType: NewNativeType(protoVer, cql.TypeMap),
			Key:        GetCassandraLongType(names[0], protoVer, logger),
			Elem:       GetCassandraLongType(names[1], protoVer, logger),
		}
	} else if strings.HasPrefix(name, prefix+"TupleType") {
		names := SplitJavaCompositeTypes(name, prefix+"TupleType")
		types := make([]TypeInfo, len(names))

		for i, name := range names {
			types[i] = GetCassandraLongType(name, protoVer, logger)
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
				Type: GetCassandraLongType(spec[1], protoVer, logger),
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
		subType := GetCassandraLongType(elem, protoVer, logger)
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
		return GetCassandraLongType(strings.TrimSpace(names[0]), protoVer, logger)
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
	var parts []string
	lessCount := 0
	segment := ""
	for _, char := range def {
		if char == ',' && lessCount == 0 {
			if segment != "" {
				parts = append(parts, strings.TrimSpace(segment))
			}
			segment = ""
			continue
		}
		segment += string(char)
		if char == typeOpen {
			lessCount++
		} else if char == typeClose {
			lessCount--
		}
	}
	if segment != "" {
		parts = append(parts, strings.TrimSpace(segment))
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
