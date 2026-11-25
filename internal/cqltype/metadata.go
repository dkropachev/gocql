/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2012, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package cqltype

import (
	"bytes"
	"fmt"

	"github.com/scylladb/gocql/v2/cql"
)

type TypeInfo interface {
	Type() cql.Kind
	Version() byte
	Custom() string
}

type NativeType struct {
	//only used for KindCustom
	custom string
	typ    cql.Kind
	proto  byte
}

// legacyText is the protocol's text ID. Text and varchar are one CQL type,
// which the driver numbers cql.KindText (varchar's ID).
const legacyText cql.Kind = 0x000A

func wireKind(typ cql.Kind) cql.Kind {
	if typ == legacyText {
		return cql.KindText
	}
	return typ
}

func NewNativeType(proto byte, typ cql.Kind) NativeType {
	return NativeType{proto: proto, typ: wireKind(typ), custom: ""}
}

func NewCustomType(proto byte, typ cql.Kind, custom string) NativeType {
	return NativeType{proto: proto, typ: wireKind(typ), custom: custom}
}

func (t NativeType) Type() cql.Kind {
	return t.typ
}

func (t NativeType) Version() byte {
	return t.proto
}

func (t NativeType) Custom() string {
	return t.custom
}

func (t NativeType) String() string {
	switch t.typ {
	case cql.KindCustom:
		return fmt.Sprintf("%s(%s)", t.typ, t.custom)
	default:
		return t.typ.String()
	}
}

func NewCollectionType(m NativeType, key, elem TypeInfo) CollectionType {
	return CollectionType{
		NativeType: m,
		Key:        key,
		Elem:       elem,
	}
}

type CollectionType struct {
	// Key is used only for KindMap
	Key TypeInfo
	// Elem is used for KindMap, KindList and KindSet
	Elem TypeInfo
	NativeType
}

type VectorType struct {
	SubType TypeInfo
	NativeType
	Dimensions int
}

// Zero returns the zero value for the vector CQL type.
func (t CollectionType) String() string {
	switch t.typ {
	case cql.KindMap:
		return fmt.Sprintf("%s(%s, %s)", t.typ, t.Key, t.Elem)
	case cql.KindList, cql.KindSet:
		return fmt.Sprintf("%s(%s)", t.typ, t.Elem)
	case cql.KindCustom:
		return fmt.Sprintf("%s(%s)", t.typ, t.custom)
	default:
		return t.typ.String()
	}
}

type TupleTypeInfo struct {
	Elems []TypeInfo
	NativeType
}

func (t TupleTypeInfo) String() string {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("%s(", t.typ))
	for _, elem := range t.Elems {
		buf.WriteString(fmt.Sprintf("%s, ", elem))
	}
	buf.Truncate(buf.Len() - 2)
	buf.WriteByte(')')
	return buf.String()
}

type UDTField struct {
	Type TypeInfo
	Name string
}

func NewUDTType(proto byte, name, keySpace string, elems ...UDTField) UDTTypeInfo {
	return UDTTypeInfo{
		NativeType: NativeType{proto: proto, typ: cql.KindUDT, custom: ""},
		Name:       name,
		KeySpace:   keySpace,
		Elements:   elems,
	}
}

type UDTTypeInfo struct {
	KeySpace string
	Name     string
	Elements []UDTField
	NativeType
}

func (t UDTTypeInfo) String() string {
	buf := &bytes.Buffer{}

	fmt.Fprintf(buf, "%s.%s{", t.KeySpace, t.Name)
	first := true
	for _, e := range t.Elements {
		if !first {
			fmt.Fprint(buf, ",")
		} else {
			first = false
		}

		fmt.Fprintf(buf, "%s=%v", e.Name, e.Type)
	}
	fmt.Fprint(buf, "}")

	return buf.String()
}
