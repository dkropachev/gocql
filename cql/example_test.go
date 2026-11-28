// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql_test

import (
	"fmt"
	"log"

	"github.com/scylladb/gocql/v2/cql"
)

// A map<text, bigint> column round-trips through a Go map. Struct mappings
// pick GoMapCodec for a map[string]int64 field without configuration.
func ExampleGoMapCodec() {
	typ, err := cql.MapType(cql.TextType, cql.BigIntType)
	if err != nil {
		log.Fatal(err)
	}
	counts := cql.GoMapCodec(cql.StringCodec, cql.Int64Codec)
	encoded, err := cql.EncodeValue(typ, counts, map[string]int64{"views": 10, "likes": 3})
	if err != nil {
		log.Fatal(err)
	}
	decoded, err := cql.DecodeValue(counts, encoded)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(decoded["views"], decoded["likes"], len(decoded))
	// Output: 10 3 2
}

// Address maps the UDT address(street text, zip_code int) by field name, in
// snake_case unless tagged.
type Address struct {
	Street  string
	ZipCode int32
}

// A UDT maps to a struct with UDTCodecOf. Passed to WithCodec, the codec maps
// Address fields of an enclosing struct; an empty Keyspace matches the UDT
// in any keyspace.
func ExampleUDTCodecOf() {
	typ, err := cql.UDTType("app", "address",
		cql.ColumnSpec{Name: "street", Type: cql.TextType},
		cql.ColumnSpec{Name: "zip_code", Type: cql.IntType})
	if err != nil {
		log.Fatal(err)
	}
	address := cql.MustUDTCodecOf[Address](cql.UDTName("", "address"))
	encoded, err := cql.EncodeValue(typ, address, Address{Street: "Main St", ZipCode: 12345})
	if err != nil {
		log.Fatal(err)
	}
	decoded, err := cql.DecodeValue(address, encoded)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(decoded.Street, decoded.ZipCode)

	type User struct {
		Name string
		Home Address
	}
	users := cql.MustRowOf[User](cql.WithCodec(address))
	_ = users
	// Output: Main St 12345
}
