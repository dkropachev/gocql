// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql_test

import (
	"fmt"
	"log"

	"github.com/scylladb/gocql/v2/cql"
)

// A list<text> value built from metadata round-trips through Codec.
func ExampleListValue() {
	typ, err := cql.ListType(cql.TextType)
	if err != nil {
		log.Fatal(err)
	}
	list, err := cql.ListValue(typ, []cql.Value{cql.TextValue("a"), cql.TextValue("b")})
	if err != nil {
		log.Fatal(err)
	}
	encoded, err := cql.EncodeValue(typ, cql.DynamicCodec, list)
	if err != nil {
		log.Fatal(err)
	}
	decoded, err := cql.DecodeValue(cql.DynamicCodec, encoded)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(decoded, decoded.Len())
	elements, err := decoded.Elements()
	if err != nil {
		log.Fatal(err)
	}
	for element, err := range elements.All() {
		if err != nil {
			log.Fatal(err)
		}
		text, err := element.Text()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(text)
	}
	// Output:
	// ['a', 'b'] 2
	// a
	// b
}
