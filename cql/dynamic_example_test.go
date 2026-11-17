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
	first, err := cql.TextValue("a")
	if err != nil {
		log.Fatal(err)
	}
	second, err := cql.TextValue("b")
	if err != nil {
		log.Fatal(err)
	}
	list, err := cql.ListValue(typ, []cql.Value{first, second})
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
	elements, err := decoded.Elements()
	if err != nil {
		log.Fatal(err)
	}
	for elements.Next() {
		text, err := elements.Value().Text()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(text.String())
	}
	if err := elements.Err(); err != nil {
		log.Fatal(err)
	}
	// Output:
	// a
	// b
}
