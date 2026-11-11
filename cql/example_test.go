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
