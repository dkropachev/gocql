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
 * Copyright (c) 2016, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package gocql_test

import (
	"fmt"
	"log"

	"github.com/scylladb/gocql/v2/cql"
)

func Example_nulls() {
	// A Nullable[string] tells NULL from an empty string, as a struct field
	// of that type does; a string field would fail on NULL with cql.ErrNull.
	codec := cql.MustCodecOf[cql.Nullable[string]]()
	null, err := cql.NewEncodedValue(cql.TextType, cql.CellNull, nil)
	if err != nil {
		log.Fatal(err)
	}
	value, err := cql.DecodeValue(codec, null)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(value.Valid)
	empty, err := cql.NewEncodedValue(cql.TextType, cql.CellPresent, []byte{})
	if err != nil {
		log.Fatal(err)
	}
	value, err = cql.DecodeValue(codec, empty)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(value.Valid, value.Value == "")
	// Output:
	// false
	// true true
}
