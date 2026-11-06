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
	"context"
	"fmt"

	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/cql"

	"log"
)

func Example_dynamicColumns() {
	ctx := context.Background()
	s, err := gocql.Connect(ctx, gocql.DefaultConfig("localhost:9042"))
	if err != nil {
		log.Fatal(err)
	}
	defer s.Close()
	b, err := gocql.NewUnpreparedQuery(s, cql.Statement[cql.EmptyParams, cql.DynamicRow]{CQL: "SELECT * FROM example.table1", Rows: cql.DynamicRows}, gocql.Options{})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = b.CloseAndWait(ctx) }()
	rows, err := b.Run(ctx, cql.EmptyParams{})
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	var row cql.DynamicRow
	for rows.NextInto(&row) {
		fmt.Println(rows.Columns(), row)
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
}
