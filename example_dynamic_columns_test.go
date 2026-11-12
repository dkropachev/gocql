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
	"github.com/scylladb/gocql/v2/config"
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cql/dynamic"

	"log"
)

func Example_dynamicColumns() {
	ctx := context.Background()
	cfg, err := config.New("localhost:9042").Build()
	if err != nil {
		log.Fatal(err)
	}
	s, err := gocql.Connect[gocql.ScyllaV4](ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer s.Close()
	p, err := s.Prepare(ctx, cql.Statement[cql.EmptyParams, dynamic.Row]{CQL: "SELECT * FROM example.table1", Rows: dynamic.Rows})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(p.Columns())
	var row dynamic.Row
	_, err = p.Each(ctx, cql.EmptyParams{}, &row, func(row *dynamic.Row) error {
		fmt.Println(*row)
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
}
