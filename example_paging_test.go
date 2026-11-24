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
	"log"

	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/config"
	"github.com/scylladb/gocql/v2/cql"
)

// Example_paging fetches a table two rows at a time. Each RunPage call sends
// one request; the page's State resumes the next call, here through the text
// cursor a web API could hand to its client once it authenticates it (see
// cql.PageState).
func Example_paging() {
	ctx := context.Background()
	cfg, err := config.New(config.ScyllaV4, "localhost:9042").Build()
	if err != nil {
		log.Fatal(err)
	}
	s, err := gocql.Connect(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer s.Close()
	p, err := s.Prepare(ctx, cql.Statement[cql.EmptyParams, int32]{CQL: "SELECT id FROM example.itoa", Rows: cql.OneColumn(cql.Int32Codec)})
	if err != nil {
		log.Fatal(err)
	}
	var cursor []byte // empty: the first page
	for {
		var state cql.PageState
		if len(cursor) > 0 {
			if err := state.UnmarshalText(cursor); err != nil {
				log.Fatal(err)
			}
		}
		page, err := p.RunPage(ctx, cql.EmptyParams{}, state, 2)
		if err != nil {
			log.Fatal(err)
		}
		// All closes the page when the loop ends; its State stays readable.
		for id, err := range page.All(nil) {
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(*id)
		}
		if page.State().Complete() {
			break
		}
		if cursor, err = page.State().MarshalText(); err != nil {
			log.Fatal(err)
		}
	}
}
