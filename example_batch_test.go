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

// ExampleSession_ExecBatch builds a batch one statement at a time and sends it.
func ExampleSession_ExecBatch() {
	// Assumes example.batches(pk int, ck int, description text, PRIMARY KEY(pk, ck)).
	ctx := context.Background()
	cfg, err := config.New(config.ScyllaV4, "localhost:9042").WithKeyspace("example").Build()
	if err != nil {
		log.Fatal(err)
	}
	session, err := gocql.Connect(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()
	type item struct {
		PK          int32  `cql:"pk"`
		CK          int32  `cql:"ck"`
		Description string `cql:"description"`
	}
	// Writing the same values again is harmless, so the batch, all of whose
	// members are idempotent, may be retried and speculated.
	insert := cql.Write("INSERT INTO example.batches (pk, ck, description) VALUES (?, ?, ?)",
		cql.MustParamsOf[item]()).WithIdempotent(true)
	queryPrepared, err := session.Prepare(ctx, insert)
	if err != nil {
		log.Fatal(err)
	}
	// The members share partition 1, so the batch needs no batch log.
	batch := gocql.Batch{Kind: cql.UnloggedBatch}
	for _, value := range []item{
		{PK: 1, CK: 2, Description: "1.2"},
		{PK: 1, CK: 3, Description: "1.3"},
		{PK: 1, CK: 4, Description: "1.4"},
		{PK: 1, CK: 5, Description: "1.5"},
	} {
		if err := queryPrepared.AddToBatch(&batch, value); err != nil {
			log.Fatal(err)
		}
	}
	if _, err := session.ExecBatch(ctx, &batch); err != nil {
		log.Fatal(err)
	}
	var row item
	selectAll := cql.Select("SELECT pk, ck, description FROM example.batches", cql.NoParams, cql.MustRowOf[item]())
	_, err = session.UnpreparedEach(ctx, selectAll, cql.EmptyParams{}, &row, func(v *item) error {
		fmt.Println(v.PK, v.CK, v.Description)
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
}
