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

// ExampleCASBatchRows consumes every typed row returned by a conditional batch.
func ExampleCASBatchRows() {
	// Assumes example.my_lwt_batch_table(pk text, ck text, version int, value text,
	// PRIMARY KEY(pk, ck)), with initial rows (pk1,ck1,1,a) and (pk1,ck2,1,A).
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
	type update struct {
		PK      string `cql:"pk"`
		CK      string `cql:"ck"`
		Value   string `cql:"value"`
		Version int32  `cql:"version"`
	}
	type previous struct {
		PK      string `cql:"pk"`
		CK      string `cql:"ck"`
		Version int32  `cql:"version"`
	}
	// The IF makes the statement conditional; the driver infers that.
	statement := cql.NewStatement("UPDATE example.my_lwt_batch_table SET value=? WHERE pk=? AND ck=? IF version=?",
		cql.MustParamsOf[update](), cql.CASRows(cql.MustRowOf[previous]()))
	queryPrepared, err := session.Prepare(ctx, statement)
	if err != nil {
		log.Fatal(err)
	}
	var batch gocql.Batch
	for _, value := range []update{
		{PK: "pk1", CK: "ck1", Value: "b", Version: 1},
		{PK: "pk1", CK: "ck2", Value: "B", Version: 0},
	} {
		if err := queryPrepared.AddToBatch(&batch, value); err != nil {
			log.Fatal(err)
		}
	}
	rows, err := session.RunConditionalBatch(ctx, &batch, statement.Rows)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	var value cql.CASResult[previous]
	for rows.NextInto(&value) {
		fmt.Println("applied:", value.Applied)
		if value.Previous.Set {
			fmt.Println(value.Previous.Value.CK, value.Previous.Value.Version)
		}
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
}

// ExampleSession_ApplyBatch reports whether a conditional batch applied,
// with nothing to close.
func ExampleSession_ApplyBatch() {
	// Assumes example.my_lwt_batch_table(pk text, ck text, version int, value text,
	// PRIMARY KEY(pk, ck)), with rows (pk1,ck1) and (pk1,ck2) at version 2.
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
	type update struct {
		PK      string `cql:"pk"`
		CK      string `cql:"ck"`
		Value   string `cql:"value"`
		Version int32  `cql:"version"`
	}
	statement := cql.NewStatement("UPDATE example.my_lwt_batch_table SET value=? WHERE pk=? AND ck=? IF version=?",
		cql.MustParamsOf[update](), cql.CASApplied)
	queryPrepared, err := session.Prepare(ctx, statement)
	if err != nil {
		log.Fatal(err)
	}
	var batch gocql.Batch
	for _, value := range []update{
		{PK: "pk1", CK: "ck1", Value: "c", Version: 2},
		{PK: "pk1", CK: "ck2", Value: "C", Version: 2},
	} {
		if err := queryPrepared.AddToBatch(&batch, value); err != nil {
			log.Fatal(err)
		}
	}
	applied, _, err := session.ApplyBatch(ctx, &batch)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("applied:", applied)
}
