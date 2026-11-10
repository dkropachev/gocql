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
	"github.com/scylladb/gocql/v2/cql"
)

// ExampleSession_RunBatch_conditional consumes every typed row returned by a conditional batch.
func ExampleSession_RunBatch_conditional() {
	// Assumes example.my_lwt_batch_table(pk text, ck text, version int, value text,
	// PRIMARY KEY(pk, ck)), with initial rows (pk1,ck1,1,a) and (pk1,ck2,1,A).
	ctx := context.Background()
	config, err := gocql.NewConfig("localhost:9042").WithKeyspace("example").Build()
	if err != nil {
		log.Fatal(err)
	}
	session, err := gocql.Connect(ctx, config)
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()
	type update struct {
		PK, CK, Value string
		Version       int32
	}
	type previous struct {
		PK, CK  string
		Version int32
	}
	statement := cql.Statement[update, cql.CASResult[previous]]{
		CQL: "UPDATE example.my_lwt_batch_table SET value=? WHERE pk=? AND ck=? IF version=?",
		Params: cql.Params(
			cql.Param("value", cql.StringCodec, func(v update) string { return v.Value }),
			cql.Param("pk", cql.StringCodec, func(v update) string { return v.PK }),
			cql.Param("ck", cql.StringCodec, func(v update) string { return v.CK }),
			cql.Param("version", cql.Int32Codec, func(v update) int32 { return v.Version }),
		),
		Rows: cql.CASRows(cql.Row(
			cql.Column("pk", cql.StringCodec, func(v *previous) *string { return &v.PK }),
			cql.Column("ck", cql.StringCodec, func(v *previous) *string { return &v.CK }),
			cql.Column("version", cql.Int32Codec, func(v *previous) *int32 { return &v.Version }),
		)),
	}
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
	page, err := session.RunBatch(ctx, cql.LoggedBatch, &batch, statement.Rows)
	if err != nil {
		log.Fatal(err)
	}
	defer page.Close()
	var value cql.CASResult[previous]
	for page.NextInto(&value) {
		fmt.Println("applied:", value.Applied)
		if value.Previous.Set {
			fmt.Println(value.Previous.Value.CK, value.Previous.Value.Version)
		}
	}
	if err := page.Err(); err != nil {
		log.Fatal(err)
	}
}
