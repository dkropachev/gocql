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

	"log"
)

func Example_dynamicColumns() {
	ctx := context.Background()
	cfg, err := config.New("localhost:9042").Build()
	if err != nil {
		log.Fatal(err)
	}
	s, err := gocql.Connect(ctx, cfg, gocql.ScyllaV4)
	if err != nil {
		log.Fatal(err)
	}
	defer s.Close()
	p, err := s.Prepare(ctx, cql.Statement[cql.EmptyParams, cql.DynamicRow]{CQL: "SELECT * FROM example.table1", Rows: cql.DynamicRows})
	if err != nil {
		log.Fatal(err)
	}
	// The result columns are known once the statement is prepared.
	columns := p.Columns()
	for i := range columns.Len() {
		column, _ := columns.At(i)
		fmt.Println(column.Name, column.Type.Kind())
	}
	var row cql.DynamicRow
	_, err = p.Each(ctx, cql.EmptyParams{}, &row, func(row *cql.DynamicRow) error {
		for i := range columns.Len() {
			value, err := row.At(i)
			if err != nil {
				return err
			}
			if value.State() == cql.Null {
				fmt.Print("null ")
				continue
			}
			// Each accessor checks the value's type.
			switch value.Type().Kind() {
			case cql.TypeTinyInt, cql.TypeSmallInt, cql.TypeInt, cql.TypeBigInt:
				n, err := value.Signed()
				if err != nil {
					return err
				}
				fmt.Print(n, " ")
			case cql.TypeFloat, cql.TypeDouble:
				f, err := value.Float()
				if err != nil {
					return err
				}
				fmt.Print(f, " ")
			case cql.TypeASCII, cql.TypeText, cql.TypeVarchar:
				text, err := value.Text()
				if err != nil {
					return err
				}
				fmt.Print(text.String(), " ")
			default:
				fmt.Print(value.Type().Kind(), " ")
			}
		}
		fmt.Println()
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
}
