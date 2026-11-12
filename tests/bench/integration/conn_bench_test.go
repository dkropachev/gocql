//go:build integration

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

package integration

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/scylladb/gocql/v2/config"
	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/exec"
	routing "github.com/scylladb/gocql/v2/routing"
)

func BenchmarkConn(b *testing.B) {
	b.Run("RoutingKey", func(b *testing.B) {
		const workers = 16
		ctx := context.Background()
		session := createV2Session(b, func(cfg *config.ConfigBuilder) {
			policy, err := routing.NewPolicy(routing.Options{TokenAware: true})
			if err != nil {
				b.Fatal(err)
			}
			cfg.WithConnectionsPerHost(1).ConfigureDefaultProfile(func(p *config.ProfileBuilder) { p.WithRouting(policy) })
		})
		defer session.Close()
		table := testTableName(b)
		if err := v2CreateTable(ctx, session, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id int primary key)", table)); err != nil {
			b.Fatal(err)
		}
		prepared, err := session.Prepare(ctx, cql.Statement[int32, cql.EmptyRow]{CQL: fmt.Sprintf("INSERT INTO %s (id) VALUES (?)", table), Params: cql.OneParam(cql.Int32Codec), Rows: cql.EmptyRowDecoder}, exec.PrepareOptions{})
		if err != nil {
			b.Fatal(err)
		}
		var seed atomic.Uint64
		b.SetParallelism(workers)
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			worker := seed.Add(1)
			// Each worker owns a reusable query. Routing encodes the parameter
			// and computes the token without executing the statement.
			query, err := prepared.Query(exec.Options{})
			if err != nil {
				b.Error(err)
				return
			}
			defer query.Close()
			for i := uint64(0); pb.Next(); i++ {
				info, err := query.Routing(int32(i * worker))
				if err != nil {
					b.Error(err)
					return
				}
				if !info.Token.Set {
					b.Error("missing routing token")
					return
				}
			}
		})
	})

	b.Run("Stress", func(b *testing.B) {
		const workers = 16
		ctx := context.Background()
		session := createV2Session(b, func(cfg *config.ConfigBuilder) { cfg.WithConnectionsPerHost(1) })
		defer session.Close()
		table := testTableName(b)
		if err := v2CreateTable(ctx, session, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id int primary key)", table)); err != nil {
			b.Fatal(err)
		}
		prepared, err := session.Prepare(ctx, cql.Statement[int32, cql.EmptyRow]{CQL: fmt.Sprintf("INSERT INTO %s (id) VALUES (?)", table), Params: cql.OneParam(cql.Int32Codec), Rows: cql.EmptyRowDecoder}, exec.PrepareOptions{})
		if err != nil {
			b.Fatal(err)
		}
		var seed atomic.Uint64
		b.SetParallelism(workers)
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			worker := seed.Add(1)
			query, err := prepared.Query(exec.Options{})
			if err != nil {
				b.Error(err)
				return
			}
			defer func() {
				query.Close()
				if err := query.Wait(ctx); err != nil {
					b.Error(err)
				}
			}()
			for i := uint64(0); pb.Next(); i++ {
				if _, err := query.Exec(ctx, int32(i*worker)); err != nil {
					b.Error(err)
					return
				}
			}
		})
	})
}
