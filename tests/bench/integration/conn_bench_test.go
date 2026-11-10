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

	gocql "github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/cql"
	routing "github.com/scylladb/gocql/v2/routing"
)

func BenchmarkConn(b *testing.B) {
	b.Run("RoutingKey", func(b *testing.B) {
		const workers = 16
		ctx := context.Background()
		session := createV2Session(b, func(config *gocql.ConfigBuilder) {
			policy, err := routing.NewPolicy(routing.Options{TokenAware: true})
			if err != nil {
				b.Fatal(err)
			}
			config.WithConnectionsPerHost(1).ConfigureDefaultProfile(func(p *gocql.ProfileBuilder) { p.WithRouting(policy) })
		})
		defer session.Close()
		table := testTableName(b)
		if err := v2CreateTable(ctx, session, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id int primary key)", table)); err != nil {
			b.Fatal(err)
		}
		prepared, err := session.Prepare(ctx, cql.Statement[int32, cql.EmptyRow]{CQL: fmt.Sprintf("INSERT INTO %s (id) VALUES (?)", table), Params: cql.OneParam(cql.Int32Codec), Rows: cql.EmptyRowDecoder}, gocql.PrepareOptions{})
		if err != nil {
			b.Fatal(err)
		}
		var seed atomic.Uint64
		b.SetParallelism(workers)
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			worker := seed.Add(1)
			for i := uint64(0); pb.Next(); i++ {
				// Immutable Bind replaces rebinding one mutable query. Routing reads
				// the token computed from its canonical parameter bytes once.
				bound, err := BindBorrowed(prepared, int32(i*worker))
				if err != nil {
					b.Error(err)
					return
				}
				routing := bound.Routing()
				bound.Close()
				err = bound.Wait(ctx)
				if err != nil {
					b.Error(err)
					return
				}
				if !routing.Token.Set {
					b.Error("missing routing token")
					return
				}
			}
		})
	})

	b.Run("Stress", func(b *testing.B) {
		const workers = 16
		ctx := context.Background()
		session := createV2Session(b, func(config *gocql.ConfigBuilder) { config.WithConnectionsPerHost(1) })
		defer session.Close()
		table := testTableName(b)
		if err := v2CreateTable(ctx, session, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id int primary key)", table)); err != nil {
			b.Fatal(err)
		}
		prepared, err := session.Prepare(ctx, cql.Statement[int32, cql.EmptyRow]{CQL: fmt.Sprintf("INSERT INTO %s (id) VALUES (?)", table), Params: cql.OneParam(cql.Int32Codec), Rows: cql.EmptyRowDecoder}, gocql.PrepareOptions{})
		if err != nil {
			b.Fatal(err)
		}
		var seed atomic.Uint64
		b.SetParallelism(workers)
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			worker := seed.Add(1)
			for i := uint64(0); pb.Next(); i++ {
				bound, err := BindOwned(prepared, int32(i*worker))
				if err != nil {
					b.Error(err)
					return
				}
				_, err = Exec(ctx, session, bound, gocql.CompiledOptions{})
				bound.Close()
				waitErr := bound.Wait(ctx)
				if err != nil {
					b.Error(err)
					return
				}
				if waitErr != nil {
					b.Error(waitErr)
					return
				}
			}
		})
	})
}
