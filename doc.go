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

// Package gocql implements the typed Cassandra and ScyllaDB native protocol API.
// Connect opens a validated config.Config as a Session of a Dialect, the database
// and native protocol version the caller connects to: ScyllaV4, CassandraV4
// or CassandraV5. The driver negotiates no version. Statements declare
// parameter encoders and row decoders. Session.Prepare creates shareable
// immutable plans.
//
// Prepared.Exec, One and Each execute a prepared statement once. They are safe
// for concurrent use and run on queries the statement keeps for them, so they
// allocate no per-call execution state. Each takes at most one CompiledOptions,
// made once by Session.CompileOptions; none uses the session's default profile.
// Exec and Each return (Result, error) and One returns (bool, Result, error),
// on Prepared and Query alike. One detaches borrowed fields before returning;
// Each lends them only until its callback returns. Returning StopIteration
// from Each stops successfully; other callback errors are returned. All is the
// range form of Each: a for-range loop over All yields every row, valid until
// the next iteration, and any error once; breaking out releases the execution.
// Prepared.Run returns Rows to iterate with NextInto and close, and
// Prepared.RunPage returns one Page from a saved cql.PageState with a page size
// argument; each handle holds a pooled query until it is closed.
//
// Prepared.Query creates a reusable query owned by one goroutine. It resolves
// its Options once and saves the pool hand-off of the one-shot calls (about 2
// to 4 percent of client CPU at high concurrency), and has the same calls. The
// page size cql.Unpaged requests the whole result in one response.
// Session.UnpreparedExec, UnpreparedOne, UnpreparedEach and UnpreparedAll run occasional CQL text statements without server preparation
// and without a query to close; prepare a statement that runs repeatedly. Parameters are
// copied during execution admission. A call while
// a Rows, Page or RowStream of the query is open, or while another goroutine's
// call runs, returns ErrBusy; a call after an execution that already returned
// waits until its accepted writes drain. Close stops a query and waits for its
// drain. Do not keep Query values in a sync.Pool: a query the pool drops stays
// open until it is closed.
//
// Prepared.AddToBatch, Query.AddToBatch and Session.UnpreparedAddToBatch
// append statements to a Batch; Session.ExecBatch sends it, and
// Session.RunBatch sends it and returns the Rows of a conditional batch.
//
// Session.PrepareStream prepares a stream.Statement directly for caller
// storage; its shareable PreparedStream creates a StreamQuery per worker. A
// StreamQuery decodes into seeded caller storage and buffers one response page
// on normal multiplexed connections by default. StreamOptions.Incremental opts into bounded
// incremental receive through an exclusive connection reservation.
// Page-buffered memory scales with concurrent response pages and is bounded by
// configured frame and decompression limits. Borrowed result views expire on
// advancement or cursor Close unless a PageLease retains their page.
// ByteSliceCodec fills reusable owned slices with RowInto.
//
// The driver logs through log/slog, to slog.Default() unless
// config.ConfigBuilder.WithLogger supplies another logger. Debug carries
// per-connection detail such as shard dialing; Info reports cluster changes
// such as nodes going up or down; Warn reports degraded but continuing
// operation; Error reports a subsystem that will not recover by itself.
// Context is in attributes (host, shard, keyspace, err, ...), and every record
// carries logger=gocql. Repeated Info-or-higher records with the same message
// and host are collapsed to one per 10 seconds with a suppressed count, and a
// panicking handler cannot break the driver. To log through a *log.Logger l
// at level lvl, use the standard library's text handler:
//
//	builder.WithLogger(slog.New(slog.NewTextHandler(l.Writer(), &slog.HandlerOptions{Level: lvl})))
package gocql
