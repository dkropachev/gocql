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
// allocate no per-call execution state. Every call takes at most one
// execopt.CompiledOptions, made once by Session.CompileOptions; none uses the
// session's default profile, or the defaults of a view made by Prepared.With,
// over which a call's options are laid field by field. Prepared.Apply runs a
// conditional statement and reports whether it applied.
// Exec and Each return (cql.Result, error) and One returns (bool, cql.Result, error),
// on Prepared and Worker alike. One detaches borrowed fields before returning;
// Each lends them only until its callback returns. Returning StopIteration
// from Each stops successfully; other callback errors are returned. All is the
// range form of Each: a for-range loop over All yields every row, valid until
// the next iteration, and any error once; breaking out releases the execution.
// Prepared.Run returns Rows to iterate with NextInto and close, and
// Prepared.RunPage returns one Page from a saved cql.PageState with a page size
// argument; each handle holds a pooled query until it is closed.
//
// Prepared.Worker creates a reusable execution owned by one goroutine, a
// Worker, with the statement's options: p.With(options).Worker() fixes others.
// It resolves its options once and saves the pool hand-off of
// the one-shot calls (about 2 to 4 percent of client CPU at high
// concurrency), and has the same calls. A call while a Rows, Page or
// RowStream of the worker is open, or while another goroutine's call runs,
// returns cqlerr.ErrBusy; a call after an execution that already returned
// waits until its accepted writes drain. Close stops a worker and returns at
// once; Wait joins its drain. Do not keep Worker values in a sync.Pool: a
// worker the pool drops stays open until it is closed. The page size
// cql.Unpaged requests the whole result in one response.
//
// Session.UnpreparedExec, UnpreparedOne, UnpreparedEach and UnpreparedAll run
// occasional CQL text statements without server preparation and without a
// worker to close; prepare a statement that runs repeatedly. Parameters are
// copied during execution admission. Session.UnpreparedCommand runs CQL text
// with nothing to bind and no result, such as DDL.
//
// Prepared.AddToBatch, Worker.AddToBatch and Session.UnpreparedAddToBatch
// append statements to a Batch; Session.ExecBatch sends it. A conditional
// batch is sent with Session.ApplyBatch, which reports whether it applied, or
// with Session.RunConditionalBatch, which returns its rows as CASBatchRows,
// whose Applied reports the same. Both reject a batch known to have no
// conditional member.
//
// Session.PrepareStream prepares a cql.StreamStatement directly for caller
// storage; its shareable PreparedStream creates a StreamWorker per goroutine.
// A StreamWorker decodes into seeded caller storage and buffers one response page
// on normal multiplexed connections by default. execopt.StreamOptions.Incremental opts into bounded
// incremental receive through an exclusive connection reservation.
// Page-buffered memory scales with concurrent response pages and is bounded by
// configured frame and decompression limits. Borrowed result views expire on
// advancement or cursor Close unless a PageLease retains their page.
// cql.ByteSliceCodec fills reusable owned slices with cql.RowInto.
//
// The driver logs through log/slog, to slog.Default() unless
// config.ConfigBuilder.WithLogger supplies another logger. Debug carries
// per-connection detail such as shard dialing; Info reports cluster changes
// such as nodes going up or down; Warn reports degraded but continuing
// operation; Error reports a subsystem that will not recover by itself.
// Context is in attributes, and every record carries logger=gocql. These
// attribute keys are stable: a later release keeps their names and meaning,
// so handlers and log queries may rely on them:
//
//	logger      always "gocql"
//	session     the session's ID, Session.ID
//	host        the address of the node
//	host_id     the node's host ID
//	dc          the node's datacenter
//	shard       the shard of a connection
//	keyspace    a keyspace
//	table       a table
//	stream      a connection's stream ID
//	err         the error that caused the record
//	suppressed  how many records like this one the rate limit dropped
//
// Messages and other attributes describe the event for a reader and may
// change. A record that one request causes, such as a panic in one of its
// observe.Hooks, is logged with the request's context, or the context its
// Request hook returned, so a handler can correlate it with the caller's
// trace. Repeated Info-or-higher records with the same message and host are
// collapsed to one per 10 seconds with a suppressed count
// (config.ConfigBuilder.WithLogRateLimit changes the window or turns it off),
// and a panicking handler cannot break the driver. To log through a
// *log.Logger l at level lvl, use the standard library's text handler:
//
//	builder.WithLogger(slog.New(slog.NewTextHandler(l.Writer(), &slog.HandlerOptions{Level: lvl})))
package gocql
