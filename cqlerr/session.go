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
 * Copyright (c) 2012, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package cqlerr

import (
	"context"
	"errors"
)

// Session errors.
var (
	// ErrUseStmt reports a USE statement, which the driver does not run:
	// set the session keyspace in the configuration, or qualify table names.
	ErrUseStmt = errors.New("gocql: USE statements are not supported: set the session keyspace in the cluster config, or qualify table names")
	// ErrNoConnections reports that no connection in the pool could take
	// the request.
	ErrNoConnections = errors.New("gocql: no connections available")
	// ErrNoKeyspace reports a missing keyspace: an empty keyspace name, or
	// one the loaded metadata does not have.
	ErrNoKeyspace = errors.New("gocql: keyspace not found or not set")
	// ErrNoTable reports a missing table: an empty table name, or one the
	// loaded metadata does not have.
	ErrNoTable = errors.New("gocql: table not found or not set")
)

// ErrRequestTimeout reports that a request used up the time its profile or
// exec.Options gave it (Timeout, or PageTimeout for one page). It matches
// context.DeadlineExceeded, so errors.Is(err, context.DeadlineExceeded)
// holds for the driver's budget and for the caller's own deadline alike;
// errors.Is(err, ErrRequestTimeout) holds only for the driver's. KindOf
// reports KindTimeout.
var ErrRequestTimeout error = requestTimeoutError{}

// requestTimeoutError is ErrRequestTimeout's type: a comparable value, so
// that the sentinel matches by identity.
type requestTimeoutError struct{}

func (requestTimeoutError) Error() string { return "gocql: request timeout exceeded" }

// Is reports whether target is context.DeadlineExceeded.
func (requestTimeoutError) Is(target error) bool { return target == context.DeadlineExceeded }

// Timeout reports true, as context.DeadlineExceeded does.
func (requestTimeoutError) Timeout() bool { return true }

// Temporary reports true, as context.DeadlineExceeded does.
func (requestTimeoutError) Temporary() bool { return true }
