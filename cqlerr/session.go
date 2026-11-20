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
	"errors"
)

// Session errors.
var (
	// ErrNotFound reports that a keyspace or table looked up in the schema
	// does not exist.
	ErrNotFound = errors.New("gocql: not found")
	// ErrUseStmt reports a USE statement, which the driver does not run:
	// set the session keyspace in the configuration, or qualify table names.
	ErrUseStmt = errors.New("gocql: USE statements are not supported: set the session keyspace in the cluster config, or qualify table names")
	// ErrSessionClosed reports use of a closed session.
	ErrSessionClosed = errors.New("gocql: session has been closed")
	// ErrNoConnections reports that no connection in the pool could take
	// the request.
	ErrNoConnections = errors.New("gocql: no hosts available in the pool")
	// ErrNoKeyspace reports a missing keyspace: an empty keyspace name, or
	// one the loaded metadata does not have.
	ErrNoKeyspace = errors.New("gocql: no keyspace provided")
	// ErrNoTable reports a missing table: an empty table name, or one the
	// loaded metadata does not have.
	ErrNoTable = errors.New("gocql: no table name provided")
	// ErrKeyspaceDoesNotExist reports that the server has no such keyspace.
	ErrKeyspaceDoesNotExist = errors.New("gocql: keyspace does not exist")
	// ErrSessionNotReady reports use of a session that has not finished
	// connecting.
	ErrSessionNotReady = errors.New("gocql: session is not ready yet")
)
