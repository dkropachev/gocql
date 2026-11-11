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

package runtimeerrors

import (
	"errors"
)

var (
	ErrNotFound             = errors.New("gocql: not found")
	ErrUseStmt              = errors.New("gocql: USE statements are not supported: set the session keyspace in the cluster config, or qualify table names")
	ErrSessionClosed        = errors.New("gocql: session has been closed")
	ErrNoConnections        = errors.New("gocql: no hosts available in the pool")
	ErrNoKeyspace           = errors.New("gocql: no keyspace provided")
	ErrNoTable              = errors.New("gocql: no table name provided")
	ErrKeyspaceDoesNotExist = errors.New("gocql: keyspace does not exist")
	ErrTabletsNotUsed       = errors.New("gocql: tablets not used")
	ErrSessionNotReady      = errors.New("gocql: session is not ready yet")
)
