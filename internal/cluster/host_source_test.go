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

package cluster

import (
	"github.com/scylladb/gocql/v2/internal/endpoint"

	"github.com/scylladb/gocql/v2/internal/tests/mock"
)

type trackingMockFramer struct {
	mock.MockFramer
	released bool
}

func (f *trackingMockFramer) Release() {
	f.released = true
}

type systemSchemaTestControl struct {
	iter *nativeResultFixture
}

func (*systemSchemaTestControl) InternalGetConn() *InternalConnHost               { return nil }
func (*systemSchemaTestControl) InternalAwaitSchemaAgreement() error              { return nil }
func (*systemSchemaTestControl) query(string, ...any) (iter *nativeResultFixture) { return nil }
func (c *systemSchemaTestControl) querySystem(string, ...any) (iter *nativeResultFixture) {
	return c.iter
}
func (*systemSchemaTestControl) InternalConnect([]*endpoint.HostInfo) error { return nil }
func (*systemSchemaTestControl) InternalClose()                             {}
func (*systemSchemaTestControl) InternalGetSession() *Core                  { return nil }
func (*systemSchemaTestControl) InternalReconnect() error                   { return nil }
