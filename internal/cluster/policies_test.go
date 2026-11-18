// Copyright (c) 2015 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

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
	"net"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/internal/endpoint"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
)

// tUUID returns a deterministic UUID for testing. Byte 0 is always set to
// a non-zero sentinel (0xFE) so that even tUUID(0) is distinguishable from
// the zero UUID, and the last two bytes encode n.
func tUUID(n int) cql.UUID {
	var u cql.UUID
	u[0] = 0xFE
	u[14] = byte(n >> 8)
	u[15] = byte(n)
	return u
}

func makeHostInfoListTestHosts(n int) []*endpoint.HostInfo {
	hosts := make([]*endpoint.HostInfo, n)
	for i := range hosts {
		hosts[i] = func() *endpoint.HostInfo {
			opaqueStorageValuexx := new(endpoint.HostInfo)
			implEndpoint.HostInfoData(opaqueStorageValuexx).InternalHostId = tUUID(i + 1)
			implEndpoint.HostInfoData(opaqueStorageValuexx).InternalConnectAddress = net.IPv4(10, 0, byte(i>>8), byte(i))
			implEndpoint.HostInfoData(opaqueStorageValuexx).InternalPort = 9042
			return opaqueStorageValuexx
		}(

		// expectHosts makes sure that the next len(hostIDs) returned from iter is a permutation of hostIDs.
		)

	}
	return hosts
}
