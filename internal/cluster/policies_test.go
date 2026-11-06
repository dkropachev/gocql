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
	"errors"
	"fmt"
	"net"
	"sort"
	"strings"
	"testing"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/endpoint"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/metadata"
)

// tUUID returns a deterministic UUID for testing. Byte 0 is always set to
// a non-zero sentinel (0xFE) so that even tUUID(0) is distinguishable from
// the zero UUID, and the last two bytes encode n.
func tUUID(n int) cqltype.UUID {
	var u cqltype.UUID
	u[0] = 0xFE
	u[14] = byte(n >> 8)
	u[15] = byte(n)
	return u
}

// tID returns the string representation of tUUID(n), suitable for passing to
// expectHosts and other string-based comparisons.
func tID(n int) string {
	return tUUID(n).String()
}

func createPolicy(keyspace string, shuffle bool) HostSelectionPolicy {
	policy := TokenAwareHostPolicy(RoundRobinHostPolicy())
	policyInternal := policy.(*InternalTokenAwareHostPolicy)
	InternalTokenAwareHostPolicyData(policyInternal).InternalGetKeyspaceName = func() string { return keyspace }
	InternalTokenAwareHostPolicyData(policyInternal).InternalGetKeyspaceMetadata = func(ks string) (*metadata.KeyspaceMetadata, error) {
		return nil, errors.New("not initalized")
	}
	policy.SetPartitioner("OrderedPartitioner")
	InternalTokenAwareHostPolicyData(policyInternal).InternalGetKeyspaceMetadata = func(keyspaceName string) (*metadata.KeyspaceMetadata, error) {
		if keyspaceName != keyspace {
			return nil, fmt.Errorf("unknown keyspace: %s", keyspaceName)
		}
		return &metadata.KeyspaceMetadata{
			Name:          keyspace,
			StrategyClass: "SimpleStrategy",
			StrategyOptions: map[string]any{
				"class":              "SimpleStrategy",
				"replication_factor": 2,
			},
		}, nil
	}
	InternalTokenAwareHostPolicyData(policyInternal).InternalShuffleReplicas = shuffle
	policy.KeyspaceChanged(KeyspaceUpdateEvent{Keyspace: keyspace})
	return policy
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

func expectHosts(t *testing.T, msg string, iter NextHost, hostIDs ...string) {
	t.Helper()

	expectedHostIDs := make(map[string]struct{}, len(hostIDs))
	for i := range hostIDs {
		expectedHostIDs[hostIDs[i]] = struct{}{}
	}

	expectedStr := func() string {
		keys := make([]string, 0, len(expectedHostIDs))
		for k := range expectedHostIDs {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		return strings.Join(keys, ", ")
	}

	for len(expectedHostIDs) > 0 {
		host := iter()
		if host == nil || host.Info() == nil {
			t.Fatalf("%s: expected hostID one of {%s}, but got nil", msg, expectedStr())
		}
		hostID := host.Info().HostID()
		if _, ok := expectedHostIDs[hostID]; !ok {
			t.Fatalf("%s: expected host ID one of {%s}, but got %s", msg, expectedStr(), hostID)
		}
		delete(expectedHostIDs, hostID)
	}
}

func expectNoMoreHosts(t *testing.T, iter NextHost) {
	t.Helper()
	host := iter()
	if host == nil {
		// success
		return
	}
	info := host.Info()
	if info == nil {
		t.Fatalf("expected no more hosts, but got host with nil Info()")
		return
	}
	t.Fatalf("expected no more hosts, but got %s", info.HostID())
}

type fixedInt64Partitioner int64

func (f fixedInt64Partitioner) Name() string               { return "FixedInt64Partitioner" }
func (f fixedInt64Partitioner) Hash([]byte) endpoint.Token { return endpoint.InternalInt64Token(f) }
func (f fixedInt64Partitioner) ParseString(s string) endpoint.Token {
	return endpoint.InternalParseInt64Token(s)
}
