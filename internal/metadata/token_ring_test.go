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

package metadata

import (
	"sort"
	"testing"

	"github.com/scylladb/gocql/v2/internal/endpoint"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
)

func TestTokenRing(t *testing.T) {
	t.Parallel()
	// Test of the token ring implementation based on example at the start of this
	// page of documentation:
	// http://www.datastax.com/docs/0.8/cluster_architecture/partitioning
	t.Run("Int", func(t *testing.T) {
		t.Parallel()

		host0 := func() *endpoint.HostInfo {
			opaqueStorageValue := new(endpoint.HostInfo)
			return opaqueStorageValue
		}()

		host25 := func() *endpoint.HostInfo {
			opaqueStorageValuex := new(endpoint.HostInfo)
			return opaqueStorageValuex
		}()

		host50 := func() *endpoint.HostInfo {
			opaqueStorageValuexx := new(endpoint.HostInfo)
			return opaqueStorageValuexx
		}()

		host75 := func() *endpoint.HostInfo {
			opaqueStorageValuexxx := new(endpoint.HostInfo)
			return opaqueStorageValuexxx
		}()

		ring := &endpoint.InternalTokenRing{
			InternalPartitioner: nil,
			// these tokens and hosts are out of order to test sorting
			InternalTokens: []endpoint.InternalHostToken{
				{InternalToken: intToken(0), InternalHost: host0},
				{InternalToken: intToken(50), InternalHost: host50},
				{InternalToken: intToken(75), InternalHost: host75},
				{InternalToken: intToken(25), InternalHost: host25},
			},
		}

		sort.Sort(ring)

		if host, endToken := ring.GetHostForToken(intToken(0)); host != host0 || endToken != intToken(0) {
			t.Error("Expected host 0 for token 0")
		}
		if host, endToken := ring.GetHostForToken(intToken(1)); host != host25 || endToken != intToken(25) {
			t.Error("Expected host 25 for token 1")
		}
		if host, endToken := ring.GetHostForToken(intToken(24)); host != host25 || endToken != intToken(25) {
			t.Error("Expected host 25 for token 24")
		}
		if host, endToken := ring.GetHostForToken(intToken(25)); host != host25 || endToken != intToken(25) {
			t.Error("Expected host 25 for token 25")
		}
		if host, endToken := ring.GetHostForToken(intToken(26)); host != host50 || endToken != intToken(50) {
			t.Error("Expected host 50 for token 26")
		}
		if host, endToken := ring.GetHostForToken(intToken(49)); host != host50 || endToken != intToken(50) {
			t.Error("Expected host 50 for token 49")
		}
		if host, endToken := ring.GetHostForToken(intToken(50)); host != host50 || endToken != intToken(50) {
			t.Error("Expected host 50 for token 50")
		}
		if host, endToken := ring.GetHostForToken(intToken(51)); host != host75 || endToken != intToken(75) {
			t.Error("Expected host 75 for token 51")
		}
		if host, endToken := ring.GetHostForToken(intToken(74)); host != host75 || endToken != intToken(75) {
			t.Error("Expected host 75 for token 74")
		}
		if host, endToken := ring.GetHostForToken(intToken(75)); host != host75 || endToken != intToken(75) {
			t.Error("Expected host 75 for token 75")
		}
		if host, endToken := ring.GetHostForToken(intToken(76)); host != host0 || endToken != intToken(0) {
			t.Error("Expected host 0 for token 76")
		}
		if host, endToken := ring.GetHostForToken(intToken(99)); host != host0 || endToken != intToken(0) {
			t.Error("Expected host 0 for token 99")
		}
		if host, endToken := ring.GetHostForToken(intToken(100)); host != host0 || endToken != intToken(0) {
			t.Error("Expected host 0 for token 100")
		}
	})
	// Test of the tokenRing with the Murmur3Partitioner
	t.Run("Murmur3", func(t *testing.T) {
		t.Parallel()

		// Note, strings are parsed directly to int64, they are not murmur3 hashed
		hosts := hostsForTests(4)
		ring, err := endpoint.InternalNewTokenRing("Murmur3Partitioner", hosts)
		if err != nil {
			t.Fatalf("Failed to create token ring due to error: %v", err)
		}

		p := endpoint.InternalMurmur3Partitioner{}

		for _, host := range hosts {
			actual, _ := ring.GetHostForToken(p.ParseString(implEndpoint.HostInfoData(host).InternalTokens[0]))
			if !actual.ConnectAddress().Equal(host.ConnectAddress()) {
				t.Errorf("Expected address %v for token %q, but was %v", host.ConnectAddress(), implEndpoint.HostInfoData(host).InternalTokens[0], actual.ConnectAddress())
			}
		}

		actual, _ := ring.GetHostForToken(p.ParseString("12"))
		if !actual.ConnectAddress().Equal(hosts[1].ConnectAddress()) {
			t.Errorf("Expected address 1 for token \"12\", but was %s", actual.ConnectAddress())
		}

		actual, _ = ring.GetHostForToken(p.ParseString("24324545443332"))
		if !actual.ConnectAddress().Equal(hosts[0].ConnectAddress()) {
			t.Errorf("Expected address 0 for token \"24324545443332\", but was %s", actual.ConnectAddress())
		}
	})
	// Test for the behavior of a nil pointer to tokenRing
	t.Run("Nil", func(t *testing.T) {
		t.Parallel()

		var ring *endpoint.InternalTokenRing = nil

		if host, endToken := ring.GetHostForToken(nil); host != nil || endToken != nil {
			t.Error("Expected nil for nil token ring")
		}
		if host, token, endToken := ring.GetHostForPartitionKey(nil); host != nil || token != nil || endToken != nil {
			t.Error("Expected nil for nil token ring")
		}
	})
	// Test of the tokenRing with the OrderedPartitioner
	t.Run("Ordered", func(t *testing.T) {
		t.Parallel()

		// Tokens here more or less are similar layout to the int tokens above due
		// to each numeric character translating to a consistently offset byte.
		hosts := hostsForTests(4)
		ring, err := endpoint.InternalNewTokenRing("OrderedPartitioner", hosts)
		if err != nil {
			t.Fatalf("Failed to create token ring due to error: %v", err)
		}

		p := endpoint.InternalOrderedPartitioner{}

		var actual *endpoint.HostInfo
		for _, host := range hosts {
			actual, _ := ring.GetHostForToken(p.ParseString(implEndpoint.HostInfoData(host).InternalTokens[0]))
			if !actual.ConnectAddress().Equal(host.ConnectAddress()) {
				t.Errorf("Expected address %v for token %q, but was %v", host.ConnectAddress(), implEndpoint.HostInfoData(host).InternalTokens[0], actual.ConnectAddress())
			}
		}

		actual, _ = ring.GetHostForToken(p.ParseString("12"))
		if !implEndpoint.HostInfoData(actual).InternalPeer.Equal(implEndpoint.HostInfoData(hosts[1]).InternalPeer) {
			t.Errorf("Expected address 1 for token \"12\", but was %s", actual.ConnectAddress())
		}

		actual, _ = ring.GetHostForToken(p.ParseString("24324545443332"))
		if !actual.ConnectAddress().Equal(hosts[1].ConnectAddress()) {
			t.Errorf("Expected address 1 for token \"24324545443332\", but was %s", actual.ConnectAddress())
		}
	})
	// Test of the tokenRing with the RandomPartitioner
	t.Run("Random", func(t *testing.T) {
		t.Parallel()

		// String tokens are parsed into big.Int in base 10
		hosts := hostsForTests(4)
		ring, err := endpoint.InternalNewTokenRing("RandomPartitioner", hosts)
		if err != nil {
			t.Fatalf("Failed to create token ring due to error: %v", err)
		}

		p := endpoint.InternalRandomPartitioner{}

		var actual *endpoint.HostInfo
		for _, host := range hosts {
			actual, _ := ring.GetHostForToken(p.ParseString(implEndpoint.HostInfoData(host).InternalTokens[0]))
			if !actual.ConnectAddress().Equal(host.ConnectAddress()) {
				t.Errorf("Expected address %v for token %q, but was %v", host.ConnectAddress(), implEndpoint.HostInfoData(host).InternalTokens[0], actual.ConnectAddress())
			}
		}

		actual, _ = ring.GetHostForToken(p.ParseString("12"))
		if !implEndpoint.HostInfoData(actual).InternalPeer.Equal(implEndpoint.HostInfoData(hosts[1]).InternalPeer) {
			t.Errorf("Expected address 1 for token \"12\", but was %s", actual.ConnectAddress())
		}

		actual, _ = ring.GetHostForToken(p.ParseString("24324545443332"))
		if !actual.ConnectAddress().Equal(hosts[0].ConnectAddress()) {
			t.Errorf("Expected address 1 for token \"24324545443332\", but was %s", actual.ConnectAddress())
		}
	})
	// Test of the recognition of the partitioner class
	t.Run("UnknownPartitioner", func(t *testing.T) {
		t.Parallel()

		_, err := endpoint.InternalNewTokenRing("UnknownPartitioner", nil)
		if err == nil {
			t.Error("Expected error for unknown partitioner value, but was nil")
		}
	})
}
