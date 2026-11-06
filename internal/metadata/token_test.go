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
	"bytes"
	"fmt"
	"math/big"
	"net"
	"sort"
	"strconv"
	"testing"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/endpoint"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
)

// Tests of the murmur3Patitioner
func TestMurmur3Partitioner(t *testing.T) {
	t.Parallel()

	token := endpoint.InternalMurmur3Partitioner{}.ParseString("-1053604476080545076")

	if "-1053604476080545076" != token.String() {
		t.Errorf("Expected '-1053604476080545076' but was '%s'", token)
	}

	// at least verify that the partitioner
	// doesn't return nil
	pk, _ := encodeFixtureCell(cqltype.NewNativeType(4, cqltype.TypeInt), 1)
	token = endpoint.InternalMurmur3Partitioner{}.Hash(pk)
	if token == nil {
		t.Fatal("token was nil")
	}
}

// TestMurmur3Partitioner_HashInt64MatchesHash verifies hashInt64 matches Hash().
func TestMurmur3Partitioner_HashInt64MatchesHash(t *testing.T) {
	t.Parallel()

	var p endpoint.InternalMurmur3Partitioner
	var _ endpoint.InternalInt64Hasher = p // compile-time contract check, mirrors token.go's var _ assertion

	keys := [][]byte{
		nil,
		{},
		{0},
		{1, 2, 3},
		[]byte("routing-key"),
		[]byte("a-somewhat-longer-partition-key-value-1234567890"),
	}
	for i := 0; i < 32; i++ {
		buf := make([]byte, i)
		for j := range buf {
			buf[j] = byte(i*7 + j)
		}
		keys = append(keys, buf)
	}

	for _, k := range keys {
		want := p.Hash(k)
		wantInt64, ok := want.(endpoint.InternalInt64Token)
		if !ok {
			t.Fatalf("Hash(%v) returned %T, want int64Token", k, want)
		}
		got := endpoint.InternalInt64Token(p.InternalHashInt64(k))
		if got != wantInt64 {
			t.Errorf("hashInt64(%v) = %d, want %d (from Hash)", k, got, wantInt64)
		}
	}
}

func TestMurmur3Partitioner_HashInt64_ZeroAlloc(t *testing.T) {
	var p endpoint.InternalMurmur3Partitioner
	pk := []byte("routing-key")

	var sink int64
	allocs := testing.AllocsPerRun(1000, func() {
		sink = p.InternalHashInt64(pk)
	})
	if allocs != 0 {
		t.Errorf("hashInt64 allocated %.2f allocs/op, want 0", allocs)
	}
	_ = sink

	// Contrast: Hash() itself allocates once, boxing the int64 into a Token.
	var tokSink endpoint.Token
	hashAllocs := testing.AllocsPerRun(1000, func() {
		tokSink = p.Hash(pk)
	})
	_ = tokSink
	if hashAllocs != 1 {
		t.Logf("Hash() allocated %.2f allocs/op (expected ~1 from Token boxing); "+
			"int64Hasher fast path may no longer be needed if this is 0", hashAllocs)
	}
}

// Tests of the int64Token
func TestInt64Token(t *testing.T) {
	t.Parallel()

	if endpoint.InternalInt64Token(42).Less(endpoint.InternalInt64Token(42)) {
		t.Errorf("Expected Less to return false, but was true")
	}
	if !endpoint.InternalInt64Token(-42).Less(endpoint.InternalInt64Token(42)) {
		t.Errorf("Expected Less to return true, but was false")
	}
	if endpoint.InternalInt64Token(42).Less(endpoint.InternalInt64Token(-42)) {
		t.Errorf("Expected Less to return false, but was true")
	}
}

// Tests of the orderedPartitioner
func TestOrderedPartitioner(t *testing.T) {
	t.Parallel()

	// at least verify that the partitioner
	// doesn't return nil
	p := endpoint.InternalOrderedPartitioner{}
	pk, _ := encodeFixtureCell(cqltype.NewNativeType(4, cqltype.TypeInt), 1)
	token := p.Hash(pk)
	if token == nil {
		t.Fatal("token was nil")
	}

	str := token.String()
	parsedToken := p.ParseString(str)

	if !bytes.Equal([]byte(token.(endpoint.InternalOrderedToken)), []byte(parsedToken.(endpoint.InternalOrderedToken))) {
		t.Errorf("Failed to convert to and from a string %s expected %x but was %x",
			str,
			[]byte(token.(endpoint.InternalOrderedToken)),
			[]byte(parsedToken.(endpoint.InternalOrderedToken)),
		)
	}
}

// Tests of the orderedToken
func TestOrderedToken(t *testing.T) {
	t.Parallel()

	if endpoint.InternalOrderedToken([]byte{0, 0, 4, 2}).Less(endpoint.InternalOrderedToken([]byte{0, 0, 4, 2})) {
		t.Errorf("Expected Less to return false, but was true")
	}
	if !endpoint.InternalOrderedToken([]byte{0, 0, 3}).Less(endpoint.InternalOrderedToken([]byte{0, 0, 4, 2})) {
		t.Errorf("Expected Less to return true, but was false")
	}
	if endpoint.InternalOrderedToken([]byte{0, 0, 4, 2}).Less(endpoint.InternalOrderedToken([]byte{0, 0, 3})) {
		t.Errorf("Expected Less to return false, but was true")
	}
}

// Tests of the randomPartitioner
func TestRandomPartitioner(t *testing.T) {
	t.Parallel()

	// at least verify that the partitioner
	// doesn't return nil
	p := endpoint.InternalRandomPartitioner{}
	pk, _ := encodeFixtureCell(cqltype.NewNativeType(4, cqltype.TypeInt), 1)
	token := p.Hash(pk)
	if token == nil {
		t.Fatal("token was nil")
	}

	str := token.String()
	parsedToken := p.ParseString(str)

	if (*big.Int)(token.(*endpoint.InternalRandomToken)).Cmp((*big.Int)(parsedToken.(*endpoint.InternalRandomToken))) != 0 {
		t.Errorf("Failed to convert to and from a string %s expected %v but was %v",
			str,
			token,
			parsedToken,
		)
	}
}

func TestRandomPartitionerMatchesReference(t *testing.T) {
	t.Parallel()

	// example taken from datastax python driver
	//    >>> from cassandra.metadata import MD5Token
	//    >>> MD5Token.hash_fn("test")
	//    12707736894140473154801792860916528374L
	var p endpoint.InternalRandomPartitioner
	expect := "12707736894140473154801792860916528374"
	actual := p.Hash([]byte("test")).String()
	if actual != expect {
		t.Errorf("expected random partitioner to generate tokens in the same way as the reference"+
			" python client. Expected %s, but got %s", expect, actual)
	}
}

// Tests of the randomToken
func TestRandomToken(t *testing.T) {
	t.Parallel()

	if ((*endpoint.InternalRandomToken)(big.NewInt(42))).Less((*endpoint.InternalRandomToken)(big.NewInt(42))) {
		t.Errorf("Expected Less to return false, but was true")
	}
	if !((*endpoint.InternalRandomToken)(big.NewInt(41))).Less((*endpoint.InternalRandomToken)(big.NewInt(42))) {
		t.Errorf("Expected Less to return true, but was false")
	}
	if ((*endpoint.InternalRandomToken)(big.NewInt(42))).Less((*endpoint.InternalRandomToken)(big.NewInt(41))) {
		t.Errorf("Expected Less to return false, but was true")
	}
}

type intToken int

func (i intToken) String() string                 { return strconv.Itoa(int(i)) }
func (i intToken) Less(token endpoint.Token) bool { return i < token.(intToken) }

// Test of the token ring implementation based on example at the start of this
// page of documentation:
// http://www.datastax.com/docs/0.8/cluster_architecture/partitioning
func TestTokenRing_Int(t *testing.T) {
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
}

// Test for the behavior of a nil pointer to tokenRing
func TestTokenRing_Nil(t *testing.T) {
	t.Parallel()

	var ring *endpoint.InternalTokenRing = nil

	if host, endToken := ring.GetHostForToken(nil); host != nil || endToken != nil {
		t.Error("Expected nil for nil token ring")
	}
	if host, token, endToken := ring.GetHostForPartitionKey(nil); host != nil || token != nil || endToken != nil {
		t.Error("Expected nil for nil token ring")
	}
}

// Test of the recognition of the partitioner class
func TestTokenRing_UnknownPartition(t *testing.T) {
	t.Parallel()

	_, err := endpoint.InternalNewTokenRing("UnknownPartitioner", nil)
	if err == nil {
		t.Error("Expected error for unknown partitioner value, but was nil")
	}
}

func hostsForTests(n int) []*endpoint.HostInfo {
	hosts := make([]*endpoint.HostInfo, n)
	for i := 0; i < n; i++ {
		host := func() *endpoint.HostInfo {
			opaqueStorageValuexxxx := new(endpoint.HostInfo)
			implEndpoint.HostInfoData(opaqueStorageValuexxxx).InternalConnectAddress = net.IPv4(1, 1, 1, byte(n))
			implEndpoint.HostInfoData(opaqueStorageValuexxxx).InternalTokens = []string{fmt.Sprintf("%d", n)}
			return opaqueStorageValuexxxx
		}()

		hosts[i] = host
	}
	return hosts
}

// Test of the tokenRing with the Murmur3Partitioner
func TestTokenRing_Murmur3(t *testing.T) {
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
}

// Test of the tokenRing with the OrderedPartitioner
func TestTokenRing_Ordered(t *testing.T) {
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
}

// Test of the tokenRing with the RandomPartitioner
func TestTokenRing_Random(t *testing.T) {
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
}
