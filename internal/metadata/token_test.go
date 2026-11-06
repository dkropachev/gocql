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
	"fmt"
	"math/big"
	"net"
	"strconv"
	"testing"

	"github.com/scylladb/gocql/v2/internal/endpoint"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
)

type intToken int

func (i intToken) String() string                 { return strconv.Itoa(int(i)) }
func (i intToken) Less(token endpoint.Token) bool { return i < token.(intToken) }

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

func TestToken(t *testing.T) {
	t.Parallel()
	// Tests of the int64Token
	t.Run("Int64", func(t *testing.T) {
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
	})
	// Tests of the orderedToken
	t.Run("Ordered", func(t *testing.T) {
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
	})
	// Tests of the randomToken
	t.Run("Random", func(t *testing.T) {
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
	})
}
