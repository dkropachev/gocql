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

package endpoint

import (
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/scylladb/gocql/v2/cql"
)

type HostInfoStorage struct {
	InternalInitialSetupDeadline time.Time
	InternalTranslatedAddresses  *InternalTranslatedAddresses
	hostID                       atomic.Pointer[cql.UUID] // InternalHostId once known; it never changes after
	InternalDataCenter           string
	InternalSchemaVersion        string
	InternalHostname             string
	InternalClusterName          string
	InternalPartitioner          string
	InternalRack                 string
	InternalRpcAddress           net.IP
	InternalBroadcastAddress     net.IP
	InternalTokens               []string
	InternalPreferredIP          net.IP
	InternalPeer                 net.IP
	InternalListenAddress        net.IP
	InternalConnectAddress       net.IP
	InternalVersion              InternalCassVersion
	InternalScyllaFeatures       ScyllaHostFeatures
	InternalLifecycleGeneration  uint64
	InternalSharderGeneration    uint64
	InternalTransportGeneration  uint64
	InternalPort                 int
	InternalMu                   sync.RWMutex
	InternalState                NodeStateCell // read without InternalMu; written under it
	InternalHostId               cql.UUID
}

func HostInfoData(v *HostInfo) *HostInfoStorage { return &v.privateStorage }
