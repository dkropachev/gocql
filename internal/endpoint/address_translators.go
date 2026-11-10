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

import "net"

// AddressTranslator translates node addresses (and ports) that are discovered
// or received as a node event, for example public IPs to private IPs in EC2.
type AddressTranslator interface {
	// TranslateHost returns the address and port to dial for addr, which belongs
	// to host. Return addr unchanged when no translation applies.
	TranslateHost(host AddressTranslatorHostInfo, addr AddressPort) (AddressPort, error)
}

// AddressTranslatorFunc adapts a function to AddressTranslator.
type AddressTranslatorFunc func(host AddressTranslatorHostInfo, addr AddressPort) (AddressPort, error)

func (fn AddressTranslatorFunc) TranslateHost(host AddressTranslatorHostInfo, addr AddressPort) (AddressPort, error) {
	return fn(host, addr)
}

// IdentityTranslator returns a translator that leaves addresses unchanged.
func IdentityTranslator() AddressTranslator {
	return AddressTranslatorFunc(func(_ AddressTranslatorHostInfo, addr AddressPort) (AddressPort, error) {
		return addr, nil
	})
}

type AddressTranslatorHostInfo interface {
	HostID() string
	Rack() string
	DataCenter() string
	BroadcastAddress() net.IP
	ListenAddress() net.IP
	RPCAddress() net.IP
	PreferredIP() net.IP
	Peer() net.IP
	UntranslatedConnectAddress() net.IP
	Port() int
	Partitioner() string
	ClusterName() string
	ScyllaShardAwarePort() uint16
	ScyllaShardAwarePortTLS() uint16
	ScyllaShardCount() int
}
