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

package cql

import (
	"fmt"
)

// Consistency is a consistency level. Its values are the native protocol's, so
// the zero Consistency is Any; a setting that may be left unset is a
// cql.Option[Consistency], never the zero level. ParseConsistency reads a
// level's name. Later releases may add values, so a switch over them needs a
// default case.
type Consistency uint16

// Consistency levels, with their native protocol values.
const (
	// Any accepts a write once any node, including a hint, stored it.
	Any Consistency = 0x00

	// One needs one replica.
	One Consistency = 0x01

	// Two needs two replicas.
	Two Consistency = 0x02

	// Three needs three replicas.
	Three Consistency = 0x03

	// Quorum needs a majority of all replicas.
	Quorum Consistency = 0x04

	// All needs every replica.
	All Consistency = 0x05

	// LocalQuorum needs a majority of the replicas in the coordinator's
	// datacenter.
	LocalQuorum Consistency = 0x06

	// EachQuorum needs a majority of the replicas in each datacenter.
	EachQuorum Consistency = 0x07

	// Serial is the serial consistency of a conditional statement across all
	// datacenters.
	Serial Consistency = 0x08

	// LocalSerial is the serial consistency of a conditional statement within
	// the coordinator's datacenter.
	LocalSerial Consistency = 0x09

	// LocalOne needs one replica in the coordinator's datacenter.
	LocalOne Consistency = 0x0A
)

// String returns the level's protocol name, such as "LOCAL_QUORUM", or
// Consistency(n) for a value it does not define.
func (c Consistency) String() string {
	switch c {
	case Any:
		return "ANY"
	case One:
		return "ONE"
	case Two:
		return "TWO"
	case Three:
		return "THREE"
	case Quorum:
		return "QUORUM"
	case All:
		return "ALL"
	case LocalQuorum:
		return "LOCAL_QUORUM"
	case EachQuorum:
		return "EACH_QUORUM"
	case Serial:
		return "SERIAL"
	case LocalSerial:
		return "LOCAL_SERIAL"
	case LocalOne:
		return "LOCAL_ONE"
	default:
		return fmt.Sprintf("Consistency(%d)", uint16(c))
	}
}

// IsSerial reports whether c is Serial or LocalSerial.
func (c Consistency) IsSerial() bool {
	return c == Serial || c == LocalSerial
}

// MarshalText implements encoding.TextMarshaler with the level's String
// form. Its error is always nil.
func (c Consistency) MarshalText() (text []byte, err error) {
	return []byte(c.String()), nil
}

// UnmarshalText implements encoding.TextUnmarshaler. It accepts the upper-case
// protocol names String returns for the defined levels, and returns an error
// for any other text.
func (c *Consistency) UnmarshalText(text []byte) error {
	switch string(text) {
	case "ANY":
		*c = Any
	case "ONE":
		*c = One
	case "TWO":
		*c = Two
	case "THREE":
		*c = Three
	case "QUORUM":
		*c = Quorum
	case "ALL":
		*c = All
	case "LOCAL_QUORUM":
		*c = LocalQuorum
	case "EACH_QUORUM":
		*c = EachQuorum
	case "SERIAL":
		*c = Serial
	case "LOCAL_SERIAL":
		*c = LocalSerial
	case "LOCAL_ONE":
		*c = LocalOne
	default:
		return fmt.Errorf("invalid consistency %q", string(text))
	}
	return nil
}
