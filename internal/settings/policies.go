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

package settings

import (
	connectivity "github.com/scylladb/gocql/v2/internal/connectivity"
)

// ConvictionPolicy interface is used by gocql to determine if a host should be
// marked as DOWN based on the error and host info
type ConvictionPolicy = connectivity.ConvictionPolicy

type ConvictionEvent = connectivity.ConvictionEvent
type ConvictionSource = connectivity.ConvictionSource

const (
	ConvictionPool    = connectivity.ConvictionPool
	ConvictionControl = connectivity.ConvictionControl
)

// SimpleConvictionPolicy convicts a host whenever it has no connection left
// or its control connection dial fails, whatever the error. It ignores
// single-shard failures.
type SimpleConvictionPolicy struct{}

func (e *SimpleConvictionPolicy) AddFailure(ev ConvictionEvent) bool {
	return !ev.Shard.Set
}

func (e *SimpleConvictionPolicy) Reset(ConvictionEvent) {}
