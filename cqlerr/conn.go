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

package cqlerr

import (
	"errors"
	"fmt"
	"time"
)

// Connection errors.
var (
	// ErrTimeoutNoResponse reports that a request got no response within
	// its timeout.
	ErrTimeoutNoResponse = errors.New("gocql: no response received from cassandra within timeout period")
	// ErrConnectionClosed reports that the connection closed before the
	// request was sent or its response arrived.
	ErrConnectionClosed = errors.New("gocql: connection closed waiting for response")
	// ErrNoStreams reports that the connection had no free stream ID for
	// the request, so it was not sent.
	ErrNoStreams = errors.New("gocql: no streams available on connection")
)

// QueryError is the failure of a single attempt on one connection.
//
// The driver returns it filled in; its zero value is an error without detail.
type QueryError struct {
	state queryErrorState
}

// ExecutionStatus reports what this attempt may have done on the cluster.
func (e *QueryError) ExecutionStatus() ExecutionStatus {
	d := &e.state
	if !d.InternalPotentiallyExecuted {
		return NotExecuted
	}
	if server, ok := errors.AsType[ServerError](d.InternalErr); ok {
		return serverErrorExecutionStatus(server)
	}
	return MaybeExecuted
}

// Error returns the attempt's error, with its timeout and in-flight count
// when it timed out and its execution status when it may have executed.
func (e *QueryError) Error() string {
	d := &e.state
	msg := "unknown failure" // a zero QueryError
	if d.InternalErr != nil {
		msg = d.InternalErr.Error()
	}
	if d.InternalTimeout > 0 {
		msg = fmt.Sprintf("%s (timeout: %v, in-flight: %d)", msg, d.InternalTimeout, d.InternalInFlight)
	}
	return msg + executionSuffix(e.ExecutionStatus())
}

// executionSuffix renders a non-default execution state for error messages.
func executionSuffix(x ExecutionStatus) string {
	if x == NotExecuted {
		return ""
	}
	return " (" + x.String() + ")"
}

// Unwrap returns the attempt's error, or nil for a zero QueryError.
func (e *QueryError) Unwrap() error {
	return e.state.InternalErr
}

// queryErrorState is what the engine records about a failed attempt. The
// engine builds it through the mirror in internal/runtimeerrors.
type queryErrorState struct {
	InternalErr                 error
	InternalTimeout             time.Duration
	InternalInFlight            int
	InternalPotentiallyExecuted bool
}
