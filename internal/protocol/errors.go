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

package protocol

import (
	. "github.com/scylladb/gocql/v2/internal/cqltype"

	"fmt"

	frm "github.com/scylladb/gocql/v2/internal/frame"
)

// See CQL Binary Protocol v5, section 8 for more details.
// https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec
const (
	// ErrCodeServer indicates unexpected error on server-side.
	//
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1246-L1247
	ErrCodeServer = 0x0000
	// ErrCodeProtocol indicates a protocol violation by some client message.
	//
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1248-L1250
	ErrCodeProtocol = 0x000A
	// ErrCodeCredentials indicates missing required authentication.
	//
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1251-L1254
	ErrCodeCredentials = 0x0100
	// ErrCodeUnavailable indicates unavailable error.
	//
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1255-L1265
	ErrCodeUnavailable = 0x1000
	// ErrCodeOverloaded returned in case of request on overloaded node coordinator.
	//
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1266-L1267
	ErrCodeOverloaded = 0x1001
	// ErrCodeBootstrapping returned from the coordinator node in bootstrapping phase.
	//
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1268-L1269
	ErrCodeBootstrapping = 0x1002
	// ErrCodeTruncate indicates truncation exception.
	//
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1270
	ErrCodeTruncate = 0x1003
	// ErrCodeWriteTimeout returned in case of timeout during the request write.
	//
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1271-L1304
	ErrCodeWriteTimeout = 0x1100
	// ErrCodeReadTimeout returned in case of timeout during the request read.
	//
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1305-L1321
	ErrCodeReadTimeout = 0x1200
	// ErrCodeReadFailure indicates request read error which is not covered by ErrCodeReadTimeout.
	//
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1322-L1340
	ErrCodeReadFailure = 0x1300
	// ErrCodeFunctionFailure indicates an error in user-defined function.
	//
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1341-L1347
	ErrCodeFunctionFailure = 0x1400
	// ErrCodeWriteFailure indicates request write error which is not covered by ErrCodeWriteTimeout.
	//
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1348-L1385
	ErrCodeWriteFailure = 0x1500
	// ErrCodeCDCWriteFailure is defined, but not yet documented in CQLv5 protocol.
	//
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1386
	ErrCodeCDCWriteFailure = 0x1600
	// ErrCodeCASWriteUnknown indicates only partially completed CAS operation.
	//
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1387-L1397
	ErrCodeCASWriteUnknown = 0x1700
	// ErrCodeSyntax indicates the syntax error in the query.
	//
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1399
	ErrCodeSyntax = 0x2000
	// ErrCodeUnauthorized indicates access rights violation by user on performed operation.
	//
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1400-L1401
	ErrCodeUnauthorized = 0x2100
	// ErrCodeInvalid indicates invalid query error which is not covered by ErrCodeSyntax.
	//
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1402
	ErrCodeInvalid = 0x2200
	// ErrCodeConfig indicates the configuration error.
	//
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1403
	ErrCodeConfig = 0x2300
	// ErrCodeAlreadyExists is returned for the requests creating the existing keyspace/table.
	//
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1404-L1413
	ErrCodeAlreadyExists = 0x2400
	// ErrCodeUnprepared returned from the host for prepared statement which is unknown.
	//
	// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1414-L1417
	ErrCodeUnprepared = 0x2500
)

type RequestError interface {
	GetCode() int
	GetMessage() string
	Error() string
}

type RequestErrUnavailable struct {
	frm.ErrorFrame
	Consistency Consistency
	Required    int
	Alive       int
}

func (e *RequestErrUnavailable) String() string {
	return fmt.Sprintf("[request_error_unavailable consistency=%s required=%d alive=%d]", e.Consistency, e.Required, e.Alive)
}

type ErrorMap map[string]uint16

type RequestErrWriteTimeout struct {
	WriteType string
	frm.ErrorFrame
	Received    int
	BlockFor    int
	Consistency Consistency
}

type RequestErrWriteFailure struct {
	ErrorMap  ErrorMap
	WriteType string
	frm.ErrorFrame
	Received    int
	BlockFor    int
	NumFailures int
	Consistency Consistency
}

type RequestErrCDCWriteFailure struct {
	frm.ErrorFrame
}

type RequestErrReadTimeout struct {
	frm.ErrorFrame
	Received    int
	BlockFor    int
	Consistency Consistency
	DataPresent byte
}

type RequestErrAlreadyExists struct {
	Keyspace string
	Table    string
	frm.ErrorFrame
}

type RequestErrUnprepared struct {
	StatementId []byte
	frm.ErrorFrame
}

type RequestErrReadFailure struct {
	ErrorMap ErrorMap
	frm.ErrorFrame
	Received    int
	BlockFor    int
	NumFailures int
	Consistency Consistency
	DataPresent bool
}

type RequestErrFunctionFailure struct {
	Keyspace string
	Function string
	ArgTypes []string
	frm.ErrorFrame
}

// RequestErrCASWriteUnknown is distinct error for ErrCodeCasWriteUnknown.
//
// See https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1387-L1397
type RequestErrCASWriteUnknown struct {
	frm.ErrorFrame
	Consistency Consistency
	Received    int
	BlockFor    int
}

// RequestErrServer is a server error whose code carries no fields of its own:
// Server, Protocol, Credentials, Overloaded, Bootstrapping, Truncate, Syntax,
// Unauthorized, Invalid and Config.
type RequestErrServer struct {
	frm.ErrorFrame
}

// ErrorCode matches every server error with its code through errors.Is.
type ErrorCode = frm.ErrorCode

type UnknownServerError struct {
	frm.ErrorFrame
}

type OpType uint8

const (
	OpTypeRead  OpType = 0
	OpTypeWrite OpType = 1
)

type RequestErrRateLimitReached struct {
	frm.ErrorFrame
	OpType                OpType
	RejectedByCoordinator bool
}

func (e *RequestErrRateLimitReached) Error() string {
	if e.Message == "" {
		return "rate limit reached"
	}
	return "rate limit reached: " + e.Message
}

func (e *RequestErrRateLimitReached) String() string {
	var opType string
	if e.OpType == OpTypeRead {
		opType = "Read"
	} else if e.OpType == OpTypeWrite {
		opType = "Write"
	} else {
		opType = "Other"
	}
	return fmt.Sprintf("[request_error_rate_limit_reached OpType=%s RejectedByCoordinator=%t]", opType, e.RejectedByCoordinator)
}

// ServerErrorExecutionStatus reports what a request that failed with err may have
// done on the cluster. It is the single classification of server error codes
// by execution outcome.
func ServerErrorExecutionStatus(err RequestError) ExecutionStatus {
	switch e := err.(type) {
	case *RequestErrWriteTimeout:
		return writeExecution(e.Received, e.WriteType)
	case *RequestErrWriteFailure:
		return writeExecution(e.Received, e.WriteType)
	case *RequestErrRateLimitReached:
		if e.RejectedByCoordinator {
			return NotExecuted
		}
		return MaybeExecuted
	}
	switch err.GetCode() {
	case ErrCodeUnavailable, ErrCodeOverloaded, ErrCodeBootstrapping, ErrCodeSyntax,
		ErrCodeInvalid, ErrCodeUnauthorized, ErrCodeCredentials, ErrCodeConfig,
		ErrCodeProtocol, ErrCodeAlreadyExists, ErrCodeUnprepared, ErrCodeFunctionFailure:
		return NotExecuted
	default:
		return MaybeExecuted
	}
}

// writeExecution classifies a write that some replicas acknowledged. Batch log
// and Paxos acknowledgements do not mean the data itself was applied.
func writeExecution(received int, writeType string) ExecutionStatus {
	if received <= 0 {
		return MaybeExecuted
	}
	switch writeType {
	case "SIMPLE", "BATCH", "UNLOGGED_BATCH", "COUNTER", "VIEW", "CDC":
		return PartiallyExecuted
	default:
		return MaybeExecuted
	}
}

// errorBody reads the fields of an ERROR response body.
type errorBody interface {
	I32() int
	U16() uint16
	U8() byte
	Str() string
	Strings() []string
	shortBytes() []byte
	errorMap() ErrorMap
}

// decodeServerError decodes an ERROR response body into its typed error.
// It is the only decoder of server errors.
func decodeServerError(r errorBody, head frm.FrameHeader, proto byte, rateLimitCode int) error {
	code, msg := r.I32(), r.Str()
	base := frm.NewErrorFrame(head, code, msg)
	switch code {
	case ErrCodeUnavailable:
		return &RequestErrUnavailable{ErrorFrame: base, Consistency: Consistency(r.U16()), Required: r.I32(), Alive: r.I32()}
	case ErrCodeWriteTimeout:
		return &RequestErrWriteTimeout{ErrorFrame: base, Consistency: Consistency(r.U16()), Received: r.I32(), BlockFor: r.I32(), WriteType: r.Str()}
	case ErrCodeReadTimeout:
		return &RequestErrReadTimeout{ErrorFrame: base, Consistency: Consistency(r.U16()), Received: r.I32(), BlockFor: r.I32(), DataPresent: r.U8()}
	case ErrCodeAlreadyExists:
		return &RequestErrAlreadyExists{ErrorFrame: base, Keyspace: r.Str(), Table: r.Str()}
	case ErrCodeUnprepared:
		return &RequestErrUnprepared{ErrorFrame: base, StatementId: r.shortBytes()}
	case ErrCodeReadFailure:
		e := &RequestErrReadFailure{ErrorFrame: base, Consistency: Consistency(r.U16()), Received: r.I32(), BlockFor: r.I32()}
		if VersionFor(proto).FailureReasonMap {
			e.ErrorMap = r.errorMap()
			e.NumFailures = len(e.ErrorMap)
		} else {
			e.NumFailures = r.I32()
		}
		e.DataPresent = r.U8() != 0
		return e
	case ErrCodeWriteFailure:
		e := &RequestErrWriteFailure{ErrorFrame: base, Consistency: Consistency(r.U16()), Received: r.I32(), BlockFor: r.I32()}
		if VersionFor(proto).FailureReasonMap {
			e.ErrorMap = r.errorMap()
			e.NumFailures = len(e.ErrorMap)
		} else {
			e.NumFailures = r.I32()
		}
		e.WriteType = r.Str()
		return e
	case ErrCodeFunctionFailure:
		return &RequestErrFunctionFailure{ErrorFrame: base, Keyspace: r.Str(), Function: r.Str(), ArgTypes: r.Strings()}
	case ErrCodeCDCWriteFailure:
		return &RequestErrCDCWriteFailure{ErrorFrame: base}
	case ErrCodeCASWriteUnknown:
		return &RequestErrCASWriteUnknown{ErrorFrame: base, Consistency: Consistency(r.U16()), Received: r.I32(), BlockFor: r.I32()}
	case ErrCodeInvalid, ErrCodeBootstrapping, ErrCodeConfig, ErrCodeCredentials, ErrCodeOverloaded, ErrCodeProtocol, ErrCodeServer, ErrCodeSyntax, ErrCodeTruncate, ErrCodeUnauthorized:
		return &RequestErrServer{ErrorFrame: base}
	default:
		if rateLimitCode != 0 && code == rateLimitCode {
			return &RequestErrRateLimitReached{ErrorFrame: base, OpType: OpType(r.U8()), RejectedByCoordinator: r.U8() != 0}
		}
		return &UnknownServerError{ErrorFrame: base}
	}
}
