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

// ErrorCode is the code of a server error. As an error it is the code's
// name, and every ServerError matches its own code through errors.Is:
//
//	errors.Is(err, CodeOverloaded)
type ErrorCode uint16

// Server error codes, with their native protocol values. See CQL Binary
// Protocol v5, section 8:
// https://github.com/apache/cassandra/blob/7337fc0/doc/native_protocol_v5.spec#L1246-L1417
const (
	// CodeServer is an unexpected server-side failure.
	CodeServer ErrorCode = 0x0000
	// CodeProtocol is a protocol violation by a client message.
	CodeProtocol ErrorCode = 0x000A
	// CodeBadCredentials is a failed authentication.
	CodeBadCredentials ErrorCode = 0x0100
	// CodeUnavailable: too few replicas were alive to try the request.
	CodeUnavailable ErrorCode = 0x1000
	// CodeOverloaded: the coordinator was overloaded.
	CodeOverloaded ErrorCode = 0x1001
	// CodeBootstrapping: the coordinator was still bootstrapping.
	CodeBootstrapping ErrorCode = 0x1002
	// CodeTruncate is a failed TRUNCATE.
	CodeTruncate ErrorCode = 0x1003
	// CodeWriteTimeout: replicas did not acknowledge a write in time.
	CodeWriteTimeout ErrorCode = 0x1100
	// CodeReadTimeout: replicas did not answer a read in time.
	CodeReadTimeout ErrorCode = 0x1200
	// CodeReadFailure: replicas failed a read.
	CodeReadFailure ErrorCode = 0x1300
	// CodeFunctionFailure: a user-defined function failed.
	CodeFunctionFailure ErrorCode = 0x1400
	// CodeWriteFailure: replicas failed a write.
	CodeWriteFailure ErrorCode = 0x1500
	// CodeCDCWriteFailure: a write to a CDC-enabled table failed.
	CodeCDCWriteFailure ErrorCode = 0x1600
	// CodeCASWriteUnknown: a conditional write timed out with an unknown
	// outcome (Cassandra, protocol 5).
	CodeCASWriteUnknown ErrorCode = 0x1700
	// CodeSyntax is a syntax error in the statement.
	CodeSyntax ErrorCode = 0x2000
	// CodeUnauthorized: the user lacks a permission the request needs.
	CodeUnauthorized ErrorCode = 0x2100
	// CodeInvalid is an invalid statement other than a syntax error.
	CodeInvalid ErrorCode = 0x2200
	// CodeConfig is a configuration error.
	CodeConfig ErrorCode = 0x2300
	// CodeAlreadyExists: a keyspace or table to create already exists.
	CodeAlreadyExists ErrorCode = 0x2400
	// CodeUnprepared: the server does not know the prepared statement ID.
	CodeUnprepared ErrorCode = 0x2500
	// CodeRateLimitReached is ScyllaDB's per-partition rate limit error. Its
	// wire value is negotiated through the SCYLLA_RATE_LIMIT_ERROR extension;
	// whatever the value, the driver reports the error with this code.
	CodeRateLimitReached ErrorCode = 0xF000
)

// Error returns the code's name.
func (c ErrorCode) Error() string {
	if name := c.name(); name != "" {
		return name
	}
	return fmt.Sprintf("server error 0x%04x", uint16(c))
}

func (c ErrorCode) name() string {
	switch c {
	case CodeServer:
		return "server error"
	case CodeProtocol:
		return "protocol error"
	case CodeBadCredentials:
		return "authentication error"
	case CodeUnavailable:
		return "unavailable"
	case CodeOverloaded:
		return "overloaded"
	case CodeBootstrapping:
		return "bootstrapping"
	case CodeTruncate:
		return "truncate error"
	case CodeWriteTimeout:
		return "write timeout"
	case CodeReadTimeout:
		return "read timeout"
	case CodeReadFailure:
		return "read failure"
	case CodeFunctionFailure:
		return "function failure"
	case CodeWriteFailure:
		return "write failure"
	case CodeCDCWriteFailure:
		return "CDC write failure"
	case CodeCASWriteUnknown:
		return "CAS write unknown"
	case CodeSyntax:
		return "syntax error"
	case CodeUnauthorized:
		return "unauthorized"
	case CodeInvalid:
		return "invalid query"
	case CodeConfig:
		return "config error"
	case CodeAlreadyExists:
		return "already exists"
	case CodeUnprepared:
		return "unprepared"
	case CodeRateLimitReached:
		return "rate limit reached"
	}
	return ""
}

// ServerError is implemented by every error a server returns.
type ServerError interface {
	error
	// Code is the error's code.
	Code() ErrorCode
	// Message is the server's description of the error.
	Message() string
	// Warnings are the warnings the server sent with the error.
	Warnings() []string
}

// serverError holds what every server error carries besides its code.
type serverError struct {
	message  string
	warnings []string
}

// Message returns the server's description of the error.
func (e *serverError) Message() string { return e.message }

// Warnings returns the warnings the server sent with the error.
func (e *serverError) Warnings() []string { return e.warnings }

// text renders a server error: the code's name and the server's message.
func (e *serverError) text(code ErrorCode) string {
	if e.message == "" {
		return code.Error()
	}
	return code.Error() + ": " + e.message
}

// UnavailableError reports that too few replicas were alive to try the
// request. Nothing was applied.
type UnavailableError struct {
	serverError
	Consistency Consistency
	Required    int
	Alive       int
}

// Code returns CodeUnavailable.
func (e *UnavailableError) Code() ErrorCode      { return CodeUnavailable }
func (e *UnavailableError) Error() string        { return e.text(CodeUnavailable) }
func (e *UnavailableError) Is(target error) bool { return target == CodeUnavailable }

// ErrorMap maps the address of each replica that failed to its failure
// reason code.
type ErrorMap map[string]uint16

// WriteTimeoutError reports that replicas did not acknowledge a write in
// time.
type WriteTimeoutError struct {
	WriteType string
	serverError
	Received    int
	BlockFor    int
	Consistency Consistency
}

// Code returns CodeWriteTimeout.
func (e *WriteTimeoutError) Code() ErrorCode      { return CodeWriteTimeout }
func (e *WriteTimeoutError) Error() string        { return e.text(CodeWriteTimeout) }
func (e *WriteTimeoutError) Is(target error) bool { return target == CodeWriteTimeout }

// WriteFailureError reports that replicas failed a write.
type WriteFailureError struct {
	// ErrorMap holds each failed replica's reason on protocol 5 and later.
	ErrorMap  ErrorMap
	WriteType string
	serverError
	Received    int
	BlockFor    int
	NumFailures int
	Consistency Consistency
}

// Code returns CodeWriteFailure.
func (e *WriteFailureError) Code() ErrorCode      { return CodeWriteFailure }
func (e *WriteFailureError) Error() string        { return e.text(CodeWriteFailure) }
func (e *WriteFailureError) Is(target error) bool { return target == CodeWriteFailure }

// CDCWriteFailureError reports that a write to a CDC-enabled table failed.
type CDCWriteFailureError struct {
	serverError
}

// Code returns CodeCDCWriteFailure.
func (e *CDCWriteFailureError) Code() ErrorCode      { return CodeCDCWriteFailure }
func (e *CDCWriteFailureError) Error() string        { return e.text(CodeCDCWriteFailure) }
func (e *CDCWriteFailureError) Is(target error) bool { return target == CodeCDCWriteFailure }

// ReadTimeoutError reports that replicas did not answer a read in time.
type ReadTimeoutError struct {
	serverError
	Received    int
	BlockFor    int
	Consistency Consistency
	// DataPresent reports whether the replica asked for data answered.
	DataPresent bool
}

// Code returns CodeReadTimeout.
func (e *ReadTimeoutError) Code() ErrorCode      { return CodeReadTimeout }
func (e *ReadTimeoutError) Error() string        { return e.text(CodeReadTimeout) }
func (e *ReadTimeoutError) Is(target error) bool { return target == CodeReadTimeout }

// AlreadyExistsError reports that a keyspace or table to create already
// exists. Table is empty for a keyspace.
type AlreadyExistsError struct {
	Keyspace string
	Table    string
	serverError
}

// Code returns CodeAlreadyExists.
func (e *AlreadyExistsError) Code() ErrorCode      { return CodeAlreadyExists }
func (e *AlreadyExistsError) Error() string        { return e.text(CodeAlreadyExists) }
func (e *AlreadyExistsError) Is(target error) bool { return target == CodeAlreadyExists }

// UnpreparedError reports that the server does not know a prepared
// statement. The driver prepares it again and retries on its own.
type UnpreparedError struct {
	serverError
	StatementID []byte
}

// Code returns CodeUnprepared.
func (e *UnpreparedError) Code() ErrorCode      { return CodeUnprepared }
func (e *UnpreparedError) Error() string        { return e.text(CodeUnprepared) }
func (e *UnpreparedError) Is(target error) bool { return target == CodeUnprepared }

// ReadFailureError reports that replicas failed a read.
type ReadFailureError struct {
	// ErrorMap holds each failed replica's reason on protocol 5 and later.
	ErrorMap ErrorMap
	serverError
	Received    int
	BlockFor    int
	NumFailures int
	Consistency Consistency
	DataPresent bool
}

// Code returns CodeReadFailure.
func (e *ReadFailureError) Code() ErrorCode      { return CodeReadFailure }
func (e *ReadFailureError) Error() string        { return e.text(CodeReadFailure) }
func (e *ReadFailureError) Is(target error) bool { return target == CodeReadFailure }

// FunctionFailureError reports that a user-defined function failed.
type FunctionFailureError struct {
	serverError
	Keyspace string
	Function string
	ArgTypes []string
}

// Code returns CodeFunctionFailure.
func (e *FunctionFailureError) Code() ErrorCode      { return CodeFunctionFailure }
func (e *FunctionFailureError) Error() string        { return e.text(CodeFunctionFailure) }
func (e *FunctionFailureError) Is(target error) bool { return target == CodeFunctionFailure }

// CASWriteUnknownError reports that a conditional write timed out with an
// unknown outcome. Cassandra sends it on protocol 5; see IsCASWriteUnknown
// for the check that also covers protocol 4 and ScyllaDB.
type CASWriteUnknownError struct {
	serverError
	Consistency Consistency
	Received    int
	BlockFor    int
}

// Code returns CodeCASWriteUnknown.
func (e *CASWriteUnknownError) Code() ErrorCode      { return CodeCASWriteUnknown }
func (e *CASWriteUnknownError) Error() string        { return e.text(CodeCASWriteUnknown) }
func (e *CASWriteUnknownError) Is(target error) bool { return target == CodeCASWriteUnknown }

// OpType is the kind of operation a rate limit rejected.
type OpType uint8

const (
	OpTypeRead  OpType = 0
	OpTypeWrite OpType = 1
)

// RateLimitReachedError reports that a request exceeded ScyllaDB's
// per-partition rate limit.
type RateLimitReachedError struct {
	serverError
	OpType OpType
	// RejectedByCoordinator reports that the coordinator rejected the
	// request before sending it to any replica, so nothing was applied.
	RejectedByCoordinator bool
}

// Code returns CodeRateLimitReached.
func (e *RateLimitReachedError) Code() ErrorCode      { return CodeRateLimitReached }
func (e *RateLimitReachedError) Error() string        { return e.text(CodeRateLimitReached) }
func (e *RateLimitReachedError) Is(target error) bool { return target == CodeRateLimitReached }

// OtherServerError is a server error whose code carries no fields of its
// own: Server, Protocol, BadCredentials, Overloaded, Bootstrapping, Truncate,
// Syntax, Unauthorized, Invalid and Config, and any code the driver does not
// know.
type OtherServerError struct {
	serverError
	code ErrorCode
}

// NewOtherServerError returns the server error with code and message.
func NewOtherServerError(code ErrorCode, message string) *OtherServerError {
	return &OtherServerError{serverError: serverError{message: message}, code: code}
}

// Code returns the error's code.
func (e *OtherServerError) Code() ErrorCode      { return e.code }
func (e *OtherServerError) Error() string        { return e.text(e.code) }
func (e *OtherServerError) Is(target error) bool { return target == e.code }

// ServerErrorExecutionStatus reports what a request that failed with err may have
// done on the cluster. It is the single classification of server error codes
// by execution outcome.
func ServerErrorExecutionStatus(err ServerError) ExecutionStatus {
	switch e := err.(type) {
	case *WriteTimeoutError:
		return writeExecution(e.Received, e.WriteType)
	case *WriteFailureError:
		return writeExecution(e.Received, e.WriteType)
	case *RateLimitReachedError:
		if e.RejectedByCoordinator {
			return NotExecuted
		}
		return MaybeExecuted
	}
	switch err.Code() {
	case CodeUnavailable, CodeOverloaded, CodeBootstrapping, CodeSyntax,
		CodeInvalid, CodeUnauthorized, CodeBadCredentials, CodeConfig,
		CodeProtocol, CodeAlreadyExists, CodeUnprepared, CodeFunctionFailure:
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

// ErrorResponseFrame is a parsed ERROR response: the frame that carries a
// server error. The header stays here, off the error a caller sees; Err is
// what the driver returns.
type ErrorResponseFrame struct {
	Err ServerError
	frm.FrameHeader
}

func (f *ErrorResponseFrame) Error() string { return f.Err.Error() }
func (f *ErrorResponseFrame) Unwrap() error { return f.Err }

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
func decodeServerError(r errorBody, warnings []string, proto byte, rateLimitCode int) ServerError {
	wire, msg := r.I32(), r.Str()
	base := serverError{message: msg, warnings: warnings}
	// Every code with fields of its own fits in 16 bits. A wider code is
	// unknown and keeps its low 16 bits.
	code := ErrorCode(wire)
	if int(code) == wire {
		switch code {
		case CodeUnavailable:
			return &UnavailableError{serverError: base, Consistency: Consistency(r.U16()), Required: r.I32(), Alive: r.I32()}
		case CodeWriteTimeout:
			return &WriteTimeoutError{serverError: base, Consistency: Consistency(r.U16()), Received: r.I32(), BlockFor: r.I32(), WriteType: r.Str()}
		case CodeReadTimeout:
			return &ReadTimeoutError{serverError: base, Consistency: Consistency(r.U16()), Received: r.I32(), BlockFor: r.I32(), DataPresent: r.U8() != 0}
		case CodeAlreadyExists:
			return &AlreadyExistsError{serverError: base, Keyspace: r.Str(), Table: r.Str()}
		case CodeUnprepared:
			return &UnpreparedError{serverError: base, StatementID: r.shortBytes()}
		case CodeReadFailure:
			e := &ReadFailureError{serverError: base, Consistency: Consistency(r.U16()), Received: r.I32(), BlockFor: r.I32()}
			if VersionFor(proto).FailureReasonMap {
				e.ErrorMap = r.errorMap()
				e.NumFailures = len(e.ErrorMap)
			} else {
				e.NumFailures = r.I32()
			}
			e.DataPresent = r.U8() != 0
			return e
		case CodeWriteFailure:
			e := &WriteFailureError{serverError: base, Consistency: Consistency(r.U16()), Received: r.I32(), BlockFor: r.I32()}
			if VersionFor(proto).FailureReasonMap {
				e.ErrorMap = r.errorMap()
				e.NumFailures = len(e.ErrorMap)
			} else {
				e.NumFailures = r.I32()
			}
			e.WriteType = r.Str()
			return e
		case CodeFunctionFailure:
			return &FunctionFailureError{serverError: base, Keyspace: r.Str(), Function: r.Str(), ArgTypes: r.Strings()}
		case CodeCDCWriteFailure:
			return &CDCWriteFailureError{serverError: base}
		case CodeCASWriteUnknown:
			return &CASWriteUnknownError{serverError: base, Consistency: Consistency(r.U16()), Received: r.I32(), BlockFor: r.I32()}
		}
	}
	if rateLimitCode != 0 && wire == rateLimitCode {
		return &RateLimitReachedError{serverError: base, OpType: OpType(r.U8()), RejectedByCoordinator: r.U8() != 0}
	}
	return &OtherServerError{serverError: base, code: code}
}
