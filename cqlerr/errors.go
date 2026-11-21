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

package cqlerr

import (
	"fmt"
	"net/netip"

	"github.com/scylladb/gocql/v2/cql"
)

// ErrorCode is the code of a server error. As an error it is the code's
// name, and every ServerError matches its own code through errors.Is:
//
//	if errors.Is(err, cqlerr.CodeOverloaded) { ... }
//
// Its values are the native protocol's, so the zero ErrorCode is CodeServer.
// Servers and later releases may add codes, so a switch over them needs a
// default case; Error prints a code it does not know in hexadecimal.
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
	// CodeRateLimitReached is ScyllaDB's rate limit error. Its wire value is
	// negotiated per connection; the driver reports it with this code.
	CodeRateLimitReached ErrorCode = 0xF000
)

// Error returns the code's name, or "server error 0x" and the code in
// hexadecimal for a code it does not define.
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

// ServerError is implemented by every error a server returns. It is sealed:
// only this package's error types, and types that embed one of them,
// implement it. Find one with
// errors.As, or a specific kind with its concrete type:
//
//	var timeout *cqlerr.WriteTimeoutError
//	if errors.As(err, &timeout) { ... }
type ServerError interface {
	error
	// Code is the error's code.
	Code() ErrorCode
	// Message is the server's description of the error.
	Message() string
	// Warnings are the warnings the server sent with the error.
	Warnings() []string
	setBase(message string, warnings []string)
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

// text renders a server error: the code's name, and the server's message
// when it is not empty.
func (e *serverError) text(code ErrorCode) string {
	if e.message == "" {
		return code.Error()
	}
	return code.Error() + ": " + e.message
}

// UnavailableError reports that too few replicas were alive to try the
// request. Nothing was applied.
//
// The driver returns it filled in; its zero value is an error without detail.
type UnavailableError struct {
	serverError
	// Consistency is the consistency level of the request.
	Consistency cql.Consistency
	// Required is the number of replicas the consistency level needs alive.
	Required int
	// Alive is the number of replicas the coordinator knew to be alive.
	Alive int
}

// Code returns CodeUnavailable.
func (e *UnavailableError) Code() ErrorCode { return CodeUnavailable }

// Error returns the code's name, followed by the server's message when it
// is not empty.
func (e *UnavailableError) Error() string { return e.text(CodeUnavailable) }

// Is reports whether target is CodeUnavailable.
func (e *UnavailableError) Is(target error) bool { return target == CodeUnavailable }

// Kind returns KindUnavailable.
func (e *UnavailableError) Kind() Kind { return KindUnavailable }

// ErrorMap maps the address of each replica that failed to its failure
// reason code.
type ErrorMap map[netip.Addr]uint16

// WriteType is the kind of write that a WriteTimeoutError or
// WriteFailureError reports. Its values are the protocol's write types; the
// zero WriteType is WriteTypeUnknown, which the driver reports for a write
// type it does not name. Later releases may add values, so a switch over
// them needs a default case.
type WriteType uint8

// The write types.
const (
	// WriteTypeUnknown is a write type the driver does not name.
	WriteTypeUnknown WriteType = iota
	// WriteTypeSimple is a write that is neither in a batch nor a counter
	// update.
	WriteTypeSimple
	// WriteTypeBatch is a logged batch whose batch log write succeeded.
	WriteTypeBatch
	// WriteTypeUnloggedBatch is an unlogged batch; no batch log write was
	// attempted.
	WriteTypeUnloggedBatch
	// WriteTypeCounter is a counter update, batched or not.
	WriteTypeCounter
	// WriteTypeBatchLog is the batch log write of a logged batch.
	WriteTypeBatchLog
	// WriteTypeCAS is a conditional (LWT) write.
	WriteTypeCAS
	// WriteTypeView is a write that updates a materialized view and could
	// not take the view's lock in time.
	WriteTypeView
	// WriteTypeCDC is a write to a table tracked by change data capture
	// once the CDC log's space is exhausted.
	WriteTypeCDC
)

var writeTypeNames = [...]string{
	WriteTypeUnknown:       "unknown",
	WriteTypeSimple:        "SIMPLE",
	WriteTypeBatch:         "BATCH",
	WriteTypeUnloggedBatch: "UNLOGGED_BATCH",
	WriteTypeCounter:       "COUNTER",
	WriteTypeBatchLog:      "BATCH_LOG",
	WriteTypeCAS:           "CAS",
	WriteTypeView:          "VIEW",
	WriteTypeCDC:           "CDC",
}

// String returns the server's name of the write type, such as "SIMPLE" or
// "BATCH_LOG", "unknown" for WriteTypeUnknown, or WriteType(n) for a value it
// does not define.
func (w WriteType) String() string {
	if int(w) < len(writeTypeNames) {
		return writeTypeNames[w]
	}
	return fmt.Sprintf("WriteType(%d)", uint8(w))
}

// parseWriteType maps the server's name of a write type to its WriteType, and
// a name the driver does not know to WriteTypeUnknown.
func parseWriteType(name string) WriteType {
	for w := WriteTypeSimple; int(w) < len(writeTypeNames); w++ {
		if writeTypeNames[w] == name {
			return w
		}
	}
	return WriteTypeUnknown
}

// WriteTimeoutError reports that replicas did not acknowledge a write in
// time.
//
// The driver returns it filled in; its zero value is an error without detail.
type WriteTimeoutError struct {
	serverError
	// Received is the number of replicas that acknowledged the request.
	Received int
	// BlockFor is the number of acknowledgements the consistency level
	// needs.
	BlockFor int
	// Consistency is the consistency level of the request.
	Consistency cql.Consistency
	// WriteType is the kind of write.
	WriteType WriteType
}

// Code returns CodeWriteTimeout.
func (e *WriteTimeoutError) Code() ErrorCode { return CodeWriteTimeout }

// Error returns the code's name, followed by the server's message when it
// is not empty.
func (e *WriteTimeoutError) Error() string { return e.text(CodeWriteTimeout) }

// Is reports whether target is CodeWriteTimeout.
func (e *WriteTimeoutError) Is(target error) bool { return target == CodeWriteTimeout }

// Kind returns KindTimeout.
func (e *WriteTimeoutError) Kind() Kind { return KindTimeout }

// WriteFailureError reports that replicas failed a write.
//
// The driver returns it filled in; its zero value is an error without detail.
type WriteFailureError struct {
	// ErrorMap holds each failed replica's reason on protocol 5 and later.
	ErrorMap ErrorMap
	serverError
	// Received is the number of replicas that acknowledged the request.
	Received int
	// BlockFor is the number of acknowledgements the consistency level
	// needs.
	BlockFor int
	// NumFailures is the number of replicas that failed.
	NumFailures int
	// Consistency is the consistency level of the request.
	Consistency cql.Consistency
	// WriteType is the kind of write.
	WriteType WriteType
}

// Code returns CodeWriteFailure.
func (e *WriteFailureError) Code() ErrorCode { return CodeWriteFailure }

// Error returns the code's name, followed by the server's message when it
// is not empty.
func (e *WriteFailureError) Error() string { return e.text(CodeWriteFailure) }

// Is reports whether target is CodeWriteFailure.
func (e *WriteFailureError) Is(target error) bool { return target == CodeWriteFailure }

// Kind returns KindServer.
func (e *WriteFailureError) Kind() Kind { return KindServer }

// CDCWriteFailureError reports that a write to a CDC-enabled table failed.
//
// The driver returns it filled in; its zero value is an error without detail.
type CDCWriteFailureError struct {
	serverError
}

// Code returns CodeCDCWriteFailure.
func (e *CDCWriteFailureError) Code() ErrorCode { return CodeCDCWriteFailure }

// Error returns the code's name, followed by the server's message when it
// is not empty.
func (e *CDCWriteFailureError) Error() string { return e.text(CodeCDCWriteFailure) }

// Is reports whether target is CodeCDCWriteFailure.
func (e *CDCWriteFailureError) Is(target error) bool { return target == CodeCDCWriteFailure }

// Kind returns KindServer.
func (e *CDCWriteFailureError) Kind() Kind { return KindServer }

// ReadTimeoutError reports that replicas did not answer a read in time.
//
// The driver returns it filled in; its zero value is an error without detail.
type ReadTimeoutError struct {
	serverError
	// Received is the number of replicas that answered.
	Received int
	// BlockFor is the number of answers the consistency level needs.
	BlockFor int
	// Consistency is the consistency level of the request.
	Consistency cql.Consistency
	// DataPresent reports whether the replica asked for data answered.
	DataPresent bool
}

// Code returns CodeReadTimeout.
func (e *ReadTimeoutError) Code() ErrorCode { return CodeReadTimeout }

// Error returns the code's name, followed by the server's message when it
// is not empty.
func (e *ReadTimeoutError) Error() string { return e.text(CodeReadTimeout) }

// Is reports whether target is CodeReadTimeout.
func (e *ReadTimeoutError) Is(target error) bool { return target == CodeReadTimeout }

// Kind returns KindTimeout.
func (e *ReadTimeoutError) Kind() Kind { return KindTimeout }

// AlreadyExistsError reports that a keyspace or table to create already
// exists. Table is empty for a keyspace.
//
// The driver returns it filled in; its zero value is an error without detail.
type AlreadyExistsError struct {
	// Keyspace is the keyspace that exists, or that holds the table.
	Keyspace string
	// Table is the table that exists, or empty for a keyspace.
	Table string
	serverError
}

// Code returns CodeAlreadyExists.
func (e *AlreadyExistsError) Code() ErrorCode { return CodeAlreadyExists }

// Error returns the code's name, followed by the server's message when it
// is not empty.
func (e *AlreadyExistsError) Error() string { return e.text(CodeAlreadyExists) }

// Is reports whether target is CodeAlreadyExists.
func (e *AlreadyExistsError) Is(target error) bool { return target == CodeAlreadyExists }

// Kind returns KindInvalid.
func (e *AlreadyExistsError) Kind() Kind { return KindInvalid }

// UnpreparedError reports that the server does not know a prepared
// statement. The driver prepares it again and retries on its own.
//
// The driver returns it filled in; its zero value is an error without detail.
type UnpreparedError struct {
	serverError
	// StatementID is the prepared statement ID the server does not know.
	StatementID []byte
}

// Code returns CodeUnprepared.
func (e *UnpreparedError) Code() ErrorCode { return CodeUnprepared }

// Error returns the code's name, followed by the server's message when it
// is not empty.
func (e *UnpreparedError) Error() string { return e.text(CodeUnprepared) }

// Is reports whether target is CodeUnprepared.
func (e *UnpreparedError) Is(target error) bool { return target == CodeUnprepared }

// Kind returns KindInvalid.
func (e *UnpreparedError) Kind() Kind { return KindInvalid }

// ReadFailureError reports that replicas failed a read.
//
// The driver returns it filled in; its zero value is an error without detail.
type ReadFailureError struct {
	// ErrorMap holds each failed replica's reason on protocol 5 and later.
	ErrorMap ErrorMap
	serverError
	// Received is the number of replicas that answered.
	Received int
	// BlockFor is the number of answers the consistency level needs.
	BlockFor int
	// NumFailures is the number of replicas that failed.
	NumFailures int
	// Consistency is the consistency level of the request.
	Consistency cql.Consistency
	// DataPresent reports whether the replica asked for data answered.
	DataPresent bool
}

// Code returns CodeReadFailure.
func (e *ReadFailureError) Code() ErrorCode { return CodeReadFailure }

// Error returns the code's name, followed by the server's message when it
// is not empty.
func (e *ReadFailureError) Error() string { return e.text(CodeReadFailure) }

// Is reports whether target is CodeReadFailure.
func (e *ReadFailureError) Is(target error) bool { return target == CodeReadFailure }

// Kind returns KindServer.
func (e *ReadFailureError) Kind() Kind { return KindServer }

// FunctionFailureError reports that a user-defined function failed.
//
// The driver returns it filled in; its zero value is an error without detail.
type FunctionFailureError struct {
	serverError
	// Keyspace is the function's keyspace.
	Keyspace string
	// Function is the function's name.
	Function string
	// ArgTypes lists the CQL types of the function's arguments.
	ArgTypes []string
}

// Code returns CodeFunctionFailure.
func (e *FunctionFailureError) Code() ErrorCode { return CodeFunctionFailure }

// Error returns the code's name, followed by the server's message when it
// is not empty.
func (e *FunctionFailureError) Error() string { return e.text(CodeFunctionFailure) }

// Is reports whether target is CodeFunctionFailure.
func (e *FunctionFailureError) Is(target error) bool { return target == CodeFunctionFailure }

// Kind returns KindInvalid.
func (e *FunctionFailureError) Kind() Kind { return KindInvalid }

// CASWriteUnknownError reports that a conditional write timed out with an
// unknown outcome. Cassandra sends it on protocol 5; see IsCASWriteUnknown
// for the check that also covers protocol 4 and ScyllaDB.
//
// The driver returns it filled in; its zero value is an error without detail.
type CASWriteUnknownError struct {
	serverError
	// Consistency is the consistency level of the request.
	Consistency cql.Consistency
	// Received is the number of replicas that acknowledged the request.
	Received int
	// BlockFor is the number of acknowledgements the consistency level
	// needs.
	BlockFor int
}

// Code returns CodeCASWriteUnknown.
func (e *CASWriteUnknownError) Code() ErrorCode { return CodeCASWriteUnknown }

// Error returns the code's name, followed by the server's message when it
// is not empty.
func (e *CASWriteUnknownError) Error() string { return e.text(CodeCASWriteUnknown) }

// Is reports whether target is CodeCASWriteUnknown.
func (e *CASWriteUnknownError) Is(target error) bool { return target == CodeCASWriteUnknown }

// Kind returns KindTimeout.
func (e *CASWriteUnknownError) Kind() Kind { return KindTimeout }

// OpType is the kind of operation a rate limit rejected. Its values are
// ScyllaDB's, so the zero OpType is OpTypeRead. Later releases may add values,
// so a switch over them needs a default case.
type OpType uint8

// Operation types of a RateLimitReachedError.
const (
	// OpTypeRead is a rejected read.
	OpTypeRead OpType = 0
	// OpTypeWrite is a rejected write.
	OpTypeWrite OpType = 1
)

// String returns the operation name, or OpType(n) for a value it does not
// define.
func (o OpType) String() string {
	switch o {
	case OpTypeRead:
		return "read"
	case OpTypeWrite:
		return "write"
	default:
		return fmt.Sprintf("OpType(%d)", uint8(o))
	}
}

// RateLimitReachedError reports that a request exceeded ScyllaDB's
// per-partition rate limit.
//
// The driver returns it filled in; its zero value is an error without detail.
type RateLimitReachedError struct {
	serverError
	// OpType is the kind of operation the limit rejected.
	OpType OpType
	// RejectedByCoordinator reports that the coordinator rejected the
	// request before sending it to any replica, so nothing was applied.
	RejectedByCoordinator bool
}

// Code returns CodeRateLimitReached.
func (e *RateLimitReachedError) Code() ErrorCode { return CodeRateLimitReached }

// Error returns the code's name, followed by the server's message when it
// is not empty.
func (e *RateLimitReachedError) Error() string { return e.text(CodeRateLimitReached) }

// Is reports whether target is CodeRateLimitReached.
func (e *RateLimitReachedError) Is(target error) bool { return target == CodeRateLimitReached }

// Kind returns KindRateLimited.
func (e *RateLimitReachedError) Kind() Kind { return KindRateLimited }

// OtherServerError is a server error whose code carries no fields of its
// own: Server, Protocol, BadCredentials, Overloaded, Bootstrapping, Truncate,
// Syntax, Unauthorized, Invalid and Config, and any code the driver does not
// know.
//
// The driver returns it filled in; its zero value is an error without detail.
type OtherServerError struct {
	serverError
	code ErrorCode
}

// NewOtherServerError returns the server error with code and message, for
// codes without an error type of their own. Tests of a retry.Policy can use
// it to build the server errors the policy sees.
func NewOtherServerError(code ErrorCode, message string) *OtherServerError {
	return &OtherServerError{serverError: serverError{message: message}, code: code}
}

// Code returns the error's code.
func (e *OtherServerError) Code() ErrorCode { return e.code }

// Error returns the code's name, followed by the server's message when it
// is not empty.
func (e *OtherServerError) Error() string { return e.text(e.code) }

// Is reports whether target is the error's code.
func (e *OtherServerError) Is(target error) bool { return target == e.code }

// Kind returns the Kind that KindOf assigns to the error's code.
func (e *OtherServerError) Kind() Kind { return codeKind(e.code) }

// CodeKind classifies a code the way KindOf classifies the server errors that
// carry it.
func codeKind(c ErrorCode) Kind {
	switch c {
	case CodeReadTimeout, CodeWriteTimeout, CodeCASWriteUnknown:
		return KindTimeout
	case CodeUnavailable:
		return KindUnavailable
	case CodeOverloaded, CodeBootstrapping:
		return KindOverloaded
	case CodeSyntax, CodeInvalid, CodeAlreadyExists, CodeConfig, CodeFunctionFailure, CodeUnprepared:
		return KindInvalid
	case CodeUnauthorized, CodeBadCredentials:
		return KindUnauthorized
	case CodeProtocol:
		return KindProtocol
	default:
		return KindServer
	}
}

// ServerErrorExecutionStatus reports what a request that failed with err may have
// done on the cluster. It is the single classification of server error codes
// by execution outcome.
func serverErrorExecutionStatus(err ServerError) ExecutionStatus {
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
func writeExecution(received int, writeType WriteType) ExecutionStatus {
	if received <= 0 {
		return MaybeExecuted
	}
	switch writeType {
	case WriteTypeSimple, WriteTypeBatch, WriteTypeUnloggedBatch, WriteTypeCounter, WriteTypeView, WriteTypeCDC:
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
	ShortBytes() []byte
	ReadErrorMap() ErrorMap
}

// decodeServerError decodes an ERROR response body into its typed error.
// It is the only decoder of server errors.
func decodeServerError(r errorBody, warnings []string, failureReasonMap bool, rateLimitCode int) ServerError {
	wire, msg := r.I32(), r.Str()
	base := serverError{message: msg, warnings: warnings}
	// Every code with fields of its own fits in 16 bits. A wider code is
	// unknown and keeps its low 16 bits.
	code := ErrorCode(wire)
	if int(code) == wire {
		switch code {
		case CodeUnavailable:
			return &UnavailableError{serverError: base, Consistency: cql.Consistency(r.U16()), Required: r.I32(), Alive: r.I32()}
		case CodeWriteTimeout:
			return &WriteTimeoutError{serverError: base, Consistency: cql.Consistency(r.U16()), Received: r.I32(), BlockFor: r.I32(), WriteType: parseWriteType(r.Str())}
		case CodeReadTimeout:
			return &ReadTimeoutError{serverError: base, Consistency: cql.Consistency(r.U16()), Received: r.I32(), BlockFor: r.I32(), DataPresent: r.U8() != 0}
		case CodeAlreadyExists:
			return &AlreadyExistsError{serverError: base, Keyspace: r.Str(), Table: r.Str()}
		case CodeUnprepared:
			return &UnpreparedError{serverError: base, StatementID: r.ShortBytes()}
		case CodeReadFailure:
			e := &ReadFailureError{serverError: base, Consistency: cql.Consistency(r.U16()), Received: r.I32(), BlockFor: r.I32()}
			if failureReasonMap {
				e.ErrorMap = r.ReadErrorMap()
				e.NumFailures = len(e.ErrorMap)
			} else {
				e.NumFailures = r.I32()
			}
			e.DataPresent = r.U8() != 0
			return e
		case CodeWriteFailure:
			e := &WriteFailureError{serverError: base, Consistency: cql.Consistency(r.U16()), Received: r.I32(), BlockFor: r.I32()}
			if failureReasonMap {
				e.ErrorMap = r.ReadErrorMap()
				e.NumFailures = len(e.ErrorMap)
			} else {
				e.NumFailures = r.I32()
			}
			e.WriteType = parseWriteType(r.Str())
			return e
		case CodeFunctionFailure:
			return &FunctionFailureError{serverError: base, Keyspace: r.Str(), Function: r.Str(), ArgTypes: r.Strings()}
		case CodeCDCWriteFailure:
			return &CDCWriteFailureError{serverError: base}
		case CodeCASWriteUnknown:
			return &CASWriteUnknownError{serverError: base, Consistency: cql.Consistency(r.U16()), Received: r.I32(), BlockFor: r.I32()}
		}
	}
	if rateLimitCode != 0 && wire == rateLimitCode {
		return &RateLimitReachedError{serverError: base, OpType: OpType(r.U8()), RejectedByCoordinator: r.U8() != 0}
	}
	return &OtherServerError{serverError: base, code: code}
}
