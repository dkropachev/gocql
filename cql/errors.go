// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import protocol "github.com/scylladb/gocql/v2/internal/protocol"

const ErrCodeAlreadyExists = protocol.ErrCodeAlreadyExists
const ErrCodeBootstrapping = protocol.ErrCodeBootstrapping
const ErrCodeCASWriteUnknown = protocol.ErrCodeCASWriteUnknown
const ErrCodeCDCWriteFailure = protocol.ErrCodeCDCWriteFailure
const ErrCodeConfig = protocol.ErrCodeConfig
const ErrCodeCredentials = protocol.ErrCodeCredentials
const ErrCodeFunctionFailure = protocol.ErrCodeFunctionFailure
const ErrCodeInvalid = protocol.ErrCodeInvalid
const ErrCodeOverloaded = protocol.ErrCodeOverloaded
const ErrCodeProtocol = protocol.ErrCodeProtocol
const ErrCodeReadFailure = protocol.ErrCodeReadFailure
const ErrCodeReadTimeout = protocol.ErrCodeReadTimeout
const ErrCodeServer = protocol.ErrCodeServer
const ErrCodeSyntax = protocol.ErrCodeSyntax
const ErrCodeTruncate = protocol.ErrCodeTruncate
const ErrCodeUnauthorized = protocol.ErrCodeUnauthorized
const ErrCodeUnavailable = protocol.ErrCodeUnavailable
const ErrCodeUnprepared = protocol.ErrCodeUnprepared
const ErrCodeWriteFailure = protocol.ErrCodeWriteFailure
const ErrCodeWriteTimeout = protocol.ErrCodeWriteTimeout

type ErrorMap = protocol.ErrorMap
type OpType = protocol.OpType

const OpTypeRead = protocol.OpTypeRead
const OpTypeWrite = protocol.OpTypeWrite

type RequestErrAlreadyExists = protocol.RequestErrAlreadyExists
type RequestErrCASWriteUnknown = protocol.RequestErrCASWriteUnknown
type RequestErrCDCWriteFailure = protocol.RequestErrCDCWriteFailure
type RequestErrFunctionFailure = protocol.RequestErrFunctionFailure
type RequestErrRateLimitReached = protocol.RequestErrRateLimitReached
type RequestErrReadFailure = protocol.RequestErrReadFailure
type RequestErrReadTimeout = protocol.RequestErrReadTimeout
type RequestErrUnavailable = protocol.RequestErrUnavailable
type RequestErrUnprepared = protocol.RequestErrUnprepared
type RequestErrWriteFailure = protocol.RequestErrWriteFailure
type RequestErrWriteTimeout = protocol.RequestErrWriteTimeout
type RequestError = protocol.RequestError
type UnknownServerError = protocol.UnknownServerError
