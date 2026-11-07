// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

import protocol "github.com/scylladb/gocql/v2/internal/protocol"

// Deprecated: use cqlerr.ErrCodeAlreadyExists.
const ErrCodeAlreadyExists = protocol.ErrCodeAlreadyExists

// Deprecated: use cqlerr.ErrCodeBootstrapping.
const ErrCodeBootstrapping = protocol.ErrCodeBootstrapping

// Deprecated: use cqlerr.ErrCodeCASWriteUnknown.
const ErrCodeCASWriteUnknown = protocol.ErrCodeCASWriteUnknown

// Deprecated: use cqlerr.ErrCodeCDCWriteFailure.
const ErrCodeCDCWriteFailure = protocol.ErrCodeCDCWriteFailure

// Deprecated: use cqlerr.ErrCodeConfig.
const ErrCodeConfig = protocol.ErrCodeConfig

// Deprecated: use cqlerr.ErrCodeCredentials.
const ErrCodeCredentials = protocol.ErrCodeCredentials

// Deprecated: use cqlerr.ErrCodeFunctionFailure.
const ErrCodeFunctionFailure = protocol.ErrCodeFunctionFailure

// Deprecated: use cqlerr.ErrCodeInvalid.
const ErrCodeInvalid = protocol.ErrCodeInvalid

// Deprecated: use cqlerr.ErrCodeOverloaded.
const ErrCodeOverloaded = protocol.ErrCodeOverloaded

// Deprecated: use cqlerr.ErrCodeProtocol.
const ErrCodeProtocol = protocol.ErrCodeProtocol

// Deprecated: use cqlerr.ErrCodeReadFailure.
const ErrCodeReadFailure = protocol.ErrCodeReadFailure

// Deprecated: use cqlerr.ErrCodeReadTimeout.
const ErrCodeReadTimeout = protocol.ErrCodeReadTimeout

// Deprecated: use cqlerr.ErrCodeServer.
const ErrCodeServer = protocol.ErrCodeServer

// Deprecated: use cqlerr.ErrCodeSyntax.
const ErrCodeSyntax = protocol.ErrCodeSyntax

// Deprecated: use cqlerr.ErrCodeTruncate.
const ErrCodeTruncate = protocol.ErrCodeTruncate

// Deprecated: use cqlerr.ErrCodeUnauthorized.
const ErrCodeUnauthorized = protocol.ErrCodeUnauthorized

// Deprecated: use cqlerr.ErrCodeUnavailable.
const ErrCodeUnavailable = protocol.ErrCodeUnavailable

// Deprecated: use cqlerr.ErrCodeUnprepared.
const ErrCodeUnprepared = protocol.ErrCodeUnprepared

// Deprecated: use cqlerr.ErrCodeWriteFailure.
const ErrCodeWriteFailure = protocol.ErrCodeWriteFailure

// Deprecated: use cqlerr.ErrCodeWriteTimeout.
const ErrCodeWriteTimeout = protocol.ErrCodeWriteTimeout

// Deprecated: use cqlerr.ErrorMap.
type ErrorMap = protocol.ErrorMap

// Deprecated: use cqlerr.OpType.
type OpType = protocol.OpType

// Deprecated: use cqlerr.OpTypeRead.
const OpTypeRead = protocol.OpTypeRead

// Deprecated: use cqlerr.OpTypeWrite.
const OpTypeWrite = protocol.OpTypeWrite

// Deprecated: use cqlerr.RequestErrAlreadyExists.
type RequestErrAlreadyExists = protocol.RequestErrAlreadyExists

// Deprecated: use cqlerr.RequestErrCASWriteUnknown.
type RequestErrCASWriteUnknown = protocol.RequestErrCASWriteUnknown

// Deprecated: use cqlerr.RequestErrCDCWriteFailure.
type RequestErrCDCWriteFailure = protocol.RequestErrCDCWriteFailure

// Deprecated: use cqlerr.RequestErrFunctionFailure.
type RequestErrFunctionFailure = protocol.RequestErrFunctionFailure

// Deprecated: use cqlerr.RequestErrRateLimitReached.
type RequestErrRateLimitReached = protocol.RequestErrRateLimitReached

// Deprecated: use cqlerr.RequestErrReadFailure.
type RequestErrReadFailure = protocol.RequestErrReadFailure

// Deprecated: use cqlerr.RequestErrReadTimeout.
type RequestErrReadTimeout = protocol.RequestErrReadTimeout

// Deprecated: use cqlerr.RequestErrUnavailable.
type RequestErrUnavailable = protocol.RequestErrUnavailable

// Deprecated: use cqlerr.RequestErrUnprepared.
type RequestErrUnprepared = protocol.RequestErrUnprepared

// Deprecated: use cqlerr.RequestErrWriteFailure.
type RequestErrWriteFailure = protocol.RequestErrWriteFailure

// Deprecated: use cqlerr.RequestErrWriteTimeout.
type RequestErrWriteTimeout = protocol.RequestErrWriteTimeout

// Deprecated: use cqlerr.RequestError.
type RequestError = protocol.RequestError

// Deprecated: use cqlerr.UnknownServerError.
type UnknownServerError = protocol.UnknownServerError
