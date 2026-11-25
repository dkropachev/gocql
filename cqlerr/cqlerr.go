// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Package cqlerr holds the errors the driver returns.
//
// A failed request returns an *ExecutionError. Its ExecutionStatus reports
// whether the request may have changed data, which decides whether replaying
// it is safe:
//
//	switch cqlerr.ExecutionStatusOf(err) {
//	case cqlerr.NotExecuted:       // nothing was applied; safe to retry
//	case cqlerr.MaybeExecuted:     // unknown; retry only idempotent requests
//	case cqlerr.PartiallyExecuted: // applied in part; reconcile before retrying
//	case cqlerr.Executed:          // err is nil: the request succeeded
//	}
//
// KindOf groups any error by what a caller can do about it (timeout,
// overloaded, invalid, connection, ...), and Kind.Transient reports whether
// trying again can help. ExecutionError.Attempts lists every failed attempt
// with the host it went to.
//
// Server errors are typed by their CQL error code (WriteTimeoutError,
// UnavailableError, ...) and all implement ServerError; find them with
// errors.As, or match a code with errors.Is(err, CodeOverloaded).
// Driver conditions are sentinels (ErrNoCurrentRow, ErrNoConnections, ...); test
// them with errors.Is.
//
// Package cql defines the errors of its own values and codecs:
// cql.ValidationError, cql.CodecError, cql.ProtocolError, the streaming
// capacity errors and cql.ErrReleased. KindOf and ExecutionStatusOf classify
// them as well.
package cqlerr
