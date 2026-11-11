/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Package logging adapts the driver to log/slog.
//
// Level policy:
//   - Debug: per-connection detail (shard dialing, address translation, parsed extensions).
//   - Info: cluster state changes the operator may want to see (topology, schema, pool resize).
//   - Warn: degraded but continuing (unsupported option, unparsable server value, dropped event).
//   - Error: a subsystem failed and will not recover by itself.
//
// Context goes into attributes named by the Key constants, never into the message.
package logging

import (
	"log/slog"
	"time"
)

// DefaultRateLimit is the window within which repeated Info and higher records are collapsed.
const DefaultRateLimit = 10 * time.Second

var nop = slog.New(slog.DiscardHandler)

// Nop returns a logger that discards everything.
func Nop() *slog.Logger { return nop }

// Or returns l, or slog.Default() when l is nil.
func Or(l *slog.Logger) *slog.Logger {
	if l == nil {
		return slog.Default()
	}
	return l
}

// Wrap prepares a user-supplied logger for driver use: handler panics are contained,
// repeated records are throttled, and every record is tagged logger=gocql.
// A nil logger means slog.Default().
func Wrap(l *slog.Logger) *slog.Logger {
	h := Or(l).Handler()
	return slog.New(NewRateLimitHandler(SafeHandler{Next: h}, DefaultRateLimit)).With(KeyLogger, "gocql")
}
