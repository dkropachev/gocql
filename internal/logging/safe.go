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

package logging

import (
	"context"
	"log/slog"
)

// SafeHandler contains panics raised by Next so a faulty user handler cannot break the driver.
type SafeHandler struct{ Next slog.Handler }

func (h SafeHandler) Enabled(ctx context.Context, level slog.Level) (enabled bool) {
	defer func() {
		if recover() != nil {
			enabled = false
		}
	}()
	return h.Next.Enabled(ctx, level)
}

func (h SafeHandler) Handle(ctx context.Context, r slog.Record) (err error) {
	defer func() { _ = recover() }()
	return h.Next.Handle(ctx, r)
}

func (h SafeHandler) WithAttrs(attrs []slog.Attr) (out slog.Handler) {
	defer func() {
		if recover() != nil {
			out = h
		}
	}()
	return SafeHandler{Next: h.Next.WithAttrs(attrs)}
}

func (h SafeHandler) WithGroup(name string) (out slog.Handler) {
	defer func() {
		if recover() != nil {
			out = h
		}
	}()
	return SafeHandler{Next: h.Next.WithGroup(name)}
}
