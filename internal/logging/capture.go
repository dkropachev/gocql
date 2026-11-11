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
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
)

// Capture records everything logged through the logger returned by NewCapture. It is for tests.
type Capture struct {
	records []slog.Record
	buf     bytes.Buffer
	mu      sync.Mutex
}

// NewCapture returns a Debug-level logger and the Capture that records its output.
func NewCapture() (*slog.Logger, *Capture) {
	c := &Capture{}
	text := slog.NewTextHandler(captureWriter{c}, &slog.HandlerOptions{
		Level: slog.LevelDebug,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if len(groups) == 0 && a.Key == slog.TimeKey {
				return slog.Attr{}
			}
			return a
		},
	})
	return slog.New(captureHandler{Handler: text, c: c}), c
}

// String returns every captured record as logfmt lines.
func (c *Capture) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.String()
}

// Records returns the captured records without attributes added through With.
func (c *Capture) Records() []slog.Record {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]slog.Record, len(c.records))
	copy(out, c.records)
	return out
}

// Contains reports whether substr occurs, unescaped, in any captured message or attribute value.
func (c *Capture) Contains(substr string) bool {
	for _, r := range c.Records() {
		if strings.Contains(r.Message, substr) {
			return true
		}
		found := false
		r.Attrs(func(a slog.Attr) bool {
			found = strings.Contains(a.Value.String(), substr)
			return !found
		})
		if found {
			return true
		}
	}
	return false
}

type captureWriter struct{ c *Capture }

func (w captureWriter) Write(b []byte) (int, error) {
	w.c.mu.Lock()
	defer w.c.mu.Unlock()
	return w.c.buf.Write(b)
}

type captureHandler struct {
	slog.Handler
	c *Capture
}

func (h captureHandler) Handle(ctx context.Context, r slog.Record) error {
	h.c.mu.Lock()
	h.c.records = append(h.c.records, r.Clone())
	h.c.mu.Unlock()
	return h.Handler.Handle(ctx, r)
}

func (h captureHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return captureHandler{Handler: h.Handler.WithAttrs(attrs), c: h.c}
}

func (h captureHandler) WithGroup(name string) slog.Handler {
	return captureHandler{Handler: h.Handler.WithGroup(name), c: h.c}
}
