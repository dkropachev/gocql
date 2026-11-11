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
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// maxRateLimitKeys bounds the limiter's memory; stale keys are pruned once it is exceeded.
const maxRateLimitKeys = 4096

// RateLimitHandler passes the first Info-or-higher record with a given level, message and host
// once per window and counts the rest. The next record that passes carries the count as
// KeySuppressed. Debug records are never limited: enabling Debug asks for everything.
type RateLimitHandler struct {
	next  slog.Handler
	state *rateLimitState
	scope string
}

type rateLimitState struct {
	now    func() time.Time
	seen   map[string]*rateLimitEntry
	window time.Duration
	mu     sync.Mutex
}

type rateLimitEntry struct {
	until      time.Time
	suppressed int
}

// NewRateLimitHandler wraps next. A window of zero or less disables limiting.
func NewRateLimitHandler(next slog.Handler, window time.Duration) *RateLimitHandler {
	return newRateLimitHandler(next, window, time.Now)
}

func newRateLimitHandler(next slog.Handler, window time.Duration, now func() time.Time) *RateLimitHandler {
	return &RateLimitHandler{next: next, state: &rateLimitState{window: window, now: now, seen: map[string]*rateLimitEntry{}}}
}

func (h *RateLimitHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.next.Enabled(ctx, level)
}

func (h *RateLimitHandler) Handle(ctx context.Context, r slog.Record) error {
	if r.Level < slog.LevelInfo || h.state.window <= 0 {
		return h.next.Handle(ctx, r)
	}
	key := h.key(r)
	suppressed, pass := h.state.admit(key)
	if !pass {
		return nil
	}
	if suppressed > 0 {
		r = r.Clone()
		r.AddAttrs(slog.Int(KeySuppressed, suppressed))
	}
	return h.next.Handle(ctx, r)
}

func (h *RateLimitHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	scope := h.scope
	for _, a := range attrs {
		if a.Key == KeyHost || a.Key == KeyShard {
			scope += "\x00" + a.Key + "=" + a.Value.String()
		}
	}
	return &RateLimitHandler{next: h.next.WithAttrs(attrs), state: h.state, scope: scope}
}

func (h *RateLimitHandler) WithGroup(name string) slog.Handler {
	return &RateLimitHandler{next: h.next.WithGroup(name), state: h.state, scope: h.scope + "\x00g=" + name}
}

func (h *RateLimitHandler) key(r slog.Record) string {
	key := fmt.Sprintf("%s\x00%d\x00%s", h.scope, r.Level, r.Message)
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == KeyHost || a.Key == KeyShard {
			key += "\x00" + a.Key + "=" + a.Value.String()
		}
		return true
	})
	return key
}

func (s *rateLimitState) admit(key string) (suppressed int, pass bool) {
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.seen[key]; ok {
		if now.Before(e.until) {
			e.suppressed++
			return 0, false
		}
		suppressed = e.suppressed
		e.suppressed = 0
		e.until = now.Add(s.window)
		return suppressed, true
	}
	if len(s.seen) >= maxRateLimitKeys {
		for k, e := range s.seen {
			if !now.Before(e.until) {
				delete(s.seen, k)
			}
		}
	}
	if len(s.seen) < maxRateLimitKeys {
		s.seen[key] = &rateLimitEntry{until: now.Add(s.window)}
	}
	return 0, true
}
