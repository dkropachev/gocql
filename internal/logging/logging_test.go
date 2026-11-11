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
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeClock struct {
	now time.Time
	mu  sync.Mutex
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

type panicHandler struct{ inEnabled bool }

func (h panicHandler) Enabled(context.Context, slog.Level) bool {
	if h.inEnabled {
		panic("enabled")
	}
	return true
}
func (panicHandler) Handle(context.Context, slog.Record) error { panic("handle") }
func (panicHandler) WithAttrs([]slog.Attr) slog.Handler        { panic("attrs") }
func (panicHandler) WithGroup(string) slog.Handler             { panic("group") }

type printfRecorder struct {
	lines []string
	mu    sync.Mutex
}

func (p *printfRecorder) Printf(format string, v ...any) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.lines = append(p.lines, fmt.Sprintf(format, v...))
}

func TestLogging(t *testing.T) {
	t.Parallel()

	t.Run("Or", func(t *testing.T) {
		t.Parallel()
		if Or(nil) != slog.Default() {
			t.Fatal("Or(nil) must be slog.Default()")
		}
		if Or(Nop()) != Nop() {
			t.Fatal("Or must keep a non-nil logger")
		}
	})

	t.Run("NopDiscards", func(t *testing.T) {
		t.Parallel()
		if Nop().Enabled(context.Background(), slog.LevelError) {
			t.Fatal("Nop must not be enabled at any level")
		}
	})

	t.Run("WrapTagsAndContainsPanics", func(t *testing.T) {
		t.Parallel()
		logger, out := NewCapture()
		Wrap(logger).Warn("hello", KeyHost, "h1")
		if got := out.String(); !strings.Contains(got, "logger=gocql") || !strings.Contains(got, "host=h1") {
			t.Fatalf("wrapped record = %q", got)
		}
		l := Wrap(slog.New(panicHandler{}))
		l.Error("x")
		l.With("a", 1).WithGroup("g").Error("x")
		Wrap(slog.New(panicHandler{inEnabled: true})).Error("x")
	})

	t.Run("RateLimit", func(t *testing.T) {
		t.Parallel()

		t.Run("CollapsesRepeatsAndReportsCount", func(t *testing.T) {
			t.Parallel()
			clock := &fakeClock{now: time.Unix(0, 0)}
			logger, out := NewCapture()
			l := slog.New(newRateLimitHandler(logger.Handler(), time.Second, clock.Now))
			for i := 0; i < 5; i++ {
				l.Warn("flap", KeyHost, "h1")
			}
			if n := len(out.Records()); n != 1 {
				t.Fatalf("records within window = %d, want 1", n)
			}
			clock.Advance(time.Second)
			l.Warn("flap", KeyHost, "h1")
			recs := out.Records()
			if len(recs) != 2 {
				t.Fatalf("records after window = %d, want 2", len(recs))
			}
			var suppressed int64 = -1
			recs[1].Attrs(func(a slog.Attr) bool {
				if a.Key == KeySuppressed {
					suppressed = a.Value.Int64()
				}
				return true
			})
			if suppressed != 4 {
				t.Fatalf("suppressed = %d, want 4", suppressed)
			}
		})

		t.Run("KeysByHostMessageAndLevel", func(t *testing.T) {
			t.Parallel()
			clock := &fakeClock{now: time.Unix(0, 0)}
			logger, out := NewCapture()
			l := slog.New(newRateLimitHandler(logger.Handler(), time.Minute, clock.Now))
			l.Warn("m", KeyHost, "h1")
			l.Warn("m", KeyHost, "h2")
			l.With(KeyHost, "h3").Warn("m")
			l.Error("m", KeyHost, "h1")
			l.Warn("other", KeyHost, "h1")
			l.Warn("m", KeyHost, "h1")
			l.With(KeyHost, "h3").Warn("m")
			if n := len(out.Records()); n != 5 {
				t.Fatalf("records = %d, want 5", n)
			}
		})

		t.Run("DebugIsNotLimited", func(t *testing.T) {
			t.Parallel()
			logger, out := NewCapture()
			l := slog.New(NewRateLimitHandler(logger.Handler(), time.Hour))
			for i := 0; i < 3; i++ {
				l.Debug("d")
			}
			if n := len(out.Records()); n != 3 {
				t.Fatalf("debug records = %d, want 3", n)
			}
		})

		t.Run("BoundedKeys", func(t *testing.T) {
			t.Parallel()
			clock := &fakeClock{now: time.Unix(0, 0)}
			h := newRateLimitHandler(slog.DiscardHandler, time.Second, clock.Now)
			l := slog.New(h)
			for i := 0; i < maxRateLimitKeys+100; i++ {
				l.Warn("m", KeyHost, i)
			}
			if n := len(h.state.seen); n > maxRateLimitKeys {
				t.Fatalf("tracked keys = %d, want <= %d", n, maxRateLimitKeys)
			}
			clock.Advance(time.Second)
			l.Warn("m", KeyHost, "fresh")
			if n := len(h.state.seen); n > maxRateLimitKeys {
				t.Fatalf("tracked keys after prune = %d, want <= %d", n, maxRateLimitKeys)
			}
		})
	})

	t.Run("PrintfHandler", func(t *testing.T) {
		t.Parallel()
		p := &printfRecorder{}
		l := slog.New(NewPrintfHandler(p, slog.LevelInfo))
		l.Debug("hidden")
		l.Warn("shown", KeyShard, 3)
		if len(p.lines) != 1 {
			t.Fatalf("lines = %q, want one", p.lines)
		}
		line := p.lines[0]
		if strings.Contains(line, "time=") || strings.HasSuffix(line, "\n") ||
			!strings.Contains(line, `level=WARN msg=shown shard=3`) {
			t.Fatalf("line = %q", line)
		}
	})
}
