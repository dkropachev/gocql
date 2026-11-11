// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"fmt"
	"log/slog"
	"strings"
	"testing"
)

type printfLines []string

func (p *printfLines) Printf(format string, v ...any) { *p = append(*p, fmt.Sprintf(format, v...)) }

func TestLoggingFacade(t *testing.T) {
	t.Parallel()

	t.Run("PrintfLoggerFiltersAndFormats", func(t *testing.T) {
		t.Parallel()
		var lines printfLines
		l := NewPrintfLogger(&lines, slog.LevelWarn)
		l.Info("dropped")
		l.Warn("kept", "host", "10.0.0.1:9042")
		if len(lines) != 1 || !strings.Contains(lines[0], `level=WARN msg=kept host=10.0.0.1:9042`) {
			t.Fatalf("lines = %q", lines)
		}
	})

	t.Run("WithLoggerBuilds", func(t *testing.T) {
		t.Parallel()
		if _, err := NewConfig("127.0.0.1").WithLogger(slog.New(slog.DiscardHandler)).Build(); err != nil {
			t.Fatal(err)
		}
		if _, err := NewConfig("127.0.0.1").WithLogger(nil).Build(); err != nil {
			t.Fatal(err)
		}
	})
}
