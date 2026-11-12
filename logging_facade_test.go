// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"log/slog"
	"testing"

	"github.com/scylladb/gocql/v2/config"
)

func TestLoggingFacade(t *testing.T) {
	t.Parallel()

	t.Run("WithLoggerBuilds", func(t *testing.T) {
		t.Parallel()
		if _, err := config.New("127.0.0.1").WithLogger(slog.New(slog.DiscardHandler)).Build(); err != nil {
			t.Fatal(err)
		}
		if _, err := config.New("127.0.0.1").WithLogger(nil).Build(); err != nil {
			t.Fatal(err)
		}
	})
}
