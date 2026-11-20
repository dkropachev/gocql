// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/logging"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/metadata"
)

// TestMetadataCallsHonorContext stalls the system queries behind
// RefreshMetadata and AwaitSchemaAgreement, whose own timeouts are a minute,
// and checks that each returns when the caller's context ends.
func TestMetadataCallsHonorContext(t *testing.T) {
	for name, call := range map[string]func(*Session, context.Context) error{
		"RefreshMetadata":      (*Session).RefreshMetadata,
		"AwaitSchemaAgreement": (*Session).AwaitSchemaAgreement,
	} {
		t.Run(name, func(t *testing.T) {
			srv := newControlTestServer(t, nil)
			config := settings.DefaultConfig(srv.Address)
			config.Protocol = 4
			config.Metadata.Mode = metadata.SchemaNone
			config.Metadata.InitialDiscovery = false
			config.Metadata.AutomaticAgreement = false
			config.Metadata.RequestTimeout = time.Minute
			config.Metadata.AgreementTimeout = time.Minute
			config.Pool.AdvancedShardAwareness.Enabled = false
			config.Pool.ConnectionsPerHost = 1
			config.Logger = logging.Nop()
			s, err := Connect(context.Background(), config)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			atomic.StoreInt32(&srv.stallSystemQueries, 1)
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			start := time.Now()
			err = call(s, ctx)
			if elapsed := time.Since(start); elapsed > 5*time.Second {
				t.Fatalf("returned after %v", elapsed)
			}
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("err %v, want the context's", err)
			}
		})
	}
}
