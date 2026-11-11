/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2012, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package session

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/logging"
	"github.com/scylladb/gocql/v2/internal/model"
	implProtocol "github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/runtimeerrors"
)

func TestClusterSettings(t *testing.T) {
	t.Run("Exec", func(t *testing.T) {
		t.Run("Closed", func(t *testing.T) {
			t.Skip("Skipping the execution of TestClusterSettings/Exec/Closed for now to try to concentrate on more important test failures on Travis")

			srv := NewTestServer(t, defaultProto, context.Background())
			defer srv.Stop()

			session, err := newTestSession(defaultProto, srv.Address)
			if err != nil {
				t.Fatalf("0x%x: newClusterSettings: %v", defaultProto, err)
			}

			session.Close()

			if err := execTestStatement(context.Background(), session, "void", CompiledOptions{}); err != runtimeerrors.ErrSessionClosed {
				t.Fatalf("0x%x: expected %#v, got %#v", defaultProto, runtimeerrors.ErrSessionClosed, err)
			}
		})

		t.Run("ContextCanceledBeforeExec", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			var reqCount uint64

			srv := newTestServerOpts{
				addr:     "127.0.0.1:0",
				protocol: defaultProto,
				recvHook: func(f *implProtocol.Framer) {
					if f.Header.Op == frm.OpStartup || f.Header.Op == frm.OpOptions {
						// ignore statup and heartbeat messages
						return
					}
					atomic.AddUint64(&reqCount, 1)
				},
			}.newServer(t, ctx)

			defer srv.Stop()

			cluster := testCluster(defaultProto, srv.Address)
			cluster.Timeout = 5 * time.Second
			db, err := cluster.Open()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()

			startupRequestCount := atomic.LoadUint64(&reqCount)

			ctx, cancel = context.WithCancel(ctx)
			cancel()

			err = execTestStatement(ctx, db, "timeout", CompiledOptions{})
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("expected to get context cancel error: %v got %v", context.Canceled, err)
			}

			// Queries are executed by separate goroutine and we don't have a synchronization point that would allow us to
			// check if a request was sent or not.
			// Fall back to waiting a little bit.
			time.Sleep(100 * time.Millisecond)

			queryRequestCount := atomic.LoadUint64(&reqCount) - startupRequestCount
			if queryRequestCount != 0 {
				t.Fatalf("expected that no request is sent to server, sent %d requests", queryRequestCount)
			}
		})

		t.Run("ContextTimeout", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			srv := NewTestServer(t, defaultProto, ctx)
			defer srv.Stop()

			cluster := testCluster(defaultProto, srv.Address)
			cluster.Timeout = 5 * time.Second
			db, err := cluster.Open()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()

			ctx, cancel = context.WithCancel(ctx)
			cancel()

			err = execTestStatement(ctx, db, "timeout", CompiledOptions{})
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("expected to get context cancel error: %v got %v", context.Canceled, err)
			}
		})

		t.Run("FrameHeaderObserver", func(t *testing.T) {
			srv := NewTestServer(t, defaultProto, context.Background())
			defer srv.Stop()

			cluster := testCluster(defaultProto, srv.Address)
			cluster.NumConns = 1
			observer := &recordingFrameHeaderObserver{t: t}
			cluster.FrameHeaderObserver = observer

			db, err := cluster.Open()
			if err != nil {
				t.Fatal(err)
			}

			if err := execTestStatement(context.Background(), db, "void", CompiledOptions{}); err != nil {
				t.Fatal(err)
			}

			frames := observer.getFrames()
			expFrames := []frm.Op{frm.OpSupported, frm.OpReady, frm.OpResult}
			if len(frames) != len(expFrames) {
				t.Fatalf("Expected to receive %d frames, instead received %d", len(expFrames), len(frames))
			}

			for i, op := range expFrames {
				if op != frames[i].Opcode {
					t.Fatalf("expected frame %d to be %v got %v", i, op, frames[i])
				}
			}
			voidResultFrame := frames[2]
			if voidResultFrame.Length != int32(4) {
				t.Fatalf("Expected to receive frame with body length 4, instead received body length %d", voidResultFrame.Length)
			}
		})

		t.Run("QueryTimeout", func(t *testing.T) {
			srv := NewTestServer(t, defaultProto, context.Background())
			defer srv.Stop()

			cluster := testCluster(defaultProto, srv.Address)
			// Set the timeout arbitrarily low so that the query hits the timeout in a
			// timely manner.
			cluster.Timeout = 1 * time.Millisecond

			db, err := cluster.Open()
			if err != nil {
				t.Fatalf("newClusterSettings: %v", err)
			}
			defer db.Close()

			ch := make(chan error, 1)

			go func() {
				err := execTestStatement(context.Background(), db, "timeout", CompiledOptions{})
				if err != nil {
					ch <- err
					return
				}
				t.Errorf("err was nil, expected to get a timeout after %v", implCluster.SessionData(db).InternalCfg.Timeout)
			}()

			select {
			case err := <-ch:
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("expected to get %v for timeout got %v", runtimeerrors.ErrTimeoutNoResponse, err)
				}
			case <-time.After(40*time.Millisecond + implCluster.SessionData(db).InternalCfg.Timeout):
				// ensure that the query goroutines have been scheduled
				t.Fatalf("query did not timeout after %v", implCluster.SessionData(db).InternalCfg.Timeout)
			}
		})

		t.Run("QueryTimeoutClose", func(t *testing.T) {
			srv := NewTestServer(t, defaultProto, context.Background())
			defer srv.Stop()

			cluster := testCluster(defaultProto, srv.Address)
			// Set the timeout arbitrarily low so that the query hits the timeout in a
			// timely manner.
			cluster.Timeout = 1000 * time.Millisecond
			cluster.NumConns = 1

			db, err := cluster.Open()
			if err != nil {
				t.Fatalf("newClusterSettings: %v", err)
			}

			ch := make(chan error)
			go func() {
				err := execTestStatement(context.Background(), db, "timeout", CompiledOptions{})
				ch <- err
			}()
			// ensure that the above goroutine gets sheduled
			time.Sleep(50 * time.Millisecond)

			db.Close()
			select {
			case err = <-ch:
			case <-time.After(1 * time.Second):
				t.Fatal("timedout waiting to get a response once cluster is closed")
			}

			if !errors.Is(err, context.Canceled) {
				t.Fatalf("expected to get %v or an error wrapping it, got %v", runtimeerrors.ErrConnectionClosed, err)
			}
		})

		t.Run("QueryTimeoutReuseStream", func(t *testing.T) {
			t.Skip("no longer tests anything")
			// TODO(zariel): move this to conn test, we really just want to check what
			// happens when a conn is

			srv := NewTestServer(t, defaultProto, context.Background())
			defer srv.Stop()

			cluster := testCluster(defaultProto, srv.Address)
			// Set the timeout arbitrarily low so that the query hits the timeout in a
			// timely manner.
			cluster.Timeout = 1 * time.Millisecond
			cluster.NumConns = 1

			db, err := cluster.Open()
			if err != nil {
				t.Fatalf("newClusterSettings: %v", err)
			}
			defer db.Close()

			execTestStatement(context.Background(), db, "slow", CompiledOptions{})

			err = execTestStatement(context.Background(), db, "void", CompiledOptions{})
			if err != nil {
				t.Fatal(err)
			}
		})

		t.Run("SkipMetadata", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			srv := NewTestServer(t, implProtocol.ProtoVersion4, ctx)
			defer srv.Stop()

			cfg := testCluster(implProtocol.ProtoVersion4, srv.Address)
			cfg.DisableSkipMetadata = false

			db, err := cfg.Open()
			if err != nil {
				t.Fatalf("newClusterSettings: %v", err)
			}
			defer db.Close()

			if err := execTestStatement(context.Background(), db, "select nometadata", CompiledOptions{}); err != nil {
				t.Fatalf("expected no error got: %v", err)
			}

			if err := execTestStatement(context.Background(), db, "select metadata", CompiledOptions{}); err != nil {
				t.Fatalf("expected no error got: %v", err)
			}
		})

		t.Run("Timeout", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())

			srv := NewTestServer(t, defaultProto, ctx)
			defer srv.Stop()

			db, err := newTestSession(defaultProto, srv.Address)
			if err != nil {
				t.Fatalf("newClusterSettings: %v", err)
			}
			defer db.Close()

			var wg sync.WaitGroup
			wg.Add(1)

			go func() {
				defer wg.Done()

				select {
				case <-time.After(5 * time.Second):
					t.Errorf("no timeout")
				case <-ctx.Done():
				}
			}()

			if err := execTestStatement(ctx, db, "kill", CompiledOptions{}); err == nil {
				t.Fatal("expected error got nil")
			}
			cancel()

			wg.Wait()
		})
	})

	t.Run("Open", func(t *testing.T) {
		t.Run("DNSLookupConnected", func(t *testing.T) {
			log, logOut := logging.NewCapture()

			// Override the default DNS resolver and restore at the end

			srv := NewTestServer(t, defaultProto, context.Background())
			defer srv.Stop()

			// Use bare IP (no port) so all entries are portless; the driver falls back
			// to cfg.Port, which is where the test server is bound.
			cluster := implCluster.InternalNewClusterSettings("cassandra1.invalid", "127.0.0.1", "cassandra2.invalid")
			cluster.Port = srv.port()
			cluster.Logger = log
			cluster.ProtoVersion = int(defaultProto)
			cluster.Data().InternalDisableControlConn = true
			cluster.DNSResolver = brokenDNSResolver{}

			// open() should attempt to resolve the DNS name "cassandraX.invalid"
			// and fail, but continue to connect via 127.0.0.1
			_, err := cluster.Open()
			if err != nil {
				t.Fatal("open() should have connected")
			}

			if !strings.Contains(logOut.String(), "failed to resolve endpoint") {
				t.Fatalf("Expected to receive 'failed to resolve endpoint' log message  - got '%s' instead", logOut.String())
			}
		})

		t.Run("DNSLookupError", func(t *testing.T) {
			log, logOut := logging.NewCapture()

			// Override the default DNS resolver and restore at the end
			hosts := []string{"cassandra1.invalid", "cassandra2.invalid"}

			cluster := implCluster.InternalNewClusterSettings(hosts...)
			cluster.Logger = log
			cluster.ProtoVersion = int(defaultProto)
			cluster.Data().InternalDisableControlConn = true
			cluster.DNSResolver = brokenDNSResolver{}

			// open() should attempt to resolve each DNS name "cassandraX.invalid"
			// and fail since it could not resolve any dns entries
			_, err := cluster.Open()
			if err == nil {
				t.Fatal("open() should have returned an error")
			}

			if !strings.Contains(logOut.String(), "failed to resolve endpoint") {
				t.Fatalf("Expected to receive 'failed to resolve endpoint' log message  - got '%s' instead", logOut.String())
			}

			if !strings.Contains(err.Error(), "unable to create session: failed to resolve any of the provided hostnames") {
				t.Fatalf("Expected open() to fail with error message that contains 'unable to create session: failed to resolve any of the provided hostnames'")
			}

			for _, host := range hosts {
				expected := fmt.Sprintf("failed to resolve endpoint \"%s\": lookup  on %s: this error comes from mocked broken resolver", host, host)
				if !strings.Contains(err.Error(), expected) {
					t.Fatalf("Expected to fail with error message that contains '%s'", expected)
				}
			}
		})

		t.Run("InitialRetryPolicy", func(t *testing.T) {
			t.Parallel()

			tcase := []struct {
				NumRetries               int
				ProtoVersion             int
				ExpectedGetIntervalCalls []int
				ExpectedErr              string
			}{
				{
					NumRetries:               1,
					ProtoVersion:             implProtocol.ProtoVersion4,
					ExpectedGetIntervalCalls: nil,
					ExpectedErr:              "gocql: unable to create session: no host reachable during control connection: "},
				{
					NumRetries:               2,
					ProtoVersion:             implProtocol.ProtoVersion4,
					ExpectedGetIntervalCalls: []int{1},
					ExpectedErr:              "gocql: unable to create session: no host reachable during control connection: "},
				{
					NumRetries:               3,
					ProtoVersion:             implProtocol.ProtoVersion4,
					ExpectedGetIntervalCalls: []int{1, 2},
					ExpectedErr:              "gocql: unable to create session: no host reachable during control connection: "},
			}

			for id := range tcase {
				tc := tcase[id]
				t.Run(fmt.Sprintf("NumRetries=%d_ProtocolVersion=%d", tc.NumRetries, tc.ProtoVersion), func(t *testing.T) {
					t.Parallel()

					// Use a loopback address with a well-known closed port so the test
					// remains deterministic even when a local Cassandra-compatible
					// service is listening on 9042.
					cluster := implCluster.InternalNewClusterSettings("127.0.0.1:1")
					policy := &TestReconnectionPolicy{NumRetries: tc.NumRetries}
					cluster.InitialReconnectionPolicy = policy
					cluster.ProtoVersion = tc.ProtoVersion
					_, err := cluster.Open()
					if err == nil {
						t.Fatal("expected to get an error")
					}
					if !strings.Contains(err.Error(), tc.ExpectedErr) {
						t.Errorf("expected error to contain %q got %q", tc.ExpectedErr, err.Error())
					}
					if !cmp.Equal(tc.ExpectedGetIntervalCalls, policy.GetIntervalCalls) {
						t.Errorf("expected GetInterval calls to be (%+v) but was (%+v) instead", tc.ExpectedGetIntervalCalls, policy.GetIntervalCalls)
					}
				})
			}
		})

		t.Run("SSL", func(t *testing.T) {
			srv := NewSSLTestServer(t, defaultProto, context.Background())
			defer srv.Stop()

			db, err := createTestSslCluster(srv.Address, defaultProto, true).Open()
			if err != nil {
				t.Fatalf("0x%x: newClusterSettings: %v", defaultProto, err)
			}

			if err := execTestStatement(context.Background(), db, "void", CompiledOptions{}); err != nil {
				t.Fatalf("0x%x: %v", defaultProto, err)
			}
		})

		t.Run("SSLNoClientCert", func(t *testing.T) {
			srv := NewSSLTestServer(t, defaultProto, context.Background())
			defer srv.Stop()

			db, err := createTestSslCluster(srv.Address, defaultProto, false).Open()
			if err != nil {
				t.Fatalf("0x%x: newClusterSettings: %v", defaultProto, err)
			}

			if err := execTestStatement(context.Background(), db, "void", CompiledOptions{}); err != nil {
				t.Fatalf("0x%x: %v", defaultProto, err)
			}
		})

		t.Run("SSLRoundRobinPool", func(t *testing.T) {
			srv := NewSSLTestServer(t, defaultProto, context.Background())
			defer srv.Stop()

			cluster := createTestSslCluster(srv.Address, defaultProto, true)
			cluster.PoolConfig.HostSelectionPolicy = implCluster.RoundRobinHostPolicy()

			db, err := cluster.Open()
			if err != nil {
				t.Fatalf("failed to create new session: %v", err)
			}

			if err := execTestStatement(context.Background(), db, "void", CompiledOptions{}); err != nil {
				t.Fatalf("query failed due to error: %v", err)
			}
			db.Close()

			// wait for the pool to drain
			time.Sleep(100 * time.Millisecond)
			size := implCluster.SessionData(db).InternalPool.Size()
			if size != 0 {
				t.Fatalf("connection pool did not drain, still contains %d connections", size)
			}
		})

		t.Run("Simple", func(t *testing.T) {
			srv := NewTestServer(t, defaultProto, context.Background())
			defer srv.Stop()

			cluster := testCluster(defaultProto, srv.Address)
			db, err := cluster.Open()
			if err != nil {
				t.Fatalf("0x%x: newClusterSettings: %v", defaultProto, err)
			}

			if err := execTestStatement(context.Background(), db, "void", CompiledOptions{}); err != nil {
				t.Fatalf("0x%x: %v", defaultProto, err)
			}
		})

		t.Run("StartupTimeout", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			log, logOut := logging.NewCapture()

			srv := NewTestServer(t, defaultProto, ctx)
			defer srv.Stop()

			// Tell the server to never respond to Startup frame
			atomic.StoreInt32(&srv.TimeoutOnStartup, 1)

			startTime := time.Now()
			cluster := implCluster.InternalNewClusterSettings(srv.Address)
			cluster.Logger = log
			cluster.ProtoVersion = int(defaultProto)
			cluster.Data().InternalDisableControlConn = true
			// Set very long query connection timeout
			// so we know open() is using the ConnectTimeout
			cluster.Timeout = time.Second * 5
			cluster.ConnectTimeout = 600 * time.Millisecond

			// Create session should timeout during connect attempt
			_, err := cluster.Open()
			if err == nil {
				t.Fatal("open() should have returned a timeout error")
			}

			elapsed := time.Since(startTime)
			if elapsed > time.Second*5 {
				t.Fatal("ConnectTimeout is not respected")
			}

			if !errors.Is(err, implCluster.ErrNoConnectionsStarted) {
				t.Fatalf("Expected to receive no connections error - got '%s'", err)
			}

			if !strings.Contains(logOut.String(), "no response to connection startup within timeout") && !strings.Contains(logOut.String(), "no response received from cassandra within timeout period") {
				t.Fatalf("Expected to receive timeout log message  - got '%s'", logOut.String())
			}

			cancel()
		})
	})

	t.Run("Prepare", func(t *testing.T) {
		t.Run("BatchMetadataMultipleKeyspaceTables", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			srv := NewTestServer(t, implProtocol.ProtoVersion4, ctx)
			defer srv.Stop()

			cfg := testCluster(implProtocol.ProtoVersion4, srv.Address)
			db, err := cfg.Open()
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer db.Close()

			conn := db.GetConn()
			if conn == nil {
				t.Fatal("expected connection, got nil")
			}

			stmt := "BEGIN BATCH INSERT INTO ks1.tbl1 (col1) VALUES (?) INSERT INTO ks2.tbl2 (col2) VALUES (?) APPLY BATCH"
			info, err := conn.InternalExecuteTyped(ctx, &model.AttemptRequest{Statement: stmt, Prepare: true, Timeout: time.Second})
			if info != nil && info.Owner != nil {
				defer info.Owner.Release()
			}
			if err != nil {
				t.Fatalf("prepareStatement failed: %v", err)
			}

			if got := info.BindColumns().Len(); got != 2 {
				t.Fatalf("expected 2 request columns, got %d", got)
			}

			col0, _ := info.BindColumns().At(0)
			if col0.Keyspace != "ks1" || col0.Table != "tbl1" || col0.Name != "col1" {
				t.Fatalf("unexpected column 0: %+v", col0)
			}

			col1, _ := info.BindColumns().At(1)
			if col1.Keyspace != "ks2" || col1.Table != "tbl2" || col1.Name != "col2" {
				t.Fatalf("unexpected column 1: %+v", col1)
			}

			if info.Keyspace() != "" || "" != "" {
				t.Fatalf("expected empty prepared keyspace/table for mixed batch, got %q/%q", info.Keyspace(), "")
			}
		})
	})
}
