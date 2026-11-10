// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/runtimeerrors"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/internal/wire"
)

func TestInputOwner(t *testing.T) {
	t.Run("Attach", func(t *testing.T) {
		t.Run("AdmissionCloseRace", func(t *testing.T) {
			for i := 0; i < 30; i++ {
				s, rt := attachmentSession()
				parent := InternalNewInputOwner()
				if err := parent.InternalAdmit(); err != nil {
					t.Fatal(err)
				}
				logical, cancel := attachmentLogical(s, parent)
				logical.installStop(stopFunc(logical.releaseInput))
				start := make(chan struct{})
				admitted := make(chan error, 1)
				closed := make(chan struct{})
				go func() { <-start; admitted <- rt.registerExecution(logical, parent) }()
				go func() { <-start; _ = s.Close(); close(closed) }()
				close(start)
				err := <-admitted
				if err != nil {
					if !errors.Is(err, runtimeerrors.ErrClosed) {
						t.Fatal(err)
					}
					parent.InternalRelease()
				}
				<-closed
				cancel()
				parent.InternalClose()
				ctx, stop := context.WithTimeout(context.Background(), time.Second)
				err = parent.InternalWait(ctx)
				stop()
				if err != nil {
					t.Fatal("admission escaped Close drain", err)
				}
			}
		})

		t.Run("PrecedesStartupAndDrainsFailure", func(t *testing.T) {
			config := settings.DefaultConfig("127.0.0.1")
			config.Metadata.InitialDiscovery = false
			config.Metadata.PeriodicRefresh = 0
			config.ReportConfiguration = false
			config.ConnectTimeout = 25 * time.Millisecond
			dialError := errors.New("attachment startup fault")
			var rt *InternalSessionRuntime
			stopped := make(chan struct{})
			config.DialHost = func(context.Context, endpoint.DialTarget) (net.Conn, error) {
				if rt == nil || rt.Core.CurrentExecution() != rt {
					t.Error("startup preceded execution attachment")
				}
				return nil, dialError
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			session, err := cluster.InternalConnectCluster(ctx, config, func(s *cluster.Core, c settings.Config) *settings.Config {
				rt = InternalRuntimeFor(s)
				if InternalRuntimeFor(s) != rt {
					t.Error("execution attachment replaced")
				}
				rt.config = c
				rt.InternalSetProfile(c.DefaultProfile)
				if !rt.beginPreparedWorker(s) {
					t.Fatal("initial prepare worker rejected")
				}
				go func() { defer rt.prepareWorkers.Done(); <-cluster.SessionData(s).InternalCtx.Done(); close(stopped) }()
				return &rt.config
			})
			if err == nil || session != nil || !strings.Contains(err.Error(), dialError.Error()) {
				t.Fatalf("startup result session=%p err=%v", session, err)
			}
			if rt == nil || !rt.Core.Closed() {
				t.Fatal("failed startup retained a live owner")
			}
			select {
			case <-stopped:
			default:
				t.Fatal("failed startup returned before joining prepare worker")
			}
			rt.prepareWorkersMu.Lock()
			closed := rt.prepareWorkersClosed
			rt.prepareWorkersMu.Unlock()
			if !closed {
				t.Fatal("failed startup left prepare admission open")
			}
		})

		t.Run("StopsAllBeforeJoiningWriter", func(t *testing.T) {
			s, rt := attachmentSession()
			parent := InternalNewInputOwner()
			if err := parent.InternalAdmit(); err != nil {
				t.Fatal(err)
			}
			if err := parent.InternalAdmit(); err != nil {
				t.Fatal(err)
			}
			first, cancelFirst := attachmentLogical(s, parent)
			defer cancelFirst()
			second, cancelSecond := attachmentLogical(s, parent)
			defer cancelSecond()
			if err := rt.registerExecution(first, parent); err != nil {
				t.Fatal(err)
			}
			if err := rt.registerExecution(second, parent); err != nil {
				t.Fatal(err)
			}
			writer := first.request.InternalOwner.InternalWriterLease(wire.Chain{})
			firstStopped, secondStopped := make(chan struct{}), make(chan struct{})
			first.installStop(stopFunc(func() { first.releaseInput(); close(firstStopped) }))
			second.installStop(stopFunc(func() { second.releaseInput(); close(secondStopped) }))
			parent.InternalClose()
			done := make(chan struct{})
			go func() { _ = s.Close(); close(done) }()
			for _, stopped := range []<-chan struct{}{firstStopped, secondStopped} {
				select {
				case <-stopped:
				case <-time.After(time.Second):
					t.Fatal("Close joined one owner before stopping all owners")
				}
			}
			select {
			case <-done:
				t.Fatal("Close retired an acknowledged writer before completion")
			default:
			}
			writer.Release()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("Close failed to join retired owners")
			}
			if err := parent.InternalWait(context.Background()); err != nil {
				t.Fatal(err)
			}
			rt.InternalMu.Lock()
			remaining := len(rt.executions)
			rt.InternalMu.Unlock()
			if remaining != 0 {
				t.Fatal("execution registry retained drained owners")
			}
		})
	})

	t.Run("WriterLease", func(t *testing.T) {
		// TestInputOwner/WriterLease/ReusedOnlyWhenFree checks that the embedded
		// writer lease is handed out again once released, that a second writer
		// while it is held gets a lease of its own, and that every lease holds and
		// returns one owner reference.
		t.Run("ReusedOnlyWhenFree", func(t *testing.T) {
			o := InternalNewInputOwner()
			first := o.InternalWriterLease(wire.Chain{})
			second := o.InternalWriterLease(wire.Chain{})
			if first == second {
				t.Fatal("a held writer lease was handed out again")
			}
			if first != &o.writer.Lease {
				t.Fatal("first writer did not use the embedded lease")
			}
			if got := o.refs(); got != 2 {
				t.Fatal("writer references", got)
			}
			second.Release()
			first.Release()
			if got := o.refs(); got != 0 {
				t.Fatal("references after release", got)
			}
			again := o.InternalWriterLease(wire.Chain{})
			if again != first || !again.Alive() {
				t.Fatal("released embedded lease not reused")
			}
			again.Release()
			o.InternalClose()
			select {
			case <-o.done:
			default:
				t.Fatal("owner did not drain")
			}
		})
	})
}
