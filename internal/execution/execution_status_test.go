// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"context"
	"encoding/binary"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/cqlerr"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/routing"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/retry"
)

// serverError is a server ERROR response with a code-specific body.
func serverError(t *testing.T, stream, code int, body []byte) []byte {
	t.Helper()
	b := binary.BigEndian.AppendUint32(nil, uint32(code))
	b = binary.BigEndian.AppendUint16(b, 4)
	b = append(b, "test"...)
	response := typedTestResponse(t, 4, stream, append(b, body...))
	response[4] = 0 // OpError
	return response
}

func unavailableBody() []byte {
	b := binary.BigEndian.AppendUint16(nil, 4) // QUORUM
	b = binary.BigEndian.AppendUint32(b, 2)
	return binary.BigEndian.AppendUint32(b, 1)
}

func writeTimeoutBody(received int, writeType string) []byte {
	b := binary.BigEndian.AppendUint16(nil, 4)
	b = binary.BigEndian.AppendUint32(b, uint32(received))
	b = binary.BigEndian.AppendUint32(b, 2)
	b = binary.BigEndian.AppendUint16(b, uint16(len(writeType)))
	return append(b, writeType...)
}

// recordingRetry retries on the same host until attempt limit and records
// the status the driver reported for each failed attempt.
type recordingRetry struct {
	mu     sync.Mutex
	limit  int
	status []cqlerr.ExecutionStatus
}

func (p *recordingRetry) Decide(a retry.Attempt) retry.Decision {
	p.mu.Lock()
	p.status = append(p.status, a.ExecutionStatus)
	p.mu.Unlock()
	if a.Number >= p.limit {
		return retry.Decision{}
	}
	return retry.Decision{Action: retry.Same}
}

// neverSpeculate routes a request through the general execution path without
// launching speculative attempts.
type neverSpeculate struct{}

func (neverSpeculate) Delay(int, routing.MetricsSnapshot) (time.Duration, bool) { return 0, false }

func TestExecutionStatusAggregation(t *testing.T) {
	t.Run("Direct", func(t *testing.T) { testExecutionStatusAggregation(t, false) })
	t.Run("General", func(t *testing.T) { testExecutionStatusAggregation(t, true) })
}

func testExecutionStatusAggregation(t *testing.T, general bool) {
	run := func(t *testing.T, ctx context.Context, policy retry.Policy, responses func(attempt, stream int) []byte, attempts int) error {
		t.Helper()
		s, conns, peers := runtimeTestSession(t, 1)
		profile := settings.InternalDefaultProfile()
		profile.Retry = policy
		if general {
			profile.Speculation = neverSpeculate{}
		}
		go func() {
			for attempt := 0; attempt < attempts; attempt++ {
				stream := typedTestReadRequest(t, conns[0], peers[0])
				response := responses(attempt, stream)
				if response == nil {
					continue // leave the attempt in flight
				}
				if _, err := peers[0].Write(response); err != nil {
					return
				}
			}
		}()
		bound := runtimeQuery(t)
		defer bound.Close()
		page, err := RunPage(ctx, s, bound, model.StartPage(), testOptions(Options{Profile: new(profile)}))
		if page != nil {
			page.Close()
		}
		if err == nil {
			t.Fatal("request succeeded")
		}
		return err
	}
	requestError := func(t *testing.T, err error) *cqlerr.ExecutionError {
		t.Helper()
		failure, ok := errors.AsType[*cqlerr.ExecutionError](err)
		if !ok {
			t.Fatalf("not an ExecutionError: %v", err)
		}
		if got := cqlerr.ExecutionStatusOf(err); got != failure.ExecutionStatus() {
			t.Fatalf("ExecutionStatusOf %v, ExecutionError %v", got, failure.ExecutionStatus())
		}
		return failure
	}

	t.Run("RejectedEveryAttempt", func(t *testing.T) {
		policy := &recordingRetry{limit: 3}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err := run(t, ctx, policy, func(_, stream int) []byte {
			return serverError(t, stream, protocol.ErrCodeUnavailable, unavailableBody())
		}, 3)
		if got := requestError(t, err).ExecutionStatus(); got != cqlerr.NotExecuted {
			t.Fatalf("status %v: %v", got, err)
		}
		for i, status := range policy.status {
			if status != cqlerr.NotExecuted {
				t.Fatalf("attempt %d reported %v", i+1, status)
			}
		}
	})

	t.Run("WorstAttemptWins", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err := run(t, ctx, &recordingRetry{limit: 2}, func(attempt, stream int) []byte {
			if attempt == 0 {
				return serverError(t, stream, protocol.ErrCodeWriteTimeout, writeTimeoutBody(0, "SIMPLE"))
			}
			return serverError(t, stream, protocol.ErrCodeUnavailable, unavailableBody())
		}, 2)
		if got := requestError(t, err).ExecutionStatus(); got != cqlerr.MaybeExecuted {
			t.Fatalf("status %v: %v", got, err)
		}
		if _, ok := errors.AsType[*cqlerr.RequestErrUnavailable](err); !ok {
			t.Fatalf("last server error lost: %v", err)
		}
		failure := requestError(t, err)
		if len(failure.Attempts) != 2 || failure.Attempts[0].Status != cqlerr.MaybeExecuted || failure.Attempts[1].Status != cqlerr.NotExecuted || failure.Attempts[0].HostID == (cqlerr.AttemptFailure{}).HostID {
			t.Fatalf("attempt history %+v", failure.Attempts)
		}
	})

	t.Run("PartialWrite", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err := run(t, ctx, &recordingRetry{limit: 1}, func(_, stream int) []byte {
			return serverError(t, stream, protocol.ErrCodeWriteTimeout, writeTimeoutBody(1, "SIMPLE"))
		}, 1)
		if got := requestError(t, err).ExecutionStatus(); got != cqlerr.PartiallyExecuted {
			t.Fatalf("status %v: %v", got, err)
		}
	})

	t.Run("CancelledInFlightKeepsCause", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sent := make(chan struct{})
		err := run(t, ctx, &recordingRetry{limit: 3}, func(attempt, stream int) []byte {
			if attempt == 0 {
				return serverError(t, stream, protocol.ErrCodeOverloaded, nil)
			}
			close(sent)
			go cancel()
			return nil
		}, 2)
		<-sent
		if got := requestError(t, err).ExecutionStatus(); got != cqlerr.MaybeExecuted {
			t.Fatalf("status %v: %v", got, err)
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation lost: %v", err)
		}
		if server, ok := errors.AsType[cqlerr.RequestError](err); !ok || server.GetCode() != cqlerr.ErrCodeOverloaded {
			t.Fatalf("earlier server error lost: %v", err)
		}
	})
}

func TestExecutionStatusBeforeAttempt(t *testing.T) {
	for _, general := range []bool{false, true} {
		s, _, _ := runtimeTestSession(t, 1)
		profile := settings.InternalDefaultProfile()
		if general {
			profile.Speculation = neverSpeculate{}
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		bound := runtimeQuery(t)
		page, err := RunPage(ctx, s, bound, model.StartPage(), testOptions(Options{Profile: new(profile)}))
		if page != nil {
			page.Close()
		}
		bound.Close()
		if err == nil {
			t.Fatal("cancelled request succeeded")
		}
		if got := cqlerr.ExecutionStatusOf(err); got != cqlerr.NotExecuted {
			t.Errorf("general=%t: status %v: %v (%T)", general, got, err, err)
		}
	}
}
