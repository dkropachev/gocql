package transport

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/wire"
)

type deadlineFailure struct {
	net.Conn
	err   error
	wrote bool
}

func (w *deadlineFailure) SetWriteDeadline(time.Time) error { return w.err }
func (w *deadlineFailure) Write(p []byte) (int, error)      { w.wrote = true; return len(p), nil }

func TestWriter(t *testing.T) {
	t.Run("Coalescer", func(t *testing.T) {
		t.Run("CancelsQueuedFrame", func(t *testing.T) {
			probe := new(queuedWriteProbe)
			quit, tick, armed := make(chan struct{}), make(chan time.Time), make(chan struct{})
			w := &InternalWriteCoalescer{InternalC: probe, InternalQuit: quit, InternalWriteCh: make(chan InternalWriteRequest)}
			flushed := make(chan struct{})
			go func() { w.InternalWriteFlusherImpl(tick, func() { close(armed) }); close(flushed) }()
			ctx, cancel := context.WithCancel(context.Background())
			chain, _ := wire.NewChain(wire.Borrow([]byte("must not be sent")))
			done := make(chan InternalWriteResult, 1)
			go func() {
				n, err := w.InternalWriteSpans(ctx, chain)
				done <- InternalWriteResult{InternalWritten: n, InternalErr: err}
			}()
			<-armed
			cancel()
			tick <- time.Now()
			result := <-done
			if result.InternalWritten != 0 || !errors.Is(result.InternalErr, context.Canceled) {
				t.Fatal(result)
			}
			close(quit)
			<-flushed
			if probe.Len() != 0 || probe.deadlines != 0 {
				t.Fatal("cancelled queued frame reached socket", probe.Len(), probe.deadlines)
			}
		})
		t.Run("LargeSpanCount", func(t *testing.T) {
			block := make([]byte, 1<<20)
			var spans []wire.Span
			for range 2048 {
				spans = append(spans, wire.Borrow(block))
			}
			spans = append(spans, wire.Borrow(block[:8]))
			chain, err := wire.NewChain(spans...)
			if err != nil {
				t.Fatal(err)
			}
			sink := new(countingWriter)
			writer := &InternalWriteCoalescer{InternalC: sink, InternalWriteCh: make(chan InternalWriteRequest), InternalQuit: make(chan struct{})}
			done := make(chan struct{})
			go func() {
				defer close(done)
				request := <-writer.InternalWriteCh
				var buffers net.Buffers
				request.chain.Each(func(s wire.Span) error { buffers = append(buffers, s.Bytes()); return nil })
				writer.InternalFlushSizes([]chan<- InternalWriteResult{request.resultChan}, buffers, []int64{request.chain.Len()})
			}()
			n, err := writer.InternalWriteSpans(context.Background(), chain)
			if err != nil || n != chain.Len() || sink.written != chain.Len() {
				t.Fatal("coalesced span count truncated", n, chain.Len(), err)
			}
			<-done
		})
	})

	t.Run("Scratch", func(t *testing.T) {
		t.Run("ReleasesPayloadsOnEveryExit", func(t *testing.T) {
			for _, outcome := range []string{"success", "short", "error", "panic"} {
				t.Run(outcome, func(t *testing.T) {
					sink := &scratchTestWriter{outcome: outcome}
					writer := &InternalDeadlineContextWriter{InternalW: sink, InternalSemaphore: make(chan struct{}, 1), InternalQuit: make(chan struct{})}
					chain := scratchTestChain(t, []byte("first"), []byte("second"))
					var err error
					var caught any
					func() {
						defer func() { caught = recover() }()
						_, err = writer.InternalWriteSpans(context.Background(), chain)
					}()
					switch outcome {
					case "success":
						if err != nil || sink.String() != "firstsecond" {
							t.Fatalf("write = %q, %v", sink.String(), err)
						}
					case "short":
						if !errors.Is(err, io.ErrShortWrite) {
							t.Fatal(err)
						}
					case "error":
						if !errors.Is(err, io.ErrClosedPipe) {
							t.Fatal(err)
						}
					case "panic":
						if caught == nil {
							t.Fatal("writer panic lost")
						}
					}
					if len(writer.InternalSemaphore) != 0 || len(writer.spanScratch) != 0 {
						t.Fatal("write did not release scratch/semaphore")
					}
					for _, part := range writer.spanScratch[:cap(writer.spanScratch)] {
						if part != nil {
							t.Fatal("idle scratch retains borrowed request payload")
						}
					}
					// A terminal write must leave the next request independent, even after
					// a partially written or panicking predecessor.
					sink.outcome = "success"
					sink.Reset()
					if _, err := writer.InternalWriteSpans(context.Background(), chain); err != nil || sink.String() != "firstsecond" {
						t.Fatalf("next write = %q, %v", sink.String(), err)
					}
				})
			}
		})
		t.Run("BoundAndConcurrentOrdering", func(t *testing.T) {
			sink := &scratchTestWriter{}
			writer := &InternalDeadlineContextWriter{InternalW: sink, InternalSemaphore: make(chan struct{}, 1), InternalQuit: make(chan struct{})}
			parts := make([][]byte, maxRetainedWriteSpans+1)
			for i := range parts {
				parts[i] = []byte{byte(i)}
			}
			if _, err := writer.InternalWriteSpans(context.Background(), scratchTestChain(t, parts...)); err != nil {
				t.Fatal(err)
			}
			if writer.spanScratch != nil {
				t.Fatal("oversized descriptor storage retained")
			}
			sink.Reset()
			var wg sync.WaitGroup
			for i := range 32 {
				chain := scratchTestChain(t, []byte{byte(i)}, []byte{byte(i)})
				wg.Add(1)
				go func() {
					defer wg.Done()
					if _, err := writer.InternalWriteSpans(context.Background(), chain); err != nil {
						t.Error(err)
					}
				}()
			}
			wg.Wait()
			if sink.Len() != 64 {
				t.Fatalf("wrong byte count: %d", sink.Len())
			}
			for i := 0; i < sink.Len(); i += 2 {
				if sink.Bytes()[i] != sink.Bytes()[i+1] {
					t.Fatal("concurrent request spans interleaved")
				}
			}
		})
	})

	t.Run("Deadline", func(t *testing.T) {
		t.Run("DisabledTimeoutClearsStartupDeadline", func(t *testing.T) {
			for _, mode := range []string{"shared", "span", "coalesced"} {
				t.Run(mode, func(t *testing.T) {
					client, peer := net.Pipe()
					defer client.Close()
					defer peer.Close()
					if err := client.SetWriteDeadline(time.Now().Add(-time.Second)); err != nil {
						t.Fatal(err)
					}
					done := make(chan error, 1)
					go func() {
						p := make([]byte, 3)
						_, err := io.ReadFull(peer, p)
						if string(p) != "abc" && err == nil {
							err = errors.New("wrong wire data")
						}
						done <- err
					}()
					writer := &InternalDeadlineContextWriter{InternalW: client, InternalSemaphore: make(chan struct{}, 1), InternalQuit: make(chan struct{})}
					writer.InternalSetWriteTimeout(0)
					var err error
					switch mode {
					case "shared":
						_, err = writer.InternalWriteContext(context.Background(), []byte("abc"))
					case "span":
						chain, _ := wire.NewChain(wire.Borrow([]byte("abc")))
						_, err = writer.InternalWriteSpans(context.Background(), chain)
					case "coalesced":
						result := make(chan InternalWriteResult, 1)
						coalescer := &InternalWriteCoalescer{InternalC: client}
						coalescer.InternalSetWriteTimeout(0)
						coalescer.InternalFlushSizes([]chan<- InternalWriteResult{result}, net.Buffers{[]byte("abc")}, []int64{3})
						err = (<-result).InternalErr
					}
					if err != nil {
						t.Fatal("disabled timeout retained expired startup deadline", err)
					}
					if err := <-done; err != nil {
						t.Fatal(err)
					}
				})
			}
		})
		t.Run("ZeroSettingError", func(t *testing.T) {
			for _, mode := range []string{"shared", "span", "coalesced"} {
				t.Run(mode, func(t *testing.T) {
					cause := errors.New("cannot clear deadline")
					socket := &deadlineFailure{err: cause}
					writer := &InternalDeadlineContextWriter{InternalW: socket, InternalSemaphore: make(chan struct{}, 1), InternalQuit: make(chan struct{})}
					var err error
					switch mode {
					case "shared":
						_, err = writer.InternalWriteContext(context.Background(), []byte("abc"))
					case "span":
						chain, _ := wire.NewChain(wire.Borrow([]byte("abc")))
						_, err = writer.InternalWriteSpans(context.Background(), chain)
					case "coalesced":
						result := make(chan InternalWriteResult, 1)
						coalescer := &InternalWriteCoalescer{InternalC: socket}
						coalescer.InternalFlushSizes([]chan<- InternalWriteResult{result}, net.Buffers{[]byte("abc")}, []int64{3})
						err = (<-result).InternalErr
					}
					if !errors.Is(err, cause) || socket.wrote {
						t.Fatal("failed deadline clear did not reject write", err, socket.wrote)
					}
				})
			}
		})
	})
}

type queuedWriteProbe struct {
	bytes.Buffer
	deadlines int
}

func (p *queuedWriteProbe) SetWriteDeadline(time.Time) error { p.deadlines++; return nil }

type countingWriter struct{ written int64 }

func (w *countingWriter) Write(p []byte) (int, error) {
	w.written += int64(len(p))
	return len(p), nil
}
func (*countingWriter) SetWriteDeadline(time.Time) error { return nil }

type scratchTestWriter struct {
	bytes.Buffer
	outcome string
}

func (*scratchTestWriter) SetWriteDeadline(time.Time) error { return nil }
func (w *scratchTestWriter) Write(p []byte) (int, error) {
	switch w.outcome {
	case "short":
		return 0, nil
	case "error":
		return 0, io.ErrClosedPipe
	case "panic":
		panic("writer panic")
	}
	return w.Buffer.Write(p)
}

func scratchTestChain(t *testing.T, parts ...[]byte) wire.Chain {
	t.Helper()
	spans := make([]wire.Span, len(parts))
	for i, part := range parts {
		spans[i] = wire.Borrow(part)
	}
	chain, err := wire.NewChain(spans...)
	if err != nil {
		t.Fatal(err)
	}
	return chain
}
