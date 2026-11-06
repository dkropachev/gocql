package transport

import (
	"context"
	"errors"
	"io"
	"net"
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

func TestDisabledWriteTimeoutClearsStartupDeadline(t *testing.T) {
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
}

func TestZeroWriteDeadlineSettingError(t *testing.T) {
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
}
