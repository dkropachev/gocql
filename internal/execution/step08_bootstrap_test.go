package execution

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/scylladb/gocql/v2/internal/endpoint"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/logging"
	"github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/settings"
)

func TestStep08BootstrapPreservesCause(t *testing.T) {
	for _, cause := range []error{errors.New("fixture dial rejected"), &frm.ErrorFrame{Code: protocol.ErrCodeCredentials, Message: "rejected"}} {
		config := settings.DefaultConfig("127.0.20.1")
		config.Protocol = 4
		config.Logger = logging.InternalNopLogger{}
		config.Dialer = endpoint.DialerFunc(func(context.Context, endpoint.DialTarget) (net.Conn, error) { return nil, cause })
		session, err := Connect(context.Background(), config)
		if session != nil {
			session.Close()
		}
		if !errors.Is(err, cause) {
			t.Fatalf("bootstrap discarded dial cause: %v", err)
		}
		if request, ok := cause.(protocol.RequestError); ok {
			var got protocol.RequestError
			if !errors.As(err, &got) || got.GetCode() != request.GetCode() {
				t.Fatal("bootstrap discarded server error", err)
			}
		}
	}
}
