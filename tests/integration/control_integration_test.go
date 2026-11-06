//go:build integration

package integration

import (
	"context"
	"net"
	"time"

	"github.com/scylladb/gocql/v2/endpoint"
)

// unixSocketDialer is a special dialer which connects only to the maintenance_socket.
type unixSocketDialer struct {
	dialer     net.Dialer
	socketPath string
}

func (d unixSocketDialer) DialContext(ctx context.Context, target endpoint.DialTarget) (net.Conn, error) {
	return d.dialer.DialContext(ctx, target.Network, target.Address)
}

type unixSocketNoReconnect struct{}

func (unixSocketNoReconnect) Delay(int) (time.Duration, bool) { return 0, false }
