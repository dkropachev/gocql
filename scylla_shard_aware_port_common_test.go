package gocql

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"
	// Each fixture supplies its own construction path; integration fixtures use
	// Config/Connect while unit servers can disable their unsupported control path.
)

type sourcePortOffByOneTestDialer struct {
	ScyllaShardAwareDialer
}

func (spobo *sourcePortOffByOneTestDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	// Simulate a NAT that always increases the source port by 1
	// This should always result in wrong shard being assigned if host
	// has more than one shard.

	sourcePort := ScyllaGetSourcePort(ctx)
	if sourcePort > 0 {
		sourcePort++
	}
	newCtx := context.WithValue(ctx, scyllaSourcePortCtx{}, sourcePort)
	return spobo.ScyllaShardAwareDialer.DialContext(newCtx, network, addr)
}

type allowOnlyNonShardAwarePortDialer struct {
	net.Dialer

	allowedPort uint16
}

func (aonsa *allowOnlyNonShardAwarePortDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	// Simulate a network configuration which allows connections to the
	// non-shard-aware port, but not to the shard-aware one.

	_, targetPort, _ := net.SplitHostPort(addr)
	if targetPort != strconv.Itoa(int(aonsa.allowedPort)) {
		return nil, fmt.Errorf("allowOnlyNonShardAwarePortDialer: tried to connect to port %s, but only %d is allowed", targetPort, aonsa.allowedPort)
	}

	return aonsa.Dialer.DialContext(ctx, network, addr)
}

type loggingTestDialer struct {
	ScyllaShardAwareDialer

	events map[string][]testConnectionEvent
	mu     sync.Mutex
}

type testConnectionEvent struct {
	sourcePort, destinationPort uint16
}

func (ltd *loggingTestDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	ipaddr, destinationPortStr, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	destinationPort, err := strconv.ParseUint(destinationPortStr, 10, 16)
	if err != nil {
		return nil, err
	}

	conn, err := ltd.ScyllaShardAwareDialer.DialContext(ctx, network, addr)

	if err == nil {
		ltd.mu.Lock()
		defer ltd.mu.Unlock()

		evt := testConnectionEvent{
			sourcePort:      uint16(conn.LocalAddr().(*net.TCPAddr).Port),
			destinationPort: uint16(destinationPort),
		}
		ltd.events[ipaddr] = append(ltd.events[ipaddr], evt)
	}

	return conn, err
}

func (ltd *loggingTestDialer) connectionEvents(address string) []testConnectionEvent {
	ltd.mu.Lock()
	defer ltd.mu.Unlock()
	return append([]testConnectionEvent(nil), ltd.events[address]...)
}
