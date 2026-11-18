// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package transport

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"syscall"

	"github.com/scylladb/gocql/v2/cqlerr"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/logging"
)

// isScyllaConn checks if conn is connected to a ScyllaDB node.

func (c *Conn) InternalIsScyllaConn() bool {
	return c.InternalGetScyllaSupported().IsPresent()
}

type InternalScyllaDialer struct {
	InternalDialer    InternalLegacyDialer
	InternalLogger    *slog.Logger
	InternalTlsConfig *tls.Config
}

func (sd *InternalScyllaDialer) DialHost(ctx context.Context, host *implEndpoint.HostInfo) (*DialedHost, error) {
	ip := host.ConnectAddress()
	port := host.Port()

	if !implEndpoint.InternalValidIpAddr(ip) {
		return nil, fmt.Errorf("host missing connect ip address: %v", ip)
	} else if port == 0 {
		return nil, fmt.Errorf("host missing port: %v", port)
	}

	addr := net.JoinHostPort(ip.String(), strconv.Itoa(port))
	translatedInfo := implEndpoint.InternalHostInfoGetTranslatedConnectionInfo(host)
	if translatedInfo != nil {
		addr = translatedInfo.CQL.String()
	}

	conn, err := sd.InternalDialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	return WrapTLS(ctx, conn, addr, sd.InternalTlsConfig)
}

func (sd *InternalScyllaDialer) DialShard(ctx context.Context, host *implEndpoint.HostInfo, shardID, nrShards int) (*DialedHost, error) {
	ip := host.ConnectAddress()
	port := host.Port()

	if !implEndpoint.InternalValidIpAddr(ip) {
		return nil, fmt.Errorf("host missing connect ip address: %v", ip)
	} else if port == 0 {
		return nil, fmt.Errorf("host missing port: %v", port)
	}

	iter := InternalNewScyllaPortIterator(shardID, nrShards)
	addr := net.JoinHostPort(ip.String(), strconv.Itoa(port))
	shardAwareAddr := ""
	translatedInfo := implEndpoint.InternalHostInfoGetTranslatedConnectionInfo(host)
	if translatedInfo != nil {
		addr = translatedInfo.CQL.String()
		if sd.InternalTlsConfig != nil {
			if implEndpoint.InternalUsableAddrPort(translatedInfo.ShardAwareTLS) {
				shardAwareAddr = translatedInfo.ShardAwareTLS.String()
			}
		} else if implEndpoint.InternalUsableAddrPort(translatedInfo.ShardAware) {
			shardAwareAddr = translatedInfo.ShardAware.String()
		}
	}

	sd.logger().Debug("connecting to shard", logging.KeyHost, addr, logging.KeyShard, shardID)

	conn, err := sd.dialShardAware(ctx, addr, shardAwareAddr, iter)
	if err != nil {
		return nil, err
	}

	return WrapTLS(ctx, conn, addr, sd.InternalTlsConfig)
}

func (sd *InternalScyllaDialer) dialShardAware(ctx context.Context, addr, shardAwareAddr string, iter *scyllaPortIterator) (net.Conn, error) {
	for {
		port, ok := iter.Next()
		if !ok {

			return sd.InternalDialer.DialContext(ctx, "tcp", addr)
		}

		ctxWithPort := context.WithValue(ctx, InternalScyllaSourcePortCtx{}, port)
		conn, err := sd.InternalDialer.DialContext(ctxWithPort, "tcp", shardAwareAddr)

		if isLocalAddrInUseErr(err) {

			continue
		} else if err != nil {
			fallback, fallbackErr := sd.InternalDialer.DialContext(ctx, "tcp", addr)
			if fallbackErr == nil {
				sd.logger().Warn("shard-aware port unreachable while the regular port works; check NAT or firewall rules",
					logging.KeyHost, addr, "shard_aware_addr", shardAwareAddr, logging.Err(err))
			}
			return fallback, fallbackErr
		}
		return conn, err
	}
}

func (sd *InternalScyllaDialer) logger() *slog.Logger { return logging.Or(sd.InternalLogger) }

func isLocalAddrInUseErr(err error) bool {
	return errors.Is(err, syscall.EADDRINUSE) || errors.Is(err, cqlerr.ErrScyllaSourcePortAlreadyInUse)
}

type scyllaPortIterator struct {
	currentPort int
	shardCount  int
}

const (
	InternalScyllaPortBasedBalancingMin = 0x8000
	InternalScyllaPortBasedBalancingMax = 0xFFFF
)

func InternalNewScyllaPortIterator(shardID, shardCount int) *scyllaPortIterator {
	if shardCount == 0 {
		panic("shardCount cannot be 0")
	}

	port := InternalScyllaPortBasedBalancingMin - InternalScyllaShardForSourcePort(InternalScyllaPortBasedBalancingMin, shardCount) + shardID
	if port < InternalScyllaPortBasedBalancingMin {
		port += shardCount
	}

	return &scyllaPortIterator{
		currentPort: port,
		shardCount:  shardCount,
	}
}

func (spi *scyllaPortIterator) Next() (uint16, bool) {
	if spi == nil {
		return 0, false
	}

	p := spi.currentPort

	if p > InternalScyllaPortBasedBalancingMax {
		return 0, false
	}

	spi.currentPort += spi.shardCount
	return uint16(p), true
}

func InternalScyllaShardForSourcePort(sourcePort uint16, shardCount int) int {
	return int(sourcePort) % shardCount
}

// InternalLegacyDialer is the net.Dialer-shaped dialer a v1-style
// configuration supplies.
type InternalLegacyDialer interface {
	DialContext(ctx context.Context, network, addr string) (net.Conn, error)
}

// ShardDialer is like HostDialer but is shard-aware.
// If the driver wants to connect to a specific shard, it will call DialShard,
// otherwise it will call DialHost.
type ShardDialer interface {
	HostDialer

	// DialShard establishes a connection to the specified shard ID out of nrShards.
	// The returned connection must be directly usable for CQL protocol,
	// specifically DialShard is responsible also for setting up the TLS session if needed.
	DialShard(ctx context.Context, host *implEndpoint.HostInfo, shardID, nrShards int) (*DialedHost, error)
}

// ScyllaShardAwareDialer wraps a net.Dialer, but uses a source port specified by gocql when connecting.
//
// Unlike in the case standard native transport ports, gocql can choose which shard will handle
// a new connection by connecting from a specific source port. If you are using your own net.Dialer
// in clusterSettings, you can use ScyllaShardAwareDialer to "upgrade" it so that it connects
// from the source port chosen by gocql.
//
// Please note that ScyllaShardAwareDialer overwrites the LocalAddr field in order to choose
// the right source port for connection.
type ScyllaShardAwareDialer struct {
	net.Dialer
}

func (d *ScyllaShardAwareDialer) DialContext(ctx context.Context, network, addr string) (conn net.Conn, err error) {
	sourcePort := ScyllaGetSourcePort(ctx)
	if sourcePort == 0 {
		return d.Dialer.DialContext(ctx, network, addr)
	}
	dialerWithLocalAddr := d.Dialer
	dialerWithLocalAddr.LocalAddr, err = net.ResolveTCPAddr(network, fmt.Sprintf(":%d", sourcePort))
	if err != nil {
		return nil, err
	}

	return dialerWithLocalAddr.DialContext(ctx, network, addr)
}

type InternalScyllaSourcePortCtx struct{}

// ScyllaGetSourcePort returns the source port that should be used when connecting to a node.
//
// Unlike in the case standard native transport ports, gocql can choose which shard will handle
// a new connection at the shard-aware port by connecting from a specific source port. Therefore,
// if you are using a custom Dialer and your nodes expose shard-aware ports, your dialer should
// use the source port specified by gocql.
//
// If this function returns 0, then your dialer can use any source port.
//
// If you aren't using a custom dialer, gocql will use a default one which uses appropriate source port.
// If you are using net.Dialer, consider wrapping it in a gocql.ScyllaShardAwareDialer.
func ScyllaGetSourcePort(ctx context.Context) uint16 {
	sourcePort, _ := ctx.Value(InternalScyllaSourcePortCtx{}).(uint16)
	return sourcePort
}
