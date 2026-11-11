// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package transport

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strconv"
	"syscall"

	"log/slog"

	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/logging"
)

// isScyllaConn checks if conn is connected to a ScyllaDB node.

func (c *Conn) InternalIsScyllaConn() bool {
	return c.InternalGetScyllaSupported().IsPresent()
}

type InternalScyllaDialer struct {
	InternalDialer    implEndpoint.InternalLegacyDialer
	InternalLogger    *slog.Logger
	InternalTlsConfig *tls.Config
}

func (sd *InternalScyllaDialer) DialHost(ctx context.Context, host *implEndpoint.HostInfo) (*implEndpoint.DialedHost, error) {
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
		addr = translatedInfo.CQL.ToNetAddr()
	}

	conn, err := sd.InternalDialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	return WrapTLS(ctx, conn, addr, sd.InternalTlsConfig)
}

func (sd *InternalScyllaDialer) DialShard(ctx context.Context, host *implEndpoint.HostInfo, shardID, nrShards int) (*implEndpoint.DialedHost, error) {
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
		addr = translatedInfo.CQL.ToNetAddr()
		if sd.InternalTlsConfig != nil {
			if translatedInfo.ShardAwareTLS.IsValid() {
				shardAwareAddr = translatedInfo.ShardAwareTLS.ToNetAddr()
			}
		} else if translatedInfo.ShardAware.IsValid() {
			shardAwareAddr = translatedInfo.ShardAware.ToNetAddr()
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

		ctxWithPort := context.WithValue(ctx, implEndpoint.InternalScyllaSourcePortCtx{}, port)
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

var ErrScyllaSourcePortAlreadyInUse = errors.New("scylla: source port is already in use")

func isLocalAddrInUseErr(err error) bool {
	return errors.Is(err, syscall.EADDRINUSE) || errors.Is(err, ErrScyllaSourcePortAlreadyInUse)
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
