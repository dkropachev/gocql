// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package transport

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/netip"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/endpoint"
	"github.com/scylladb/gocql/v2/internal/connectivity"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/metadata"
)

type InternalV2TLSHostDialer struct {
	InternalUnixHostID func() cql.UUID
	// InternalKnownHosts lists the host IDs of the session's nodes. With
	// TLSFallback, once each of them has answered TLS, no node is dialed in
	// plaintext again.
	InternalKnownHosts func() []cql.UUID
	// answered holds the keys (tlsHostKey) of the nodes that answered TLS,
	// guarded by mu; allTLS is set once every known node is among them.
	answered       map[string]struct{}
	InternalConfig connectivity.Config
	mu             sync.Mutex
	allTLS         atomic.Bool
}

// tlsHostKey names a node for the fallback and session cache bookkeeping:
// its host ID, or its address before the driver has learned the ID.
func tlsHostKey(host metadata.HostSnapshot) string {
	if !host.HostID.IsZero() {
		return host.HostID.String()
	}
	return host.Address.String()
}

// answeredTLS reports whether the node keyed key, or every known node, has
// answered TLS, so a TLS failure must not fall back to plaintext.
func (d *InternalV2TLSHostDialer) answeredTLS(key string) bool {
	if d.allTLS.Load() {
		return true
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	_, ok := d.answered[key]
	return ok
}

// markTLS records that the node keyed key answered TLS, and turns plaintext
// fallback off for the session once every known node has.
func (d *InternalV2TLSHostDialer) markTLS(key string) {
	if d.allTLS.Load() {
		return
	}
	d.mu.Lock()
	if d.answered == nil {
		d.answered = map[string]struct{}{}
	}
	d.answered[key] = struct{}{}
	all := d.InternalKnownHosts != nil
	if all {
		hosts := d.InternalKnownHosts()
		all = len(hosts) > 0
		for _, id := range hosts {
			if _, ok := d.answered[id.String()]; !ok {
				all = false
				break
			}
		}
	}
	d.mu.Unlock()
	if all {
		d.allTLS.Store(true)
	}
}

// notTLS reports whether err shows the node does not speak TLS where it was
// dialed, rather than that it failed verification: an answer that is not a
// TLS record, a connection the node closed or reset during the handshake,
// or, with a TLS port of its own, that port refusing the connection. A
// certificate or alert error never falls back.
func notTLS(err error, tlsPort bool) bool {
	var record tls.RecordHeaderError
	switch {
	case errors.As(err, &record):
		return true
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF), errors.Is(err, syscall.ECONNRESET):
		var alert tls.AlertError
		var verify *tls.CertificateVerificationError
		return !errors.As(err, &alert) && !errors.As(err, &verify)
	case errors.Is(err, syscall.ECONNREFUSED):
		return tlsPort
	}
	return false
}

// connectionTLS returns the configuration of one connection to target: the
// default name from logicalAddress, the identity the session's
// TLSIdentity chooses, and a session cache keyed by the node, so a ticket of
// one node is never offered to another that shares its SNI. tlsConfig is a
// copy the dial owns.
func (d *InternalV2TLSHostDialer) connectionTLS(tlsConfig *tls.Config, target endpoint.DialTarget, logicalAddress string) *tls.Config {
	tlsConfig = InternalTlsConfigForAddr(tlsConfig, logicalAddress)
	if tlsConfig.ClientSessionCache != nil {
		tlsConfig.ClientSessionCache = hostSessionCache{inner: tlsConfig.ClientSessionCache, host: tlsHostKey(target.Host) + "\x00"}
	}
	if d.InternalConfig.TLSIdentity == nil {
		return tlsConfig
	}
	identity := d.InternalConfig.TLSIdentity(cloneV2DialTarget(target))
	if identity.ServerName != "" {
		tlsConfig.ServerName = identity.ServerName
	}
	name := identity.VerifyName
	if name == "" {
		name = tlsConfig.ServerName
	}
	if identity.Verify == nil && !identity.ChainOnly && name == tlsConfig.ServerName {
		return tlsConfig
	}
	// crypto/tls verifies only ServerName, so the dial verifies the chain
	// itself, against name or none, and then runs the checks of the
	// configuration and of the identity.
	skip := tlsConfig.InsecureSkipVerify
	roots, now := tlsConfig.RootCAs, tlsConfig.Time
	if identity.ChainOnly {
		name = ""
	}
	connection, peer := tlsConfig.VerifyConnection, tlsConfig.VerifyPeerCertificate
	tlsConfig.InsecureSkipVerify = true
	tlsConfig.VerifyPeerCertificate = nil
	tlsConfig.VerifyConnection = func(state tls.ConnectionState) error {
		if !skip {
			chains, err := verifyChain(state.PeerCertificates, roots, now, name)
			if err != nil {
				return err
			}
			state.VerifiedChains = chains
		}
		if peer != nil {
			raw := make([][]byte, len(state.PeerCertificates))
			for i, cert := range state.PeerCertificates {
				raw[i] = cert.Raw
			}
			if err := peer(raw, state.VerifiedChains); err != nil {
				return err
			}
		}
		if connection != nil {
			if err := connection(state); err != nil {
				return err
			}
		}
		if identity.Verify != nil {
			return identity.Verify(state)
		}
		return nil
	}
	return tlsConfig
}

// verifyChain verifies a node's certificates as crypto/tls does for a
// client, against roots, or the system pool when roots is nil, and name
// unless it is empty.
func verifyChain(certs []*x509.Certificate, roots *x509.CertPool, now func() time.Time, name string) ([][]*x509.Certificate, error) {
	if len(certs) == 0 {
		return nil, errors.New("tls: the node sent no certificate")
	}
	options := x509.VerifyOptions{Roots: roots, DNSName: name, Intermediates: x509.NewCertPool()}
	if now != nil {
		options.CurrentTime = now()
	}
	for _, cert := range certs[1:] {
		options.Intermediates.AddCert(cert)
	}
	chains, err := certs[0].Verify(options)
	if err != nil {
		return nil, &tls.CertificateVerificationError{UnverifiedCertificates: certs, Err: err}
	}
	return chains, nil
}

// hostSessionCache keys a session cache by node, as crypto/tls keys it by
// SNI or address only.
type hostSessionCache struct {
	inner tls.ClientSessionCache
	host  string
}

func (c hostSessionCache) Get(key string) (*tls.ClientSessionState, bool) {
	return c.inner.Get(c.host + key)
}

func (c hostSessionCache) Put(key string, state *tls.ClientSessionState) {
	c.inner.Put(c.host+key, state)
}

func (d *InternalV2TLSHostDialer) selectedTLS(ctx context.Context, host metadata.HostSnapshot) (*tls.Config, error) {
	c := d.InternalConfig.TLS
	if source := d.InternalConfig.TLSSource; source != nil {
		if c = source.TLSConfig(); c == nil {
			return nil, cqltype.TypeValidation("TLS source", "returned no config")
		}
	}
	if c != nil {
		c = connectivity.CloneTLSConfig(c)
	}
	return c, nil
}
func (d *InternalV2TLSHostDialer) raw(ctx context.Context, target endpoint.DialTarget) (net.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var c net.Conn
	var err error
	if d.InternalConfig.DialHost != nil {
		c, err = d.InternalConfig.DialHost(ctx, cloneV2DialTarget(target))
	} else if d.InternalConfig.Dialer != nil {
		c, err = d.InternalConfig.Dialer.DialContext(ctx, cloneV2DialTarget(target))
	} else {
		c, err = endpoint.DefaultDialer{}.DialContext(ctx, target)
	}
	if err != nil {
		if c != nil {
			c.Close()
		}
		return nil, err
	}
	if c == nil {
		return nil, cqltype.TypeValidation("dial", "hook returned nil connection")
	}
	if _, ok := c.(*tls.Conn); ok {
		c.Close()
		return nil, cqltype.TypeValidation("dial", "hook must return a raw connection")
	}
	if target.LocalAddress.IsValid() {
		a, ok := c.LocalAddr().(*net.TCPAddr)
		if !ok || (target.LocalAddress.Port() != 0 && a.Port != int(target.LocalAddress.Port())) || (!target.LocalAddress.Addr().IsUnspecified() && a.AddrPort().Addr().Unmap() != target.LocalAddress.Addr().Unmap()) {
			c.Close()
			return nil, &connectivity.UnsupportedSettingError{Setting: "WithDialer", Reason: "the dialer did not honor the selected source " + target.LocalAddress.String()}
		}
	}
	if err = InternalApplyV2SocketOptions(c, d.InternalConfig, target.Network); err != nil {
		c.Close()
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		c.Close()
		return nil, err
	}
	return c, nil
}

// InternalApplyV2SocketOptions applies the socket options to a raw
// connection. It finds each control on the connection or on the connections
// it wraps, through an Unwrap() net.Conn method. A default the connection
// cannot apply is skipped; an option a setter chose fails with an
// UnsupportedSettingError.
func InternalApplyV2SocketOptions(c net.Conn, config connectivity.Config, network string) error {
	if network == "unix" || network == "unixpacket" {
		return nil
	}
	unsupported := func(setting string) error {
		return &connectivity.UnsupportedSettingError{Setting: setting, Reason: "raw TCP connection wrapper does not expose requested socket controls"}
	}
	chosen := func(option connectivity.SocketOptions) bool { return config.ChosenSocketOptions&option != 0 }
	if noDelay, ok := socketControl[interface{ SetNoDelay(bool) error }](c); ok {
		if err := noDelay.SetNoDelay(config.NoDelay); err != nil {
			return err
		}
	} else if chosen(connectivity.SocketNoDelay) {
		return unsupported("WithNagle")
	}
	if keepalive, ok := socketControl[interface{ SetKeepAlive(bool) error }](c); ok {
		if err := keepalive.SetKeepAlive(config.SocketKeepalive > 0); err != nil {
			return err
		}
	} else if chosen(connectivity.SocketKeepalive) || config.SocketKeepalive > 0 {
		return unsupported("WithTCPKeepalive")
	}
	if config.SocketKeepalive > 0 {
		period, ok := socketControl[interface{ SetKeepAlivePeriod(time.Duration) error }](c)
		if !ok {
			return unsupported("WithTCPKeepalive")
		}
		if err := period.SetKeepAlivePeriod(config.SocketKeepalive); err != nil {
			return err
		}
	}
	if config.ReceiveBuffer > 0 {
		setter, ok := socketControl[interface{ SetReadBuffer(int) error }](c)
		if !ok {
			return unsupported("WithReceiveBuffer")
		}
		if err := setter.SetReadBuffer(config.ReceiveBuffer); err != nil {
			return err
		}
	}
	if config.SendBuffer > 0 {
		setter, ok := socketControl[interface{ SetWriteBuffer(int) error }](c)
		if !ok {
			return unsupported("WithSendBuffer")
		}
		if err := setter.SetWriteBuffer(config.SendBuffer); err != nil {
			return err
		}
	}
	if config.Linger.Set {
		setter, ok := socketControl[interface{ SetLinger(int) error }](c)
		if !ok {
			return unsupported("WithLinger")
		}
		if err := setter.SetLinger(int(config.Linger.Value / time.Second)); err != nil {
			return err
		}
	}
	return nil
}

// socketControl finds a socket control on c or on a connection c wraps,
// following Unwrap() net.Conn methods.
func socketControl[T any](c net.Conn) (T, bool) {
	for depth := 0; c != nil && depth < 16; depth++ {
		if control, ok := c.(T); ok {
			return control, true
		}
		wrapper, ok := c.(interface{ Unwrap() net.Conn })
		if !ok {
			break
		}
		c = wrapper.Unwrap()
	}
	var zero T
	return zero, false
}

func (d *InternalV2TLSHostDialer) DialHost(ctx context.Context, host *implEndpoint.HostInfo) (*DialedHost, error) {
	return d.dial(ctx, host, -1, 0)
}
func (d *InternalV2TLSHostDialer) DialShard(ctx context.Context, host *implEndpoint.HostInfo, shard, shards int) (*DialedHost, error) {
	return d.dial(ctx, host, shard, shards)
}
func (d *InternalV2TLSHostDialer) dial(ctx context.Context, host *implEndpoint.HostInfo, shard, shards int) (*DialedHost, error) {
	epoch := implEndpoint.InternalHostInfoTransportEpoch(host)
	sharderEpoch := implEndpoint.InternalHostInfoSharderEpoch(host)
	snapshot, err := implEndpoint.SnapshotHost(host)
	if err != nil {
		return nil, err
	}
	if path, e := connectivity.UnixContact(d.InternalConfig); e != nil {
		return nil, e
	} else if path != "" {
		snapshot.Address = netip.AddrPort{}
		if d.InternalUnixHostID != nil {
			if local := d.InternalUnixHostID(); !local.IsZero() && !snapshot.HostID.IsZero() && local != snapshot.HostID {
				return nil, &connectivity.UnsupportedSettingError{Setting: "WithContactPoints", Reason: "a Unix socket contact point reaches only its local node, not another one"}
			}
		}
		c, e := d.raw(ctx, endpoint.DialTarget{Host: snapshot, Network: "unix", Address: path})
		if e != nil {
			return nil, e
		}
		return func() *DialedHost {
			opaqueStorageValue := new(DialedHost)
			opaqueStorageValue.Conn = c
			DialedHostData(opaqueStorageValue).InternalTargetEpoch = epoch
			DialedHostData(opaqueStorageValue).InternalSharderEpoch = sharderEpoch
			return opaqueStorageValue
		}(), nil
	}
	tlsConfig, err := d.selectedTLS(ctx, snapshot)
	if err != nil {
		return nil, err
	}
	if tlsConfig == nil {
		return d.dialWith(ctx, host, snapshot, shard, shards, nil, epoch, sharderEpoch)
	}
	key := tlsHostKey(snapshot)
	fallback := d.InternalConfig.TLSFallback && !d.answeredTLS(key)
	result, err := d.dialWith(ctx, host, snapshot, shard, shards, tlsConfig, epoch, sharderEpoch)
	if err == nil {
		if d.InternalConfig.TLSFallback {
			d.markTLS(key)
		}
		return result, nil
	}
	if !fallback || ctx.Err() != nil || !notTLS(err, d.InternalConfig.TLSPort != 0) {
		return nil, err
	}
	return d.dialWith(ctx, host, snapshot, shard, shards, nil, epoch, sharderEpoch)
}

// dialWith dials host, through TLS when tlsConfig is not nil.
func (d *InternalV2TLSHostDialer) dialWith(ctx context.Context, host *implEndpoint.HostInfo, snapshot metadata.HostSnapshot, shard, shards int, tlsConfig *tls.Config, epoch, sharderEpoch uint64) (*DialedHost, error) {
	address := net.JoinHostPort(host.ConnectAddress().String(), strconv.Itoa(host.Port()))
	logicalAddress := logicalV2TLSAddress(host)
	translated := implEndpoint.InternalHostInfoGetTranslatedConnectionInfo(host)
	if translated != nil && implEndpoint.InternalUsableAddrPort(translated.CQL) {
		address = translated.CQL.String()
	}
	if port := d.InternalConfig.TLSPort; tlsConfig != nil && port != 0 {
		address = net.JoinHostPort(host.ConnectAddress().String(), strconv.Itoa(int(port)))
		if translated != nil && implEndpoint.InternalUsableAddrPort(translated.CQLTLS) {
			address = translated.CQLTLS.String()
		}
	}
	target := endpoint.DialTarget{Host: snapshot, Network: "tcp", Address: address}
	if d.InternalConfig.LocalAddress.IsValid() {
		target.LocalAddress = netip.AddrPortFrom(d.InternalConfig.LocalAddress, 0)
	}
	var c net.Conn
	var err error
	var selectedShard cql.Option[uint32]
	dialed := target
	if shards > 0 && d.InternalConfig.Pool.AdvancedShardAwareness.Enabled && d.InternalConfig.Pool.ConnectionsPerHost == 0 {
		if shard < 0 || shard >= shards {
			return nil, cqltype.TypeValidation("shard", "target out of range")
		}
		endpoint := ""
		if translated != nil {
			if tlsConfig != nil && implEndpoint.InternalUsableAddrPort(translated.ShardAwareTLS) {
				endpoint = translated.ShardAwareTLS.String()
			} else if tlsConfig == nil && implEndpoint.InternalUsableAddrPort(translated.ShardAware) {
				endpoint = translated.ShardAware.String()
			}
		}
		if endpoint != "" {
			sa := d.InternalConfig.Pool.AdvancedShardAwareness
			for port := int(sa.SourcePortStart); port <= int(sa.SourcePortEnd); port++ {
				if port%shards != shard {
					continue
				}
				attempt := target
				attempt.Address = endpoint
				attempt.ShardID = cql.Option[uint32]{Value: uint32(shard), Set: true}
				ip := d.InternalConfig.LocalAddress
				if !ip.IsValid() {
					ip = netip.IPv4Unspecified()
					if snapshot.Address.Addr().Is6() {
						ip = netip.IPv6Unspecified()
					}
				}
				attempt.LocalAddress = netip.AddrPortFrom(ip, uint16(port))
				c, err = d.raw(ctx, attempt)
				if err == nil {
					selectedShard = attempt.ShardID
					dialed = attempt
					break
				}
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				if !isLocalAddrInUseErr(err) {
					break
				}
			}
		}
	}
	if c == nil {
		c, err = d.raw(ctx, target)
		if err != nil {
			return nil, err
		}
	}
	if tlsConfig != nil {
		tlsConfig = d.connectionTLS(tlsConfig, dialed, logicalAddress)
		if tlsConfig.ServerName == "" && !tlsConfig.InsecureSkipVerify && !snapshot.Address.IsValid() {
			c.Close()
			return nil, cqltype.TypeValidation("TLS identity", "missing logical host name")
		}
	}
	result, err := WrapTLS(ctx, c, logicalAddress, tlsConfig)
	if err != nil {
		return nil, err
	}
	DialedHostData(result).InternalShardTarget = selectedShard
	DialedHostData(result).InternalTargetEpoch = epoch
	DialedHostData(result).InternalSharderEpoch = sharderEpoch
	DialedHostData(result).InternalValidateTarget = func(ctx context.Context) error {
		current, err := implEndpoint.SnapshotHost(host)
		if err != nil {
			return err
		}
		if current.HostID != snapshot.HostID || current.Address != snapshot.Address || current.Datacenter != snapshot.Datacenter || current.Rack != snapshot.Rack || logicalV2TLSAddress(host) != logicalAddress {
			return cqltype.TypeValidation("dial target generation", "host identity/endpoint changed during opening")
		}
		return nil
	}
	return result, nil
}

func cloneV2HostSnapshot(v metadata.HostSnapshot) metadata.HostSnapshot {
	v.Tokens = slices.Clone(v.Tokens)
	return v
}
func cloneV2DialTarget(v endpoint.DialTarget) endpoint.DialTarget {
	v.Host = cloneV2HostSnapshot(v.Host)
	return v
}

func logicalV2TLSAddress(host *implEndpoint.HostInfo) string {
	implEndpoint.HostInfoData(host).InternalMu.RLock()
	defer implEndpoint.HostInfoData(host).InternalMu.RUnlock()
	name := implEndpoint.HostInfoData(host).InternalHostname
	if name == "" {
		switch {
		case implEndpoint.InternalValidIpAddr(implEndpoint.HostInfoData(host).InternalConnectAddress):
			name = implEndpoint.HostInfoData(host).InternalConnectAddress.String()
		case implEndpoint.InternalValidIpAddr(implEndpoint.HostInfoData(host).InternalRpcAddress):
			name = implEndpoint.HostInfoData(host).InternalRpcAddress.String()
		case implEndpoint.InternalValidIpAddr(implEndpoint.HostInfoData(host).InternalPeer):
			name = implEndpoint.HostInfoData(host).InternalPeer.String()
		default:
			name = implEndpoint.HostInfoData(host).InternalBroadcastAddress.String()
		}
	}
	return net.JoinHostPort(name, strconv.Itoa(implEndpoint.HostInfoData(host).InternalPort))
}
