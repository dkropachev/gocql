/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2012, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package transport

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"

	"log/slog"

	"github.com/scylladb/gocql/v2/internal/connectivity"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/logging"
	"github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/syncx"

	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"

	frm "github.com/scylladb/gocql/v2/internal/frame"

	"io"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	runtimeerrors "github.com/scylladb/gocql/v2/internal/runtimeerrors"
	"github.com/scylladb/gocql/v2/internal/segment"
	"github.com/scylladb/gocql/v2/internal/streams"
	"github.com/scylladb/gocql/v2/internal/wire"
)

func InternalApprove(authenticator string, approvedAuthenticators []string) bool {
	if len(approvedAuthenticators) == 0 {
		return true
	}
	for _, s := range approvedAuthenticators {
		if authenticator == s {
			return true
		}
	}
	return false
}

type Authenticator interface {
	Challenge(req []byte) (resp []byte, auth Authenticator, err error)
	Success(data []byte) error
}

// PasswordAuthenticator specifies credentials to be used when authenticating.
// It can be configured with an "allow list" of authenticator class names to avoid
// attempting to authenticate with Cassandra if it doesn't provide an expected authenticator.
type PasswordAuthenticator struct {
	Username string
	Password string
	// Setting this to nil or empty will allow authenticating with any authenticator
	// provided by the server.  This is the default behavior of most other driver
	// implementations.
	AllowedAuthenticators []string
}

func (p PasswordAuthenticator) Challenge(req []byte) ([]byte, Authenticator, error) {
	if !InternalApprove(string(req), p.AllowedAuthenticators) {
		return nil, nil, fmt.Errorf("unexpected authenticator %q", req)
	}
	resp := make([]byte, 2+len(p.Username)+len(p.Password))
	resp[0] = 0
	copy(resp[1:], p.Username)
	resp[len(p.Username)+1] = 0
	copy(resp[2+len(p.Username):], p.Password)
	return resp, nil, nil
}

func (p PasswordAuthenticator) Success(data []byte) error {
	return nil
}

// SslOptions configures TLS use.
//
// Warning: Due to historical reasons, the SslOptions is insecure by default, so you need to set EnableHostVerification
// to true if no Config is set. Most users should set SslOptions.Config to a *tls.Config.
// SslOptions and Config.InsecureSkipVerify interact as follows:
//
//	Config.InsecureSkipVerify | EnableHostVerification | Result
//	Config is nil             | false                  | do not verify host
//	Config is nil             | true                   | verify host
//	false                     | false                  | verify host
//	true                      | false                  | do not verify host
//	false                     | true                   | verify host
//	true                      | true                   | verify host
type SslOptions struct {
	*tls.Config

	// CertPath and KeyPath are optional depending on server
	// config, but both fields must be omitted to avoid using a
	// client certificate
	CertPath string
	KeyPath  string
	CaPath   string //optional depending on server config
	// If you want to verify the hostname and server cert (like a wildcard for cass cluster) then you should turn this
	// on.
	// This option is basically the inverse of tls.Config.InsecureSkipVerify.
	// See InsecureSkipVerify in http://golang.org/pkg/crypto/tls/ for more info.
	//
	// See SslOptions documentation to see how EnableHostVerification interacts with the provided tls.Config.
	EnableHostVerification bool
	// DisableStrictCertificateValidation disables strict chain validation.
	// Strict validation requires TLS verification (InsecureSkipVerify=false).
	// When false (default) with verification enabled, the driver validates the
	// entire chain up to a self-signed root. When true, Go's default applies.
	//
	// Deprecated: This option is provided for backward compatibility and will be removed
	// in a future version. You should ensure your certificate chains are properly configured
	// and avoid using this option.
	DisableStrictCertificateValidation bool
}

type ConnConfig struct {
	TypedAuth            connectivity.AuthProvider
	Dialer               implEndpoint.InternalLegacyDialer
	Logger               *slog.Logger
	Authenticator        Authenticator
	Compressor           protocol.Compressor
	HostDialer           implEndpoint.HostDialer
	InternalReceiveCache *wire.ReceiveCache
	// InternalOrphans, when set, counts streams the session's connections
	// abandoned while the node may still answer them. Copies of the config
	// share it.
	InternalOrphans         *atomic.Uint64
	TypedConfig             *connectivity.Config
	AuthProvider            func(h *implEndpoint.HostInfo) (Authenticator, error)
	InternalTlsConfig       *tls.Config
	CQLVersion              string
	TypedLimits             connectivity.ProtocolLimits
	ConnectTimeout          time.Duration
	ReadTimeout             time.Duration
	WriteTimeout            time.Duration
	ProtoVersion            int
	Keepalive               time.Duration
	InternalDisableCoalesce bool
	// isControlConn marks the connection used by the control connection, which is
	// the only one reporting the driver configuration on startup.
	InternalIsControlConn bool
}

func (c *ConnConfig) logger() *slog.Logger {
	if c.Logger == nil {
		return slog.Default()
	}
	return c.Logger
}

type ConnErrorHandler interface {
	HandleError(conn *Conn, err error, closed bool)
}

type InternalConnErrorHandlerFn func(conn *Conn, err error, closed bool)

func (fn InternalConnErrorHandlerFn) HandleError(conn *Conn, err error, closed bool) {
	fn(conn, err, closed)
}

type ConnInterface interface {
	Close()
	InternalExec(ctx context.Context, req protocol.FrameBuilder, tracer Tracer, requestTimeout time.Duration) (*protocol.Framer, error)
	InternalGetIsSchemaV2() bool
	InternalSetSchemaV2(s bool)
	InternalIsScyllaConn() bool
	InternalGetScyllaSupported() implEndpoint.ScyllaConnectionFeatures
}

// Conn is a single connection to a Cassandra node. It can be used to execute
// queries, but users are usually advised to use a more reliable, higher
// level API.
func (c *Conn) logger() *slog.Logger { return logging.Or(c.InternalLogger) }

type Conn struct {
	InternalOpeningDeadline time.Time
	auth                    Authenticator
	InternalFrameObserver   FrameHeaderObserver
	InternalStreamObserver  StreamObserver
	InternalW               InternalContextWriter
	InternalLogger          *slog.Logger
	InternalCtx             context.Context
	InternalErrorHandler    ConnErrorHandler
	InternalCompressor      protocol.Compressor
	// segCompressor is compressor narrowed to the two Append methods the v5 segment
	// codec takes, resolved once by resolveSegmentCompressor during the handshake. The
	// receive path would otherwise repeat that assertion for every segment it reads,
	// for a result that cannot change: compressor is written at dial and once more by
	// startupCoordinator.startup, and never again.
	//
	// Nil is the uncompressed segment layout, and is also what a pre-v5 connection
	// leaves it as -- it never reaches the segment codec, and its compressor need not
	// support segments at all.
	InternalSegCompressor segment.Compressor
	InternalR             InternalConnReadSource
	InternalHooks         InternalConnectionHooks
	startupReporter       InternalStartupReporter
	// typedSettings is published once the framer cache is initialised, when
	// every input to the native protocol settings is final.
	typedSettings                             atomic.Pointer[protocol.Settings]
	hostID                                    atomic.Pointer[cqltype.UUID] // InternalHost's ID once known
	stopCloseOnCancel                         func() bool
	typedWriter                               *submissionWriter
	eventDone                                 chan struct{}
	InternalValidateTarget                    func(context.Context) error
	InternalStreamBudget                      *protocol.StreamMetadataBudget
	eventQueue                                chan connEvent
	sharedChanged                             chan struct{}
	readerPaused, readerResume, readerResumed chan struct{}
	InternalCfg                               *ConnConfig
	InternalSupported                         map[string][]string
	InternalStreams                           *streams.IDGenerator
	InternalHost                              *implEndpoint.HostInfo
	// InternalCalls maps stream IDs to in-flight calls. It is lock-free (see
	// InternalCallTable); close drains it in place after setting the closed
	// admission flag.
	InternalCalls  *InternalCallTable
	InternalCancel context.CancelFunc
	// currentKeyspace is the keyspace this connection was switched to by
	// Conn.UseKeyspace, and the default the prepared-statement cache is keyed by
	// (see executeQuery/executeBatch). It deliberately tracks only driver-issued
	// USE: a `USE` statement a caller executes as an ordinary query switches the
	// server side of whichever single pooled connection it landed on, and the
	// driver does not follow it — so cache keys keep using the configured keyspace.
	//
	// Atomic because UseKeyspace is exported: a caller invoking it on a live
	// connection would otherwise race the request goroutines reading it.
	currentKeyspace atomic.Pointer[string]
	// systemRequest carries the timeouts driver-issued system queries are sent
	// with. See systemRequestState for why the two travel together.
	//
	// Atomic because finalizeConnection switches the timeout from
	// cfg.ConnectTimeout to cfg.MetadataSchemaRequestTimeout at the very end of
	// Session.init, by which point the control connection is already registered
	// for server push events: a SCHEMA_CHANGE arriving inside that window makes
	// the event-debouncer goroutine run querySystem on this connection
	// concurrently with the write.
	systemRequest   atomic.Pointer[InternalSystemRequestState]
	startupIdentity InternalStartupIdentity
	// headerReader is the reader the current frame or segment header is read
	// through (see readFrameHeader, readFirstSegmentHeader). Reused rather than
	// allocated per header, and like segScratch only touched by whichever
	// goroutine is currently receiving. The two header reads never nest, and the
	// startup coordinator's reader never overlaps serve(): a frameTicker tick is
	// only sent while the previous response is still outstanding, and
	// processFrameSource touches neither field after handing that response to its
	// caller.
	//
	// A successful setupConn also waits for the startup reader to return
	// before serve() starts.
	headerReader headerReader
	InternalAddr string
	// segScratch holds the reusable buffers inbound v5 segments are read into.
	// Only touched by the receive path, which runs on the serve() goroutine.
	segScratch                  segment.Scratch
	InternalCqlProtoExts        []implEndpoint.InternalCqlProtocolExtension
	InternalFramers             connFramers
	typedMetadataReuse          *protocol.MetadataReuse
	InternalScyllaSupported     implEndpoint.ScyllaConnectionFeatures
	InternalOpeningSharderEpoch uint64
	InternalTargetEpoch         uint64
	InternalMetadataTimeout     time.Duration
	coalesceWait                time.Duration
	InternalWriteTimeout        atomic.Int64
	typedWriterOnce             sync.Once
	eventOnce                   sync.Once
	eventMu                     sync.Mutex
	InternalPoolPolicy          InternalConnectionPoolPolicy
	InternalMu                  sync.Mutex
	InternalRequestedShard      cqltype.Option[uint32]
	// admission packs the shared-operation count with the closed and
	// exclusive flags; see connection_state.go.
	admission        atomic.Uint64
	pauseRequested   atomic.Bool // mirrors pauseWanted for the reader's lock-free check
	tabletsRoutingV1 int32
	orphans          atomic.Int32 // calls that stopped waiting for an outstanding response
	// headerBuf holds the frame or segment header, or segment trailer, being
	// read or decoded, so none escapes through the reader. Like headerReader it
	// is only touched by the receiving goroutine, one header at a time.
	headerBuf [protocol.HeadSize]byte
	// closesOnCancel reports that cancelling InternalCtx closes the connection,
	// so every waiting typed call receives the closure through its response.
	closesOnCancel       bool
	eventsClosed         bool
	pauseWanted          bool
	InternalIsShardAware bool
	// true if connection close process for the connection started.
	// closed is protected by mu.
	InternalClosed     bool
	InternalIsSchemaV2 bool
	InternalVersion    uint8
}

func (c *Conn) InternalGetIsSchemaV2() bool {
	return c.InternalIsSchemaV2
}

func (c *Conn) InternalSetSchemaV2(s bool) {
	c.InternalIsSchemaV2 = s
}

// systemRequestState is the immutable pair of timeouts a driver-issued system
// query is sent with: the client-side deadline, and the ScyllaDB-only
// " USING TIMEOUT ...ms" clause pre-rendered from it (empty when the clause does
// not apply). The two are published as one snapshot so a reader can never pair
// one with a stale version of the other - which would ask the server to abort a
// system query on a deadline the client is not waiting on, or vice versa.
//
// Field order keeps the string first so the GC scans 8 pointer bytes, not 16
// (govet's fieldalignment).
type InternalSystemRequestState struct {
	InternalUsingClause string
	InternalTimeout     time.Duration
}

// systemRequestStatement returns stmt with the USING TIMEOUT clause appended and
// the client-side timeout to send it with, both taken from one snapshot so the
// two can never disagree. It is the only way the query paths should reach them.
func (c *Conn) InternalSystemRequestStatement(stmt string) (string, time.Duration) {
	state := c.InternalGetSystemRequestState()
	return stmt + state.InternalUsingClause, state.InternalTimeout
}

// getSystemRequestState returns the current snapshot.
func (c *Conn) InternalGetSystemRequestState() InternalSystemRequestState {
	if state := c.systemRequest.Load(); state != nil {
		return *state
	}
	return InternalSystemRequestState{}
}

// setSystemRequestTimeout publishes t together with the clause derived from it.
// The clause is ScyllaDB-only and needs a positive timeout, so it is empty
// otherwise - a timeout the caller disabled must not leave an older clause in
// force.
func (c *Conn) InternalSetSystemRequestTimeout(t time.Duration) {
	next := InternalSystemRequestState{InternalTimeout: t}
	if t > time.Duration(0) && c.InternalIsScyllaConn() {
		next.InternalUsingClause = " USING TIMEOUT " + strconv.FormatInt(t.Milliseconds(), 10) + "ms"
	}
	c.systemRequest.Store(&next)
}

// recalculateSystemRequestTimeout re-renders the clause for the timeout already
// in effect. It is called once the connection knows whether it talks to
// ScyllaDB, which the clause depends on.
//
// It re-reads the timeout rather than naming the value its only caller knows is
// in force (cfg.ConnectTimeout, from dialWithoutObserver): republishing whatever
// is current cannot overwrite a timeout some later change publishes earlier in
// startup, which would put every system query back under an unrelated setting.
func (c *Conn) recalculateSystemRequestTimeout() {
	c.InternalSetSystemRequestTimeout(c.InternalGetSystemRequestState().InternalTimeout)
}

func (c *Conn) InternalFinalizeConnection() {

	c.InternalWriteTimeout.Store(int64(c.InternalCfg.WriteTimeout))
	c.InternalSetSystemRequestTimeout(c.InternalMetadataTimeout)
	c.InternalW.InternalSetWriteTimeout(c.InternalCfg.WriteTimeout)
	c.InternalR.SetTimeout(c.InternalCfg.ReadTimeout)
}

func (c *Conn) InternalGetScyllaSupported() implEndpoint.ScyllaConnectionFeatures {
	return c.InternalScyllaSupported
}

func InternalTranslateHostAddresses(ctx context.Context, addressTranslator implEndpoint.AddressTranslator, host *implEndpoint.HostInfo, logger *slog.Logger) (implEndpoint.InternalTranslatedAddresses, error) {
	logger = logging.Or(logger)
	port := host.Port()
	if port < 1 || port > 65535 {
		return implEndpoint.InternalTranslatedAddresses{}, fmt.Errorf("invalid CQL port %d for host %s: port must be between 1 and 65535", port, host.UntranslatedConnectAddress())
	}
	peer := implEndpoint.InternalPeerOf(host)
	ip := implEndpoint.InternalNetIPAddr(host.UntranslatedConnectAddress())
	addr, err := implEndpoint.InternalTranslateAddressPort(ctx, addressTranslator, peer, netip.AddrPortFrom(ip, uint16(port)), logger)
	if err != nil {
		return implEndpoint.InternalTranslatedAddresses{}, fmt.Errorf("unable to translate regular cql address: %w", err)
	}
	resultedInfo := implEndpoint.InternalTranslatedAddresses{
		CQL: addr,
	}

	if port := peer.ShardAwarePort; port != 0 {
		addr, err = implEndpoint.InternalTranslateAddressPort(ctx, addressTranslator, peer, netip.AddrPortFrom(ip, port), logger)
		if err != nil {
			return implEndpoint.InternalTranslatedAddresses{}, fmt.Errorf("unable to translate shard aware address: %w", err)
		}
		resultedInfo.ShardAware = addr
	}
	if port := peer.ShardAwarePortTLS; port != 0 {
		addr, err = implEndpoint.InternalTranslateAddressPort(ctx, addressTranslator, peer, netip.AddrPortFrom(ip, port), logger)
		if err != nil {
			return implEndpoint.InternalTranslatedAddresses{}, fmt.Errorf("unable to translate shard aware tls address: %w", err)
		}
		resultedInfo.ShardAwareTLS = addr
	}
	return resultedInfo, nil
}

// dialShard establishes a connection to a host/shard and notifies the session's connectObserver.
// If nrShards is zero, shard-aware dialing is disabled.
// note: every connection needs to get `conn.finalizeConnection` called on it when initialization process is done
type InternalConnectionLifetimeKey struct{}

func (c *Conn) init(ctx context.Context, dialedHost *implEndpoint.DialedHost) error {
	c.InternalR.SetTimeout(c.InternalCfg.ConnectTimeout)
	c.InternalWriteTimeout.Store(int64(c.InternalCfg.ConnectTimeout))
	c.InternalW.InternalSetWriteTimeout(c.InternalCfg.ConnectTimeout)

	if c.InternalCfg.AuthProvider != nil {
		var err error
		c.auth, err = c.InternalCfg.AuthProvider(c.InternalHost)
		if err != nil {
			return err
		}
	} else {
		c.auth = c.InternalCfg.Authenticator
	}

	startup := &startupCoordinator{
		frameTicker: make(chan struct{}),
		conn:        c,
	}

	if c.InternalCfg.InternalIsControlConn {
		startup.driverConfigReporter = c.startupReporter
	}

	if err := startup.setupConn(ctx); err != nil {
		return err
	}

	// A typed connection's submission writer already sends every request
	// queued while it was busy in one socket write; a coalescer in front of
	// the socket would only add a goroutine hop and a timer wait.
	if c.InternalCfg.TypedConfig != nil {
		c.coalesceWait = 0
	}
	if c.coalesceWait > 0 && !c.InternalCfg.InternalDisableCoalesce && !dialedHost.DisableCoalesce {
		c.InternalW = InternalNewWriteCoalescer(dialedHost.Conn, c.InternalCfg.ConnectTimeout, c.coalesceWait, c.InternalCtx.Done())
	}

	if c.InternalIsScyllaConn() {
		c.InternalSetSchemaV2(false)
	}

	go c.InternalServe(c.InternalCtx)
	if c.InternalCfg.TypedConfig != nil {
		c.startTypedHeartbeat(c.InternalCtx)
	} else {
		go c.InternalHeartBeat(c.InternalCtx)
	}

	return nil
}

func (c *Conn) Write(p []byte) (n int, err error) {
	if err = c.beginShared(c.InternalCtx); err != nil {
		return 0, err
	}
	defer c.endShared()
	return c.InternalW.InternalWriteContext(context.Background(), p)
}

// Read reads data from the connection.
//
// The driver itself reads through the connection's connReader (which owns the
// read-deadline handling); Read is retained so that *Conn keeps satisfying
// io.Reader for external callers, as it does io.Writer via Write.
func (c *Conn) Read(p []byte) (n int, err error) {
	if c.InternalExclusive() {
		return 0, &connectivity.UnsupportedSettingError{Setting: "raw connection read", Reason: "reader is reserved for streaming"}
	}
	return c.InternalR.Read(p)
}

type startupCoordinator struct {
	conn                 *Conn
	frameTicker          chan struct{}
	driverConfigReporter InternalStartupReporter
}

func (s *startupCoordinator) setupConn(ctx context.Context) error {
	var cancel context.CancelFunc
	if s.conn.InternalCfg.ConnectTimeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, s.conn.InternalCfg.ConnectTimeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	startupCompleted := &atomic.Bool{}
	startupCompleted.Store(false)

	// The reader runs on a goroutine of its own, reading one response per
	// request that options sends on this goroutine. A read failure cancels
	// ctx, which ends the request waiting for that response; the error is
	// queued before the cancel, so options never returns ahead of it.
	readErr := make(chan error, 1)
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		for range s.frameTicker {
			if err := s.conn.InternalRecv(ctx, startupCompleted.Load()); err != nil {
				readErr <- err
				cancel()
				return
			}
		}
	}()

	err := s.options(ctx, startupCompleted)
	close(s.frameTicker)
	if err == nil {
		// Every request got its response, so the reader is back waiting for
		// a tick and returns now: serve() never overlaps it.
		<-readDone
		return nil
	}
	// A startup that ran out of time reports the timeout even when the
	// reader failed first; otherwise a read failure explains the error.
	timedOut := errors.Is(ctx.Err(), context.DeadlineExceeded)
	cancel()
	if !timedOut {
		select {
		case err := <-readErr:
			return err
		default:
		}
	}
	if ctx.Err() != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
		return errors.New("gocql: no response to connection startup within timeout")
	}
	return err
}

// write sends the given frame on the connection during startup and returns
// the parsed response frame.
//
// NOTE: The returned frame must not retain any byte-slice references to the
// framer's read buffer, because the framer is released back to the pool
// immediately after parseFrame returns (via defer). Frame types that use
// readBytesCopy (e.g. SupportedFrame, AuthChallengeFrame, AuthSuccessFrame)
// are safe; frame types that use readBytes and expose []byte fields would not
// be safe and must not be returned from this function.
func (s *startupCoordinator) write(ctx context.Context, frame protocol.FrameBuilder, startupCompleted *atomic.Bool) (protocol.Frame, error) {
	select {
	case s.frameTicker <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	framer, err := s.conn.execInternal(ctx, frame, nil, s.conn.InternalCfg.ConnectTimeout, startupCompleted.Load())
	if err != nil {
		return nil, err
	}
	defer framer.Release()

	return framer.ParseFrame()
}

func (s *startupCoordinator) options(ctx context.Context, startupCompleted *atomic.Bool) error {
	frame, err := s.write(ctx, &protocol.WriteOptionsFrame{}, startupCompleted)
	if err != nil {
		return err
	}

	v, ok := frame.(*frm.SupportedFrame)
	if !ok {
		return cqltype.NewFrameError("Unknown type of response to startup frame: %T", frame)
	}

	if config := s.conn.InternalCfg.TypedConfig; config != nil {
		if err := InternalValidateV2Supported(v.Supported, config.Pool); err != nil {
			return err
		}
	}
	s.conn.InternalSupported = v.Supported
	s.conn.InternalScyllaSupported = implEndpoint.InternalParseSupported(s.conn.InternalSupported, s.conn.InternalLogger)
	s.conn.recalculateSystemRequestTimeout()
	if _, poolOwned := s.conn.InternalErrorHandler.(poolOwner); !poolOwned || s.conn.InternalCfg.TypedConfig == nil {
		if current := s.conn.InternalHost.ScyllaFeatures(); current != s.conn.InternalScyllaSupported.ScyllaHostFeatures {
			implEndpoint.InternalHostInfoSetScyllaFeatures(s.conn.InternalHost, s.conn.InternalScyllaSupported.ScyllaHostFeatures)
		}
	}
	s.conn.InternalCqlProtoExts = implEndpoint.InternalParseCQLProtocolExtensions(s.conn.InternalSupported, s.conn.InternalVersion, s.conn.InternalLogger)

	if s.conn.InternalCfg.TypedConfig != nil {
		s.conn.InternalCqlProtoExts = implEndpoint.InternalTypedTabletExtensions(s.conn.InternalCqlProtoExts, s.conn.InternalSupported)
	}

	err = s.startup(ctx, startupCompleted)
	if err != nil {
		return err
	}
	s.conn.InternalInitFramerCache()
	return nil
}

// startupOptions builds the STARTUP options the driver always sends.
//
// The application's options go in first so the driver-owned keys below can
// overwrite them, never the other way round. They describe the driver, the
// protocol it is speaking and the session the connection belongs to, and are
// not the application's to change: a callback that set CQL_VERSION could make
// every connection in the cluster fail the handshake, one that set DRIVER_NAME
// or DRIVER_VERSION would misreport the driver to the server for the life of
// the connection, and one that set SESSION_ID would break correlating a
// session's connections in system.clients.
//
// The ordering is easy to lose, which is how it was lost: upstream has no
// ApplicationInfo hook and writes these three as a map literal, so merging the
// two put the callback last.
func InternalStartupOptions(cqlVersion, driverName, driverVersion string, info implEndpoint.ApplicationInfo, driverConfig InternalStartupReporter, sessionID string, isScyllaConn bool) map[string]string {
	m := map[string]string{}

	if info != nil {
		info.UpdateStartupOptions(m)
	}

	if driverConfig != nil {
		driverConfig.InternalUpdateStartupOptions(m, isScyllaConn)
	}

	m[InternalSessionIDStartupKey] = sessionID
	m["CQL_VERSION"] = cqlVersion
	m["DRIVER_NAME"] = driverName
	m["DRIVER_VERSION"] = driverVersion

	return m
}

func (s *startupCoordinator) startup(ctx context.Context, startupCompleted *atomic.Bool) error {

	m := InternalStartupOptions(
		s.conn.InternalCfg.CQLVersion,
		s.conn.startupIdentity.DriverName,
		s.conn.startupIdentity.DriverVersion,
		s.conn.startupIdentity.ApplicationInfo,
		s.driverConfigReporter,
		s.conn.startupIdentity.SessionID,
		s.conn.InternalIsScyllaConn(),
	)

	delete(m, "COMPRESSION")

	if s.conn.InternalCompressor != nil {
		comp := s.conn.InternalSupported["COMPRESSION"]
		name := s.conn.InternalCompressor.Name()
		for _, compressor := range comp {
			if compressor == name {
				m["COMPRESSION"] = compressor
				break
			}
		}

		if _, ok := m["COMPRESSION"]; !ok {
			if s.conn.InternalCfg.TypedConfig != nil {
				return &connectivity.UnsupportedSettingError{Setting: "compression", Reason: "server does not advertise requested " + name}
			}
			s.conn.logger().Warn("server does not support requested compression; continuing uncompressed",
				"requested", name, "offered", comp)
			s.conn.InternalCompressor = nil
		}
	}

	if err := s.conn.resolveSegmentCompressor(); err != nil {
		return err
	}

	for _, ext := range s.conn.InternalCqlProtoExts {
		serialized := ext.InternalSerialize()
		for k, v := range serialized {
			m[k] = v
		}
	}

	frame, err := s.write(ctx, &protocol.WriteStartupFrame{Opts: m}, startupCompleted)
	if err != nil {
		return err
	}

	switch v := frame.(type) {
	case *protocol.ErrorResponseFrame:
		return v.Err
	case *frm.ReadyFrame:

		startupCompleted.Store(true)
		return nil
	case *frm.AuthenticateFrame:

		startupCompleted.Store(true)
		return s.authenticateHandshake(ctx, v, startupCompleted)
	default:
		return cqltype.NewFrameError("Unknown type of response to startup frame: %s", v)
	}
}

func (s *startupCoordinator) authenticateHandshake(ctx context.Context, authFrame *frm.AuthenticateFrame, startupCompleted *atomic.Bool) error {
	if s.conn.InternalCfg != nil && s.conn.InternalCfg.TypedAuth != nil {
		host, _ := implEndpoint.SnapshotHost(s.conn.InternalHost)
		auth := InternalNewV2Authenticator(ctx, host, s.conn.InternalCfg.TypedAuth)
		s.conn.auth = auth
		defer auth.Close()
	}

	if s.conn.auth == nil {
		return fmt.Errorf("authentication required (using %q)", authFrame.Class)
	}

	resp, challenger, err := s.conn.auth.Challenge([]byte(authFrame.Class))
	if err != nil {
		return err
	}

	req := &protocol.WriteAuthResponseFrame{Data: resp}
	for {
		frame, err := s.write(ctx, req, startupCompleted)
		if err != nil {
			return err
		}

		switch v := frame.(type) {
		case *protocol.ErrorResponseFrame:
			return v.Err
		case *frm.AuthSuccessFrame:
			if challenger != nil {
				return challenger.Success(v.Data)
			}
			return nil
		case *frm.AuthChallengeFrame:

			if challenger == nil {
				return fmt.Errorf("unexpected authentication challenge from %s: the authenticator for %q does not support multi-step authentication", s.conn.InternalAddr, authFrame.Class)
			}
			resp, challenger, err = challenger.Challenge(v.Data)
			if err != nil {
				return err
			}

			req = &protocol.WriteAuthResponseFrame{
				Data: resp,
			}
		default:
			return fmt.Errorf("unknown frame response during authentication: %v", v)
		}
	}
}

func (c *Conn) InternalCloseWithError(err error) {
	if c == nil {
		return
	}

	c.InternalMu.Lock()
	if c.InternalClosed {
		c.InternalMu.Unlock()
		return
	}
	c.InternalClosed = true
	// Setting the flag before draining the table is what keeps a call from
	// staying registered after close: a registration racing this close
	// either lands before the drain reads its slot, or reads the flag after
	// it and withdraws itself (see allocCall).
	c.admission.Or(admissionClosed)
	c.changeLocked()
	c.InternalMu.Unlock()

	// Close the socket before joining calls: an accepted span write may be
	// blocked in the kernel (or TLS). Waiting for that call before interrupting
	// the socket would deadlock shutdown and retain its input indefinitely.
	var cerr error
	if err == nil {

		c.InternalCancel()
	}
	cerr = c.close()

	// A call sits in one slot unless a test arranged otherwise.
	var seen map[*InternalCallReq]bool
	c.InternalCalls.drain(func(req *InternalCallReq) {
		if seen[req] {
			return
		}
		if seen == nil {
			seen = make(map[*InternalCallReq]bool)
		}
		seen[req] = true

		if err != nil {

			req.deliver(InternalCallResp{InternalErr: err})
		} else if req.typed {
			// Typed callers of a connection that closes on cancellation do
			// not watch InternalCtx; tell them directly.
			req.deliver(InternalCallResp{InternalErr: runtimeerrors.ErrConnectionClosed})
		}
		req.waitExecDone("closeWithError")
		if req.streamObserverContext != nil {
			req.streamObserverEndOnce.Do(func() {
				req.streamObserverContext.StreamAbandoned(ObservedStream{
					Host: c.InternalHost,
				})
			})
		}
		putCallReq(req)
	})

	c.InternalFramers.close()

	if err != nil {
		c.InternalCancel()
	}

	if err != nil {
		c.InternalErrorHandler.HandleError(c, err, true)
	} else if cerr != nil {

		c.InternalErrorHandler.HandleError(c, cerr, true)
	}
}

func (c *Conn) InternalIsTabletSupported() bool {
	return atomic.LoadInt32(&c.tabletsRoutingV1) == 1
}

func (c *Conn) setTabletSupported(val bool) {
	intVal := int32(0)
	if val {
		intVal = 1
	}
	atomic.StoreInt32(&c.tabletsRoutingV1, intVal)
}

// usesMetadataID reports whether SCYLLA_USE_METADATA_ID was negotiated on this
// connection. This is the extension alone; see tracksResultMetadataID for the
// question the request path actually asks.
//
// It reads the framer config rather than a separate field on Conn, so the request
// path and the pooled framers that encode and decode the result metadata ID cannot
// disagree: a Conn that believed the extension was on while its framers did not
// would ask the server to skip metadata while writing no ID to compare against.
// initFramerCache populates the config during connection setup, before any query
// can run.
//
// One framer is not built from that config: framerPool.get falls back to newFramer
// when the pool is disabled, which yields scyllaUseMetadataID false regardless of
// what was negotiated. That is correct during the handshake, and unreachable from
// the request path afterwards — execInternal acquires its framer before addCall
// rejects a closed connection, so a framer taken after the pool closed belongs to a
// call that never writes a frame. It is a property of call ordering rather than of
// construction, so see #982, which also covers flagLWT and tabletsRoutingV1.
func (c *Conn) InternalUsesMetadataID() bool {
	return c.InternalFramers.InternalDefaults.InternalScyllaUseMetadataID
}

// tracksResultMetadataID reports whether an EXECUTE on this connection carries a
// result metadata ID for the server to compare its own against — either because
// native protocol v5 makes the field mandatory, or because
// SCYLLA_USE_METADATA_ID backported it to v4. Either way the server answers a
// stale ID with METADATA_CHANGED, which is what makes skipping result metadata
// recoverable. See shouldSkipResultMetadata.
//
// Read from the same framer config as usesMetadataID, for the same reason, and
// because initCache has already masked the protocol version there.
func (c *Conn) InternalTracksResultMetadataID() bool {
	return protocol.VersionFor(c.InternalFramers.InternalDefaults.InternalProto).ResultMetadataID || c.InternalFramers.InternalDefaults.InternalScyllaUseMetadataID
}

// InternalWire returns what c's protocol version puts on the wire.
func (c *Conn) InternalWire() *protocol.Version { return protocol.VersionFor(c.InternalVersion) }

func (c *Conn) close() error {
	return c.InternalR.Close()
}

func (c *Conn) Close() {
	c.InternalCloseWithError(nil)
}

// Serve starts the stream multiplexer for this connection, which is required
// to execute any queries. This method runs as long as the connection is
// open and is therefore usually called in a separate goroutine.
func (c *Conn) InternalServe(ctx context.Context) {
	var err error
	for {
		if c.pauseSharedReader(ctx) {
			if ctx.Err() != nil {
				break
			}
			continue
		}
		err = c.InternalRecv(ctx, true)
		if err == nil {
			continue
		}

		if errors.Is(err, protocol.ErrReadHeaderTimeout) {
			// An exclusive reservation interrupts the idle header wait on
			// purpose to pause this reader; that is not a timeout.
			if !c.pauseRequested.Load() {
				c.logger().Debug("read header timeout")
			}
			err = nil
			continue
		}
		break
	}

	c.InternalCloseWithError(err)
}

func (c *Conn) discardFrame(r io.Reader, head frm.FrameHeader) error {

	_, err := io.CopyN(io.Discard, r, int64(head.Length))
	if err != nil {
		return err
	}
	return nil
}

type InternalProtocolError struct {
	InternalFrame protocol.Frame
}

func (p *InternalProtocolError) Error() string {
	if err := p.Unwrap(); err != nil {
		return err.Error()
	}
	return fmt.Sprintf("gocql: received unexpected frame on stream %d: %v", p.InternalFrame.Header().Stream, p.InternalFrame)
}

// Unwrap returns the server error when the frame is an ERROR response.
func (p *InternalProtocolError) Unwrap() error {
	if response, ok := p.InternalFrame.(*protocol.ErrorResponseFrame); ok {
		return response.Err
	}
	return nil
}

// InternalHeartBeat sends heartbeats until ctx ends. A typed connection's
// heartbeats run from a timer: the call schedules them and returns.
func (c *Conn) InternalHeartBeat(ctx context.Context) {
	if c.InternalCfg != nil && c.InternalCfg.TypedConfig != nil {
		c.startTypedHeartbeat(ctx)
		return
	}
	sleepTime := 1 * time.Second
	timer := time.NewTimer(sleepTime)
	defer timer.Stop()

	var failures int

	for {
		if failures > 5 {
			c.InternalCloseWithError(fmt.Errorf("gocql: heartbeat failed"))
			return
		}

		timer.Reset(sleepTime)

		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}

		framer, err := c.InternalExec(context.Background(), &protocol.WriteOptionsFrame{}, nil, c.InternalCfg.ConnectTimeout)
		if err != nil {
			failures++
			continue
		}

		resp, err := framer.ParseFrame()
		framer.Release()
		if err != nil {

			failures++
			continue
		}

		switch resp.(type) {
		case *frm.SupportedFrame:

			sleepTime = 30 * time.Second
			failures = 0
		case error:

			failures++
		default:

			c.logger().Warn("unexpected frame in response to OPTIONS", "frame", fmt.Sprintf("%T", resp))
			failures++

			sleepTime = 1 * time.Second
		}
	}
}

func (c *Conn) InternalRecv(ctx context.Context, startupCompleted bool) error {

	if startupCompleted && c.InternalWire().Segments {
		return c.InternalRecvSegment(ctx)
	}

	return c.InternalProcessFrame(ctx, c.InternalR)
}

func (c *Conn) InternalProcessFrame(ctx context.Context, r io.Reader) error {
	return c.InternalProcessFrameSource(ctx, InternalFrameSource{InternalR: r})
}

// readFrameHeader reads one CQL frame header from r with the read deadline
// disarmed for its first byte: the serve() loop waits indefinitely for the next
// inbound frame, so a short ReadTimeout must not fire on an idle connection. Once
// the peer has started sending, headerReader re-arms the deadline, so the rest of
// the header — and the body read that follows — are bounded by the operational
// timeout. The disarm is also cleared via defer, which covers the paths that
// deliver no byte at all, and means a panic cannot leave the connection
// deadline-free.
//
// Disarming through a dedicated flag (rather than zeroing and restoring the
// connReader timeout) keeps the operational timeout intact, so a concurrent
// finalizeConnection switching the reader from ConnectTimeout to ReadTimeout is
// never lost. The header read itself clears any stale deadline (connReader.Read).
//
// Unlike readFirstSegmentHeader, the disarm here is a dynamic check: on proto v5 r
// is a reader over an already-received segment payload, which has no deadline to
// disarm. Missing it in that case is correct, not a bug.
func (c *Conn) InternalReadFrameHeader(r io.Reader) (frm.FrameHeader, error) {
	d, _ := r.(deadlineDisarmer)
	if d != nil {
		d.InternalSetDisarm(true)
		defer d.InternalSetDisarm(false)
	}

	c.headerReader.reset(r, d)
	return protocol.ReadHeader(&c.headerReader, c.headerBuf[:])
}

func (c *Conn) InternalProcessFrameSource(ctx context.Context, src InternalFrameSource) error {
	return c.processFrameSource(ctx, src, nil)
}

// processFrameSource reads one frame header from src.InternalR and dispatches
// the frame, unless parsed is non-nil: then the caller already decoded the
// header out of a received v5 segment, src.InternalR is unused and the body is
// src.chain.
func (c *Conn) processFrameSource(ctx context.Context, src InternalFrameSource, parsed *frm.FrameHeader) error {

	r := src.InternalR

	headStartTime, headEndTime := src.netStart, src.netEnd
	var head frm.FrameHeader
	var err error
	if parsed != nil {
		head = *parsed
	} else {
		measureHere := headStartTime.IsZero() && c.InternalFrameObserver != nil

		if measureHere {
			headStartTime = time.Now()
		}

		head, err = c.InternalReadFrameHeader(r)

		if measureHere {
			headEndTime = time.Now()
		}
		if err != nil {
			return err
		}
	}

	if c.InternalFrameObserver != nil {
		c.InternalFrameObserver.ObserveFrameHeader(context.Background(), ObservedFrameHeader{
			Version: head.Version,
			Flags:   head.Flags,
			Stream:  int16(head.Stream),
			Opcode:  head.Op,
			Length:  int32(head.Length),
			Start:   headStartTime,
			End:     headEndTime,
			Host:    c.InternalHost,
		})
	}

	if src.segment != nil && head.Length > src.segment.Len() {
		return fmt.Errorf("gocql: frame header declares a %d byte body but only %d bytes remain in the self-contained segment", head.Length, src.segment.Len())
	}

	if head.Stream >= c.InternalStreams.NumStreams {
		return fmt.Errorf("gocql: frame header stream is beyond call expected bounds: %d", head.Stream)
	} else if head.Stream < 0 {

		framer, err := c.readFrameIntoFramer(src, head)
		if err != nil {
			return err
		}

		frame, err := framer.ParseFrame()

		c.InternalReleaseReadFramer(framer)
		if err != nil {
			if head.Stream == -1 {

				c.logger().Warn("unparsable event frame", logging.Err(err))
				return nil
			}
			return err
		}

		if head.Stream == -1 {
			if c.InternalHooks != nil {
				if err := c.dispatchEvent(frame); err != nil {
					return err
				}
			}
			return nil
		}

		return &InternalProtocolError{
			InternalFrame: frame,
		}
	}

	// The reader takes the call over by emptying its slot. Losing the CAS means
	// the caller withdrew an unsent request or close took the call.
	if c.admission.Load()&admissionClosed != 0 {
		return runtimeerrors.ErrConnectionClosed
	}
	call := c.InternalCalls.Get(head.Stream)
	if call != nil && !c.InternalCalls.remove(head.Stream, call) {
		if c.admission.Load()&admissionClosed != 0 {
			return runtimeerrors.ErrConnectionClosed
		}
		call = nil
	}
	if call != nil && call.InternalStreamID != head.Stream {
		return c.misplacedCall(call, head.Stream)
	}
	if call == nil {
		c.logger().Warn("response for stream with no handler", logging.KeyStream, head.Stream, "opcode", head.Op.String())
		if parsed != nil {
			// The body is already in memory; nothing is left on the wire.
			return nil
		}
		return c.discardFrame(r, head)
	}

	if call.typed {
		return c.receiveTypedCall(ctx, call, src, head)
	}

	framer := c.InternalGetReadFramer()

	// A copy keeps head on the stack for typed frames, which return above.
	legacyHead := head
	err = src.readBody(framer, &legacyHead)

	desynced := InternalBodyReadDesyncedConn(err)

	if !call.deliver(InternalCallResp{InternalFramer: framer, InternalErr: err, removedFromCalls: true}) {
		c.InternalAbandonRecvCall(call, framer)
	}

	if desynced {
		return err
	}

	return nil
}

// bodyReadDesyncedConn reports whether a frame-body read failure left the
// connection at an unknown stream offset, in which case it is fatal: the
// unconsumed remainder of the body is still on the wire and every subsequent read
// would be mis-framed. A decode failure, or an over-long frame whose body was
// successfully discarded, leaves the stream aligned and stays a per-request error.
//
// Two kinds of failure qualify.
//
// A network error means the body was read partially or not at all. errors.As
// rather than a type assertion: readFrame wraps the read error, so an assertion on
// the wrapper never matches (which is precisely how this used to leave desynced
// connections in the pool).
//
// A failed read deadline arm means connReader.Read never reached the socket, so
// the whole body is still queued on a connection that is otherwise healthy — the
// worst case, because nothing else will surface the problem and serve() reads the
// body as its next frame header. It needs naming rather than being left to the
// net.Error test: net.Conn.SetReadDeadline is only conventionally a *net.OpError,
// and a connection from a user-supplied Dialer or HostDialer may return a plain
// error, which would classify this as per-request.
func InternalBodyReadDesyncedConn(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, InternalErrArmReadDeadline) {
		return true
	}
	_, ok := errors.AsType[net.Error](err)
	return ok
}

func (c *Conn) readFrameIntoFramer(src InternalFrameSource, head frm.FrameHeader) (*protocol.Framer, error) {
	framer := c.InternalGetReadFramer()

	if err := src.readBody(framer, &head); err != nil {
		c.InternalReleaseReadFramer(framer)
		return nil, err
	}
	return framer, nil
}

// misplacedCall puts back a call found under a stream other than its own
// (a corrupt table) and reports the fatal error, whose close then answers the
// call. If the slot was taken meanwhile, or close may already have drained the
// table, the reader still owns the call and answers it here.
//
//go:noinline
func (c *Conn) misplacedCall(call *InternalCallReq, stream int) error {
	err := fmt.Errorf("gocql: response for stream %d dispatched to a call on stream %d", stream, call.InternalStreamID)
	if c.InternalCalls.register(stream, call) &&
		(c.admission.Load()&admissionClosed == 0 || !c.InternalCalls.remove(stream, call)) {
		return err
	}
	// The table is corrupt, so the call's stream is left alone and the call
	// is not recycled. Like close, this abandons the stream for the observer.
	call.deliver(InternalCallResp{InternalErr: err})
	if call.streamObserverContext != nil {
		call.streamObserverEndOnce.Do(func() {
			call.streamObserverContext.StreamAbandoned(ObservedStream{Host: c.InternalHost})
		})
	}
	return err
}

func (c *Conn) InternalAbandonRecvCall(call *InternalCallReq, framer *protocol.Framer) {
	c.InternalReleaseReadFramer(framer)
	c.releaseStream(call)
	call.waitExecDone("abandonRecvCall")
	putCallReq(call)
}

// releaseStream frees call's stream, settles an orphan, and ends the stream
// for the observer. Its caller owns call's cleanup: it received the reader's
// response, or the call never reached the reader, or it is the reader and the
// caller had stopped waiting. call is out of the call table by then, so the
// stream can be reused at once.
func (c *Conn) releaseStream(call *InternalCallReq) {
	if call.orphaned {
		call.orphaned = false
		c.orphans.Add(-1)
	}
	c.freeStream(call.InternalStreamID)

	if call.streamObserverContext != nil {
		call.streamObserverEndOnce.Do(func() {
			call.streamObserverContext.StreamFinished(ObservedStream{
				Host: c.InternalHost,
			})
		})
	}
}

func (c *Conn) InternalRecvSegment(ctx context.Context) error {
	return c.InternalRecvLeasedSegment(ctx)
}

// observedNow returns the current time when a frame header observer is
// installed, and the zero time otherwise. Callers thread the result into
// frameSource, where a zero value means "not measured" and processFrameSource
// falls back to timing the header read itself.
func (c *Conn) observedNow() time.Time {
	if c.InternalFrameObserver == nil {
		return time.Time{}
	}
	return time.Now()
}

// readFirstSegmentHeader reads the header of the next segment, with the read
// deadline disarmed for its first byte so an idle serve() loop can block
// indefinitely waiting for the next frame to begin. The deadline is disarmed via a
// dedicated flag (so connReader.Read does not re-arm it), which leaves the
// operational timeout value intact, so a concurrent finalizeConnection switching
// the reader from ConnectTimeout to ReadTimeout is never clobbered.
//
// Only the idle wait is unbounded. headerReader re-arms the deadline as soon as the
// peer delivers a byte, so the rest of the header is bounded by ReadTimeout and a
// peer that sends a header prefix and then stalls cannot hold the serve goroutine;
// the deferred clear covers the paths that deliver no byte at all, and a panic.
//
// A read timeout during the idle wait is normalised to ErrReadHeaderTimeout so
// serve() treats it as a benign idle timeout instead of closing the connection —
// but only if the read consumed nothing. A timeout partway through a header leaves
// the stream at an unknown offset, so it stays a plain error and takes the
// connection down rather than mis-framing everything that follows. That is the
// timeout the re-arm above makes reachable.
func (c *Conn) InternalReadFirstSegmentHeader() (segment.Header, error) {

	c.InternalR.InternalSetDisarm(true)
	defer c.InternalR.InternalSetDisarm(false)

	c.headerReader.reset(c.InternalR, c.InternalR)

	hdr, err := segment.ReadHeaderInto(&c.headerReader, c.InternalSegCompressor != nil, c.headerBuf[:])
	if err != nil {
		if netErr, ok := errors.AsType[net.Error](err); c.headerReader.n == 0 && ok && netErr.Timeout() {
			return segment.Header{}, fmt.Errorf("%w: %w", protocol.ErrReadHeaderTimeout, err)
		}
		return segment.Header{}, err
	}
	return hdr, nil
}

// readSegmentPayload reads the payload of a segment whose header has already been
// read, in the layout c.segCompressor selects — nil being the uncompressed one. The
// narrowing behind that field happened once during the handshake; see
// Conn.resolveSegmentCompressor.
//
// The header readers above take their layout from the same field. They branch on a
// bool where this branches on the compressor being non-nil, and a connection whose two
// answers disagreed would read an 8-byte compressed header as a 6-byte uncompressed
// one and mis-frame everything after it.
func (c *Conn) readSegmentPayload(hdr segment.Header) ([]byte, error) {
	return segment.ReadPayload(c.InternalR, hdr, c.InternalSegCompressor, &c.segScratch)
}

// recvSplitFrame reassembles a single CQL frame that the peer split across
// multiple non-self-contained segments and processes it. first is the payload of
// the segment already consumed by recvSegment.
//
// Every read here runs with the deadline armed (the peer is mid-transfer), so a
// peer that stops sending fails within one ReadTimeout — see connReader.Read for
// how a read that stalls but keeps progressing is resumed instead. The
// reassembly buffer is allocated exactly once, sized to the frame length the peer
// declared in the CQL frame header, and appending the arriving payloads is bounded
// by that length. So neither a lying header nor incremental growth can inflate it:
// growing a buffer to a frm.MaxFrameSize frame would end up holding ~512 MiB for a
// valid 256 MiB response. Ownership of the buffer is then handed to the read
// framer rather than copied into it, so the frame is never resident twice.
//
// The declared length itself is the peer's to choose, up to frm.MaxFrameSize, so a
// small hostile prologue still buys this one allocation before any body byte has
// arrived. Accepted deliberately, because it is bounded: at most once per
// connection — the continuation reads run under ReadTimeout, so a peer that
// stalls after declaring takes the connection down with it. Growing the buffer
// as payloads arrive would blunt that, at the cost of roughly doubling peak
// memory for every valid large frame, which is the common case. Contrast the
// self-contained path, where the same lie is repeatable per-request and is
// therefore rejected before the allocation instead (see frameSource).
//
// netStart/netEnd are the network-read window of the first segment, for
// FrameHeaderObserver. netEnd is extended below if the CQL header itself needed
// more segments to arrive — but only that far: the observer's End is when the
// header finished arriving, not the rest of the frame.
func (c *Conn) recvSplitFrame(ctx context.Context, first []byte, netStart, netEnd time.Time) error {

	if len(first) < protocol.HeadSize {
		accumulated := append([]byte(nil), first...)
		for len(accumulated) < protocol.HeadSize {
			payload, err := c.readContinuationSegment()
			if err != nil {
				return err
			}
			accumulated = append(accumulated, payload...)
		}
		first = accumulated
		netEnd = c.observedNow()
	}

	head, err := protocol.ReadHeader(bytes.NewReader(first[:protocol.HeadSize]), c.headerBuf[:])
	if err != nil {
		return err
	}
	if head.Length < 0 || head.Length > frm.MaxFrameSize {
		return fmt.Errorf("gocql: invalid frame body length in segmented frame: %d", head.Length)
	}
	total := protocol.HeadSize + head.Length
	if len(first) > total {
		return fmt.Errorf("gocql: segmented frame exceeds its declared length %d", total)
	}

	frame := make([]byte, 0, total)
	frame = append(frame, first...)
	for len(frame) < total {
		payload, err := c.readContinuationSegment()
		if err != nil {
			return err
		}
		if len(frame)+len(payload) > total {
			return fmt.Errorf("gocql: segmented frame exceeds its declared length %d", total)
		}
		frame = append(frame, payload...)
	}

	return c.InternalProcessFrameSource(ctx, InternalFrameSource{
		InternalR: bytes.NewReader(frame),
		body:      frame[protocol.HeadSize:],
		netStart:  netStart,
		netEnd:    netEnd,
	})
}

// readContinuationSegment reads the next segment of a frame split across several
// segments and returns its payload, which aliases c.segScratch and is therefore
// only valid until the next segment is read. The reads run with the deadline armed,
// so a peer that stops sending fails within one ReadTimeout (connReader.Read).
// A self-contained segment, or one that makes no forward progress (empty
// payload), is rejected so a hostile peer cannot drive an infinite reassembly
// loop.
func (c *Conn) readContinuationSegment() ([]byte, error) {
	hdr, err := segment.ReadHeader(c.InternalR, c.InternalSegCompressor != nil)
	if err != nil {
		return nil, fmt.Errorf("gocql: failed to read continuation segment header: %w", err)
	}
	if hdr.IsSelfContained {
		return nil, fmt.Errorf("gocql: received self-contained segment, but expected a continuation")
	}
	payload, err := c.readSegmentPayload(hdr)
	if err != nil {
		return nil, fmt.Errorf("gocql: failed to read continuation segment payload: %w", err)
	}
	if len(payload) == 0 {
		return nil, fmt.Errorf("gocql: continuation segment made no progress (empty payload)")
	}
	return payload, nil
}

var _ InternalConnReadSource = (*InternalConnReader)(nil)

type InternalCallReq struct {
	// streamObserverContext is notified about events regarding this stream
	streamObserverContext StreamObserverContext
	// resp will receive the frame that was sent as a response to this stream.
	// It has one slot, so a sender never blocks; deliver and stopWaiting decide
	// through state which side owns a response.
	InternalResp chan InternalCallResp
	// ownResp is the call's own channel; InternalResp is a context's wake
	// channel (attemptWake) for the duration of one typed wait.
	ownResp chan InternalCallResp
	// dest is caller-owned storage for a typed response. The reader parses
	// into it only after claiming the call while the caller still waits; a
	// caller that stops waiting then receives the claimed response.
	dest             *InternalResponsePage
	timer            *time.Timer
	done             sync.WaitGroup
	InternalStreamID int // current stream in use
	// streamObserverEndOnce ensures that either StreamAbandoned or StreamFinished is called,
	// but not both.
	streamObserverEndOnce sync.Once
	state                 atomic.Uint32 // callWaiting, callClaimed, callDelivered or callStopped
	orphaned              bool
	typed                 bool
}

var callReqPool = syncx.NewPool(func() *InternalCallReq {
	resp := make(chan InternalCallResp, 1)
	return &InternalCallReq{InternalResp: resp, ownResp: resp}
})

func getCallReq(streamID int) *InternalCallReq {
	call := callReqPool.Get()
	call.state.Store(callWaiting)
	call.typed = false
	call.orphaned = false
	call.InternalStreamID = streamID
	call.dest = nil
	call.streamObserverContext = nil
	call.streamObserverEndOnce = sync.Once{}
	call.done = sync.WaitGroup{}
	call.done.Add(1)
	return call
}

func putCallReq(call *InternalCallReq) {
	if call.timer != nil {
		if !call.timer.Stop() {
			select {
			case <-call.timer.C:
			default:
			}
		}
	}
	call.streamObserverContext = nil
	call.streamObserverEndOnce = sync.Once{}
	call.InternalStreamID = 0
	call.dest = nil
	if call.ownResp != nil {
		call.InternalResp = call.ownResp
	}
	// A correct exchange leaves the slot empty; never hand a stale response to
	// the next call.
	select {
	case <-call.InternalResp:
	default:
	}
	callReqPool.Put(call)
}

const (
	callWaiting uint32 = iota
	callDelivered
	callStopped
	callClaimed // the reader is building the response in dest
)

// deliver hands resp to the waiting caller without blocking. It reports false
// when the caller already stopped waiting; the sender then still owns resp and
// the call cleanup.
func (call *InternalCallReq) deliver(resp InternalCallResp) bool {
	if !call.state.CompareAndSwap(callWaiting, callDelivered) && !call.state.CompareAndSwap(callClaimed, callDelivered) {
		return false
	}
	call.InternalResp <- resp
	return true
}

// claim reserves the call's response for the reader while the caller still
// waits, so that the reader may build it in the caller's dest.
func (call *InternalCallReq) claim() bool {
	return call.state.CompareAndSwap(callWaiting, callClaimed)
}

// stopWaiting ends a caller's wait that did not receive from
// InternalResp. If a response was already delivered, it is returned and the
// caller owns it; otherwise the sender keeps ownership of any later response.
func (call *InternalCallReq) stopWaiting() (InternalCallResp, bool) {
	if call.state.CompareAndSwap(callWaiting, callStopped) {
		return InternalCallResp{}, false
	}
	for {
		// A context's wake channel can hold a wake ahead of the response.
		if resp := <-call.InternalResp; !resp.wake {
			return resp, true
		}
	}
}

func (call *InternalCallReq) finishExec() {
	call.done.Done()
}

func (call *InternalCallReq) waitExecDone(where string) {
	call.waitCallReqDone(where)
}

// InternalRemoveCallIfOpen removes call from the call table only if exec()
// still owns its cleanup and call is still the call registered for its
// stream. Removal is by identity: the stream may already have been freed and
// reused by another call, which must stay registered. Once the connection has
// started closing, closeWithError() becomes responsible for draining and
// recycling detached callReqs. A close that sets its flag after the check
// below races this CAS against its drain: whichever empties the slot owns the
// call.
func (c *Conn) InternalRemoveCallIfOpen(call *InternalCallReq) bool {
	if c.admission.Load()&admissionClosed != 0 {
		return false
	}
	return c.InternalCalls.remove(call.InternalStreamID, call)
}

type InternalCallResp struct {
	page *InternalResponsePage
	// framer is the response frame.
	// May be nil if err is not nil.
	InternalFramer *protocol.Framer
	// err is error encountered, if any.
	InternalErr error
	// removedFromCalls means the sender already deleted this call from c.calls, so
	// closeWithError's drain will never see it and the receiver owns releasing the
	// stream and recycling the callReq. Set by processFrameSource on every outcome
	// it delivers directly; Conn.closed races this delivery and cannot answer it.
	removedFromCalls bool
	// wake marks a cancellation sent by the wait's context, not a response.
	wake bool
}

// InternalNewWakeChannel makes the channel a context hands to typed waits
// (see attemptWake): one response and one wake fit without blocking.
func InternalNewWakeChannel() chan InternalCallResp { return make(chan InternalCallResp, 2) }

// InternalWake sends a cancellation's wake to a wait on ch without blocking.
// A full channel already holds what ends the wait.
func InternalWake(ch chan InternalCallResp) {
	select {
	case ch <- InternalCallResp{wake: true}:
	default:
	}
}

// InternalDrainWake discards a wake left from a previous use of ch.
func InternalDrainWake(ch chan InternalCallResp) {
	select {
	case <-ch:
	default:
	}
}

// allocCall allocates a stream for call and registers call under it, without
// the connection lock. It fails if the connection already started closing or
// has no free stream.
func (c *Conn) allocCall(call *InternalCallReq) error {
	stream, ok := c.InternalStreams.GetStream()
	if !ok {
		return runtimeerrors.ErrNoStreams
	}
	call.InternalStreamID = stream
	if !c.InternalCalls.register(stream, call) {
		return c.streamTaken(stream)
	}
	// Close sets its flag before draining the table; this reads the flag
	// after registering. So either the drain finds the call, or this sees
	// the flag and withdraws it.
	if c.admission.Load()&admissionClosed != 0 {
		return c.registeredOnClosed(call)
	}
	return nil
}

// streamTaken reports a free stream whose slot still holds a call.
//
//go:noinline
func (c *Conn) streamTaken(stream int) error {
	existing := -1
	if call := c.InternalCalls.Get(stream); call != nil {
		existing = call.InternalStreamID
	}
	c.freeStream(stream)
	return fmt.Errorf("attempting to use stream already in use: %d -> %d", stream, existing)
}

// registeredOnClosed withdraws a call registered on a connection that started
// closing. If close's drain took it first, the call stays with close, which
// answers it, and the caller proceeds as for any registered call.
//
//go:noinline
func (c *Conn) registeredOnClosed(call *InternalCallReq) error {
	if !c.InternalCalls.remove(call.InternalStreamID, call) {
		return nil
	}
	c.freeStream(call.InternalStreamID)
	return runtimeerrors.ErrConnectionClosed
}

// allocSharedCall is beginShared followed by allocCall. Admission takes the
// connection lock only while the connection is reserved. On success the caller
// owns a shared admission it must end with endShared.
func (c *Conn) allocSharedCall(ctx context.Context, call *InternalCallReq) error {
	if err := c.beginShared(ctx); err != nil {
		return err
	}
	if err := c.allocCall(call); err != nil {
		c.endShared()
		return err
	}
	return nil
}

// exec executes a frame on the connection and returns the response framer.
//
// IMPORTANT: The caller takes ownership of the returned framer and MUST call
// framer.Release() when done reading the response. Failure to release the framer
// will leak memory and prevent buffer reuse.
//
// The framer should be released as soon as the response data is no longer needed,
// typically via defer immediately after parsing or after transferring ownership
// to an Iter.
func (c *Conn) InternalExec(ctx context.Context, req protocol.FrameBuilder, tracer Tracer, requestTimeout time.Duration) (*protocol.Framer, error) {
	return c.execInternal(ctx, req, tracer, requestTimeout, true)
}

func (c *Conn) execInternal(ctx context.Context, req protocol.FrameBuilder, tracer Tracer, requestTimeout time.Duration, startupCompleted bool) (*protocol.Framer, error) {
	if err := c.beginShared(ctx); err != nil {
		return nil, runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: err})
	}
	defer c.endShared()
	return c.execAdmitted(ctx, req, tracer, requestTimeout, startupCompleted)
}

func (c *Conn) execAdmitted(ctx context.Context, req protocol.FrameBuilder, tracer Tracer, requestTimeout time.Duration, startupCompleted bool) (*protocol.Framer, error) {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: ctxErr, InternalPotentiallyExecuted: false})
	}

	call := getCallReq(0)
	if err := c.allocCall(call); err != nil {
		call.finishExec()
		putCallReq(call)
		return nil, runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: err, InternalPotentiallyExecuted: false})
	}
	stream := call.InternalStreamID

	framer := c.InternalGetWriteFramer()

	if c.InternalStreamObserver != nil {
		call.streamObserverContext = c.InternalStreamObserver.StreamContext(ctx)
	}

	var (
		stopWaiting   bool
		releaseStream bool
		recycleCall   bool
		closeErr      error
	)

	defer func() {
		if closeErr != nil {
			c.InternalCloseWithError(closeErr)
		}
	}()

	defer func() {
		if stopWaiting {
			if resp, ok := call.stopWaiting(); ok {
				// The response arrived as we stopped waiting; it is ours to drop.
				if resp.InternalFramer != nil {
					c.InternalReleaseReadFramer(resp.InternalFramer)
				}
				if resp.removedFromCalls {
					releaseStream = true
					recycleCall = true
				}
			}
		}
		call.finishExec()
		if releaseStream {
			c.releaseStream(call)
		}
		if recycleCall {
			putCallReq(call)
		}
	}()

	if tracer != nil {
		framer.Trace()
	}

	if call.streamObserverContext != nil {
		call.streamObserverContext.StreamStarted(ObservedStream{
			Host: c.InternalHost,
		})
	}

	err := req.BuildFrame(framer, stream)
	if err != nil {
		c.InternalReleaseWriteFramer(framer)

		stopWaiting = true
		if c.InternalRemoveCallIfOpen(call) {

			releaseStream = true
			recycleCall = true
		}
		return nil, runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: err, InternalPotentiallyExecuted: false})
	}

	if c.InternalWire().Segments && startupCompleted {
		if err = framer.PrepareModernLayout(); err != nil {
			c.InternalReleaseWriteFramer(framer)

			stopWaiting = true
			if c.InternalRemoveCallIfOpen(call) {
				releaseStream = true
				recycleCall = true
			}
			return nil, runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: err, InternalPotentiallyExecuted: false})
		}
	}

	n, err := c.InternalW.InternalWriteContext(ctx, framer.Buf)
	c.InternalReleaseWriteFramer(framer)
	if err != nil {

		stopWaiting = true
		if (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) && n == 0 {

			if c.InternalRemoveCallIfOpen(call) {

				releaseStream = true
				recycleCall = true
			}
		} else {

			closeErr = err
		}
		return nil, runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: err, InternalPotentiallyExecuted: true})
	}

	var timeoutCh <-chan time.Time
	if requestTimeout > 0 {
		if call.timer == nil {
			call.timer = time.NewTimer(requestTimeout)
		} else {
			if !call.timer.Stop() {
				select {
				case <-call.timer.C:
				default:
				}
			}
			call.timer.Reset(requestTimeout)
		}
		timeoutCh = call.timer.C
	}

	var ctxDone <-chan struct{}
	if ctx != nil {
		ctxDone = ctx.Done()
	}

	select {
	case resp := <-call.InternalResp:
		if resp.InternalErr != nil {
			c.InternalReleaseReadFramer(resp.InternalFramer)
			if resp.removedFromCalls {

				releaseStream = true
				recycleCall = true
			}
			return nil, runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: resp.InternalErr, InternalPotentiallyExecuted: true})
		}

		releaseStream = true
		recycleCall = true

		if v := resp.InternalFramer.Header.Version.Version(); v != c.InternalVersion {
			c.InternalReleaseReadFramer(resp.InternalFramer)
			return nil, runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: cqltype.NewFrameError("unexpected protocol version in response: got %d expected %d", v, c.InternalVersion), InternalPotentiallyExecuted: true})
		}

		return resp.InternalFramer, nil
	case <-timeoutCh:
		stopWaiting = true
		if c.markOrphan(call) {
			closeErr = errOrphanBudget
		}
		return nil, runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: runtimeerrors.ErrTimeoutNoResponse, InternalPotentiallyExecuted: true, InternalTimeout: requestTimeout, InternalInFlight: c.InternalStreams.InUse()})
	case <-ctxDone:
		stopWaiting = true
		if c.markOrphan(call) {
			closeErr = errOrphanBudget
		}
		return nil, runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: ctx.Err(), InternalPotentiallyExecuted: true, InternalTimeout: requestTimeout, InternalInFlight: c.InternalStreams.InUse()})
	case <-c.InternalCtx.Done():
		stopWaiting = true
		return nil, runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: runtimeerrors.ErrConnectionClosed, InternalPotentiallyExecuted: true})
	}
}

// ObservedStream observes a single request/response stream.
type ObservedStream struct {
	// Host of the connection used to send the stream.
	Host *implEndpoint.HostInfo
}

// StreamObserver is notified about request/response pairs.
// Streams are created for executing queries/batches or
// internal requests to the database and might live longer than
// execution of the query - the stream is still tracked until
// response arrives so that stream IDs are not reused.
type StreamObserver interface {
	// StreamContext is called before creating a new stream.
	// ctx is context passed to Session.Query / Session.Batch,
	// but might also be an internal context (for example
	// for internal requests that use control connection).
	// StreamContext might return nil if it is not interested
	// in the details of this stream.
	// StreamContext is called before the stream is created
	// and the returned StreamObserverContext might be discarded
	// without any methods called on the StreamObserverContext if
	// creation of the stream fails.
	// Note that if you don't need to track per-stream data,
	// you can always return the same StreamObserverContext.
	StreamContext(ctx context.Context) StreamObserverContext
}

// StreamObserverContext is notified about state of a stream.
// A stream is started every time a request is written to the server
// and is finished when a response is received.
// It is abandoned when the underlying network connection is closed
// before receiving a response.
type StreamObserverContext interface {
	// StreamStarted is called when the stream is started.
	// This happens just before a request is written to the wire.
	StreamStarted(observedStream ObservedStream)

	// StreamAbandoned is called when we stop waiting for response.
	// This happens when the underlying network connection is closed.
	// StreamFinished won't be called if StreamAbandoned is.
	StreamAbandoned(observedStream ObservedStream)

	// StreamFinished is called when we receive a response for the stream.
	StreamFinished(observedStream ObservedStream)
}

func InternalMetadataIDTracked(idExchangeActive bool, resultMetadataID []byte) bool {
	return idExchangeActive && len(resultMetadataID) > 0
}

func (c *Conn) Closed() bool {
	c.InternalMu.Lock()
	defer c.InternalMu.Unlock()
	return c.InternalClosed
}

func (c *Conn) Address() string {
	return c.InternalAddr
}

func (c *Conn) AvailableStreams() int {
	if c.InternalExclusive() {
		return 0
	}
	return c.InternalStreams.Available()
}

func InternalUseKeyspaceStmt(keyspace string) string {
	return `USE "` + strings.ReplaceAll(keyspace, `"`, `""`) + `"`
}

func (c *Conn) UseKeyspace(keyspace string) error {
	return c.InternalUseKeyspaceContext(c.InternalCtx, keyspace)
}

func (c *Conn) InternalUseKeyspaceContext(ctx context.Context, keyspace string) error {
	q := &protocol.UseKeyspaceFrame{Statement: InternalUseKeyspaceStmt(keyspace)}

	framer, err := c.InternalExec(ctx, q, nil, c.InternalCfg.ConnectTimeout)
	if err != nil {
		return err
	}
	defer framer.Release()

	resp, err := framer.ParseFrame()
	if err != nil {
		return err
	}

	switch x := resp.(type) {
	case *protocol.ResultKeyspaceFrame:
	case *protocol.ErrorResponseFrame:
		return x.Err
	default:
		return cqltype.NewFrameError("unknown frame in response to USE: %v", x)
	}

	c.setCurrentKeyspace(keyspace)

	return nil
}

// setCurrentKeyspace records the keyspace this connection was switched to. See the
// currentKeyspace field for why it is atomic.
func (c *Conn) setCurrentKeyspace(keyspace string) {
	c.currentKeyspace.Store(&keyspace)
}

const InternalQrySystemPeers = "SELECT peer, data_center, host_id, rack, release_version, rpc_address, schema_version, tokens FROM system.peers"

const InternalQrySystemPeersCassandra = "SELECT peer, data_center, host_id, preferred_ip, rack, release_version, rpc_address, schema_version, tokens FROM system.peers"

const InternalQrySystemPeersV2 = "SELECT peer, data_center, host_id, native_address, native_port, preferred_ip, rack, release_version, schema_version, tokens FROM system.peers_v2"

const InternalQrySystemLocal = "SELECT broadcast_address, cluster_name, data_center, host_id, listen_address, partitioner, rack, release_version, rpc_address, schema_version, tokens FROM system.local WHERE key='local'"

// connectionHooks is the session-owned event/agreement boundary. Transport owns
// socket readers and dispatch queues; the coordinator owns publication workers.
type InternalConnectionHooks interface {
	InternalStartConnectionWorker(func()) bool
	InternalConnectionWorkersDone() <-chan struct{}
	InternalHandleConnectionEvent(context.Context, protocol.Frame, func()) bool
}

type InternalStartupIdentity struct {
	DriverName, DriverVersion string
	ApplicationInfo           implEndpoint.ApplicationInfo
	SessionID                 string
}

type InternalStartupReporter interface{ InternalUpdateStartupOptions(map[string]string, bool) }

type poolOwner interface{ InternalNotifyReservationChanged() }

type InternalConnectionPoolPolicy struct {
	InternalDisableShardAware bool
	InternalMaxExcessRate     float32
}

// frameSource supplies one CQL frame to processFrame. The header always comes
// from r — the socket, or a buffer holding an already-received segment payload.
//
// Field order is dictated by govet's fieldalignment rather than by how the fields
// relate: body's length and capacity are the only trailing words here that hold no
// pointer, so it goes last to keep the pointer-scanned prefix short. It belongs
// with r.
type InternalFrameSource struct {
	// netStart/netEnd are the window of the network read that delivered these
	// bytes, for FrameHeaderObserver. On v5 the CQL header is parsed out of a
	// segment that has already arrived, so processFrameSource cannot measure the
	// network wait itself — the reader that could is recvSegment, and it passes
	// the window down here.
	//
	// Zero means "not measured", which is the pre-v5 socket path: there
	// processFrameSource reads the header off the wire and times it directly.
	netStart, netEnd time.Time
	InternalR        io.Reader
	owner            *wire.Lease
	// segment, when non-nil, is the self-contained v5 segment payload r reads
	// from. Such a segment carries only whole frames, so a header declaring a body
	// longer than what is left in the segment is a framing violation, and
	// processFrameSource rejects it before readFrame acts on the declared length.
	//
	// That check is what keeps the length honest here. Off the socket, a lying
	// header costs the peer the bytes it claimed or stalls into a net.Error that
	// takes the connection down; out of a segment the short read is immediate and
	// yields io.ErrUnexpectedEOF, which is not a net.Error, so processFrameSource
	// keeps it per-request and leaves the connection up. A ~20-byte segment could
	// otherwise buy a frm.MaxFrameSize allocation, repeatable for as long as the peer
	// cares to send them.
	//
	// Nil on the pre-v5 socket path, and nil for a reassembled frame, where
	// framer.adoptFrameBody already matches the declared length exactly.
	segment *bytes.Reader
	// body is set only when the whole frame body is already in memory and the
	// buffer holding it can be given away: the read framer then adopts it instead
	// of reading and copying the body a second time (see framer.adoptFrameBody).
	body  []byte
	chain wire.Chain
}

// readBody fills f with the frame body described by head, either by reading it
// from s.r or by adopting the buffer s already holds.
func (s InternalFrameSource) readBody(f *protocol.Framer, head *frm.FrameHeader) error {
	if s.owner != nil {
		return f.ReadFrame(newChainReader(s.chain), head)
	}
	if s.body != nil {
		return f.AdoptFrameBody(s.body, head)
	}
	return f.ReadFrame(s.InternalR, head)
}

// headerReader reads one frame or segment header, bounding the read-deadline
// disarm to the wait for its first byte.
//
// The idle wait has to be unbounded: serve() blocks on the next header for as long
// as the peer has nothing to send, and a short ReadTimeout must not drop a healthy
// connection. But once the first byte has arrived the peer is mid-header, and the
// rest belongs under ReadTimeout like any other transfer — otherwise a peer that
// sends one byte and then stops holds the serve goroutine for as long as it keeps
// the socket open, and the connection is never dropped.
//
// The disarm is per-read (connReader.armDeadline runs once per read attempt), so
// bounding it means capping the first read to a single byte and clearing the disarm
// before the next one; the remainder is then read with the deadline armed. Capping
// performs a separate socket read. The production reader intentionally does not
// prefetch payload bytes: bodies are read directly into their final leased storage.
//
// n counts the bytes delivered. That is the caller's benign-vs-fatal signal for a
// header-read timeout: nothing consumed is the idle peer, part of a header consumed
// leaves the stream at an unknown offset (readFirstSegmentHeader, readHeader).
type headerReader struct {
	r io.Reader
	// disarm is the reader whose deadline is disarmed for the first byte, and is
	// cleared once that byte arrives so the cap and the re-arm happen exactly once.
	// Nil when there is no deadline to bound: on proto v5 the CQL header is parsed
	// out of a segment payload that has already been received.
	disarm deadlineDisarmer
	n      int
}

func (h *headerReader) reset(r io.Reader, disarm deadlineDisarmer) {
	h.r = r
	h.disarm = disarm
	h.n = 0
}

func (h *headerReader) Read(p []byte) (int, error) {
	if h.disarm != nil && len(p) > 1 {

		p = p[:1]
	}
	n, err := h.r.Read(p)
	h.n += n
	if n > 0 && h.disarm != nil {

		h.disarm.InternalSetDisarm(false)
		h.disarm = nil
	}
	return n, err
}

// deadlineDisarmer is a reader whose read deadline can be disarmed for a read
// that is expected to wait indefinitely (the idle wait for the next frame or
// segment header). See connReader.armDeadline.
type deadlineDisarmer interface {
	InternalSetDisarm(bool)
}

// connReadSource is the reader Conn reads through: the part of net.Conn the
// receive path actually uses, plus read-timeout control and the deadline disarm.
//
// Unexported, deliberately. The disarm is not optional for the connection's own
// reader, so requiring it here turns "this reader silently loses the disarm"
// from a runtime surprise into a compile error, and an unexported method means
// only this package can implement it. That costs nothing: Conn.r is unexported
// and no exported API accepts or returns one.
type InternalConnReadSource interface {
	// Read reads data from the connection.
	Read(p []byte) (n int, err error)

	// Close closes the connection.
	Close() error

	// RemoteAddr returns the remote network address, if known.
	RemoteAddr() net.Addr

	// SetTimeout sets the timeout duration for reading data from the conn.
	SetTimeout(timeout time.Duration)

	// GetTimeout returns the timeout duration.
	GetTimeout() time.Duration

	deadlineDisarmer
}

// connReader implements connReadSource.
type InternalConnReader struct {
	InternalConn net.Conn
	// Production uses an 8 KiB shared buffer. Exclusive acquisition transfers
	// this reader at a checked frame boundary, preserving all unread bytes.
	InternalR      *bufio.Reader
	timeout        atomic.Int64
	deadlineMu     sync.Mutex
	pause          atomic.Bool
	InternalDisarm atomic.Bool
	transferable   bool
	// lastDeadlineZero records that the socket's read deadline is already zero:
	// the last SetReadDeadline succeeded with a zero time. It lets the default
	// no-timeout configuration skip the syscall on every read. Guarded by
	// deadlineMu, and cleared by every call that applies a non-zero deadline.
	lastDeadlineZero bool
}

// maxReadAttempts bounds how many times Read re-arms the deadline and resumes a
// read that timed out while still making progress. See Read.
const InternalMaxReadAttempts = 5

// errArmReadDeadline marks a read that failed before it reached the socket,
// because the read deadline could not be armed. Nothing was consumed, which makes
// it fatal to a frame body read: the body is still queued on a connection that is
// otherwise healthy (see bodyReadDesyncedConn). It is a named error rather than a
// bare wrap because that classification cannot be inferred from the underlying
// error, which need not be a net.Error.
var InternalErrArmReadDeadline = errors.New("gocql: unable to arm the read deadline")

// Read fills p, resuming across read-deadline expiries for as long as the peer
// keeps delivering bytes.
//
// The retry exists because ReadTimeout is a per-read budget, not a transfer budget:
// a large frame body arriving over a slow link can need more than one budget while
// being perfectly healthy. Each attempt arms a fresh deadline and resumes at p[n:],
// so no bytes are dropped.
//
// It is gated on forward progress, which is what keeps ReadTimeout meaningful as
// "identify faulty connections early and drop it" (see clusterSettings.ReadTimeout):
// an attempt that delivered nothing means the peer has stopped, and that fails
// immediately in a single ReadTimeout rather than after maxReadAttempts of them.
// Only a timeout is resumable — for any other network error the stream's position
// is no longer trustworthy, so resuming could silently mis-frame.
//
// Note the caller can still observe a partial read: on failure n bytes of p were
// consumed off the connection and cannot be put back, so a frame reader that gets
// an error here has to treat the connection as desynced (see processFrameSource).
func (c *InternalConnReader) Read(p []byte) (n int, err error) {
	if c.InternalR != nil && c.InternalR.Buffered() >= len(p) {
		return io.ReadFull(c.InternalR, p)
	}
	for attempt := 0; attempt < InternalMaxReadAttempts; attempt++ {
		if aerr := c.armDeadline(); aerr != nil {

			return n, fmt.Errorf("%w: %w", InternalErrArmReadDeadline, aerr)
		}

		var source io.Reader = c.InternalConn
		if c.InternalR != nil {
			source = c.InternalR
		}
		nn, rerr := io.ReadFull(source, p[n:])
		n += nn
		if rerr == nil {
			return n, nil
		}
		err = rerr

		if netErr, ok := errors.AsType[net.Error](err); !ok || !netErr.Timeout() {

			break
		}
		if nn == 0 {

			break
		}
	}

	return n, err
}

// readAvailable returns after one source read so exclusive body read-ahead never
// waits to fill speculative capacity. The ordinary shared Read keeps its full-
// fill and progressive timeout behavior.
func (c *InternalConnReader) readAvailable(p []byte) (int, error) {
	if err := c.armDeadline(); err != nil {
		return 0, fmt.Errorf("%w: %w", InternalErrArmReadDeadline, err)
	}
	if c.InternalR != nil {
		return c.InternalR.Read(p)
	}
	return c.InternalConn.Read(p)
}

// armDeadline arms (or clears) the underlying read deadline for the read attempt
// that is about to start — once per attempt, so a Read that resumes after a timeout
// gets a fresh deadline, and so a concurrent finalizeConnection switching the
// reader from ConnectTimeout to ReadTimeout is picked up by the next attempt.
//
// The setter error is returned rather than dropped: a net.Conn
// whose SetReadDeadline fails would otherwise be read with no deadline at all,
// or with a stale one from an earlier read. This mirrors the write path, where
// deadlineContextWriter.writeContext and writeCoalescer.flush both report
// SetWriteDeadline failures instead of writing anyway.
func (c *InternalConnReader) armDeadline() error {
	c.deadlineMu.Lock()
	defer c.deadlineMu.Unlock()
	if c.InternalConn == nil {
		return nil
	}
	if c.InternalDisarm.Load() {
		if c.pause.Load() {
			c.lastDeadlineZero = false
			return c.InternalConn.SetReadDeadline(time.Now())
		}

		return c.setZeroReadDeadline()
	}
	if timeout := c.GetTimeout(); timeout > 0 {
		c.lastDeadlineZero = false
		return c.InternalConn.SetReadDeadline(time.Now().Add(timeout))
	}

	return c.setZeroReadDeadline()
}

// setZeroReadDeadline clears the read deadline unless it is already clear.
// The caller holds deadlineMu.
func (c *InternalConnReader) setZeroReadDeadline() error {
	if c.lastDeadlineZero {
		return nil
	}
	err := c.InternalConn.SetReadDeadline(time.Time{})
	c.lastDeadlineZero = err == nil
	return err
}

// setDisarm enables or disables the read-deadline disarm used around the
// frame/segment header read (see Read). It deliberately leaves the operational
// timeout value untouched so it can be toggled without racing a concurrent
// finalizeConnection.
//
// It needs no lock. Only the reader goroutine calls it, and that goroutine
// arms the deadline under deadlineMu before every socket read, re-reading
// both flags. InternalRequestPause runs under the same lock, so it either
// precedes that arming, which then sees the pause, or follows it and sees
// the disarm stored before it.
func (c *InternalConnReader) InternalSetDisarm(v bool) {
	c.InternalDisarm.Store(v)
}

func (c *InternalConnReader) Close() error {
	return c.InternalConn.Close()
}

func (c *InternalConnReader) RemoteAddr() net.Addr {
	return c.InternalConn.RemoteAddr()
}

func (c *InternalConnReader) SetTimeout(timeout time.Duration) {
	c.timeout.Store(int64(timeout))
}

func (c *InternalConnReader) GetTimeout() time.Duration {
	return time.Duration(c.timeout.Load())
}

// contextWriter is like io.Writer, but takes context as well.
type InternalContextWriter interface {
	// writeContext writes p to the connection.
	//
	// If ctx is canceled before we start writing p (e.g. during waiting while another write is currently in progress),
	// p is not written and ctx.Err() is returned. Context is ignored after we start writing p (i.e. we don't interrupt
	// blocked writes that are in progress) so that we always either write the full frame or not write it at all.
	//
	// It returns the number of bytes written from p (0 <= n <= len(p)) and any error that caused the write to stop
	// early. writeContext must return a non-nil error if it returns n < len(p). writeContext must not modify the
	// data in p, even temporarily.
	InternalWriteContext(ctx context.Context, p []byte) (n int, err error)

	InternalSetWriteTimeout(timeout time.Duration)
}

type deadlineWriter interface {
	SetWriteDeadline(time.Time) error
	io.Writer
}

type InternalDeadlineContextWriter struct {
	InternalW deadlineWriter
	// semaphore protects critical section for SetWriteDeadline/Write.
	// It is a channel with capacity 1.
	InternalSemaphore chan struct{}
	// quit closed once the connection is closed.
	InternalQuit chan struct{}
	// spanScratch is protected by InternalSemaphore. Only cleared descriptors
	// are retained between writes, never request payloads.
	spanScratch net.Buffers
	// writeBuffers is the header net.Buffers.WriteTo consumes, also under
	// InternalSemaphore; it lives here so that a write does not allocate it.
	writeBuffers    net.Buffers
	InternalTimeout atomic.Int64
	// lastDeadlineZero records that the socket's write deadline is already
	// zero, so a no-timeout writer skips the syscall. Guarded by
	// InternalSemaphore.
	lastDeadlineZero bool
}

// setWriteDeadline applies timeout's deadline unless the socket already has a
// zero deadline and timeout asks for none. *lastZero is guarded by the caller.
func setWriteDeadline(w deadlineWriter, lastZero *bool, timeout int64) error {
	if timeout > 0 {
		*lastZero = false
		return w.SetWriteDeadline(time.Now().Add(time.Duration(timeout)))
	}
	if *lastZero {
		return nil
	}
	err := w.SetWriteDeadline(time.Time{})
	*lastZero = err == nil
	return err
}

func (c *InternalDeadlineContextWriter) InternalSetWriteTimeout(timeout time.Duration) {
	c.InternalTimeout.Store(int64(timeout))
}

// writeContext implements contextWriter.
func (c *InternalDeadlineContextWriter) InternalWriteContext(ctx context.Context, p []byte) (int, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-c.InternalQuit:
		return 0, runtimeerrors.ErrConnectionClosed
	case c.InternalSemaphore <- struct{}{}:

	}

	defer func() {

		<-c.InternalSemaphore
	}()

	if err := setWriteDeadline(c.InternalW, &c.lastDeadlineZero, c.InternalTimeout.Load()); err != nil {
		return 0, err
	}

	return c.InternalW.Write(p)
}

func InternalNewWriteCoalescer(conn deadlineWriter, writeTimeout, coalesceDuration time.Duration,
	quit <-chan struct{}) *InternalWriteCoalescer {
	wc := &InternalWriteCoalescer{
		InternalWriteCh: make(chan InternalWriteRequest),
		InternalC:       conn,
		InternalQuit:    quit,
	}
	wc.InternalSetWriteTimeout(writeTimeout)
	go wc.writeFlusher(coalesceDuration)
	return wc
}

type InternalWriteCoalescer struct {
	InternalC                deadlineWriter
	InternalQuit             <-chan struct{}
	InternalWriteCh          chan InternalWriteRequest
	InternalTestEnqueuedHook func()
	InternalTestFlushedHook  func()
	timeout                  atomic.Int64
}

func (w *InternalWriteCoalescer) InternalSetWriteTimeout(timeout time.Duration) {
	w.timeout.Store(int64(timeout))
}

type InternalWriteRequest struct {
	ctx context.Context
	// resultChan is a channel (with buffer size 1) where to send results of the write.
	resultChan chan<- InternalWriteResult
	// data to write.
	data  []byte
	chain wire.Chain
}

type InternalWriteResult struct {
	InternalErr error
	InternalN   int
	// A span request can be MaxInt32 body bytes plus its header on 32-bit.
	// Keep its completion independent of the io.Writer-sized legacy result.
	written int64
}

// writeResultChanPool pools buffered channels used for write coalescer results.
// Each channel is used in a strict produce-once/consume-once pattern:
// the flusher goroutine sends exactly one writeResult, and writeContext
// reads exactly one. After reading, the channel is empty and safe to reuse.
var writeResultChanPool = syncx.NewPool(func() chan InternalWriteResult {
	return make(chan InternalWriteResult, 1)
})

// writeContext implements contextWriter.
func (w *InternalWriteCoalescer) InternalWriteContext(ctx context.Context, p []byte) (int, error) {
	resultChan := writeResultChanPool.Get()
	wr := InternalWriteRequest{
		ctx:        ctx,
		resultChan: resultChan,
		data:       p,
	}

	select {
	case <-ctx.Done():
		writeResultChanPool.Put(resultChan)
		return 0, ctx.Err()
	case <-w.InternalQuit:
		writeResultChanPool.Put(resultChan)
		return 0, io.EOF
	case w.InternalWriteCh <- wr:

	}

	if w.InternalTestEnqueuedHook != nil {
		w.InternalTestEnqueuedHook()
	}

	result := <-resultChan
	writeResultChanPool.Put(resultChan)
	return result.InternalN, result.InternalErr
}

func (w *InternalWriteCoalescer) writeFlusher(interval time.Duration) {
	timer := time.NewTimer(interval)
	defer timer.Stop()

	if !timer.Stop() {
		<-timer.C
	}

	w.InternalWriteFlusherImpl(timer.C, func() { timer.Reset(interval) })
}

func (w *InternalWriteCoalescer) InternalWriteFlusherImpl(timerC <-chan time.Time, resetTimer func()) {
	running := false
	var requests []InternalWriteRequest

	for {
		select {
		case req := <-w.InternalWriteCh:
			requests = append(requests, req)
			if !running {

				resetTimer()
				running = true
			}
		case <-w.InternalQuit:
			result := InternalWriteResult{
				InternalN:   0,
				InternalErr: io.EOF,
			}

			for _, request := range requests {
				request.resultChan <- result
			}
			return
		case <-timerC:
			running = false
			var buffers net.Buffers
			var resultChans []chan<- InternalWriteResult
			var sizes []int64
			for _, request := range requests {
				if request.ctx != nil && request.ctx.Err() != nil {
					request.resultChan <- InternalWriteResult{InternalErr: request.ctx.Err()}
					continue
				}
				if request.data != nil {
					buffers = append(buffers, request.data)
					sizes = append(sizes, int64(len(request.data)))
				} else {
					_ = request.chain.Each(func(s wire.Span) error { buffers = append(buffers, s.Bytes()); return nil })
					sizes = append(sizes, request.chain.Len())
				}
				resultChans = append(resultChans, request.resultChan)
			}
			if len(resultChans) > 0 {
				w.InternalFlushSizes(resultChans, buffers, sizes)
			}
			clear(requests)
			requests = nil
			if w.InternalTestFlushedHook != nil {
				w.InternalTestFlushedHook()
			}
		}
	}
}

func (w *InternalWriteCoalescer) InternalFlushSizes(resultChans []chan<- InternalWriteResult, buffers net.Buffers, sizes []int64) {
	if err := w.InternalC.SetWriteDeadline(effectiveWriteDeadline(w.timeout.Load())); err != nil {
		for _, ch := range resultChans {
			ch <- InternalWriteResult{InternalErr: err}
		}
		return
	}

	scratch := append(net.Buffers(nil), buffers...)
	n, err := writeSpanBuffers(w.InternalC, &scratch)
	var total int64
	for _, size := range sizes {
		total += size
	}
	if err == nil && n != total {
		err = io.ErrShortWrite
	}
	for i, size := range sizes {
		completed := min(n, size)
		if completed < 0 {
			completed = 0
		}
		resultErr := err
		if completed == size {
			resultErr = nil
		}
		resultChans[i] <- InternalWriteResult{InternalN: int(min(completed, int64(^uint(0)>>1))), written: completed, InternalErr: resultErr}
		n -= completed
	}
}

// Socket deadlines are absolute. Disabling the operational timeout must clear
// the startup deadline, which may otherwise expire during a long-lived session.
func effectiveWriteDeadline(timeout int64) time.Time {
	if timeout > 0 {
		return time.Now().Add(time.Duration(timeout))
	}
	return time.Time{}
}
