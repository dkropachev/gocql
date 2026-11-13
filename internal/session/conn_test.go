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

package session

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/netip"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"log/slog"

	"github.com/stretchr/testify/require"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/endpoint"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/logging"
	implProtocol "github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/runtimeerrors"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/internal/streams"
	"github.com/scylladb/gocql/v2/internal/transport"
)

const (
	defaultProto = implProtocol.ProtoVersion4
)

type brokenDNSResolver struct{}

func (b brokenDNSResolver) LookupNetIP(_ context.Context, _, host string) ([]netip.Addr, error) {
	err := errors.New("this error comes from mocked broken resolver")
	return nil, &net.DNSError{
		UnwrapErr: err,
		Err:       err.Error(),
		Server:    host,
	}
}

func testCluster(proto frm.ProtoVersion, addresses ...string) *implCluster.InternalClusterSettings {
	cluster := implCluster.InternalNewClusterSettings(addresses...)
	cluster.ProtoVersion = int(proto)
	cluster.Data().InternalDisableControlConn = true
	cluster.PoolConfig.HostSelectionPolicy = implCluster.RoundRobinHostPolicy()
	return cluster
}

// waitForPoolSize blocks until session's connection pool holds at least want
// connections, or timeout elapses.
//
// open only guarantees the first connection to each host: the rest of
// a pool is filled asynchronously, so a test making a claim about every
// connection of a session has to wait for them rather than assert on whichever
// ones it happened to observe.
func waitForPoolSize(session *implCluster.Core, want int, timeout time.Duration) error {
	deadline := time.After(timeout)
	for {
		if got := implCluster.SessionData(session).InternalPool.Size(); got >= want {
			return nil
		}

		select {
		case <-deadline:
			return fmt.Errorf("pool reached %d of %d connections in %s", implCluster.SessionData(session).InternalPool.Size(), want, timeout)
		case <-time.After(time.Millisecond):
		}
	}
}

func createTestSslCluster(addr string, proto frm.ProtoVersion, useClientCert bool) *implCluster.InternalClusterSettings {
	cluster := testCluster(proto, addr)
	sslOpts := &transport.SslOptions{
		CaPath:                 "../../testdata/pki/ca.crt",
		EnableHostVerification: false,
	}

	if useClientCert {
		sslOpts.CertPath = "../../testdata/pki/gocql.crt"
		sslOpts.KeyPath = "../../testdata/pki/gocql.key"
	}

	cluster.SslOpts = sslOpts
	return cluster
}

func newTestSession(proto frm.ProtoVersion, addresses ...string) (*implCluster.Core, error) {
	return testCluster(proto, addresses...).Open()
}

var _ endpoint.Resolver = brokenDNSResolver{}

type testRetryPolicy struct {
	NumRetries int
}

type TestReconnectionPolicy struct {
	NumRetries       int
	GetIntervalCalls []int
}

func (c *TestReconnectionPolicy) GetInterval(currentRetry int) time.Duration {
	c.GetIntervalCalls = append(c.GetIntervalCalls, currentRetry)
	return time.Duration(0)
}

func (c *TestReconnectionPolicy) GetMaxRetries() int {
	return c.NumRetries
}

// unexpectedOptionsLogger closes seen once heartBeat logs an unexpected OPTIONS
// reply -- proof the driver parsed it. A server-side request counter would not do:
// it advances before the response is built, so the test could pass without it.
type unexpectedOptionsLogger struct {
	seen     chan struct{}
	seenOnce sync.Once
}

func (l *unexpectedOptionsLogger) Enabled(context.Context, slog.Level) bool { return true }
func (l *unexpectedOptionsLogger) Handle(_ context.Context, r slog.Record) error {
	if strings.Contains(r.Message, "unexpected frame in response to OPTIONS") {
		l.seenOnce.Do(func() { close(l.seen) })
	}
	return nil
}
func (l *unexpectedOptionsLogger) WithAttrs([]slog.Attr) slog.Handler { return l }
func (l *unexpectedOptionsLogger) WithGroup(string) slog.Handler      { return l }

type testContextWriter struct {
	n       int
	err     error
	onWrite func()
}

func (w testContextWriter) InternalWriteContext(ctx context.Context, p []byte) (int, error) {
	if w.onWrite != nil {
		w.onWrite()
	}
	if w.n == 0 && w.err == nil {
		return len(p), nil
	}
	return w.n, w.err
}

func (w testContextWriter) InternalSetWriteTimeout(timeout time.Duration) {}

type contextWriterFunc func(context.Context, []byte) (int, error)

func (fn contextWriterFunc) InternalWriteContext(ctx context.Context, p []byte) (int, error) {
	return fn(ctx, p)
}

func (fn contextWriterFunc) InternalSetWriteTimeout(timeout time.Duration) {}

func newTestExecConn(t *testing.T, w transport.InternalContextWriter) (*transport.Conn, net.Conn) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	server, client := net.Pipe()
	c := newTestConnWithFramerPool()
	c.InternalCtx = ctx
	c.InternalCancel = cancel
	c.InternalR = &transport.InternalConnReader{
		InternalConn: client,
		InternalR:    bufio.NewReader(client),
	}
	c.InternalW = w
	c.InternalLogger = logging.Nop()
	c.InternalErrorHandler = transport.InternalConnErrorHandlerFn(func(*transport.Conn, error, bool) {})
	c.InternalStreams = streams.New()
	c.InternalCalls = newCallTable()

	return c, server
}

func waitForSingleCall(t *testing.T, c *transport.Conn) *transport.InternalCallReq {
	t.Helper()

	return waitForSingleCallWithin(t, c, 2*time.Second)
}

func waitForSingleCallWithin(t *testing.T, c *transport.Conn, timeout time.Duration) *transport.InternalCallReq {
	t.Helper()

	deadline := time.After(timeout)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	for {
		c.InternalMu.Lock()
		for _, call := range c.InternalCalls.All() {
			c.InternalMu.Unlock()
			return call
		}
		c.InternalMu.Unlock()

		select {
		case <-deadline:
			t.Fatal("timed out waiting for in-flight call")
		case <-ticker.C:
		}
	}
}

func detachSingleCall(t *testing.T, c *transport.Conn) *transport.InternalCallReq {
	t.Helper()

	c.InternalMu.Lock()
	defer c.InternalMu.Unlock()
	for streamID, call := range c.InternalCalls.All() {
		c.InternalCalls.Delete(streamID)
		return call
	}

	t.Fatal("expected an in-flight call")
	return nil
}

type testStreamObserver struct {
	ctx *testStreamObserverContext
}

func (o *testStreamObserver) StreamContext(context.Context) transport.StreamObserverContext {
	return o.ctx
}

type testStreamObserverContext struct {
	started   chan struct{}
	abandoned chan struct{}
	finished  chan struct{}
}

func newTestStreamObserverContext() *testStreamObserverContext {
	return &testStreamObserverContext{
		started:   make(chan struct{}, 1),
		abandoned: make(chan struct{}, 1),
		finished:  make(chan struct{}, 1),
	}
}

func (o *testStreamObserverContext) StreamStarted(transport.ObservedStream) {
	select {
	case o.started <- struct{}{}:
	default:
	}
}

func (o *testStreamObserverContext) StreamAbandoned(transport.ObservedStream) {
	select {
	case o.abandoned <- struct{}{}:
	default:
	}
}

func (o *testStreamObserverContext) StreamFinished(transport.ObservedStream) {
	select {
	case o.finished <- struct{}{}:
	default:
	}
}

// tcpConnPair returns a matching set of a TCP client side and server side connection.
func tcpConnPair() (s, c net.Conn, err error) {
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		// maybe ipv6 works, if ipv4 fails?
		l, err = net.Listen("tcp6", "[::1]:0")
		if err != nil {
			return nil, nil, err
		}
	}
	defer l.Close() // we only try to accept one connection, so will stop listening.

	addr := l.Addr()
	done := make(chan struct{})
	var errDial error
	go func(done chan<- struct{}) {
		c, errDial = net.Dial(addr.Network(), addr.String())
		close(done)
	}(done)

	s, err = l.Accept()
	<-done

	if err == nil {
		err = errDial
	}

	if err != nil {
		if s != nil {
			s.Close()
		}
		if c != nil {
			c.Close()
		}
	}

	return s, c, err
}

type recordingFrameHeaderObserver struct {
	t      *testing.T
	mu     sync.Mutex
	frames []transport.ObservedFrameHeader
}

func (r *recordingFrameHeaderObserver) ObserveFrameHeader(ctx context.Context, frm transport.ObservedFrameHeader) {
	r.mu.Lock()
	r.frames = append(r.frames, frm)
	r.mu.Unlock()
}

func (r *recordingFrameHeaderObserver) getFrames() []transport.ObservedFrameHeader {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.frames
}

func NewTestServerWithAddress(addr string, t testing.TB, protocol uint8, ctx context.Context) *TestServer {
	return newTestServerOpts{
		addr:     addr,
		protocol: protocol,
	}.newServer(t, ctx)
}

func NewTestServerWithAddressAndSupportedFactory(addr string, t testing.TB, protocol uint8, ctx context.Context, supportedFactory testSupportedFactory) *TestServer {
	return newTestServerOpts{
		addr:             addr,
		protocol:         protocol,
		supportedFactory: supportedFactory,
	}.newServer(t, ctx)
}

type newTestServerOpts struct {
	addr             string
	protocol         uint8
	supportedFactory testSupportedFactory
	recvHook         func(*implProtocol.Framer)
}

func (nts newTestServerOpts) newServer(t testing.TB, ctx context.Context) *TestServer {
	laddr, err := net.ResolveTCPAddr("tcp", nts.addr)
	if err != nil {
		t.Fatal(err)
	}

	listen, err := net.ListenTCP("tcp", laddr)
	if err != nil {
		t.Fatal(err)
	}

	headerSize := 9

	ctx, cancel := context.WithCancel(ctx)
	srv := &TestServer{
		Address:    listen.Addr().String(),
		listen:     listen,
		t:          t,
		protocol:   nts.protocol,
		headerSize: headerSize,
		ctx:        ctx,
		cancel:     cancel,

		supportedFactory: nts.supportedFactory,
		onRecv:           nts.recvHook,
	}

	go srv.closeWatch()
	go srv.serve()

	return srv
}

func NewTestServer(t testing.TB, protocol uint8, ctx context.Context) *TestServer {
	return NewTestServerWithAddress("127.0.0.1:0", t, protocol, ctx)
}

func NewSSLTestServer(t testing.TB, protocol uint8, ctx context.Context) *TestServer {
	return NewSSLTestServerWithSupportedFactory(t, protocol, ctx, nil)
}

func NewSSLTestServerWithSupportedFactory(t testing.TB, protocol uint8, ctx context.Context, supportedFactory testSupportedFactory) *TestServer {
	pem, err := os.ReadFile("../../testdata/pki/ca.crt")
	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(pem) {
		t.Fatalf("Failed parsing or appending certs")
	}
	mycert, err := tls.LoadX509KeyPair("../../testdata/pki/cassandra.crt", "../../testdata/pki/cassandra.key")
	if err != nil {
		t.Fatalf("could not load cert")
	}
	config := &tls.Config{
		Certificates: []tls.Certificate{mycert},
		RootCAs:      certPool,
	}
	listen, err := tls.Listen("tcp", "127.0.0.1:0", config)
	if err != nil {
		t.Fatal(err)
	}

	headerSize := 9

	ctx, cancel := context.WithCancel(ctx)
	srv := &TestServer{
		Address:    listen.Addr().String(),
		listen:     listen,
		t:          t,
		protocol:   protocol,
		headerSize: headerSize,
		ctx:        ctx,
		cancel:     cancel,

		supportedFactory: supportedFactory,
	}

	go srv.closeWatch()
	go srv.serve()
	return srv
}

type TestServer struct {
	Address          string
	TimeoutOnStartup int32
	t                testing.TB
	listen           net.Listener
	nKillReq         atomic.Int64
	supportedFactory testSupportedFactory

	// stallSystemQueries, when non-zero, makes system.peers/system.local queries
	// hang until the server is stopped, simulating a stalled backend.
	stallSystemQueries int32

	// readyOnOptions answers OPTIONS with READY instead of SUPPORTED -- the reply
	// heartBeat has no arm for. Startup's own OPTIONS is always answered normally
	// (optionsSeen counts them), so this can be set before the session exists.
	readyOnOptions int32

	// optionsSeen counts OPTIONS frames per connection: net.Conn -> *int32.
	optionsSeen sync.Map

	protocol   byte
	headerSize int
	ctx        context.Context
	cancel     context.CancelFunc

	mu     sync.Mutex
	closed bool

	// onRecv is a hook point for tests, called in receive loop.
	onRecv func(*implProtocol.Framer)

	// prepareHook, when set, is offered every PREPARE before the canned
	// statements in process. It writes the whole response into resp and returns
	// true, or returns false to fall through to them. Set it via setPrepareHook.
	prepareHook testPrepareHook

	// batchHook, when set, answers every BATCH: req is positioned at the start
	// of the body and the hook writes the whole response into resp. Set it via
	// setBatchHook. Without it BATCH gets the generic "not supported" error.
	batchHook func(req *implProtocol.Framer, stream int, resp *implProtocol.Framer)

	// authHook, when set, answers STARTUP and AUTH_RESPONSE instead of the
	// default READY and "not supported", so a test can drive an authentication
	// exchange. req is positioned at the start of the body; req.header.Op tells
	// the two apart. Set it via setAuthHook.
	authHook func(req *implProtocol.Framer, stream int, resp *implProtocol.Framer)

	// queryHook, when set, is offered every QUERY before the canned statements
	// in process. It writes the whole response into resp and returns true, or
	// returns false to fall through to them. Set it via setQueryHook.
	queryHook func(query string, stream int, resp *implProtocol.Framer) bool

	// registerHook, when set, answers REGISTER. Without it REGISTER gets the
	// generic "not supported" error. Set it via setRegisterHook.
	registerHook func(events []string, stream int, resp *implProtocol.Framer)

	// executeHook, when set, is offered every EXECUTE before the canned prepared
	// ids in process. It writes the whole response into resp and returns true,
	// or returns false to fall through to them. Set it via setExecuteHook.
	executeHook func(id uint64, stream int, resp *implProtocol.Framer) bool
}

func (srv *TestServer) setExecuteHook(hook func(id uint64, stream int, resp *implProtocol.Framer) bool) {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	srv.executeHook = hook
}

func (srv *TestServer) setQueryHook(hook func(query string, stream int, resp *implProtocol.Framer) bool) {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	srv.queryHook = hook
}

func (srv *TestServer) setRegisterHook(hook func(events []string, stream int, resp *implProtocol.Framer)) {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	srv.registerHook = hook
}

func (srv *TestServer) setAuthHook(hook func(req *implProtocol.Framer, stream int, resp *implProtocol.Framer)) {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	srv.authHook = hook
}

// testPrepareHook answers a PREPARE for query. writeResultMetadataID must be
// called right after the prepared id, as the canned statements do; it is a no-op
// unless the server advertised SCYLLA_USE_METADATA_ID.
type testPrepareHook func(query string, stream int, resp *implProtocol.Framer, writeResultMetadataID func(preparedID uint64)) bool

func (srv *TestServer) setBatchHook(hook func(req *implProtocol.Framer, stream int, resp *implProtocol.Framer)) {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	srv.batchHook = hook
}

func (srv *TestServer) setPrepareHook(hook testPrepareHook) {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	srv.prepareHook = hook
}

type testSupportedFactory func(conn net.Conn) map[string][]string

func (srv *TestServer) port() int {
	return srv.listen.Addr().(*net.TCPAddr).Port
}

func (srv *TestServer) session() (*implCluster.Core, error) {
	return testCluster(frm.ProtoVersion(srv.protocol), srv.Address).Open()
}

func (srv *TestServer) host() *endpoint.HostInfo {
	hosts, err := endpoint.InternalResolveInitialEndpoint(nil, srv.Address, 9042)
	if err != nil {
		srv.t.Fatal(err)
	}
	return hosts[0]
}

func (srv *TestServer) closeWatch() {
	<-srv.ctx.Done()

	srv.mu.Lock()
	defer srv.mu.Unlock()

	srv.closeLocked()
}

func (srv *TestServer) serve() {
	defer srv.listen.Close()
	for !srv.isClosed() {
		conn, err := srv.listen.Accept()
		if err != nil {
			break
		}

		var exts map[string][]string
		if srv.supportedFactory != nil {
			exts = (srv.supportedFactory)(conn)
		}

		go func(conn net.Conn, exts map[string][]string) {
			defer conn.Close()
			for !srv.isClosed() {
				framer, err := srv.readFrame(conn)
				if err != nil {
					if err == io.EOF || errors.Is(err, net.ErrClosed) || errors.Is(err, syscall.ECONNRESET) {
						return
					}
					srv.errorLocked(err)
					return
				}

				if srv.onRecv != nil {
					srv.onRecv(framer)
				}

				go srv.process(conn, framer, exts)
			}
		}(conn, exts)
	}
}

func (srv *TestServer) isClosed() bool {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	return srv.closed
}

func (srv *TestServer) closeLocked() {
	if srv.closed {
		return
	}

	srv.closed = true

	srv.listen.Close()
	srv.cancel()
}

func (srv *TestServer) Stop() {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	srv.closeLocked()
}

func (srv *TestServer) errorLocked(err any) {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if srv.closed {
		return
	}
	srv.t.Error(err)
}

const testMetadataIDOffset = 0x1000

// countOptions returns this connection's OPTIONS count, from one.
func (srv *TestServer) countOptions(conn net.Conn) int32 {
	v, _ := srv.optionsSeen.LoadOrStore(conn, new(int32))
	return atomic.AddInt32(v.(*int32), 1)
}

func (srv *TestServer) process(conn net.Conn, reqFrame *implProtocol.Framer, exts map[string][]string) {
	head := reqFrame.Header
	if head == nil {
		srv.errorLocked("process frame with a nil header")
		return
	}
	respFrame := implProtocol.NewFramer(nil, reqFrame.Proto)

	// SCYLLA_USE_METADATA_ID puts a result metadata id after the prepared id in both
	// RESULT/Prepared and EXECUTE. The driver opts in whenever the server advertised
	// the key, so the advertised map is the same signal it used, and the canned
	// frames below have to match the layout it will then read and write.
	_, useMetadataID := exts[endpoint.InternalScyllaUseMetadataID]

	// writeResultMetadataID emits that field for a prepared statement. It is a no-op
	// unless the extension was advertised, so servers that do not advertise it keep
	// producing exactly the frames they did before.
	writeResultMetadataID := func(preparedID uint64) {
		if useMetadataID {
			respFrame.WriteShortBytes(binary.BigEndian.AppendUint64(nil, preparedID+testMetadataIDOffset))
		}
	}

	switch head.Op {
	case frm.OpStartup:
		if atomic.LoadInt32(&srv.TimeoutOnStartup) > 0 {
			// Do not respond to startup command
			// wait until we get a cancel signal
			select {
			case <-srv.ctx.Done():
				return
			}
		}
		srv.mu.Lock()
		hook := srv.authHook
		srv.mu.Unlock()
		if hook != nil {
			hook(reqFrame, head.Stream, respFrame)
			break
		}
		respFrame.WriteHeader(0, frm.OpReady, head.Stream)
	case frm.OpRegister:
		srv.mu.Lock()
		hook := srv.registerHook
		srv.mu.Unlock()
		if hook == nil {
			respFrame.WriteHeader(0, frm.OpError, head.Stream)
			respFrame.WriteInt(0)
			respFrame.WriteString("not supported")
			break
		}
		hook(reqFrame.ReadStringList(), head.Stream, respFrame)
	case frm.OpAuthResponse:
		srv.mu.Lock()
		hook := srv.authHook
		srv.mu.Unlock()
		if hook == nil {
			respFrame.WriteHeader(0, frm.OpError, head.Stream)
			respFrame.WriteInt(0)
			respFrame.WriteString("not supported")
			break
		}
		hook(reqFrame, head.Stream, respFrame)
	case frm.OpOptions:
		if atomic.LoadInt32(&srv.readyOnOptions) != 0 && srv.countOptions(conn) > 1 {
			respFrame.WriteHeader(0, frm.OpReady, head.Stream)
			break
		}
		respFrame.WriteHeader(0, frm.OpSupported, head.Stream)
		respFrame.WriteStringMultiMap(exts)
	case frm.OpQuery:
		query := reqFrame.ReadLongString()
		first := query
		if n := strings.Index(query, " "); n > 0 {
			first = first[:n]
		}
		if atomic.LoadInt32(&srv.stallSystemQueries) != 0 &&
			(strings.Contains(query, "system.peers") || strings.Contains(query, "system.local")) {
			<-srv.ctx.Done()
			return
		}
		srv.mu.Lock()
		queryHook := srv.queryHook
		srv.mu.Unlock()
		if queryHook != nil && queryHook(query, head.Stream, respFrame) {
			break
		}
		switch strings.ToLower(first) {
		case "kill":
			srv.nKillReq.Add(1)
			respFrame.WriteHeader(0, frm.OpError, head.Stream)
			respFrame.WriteInt(0x1001)
			respFrame.WriteString("query killed")
		case "use":
			respFrame.WriteInt(frm.ResultKindKeyspace)
			respFrame.WriteString(strings.TrimSpace(query[3:]))
		case "void":
			respFrame.WriteHeader(0, frm.OpResult, head.Stream)
			respFrame.WriteInt(frm.ResultKindVoid)
		case "timeout":
			<-srv.ctx.Done()
			return
		case "slow":
			go func() {
				respFrame.WriteHeader(0, frm.OpResult, head.Stream)
				respFrame.WriteInt(frm.ResultKindVoid)
				respFrame.Buf[0] = srv.protocol | 0x80
				select {
				case <-srv.ctx.Done():
					return
				case <-time.After(50 * time.Millisecond):
					respFrame.Finish()
					respFrame.WriteFrameTo(conn)
				}
			}()
			return
		case "speculative":
			srv.nKillReq.Add(1)
			if srv.nKillReq.Load() > 3 {
				respFrame.WriteHeader(0, frm.OpResult, head.Stream)
				respFrame.WriteInt(frm.ResultKindVoid)
				respFrame.WriteString("speculative query success on the node " + srv.Address)
			} else {
				respFrame.WriteHeader(0, frm.OpError, head.Stream)
				respFrame.WriteInt(0x1001)
				respFrame.WriteString("speculative error")
				rand.Seed(time.Now().UnixNano())
				<-time.After(time.Millisecond * 120)
			}
		default:
			respFrame.WriteHeader(0, frm.OpResult, head.Stream)
			respFrame.WriteInt(frm.ResultKindVoid)
		}
	case frm.OpBatch:
		srv.mu.Lock()
		hook := srv.batchHook
		srv.mu.Unlock()
		if hook == nil {
			respFrame.WriteHeader(0, frm.OpError, head.Stream)
			respFrame.WriteInt(0)
			respFrame.WriteString("not supported")
			break
		}
		hook(reqFrame, head.Stream, respFrame)
	case frm.OpError:
		respFrame.WriteHeader(0, frm.OpError, head.Stream)
		respFrame.Buf = append(respFrame.Buf, reqFrame.Buf...)
	case frm.OpPrepare:
		query := strings.TrimSpace(reqFrame.ReadLongString())
		srv.mu.Lock()
		hook := srv.prepareHook
		srv.mu.Unlock()
		if hook != nil && hook(query, head.Stream, respFrame, writeResultMetadataID) {
			break
		}
		lower := strings.ToLower(query)
		name := ""
		if strings.HasPrefix(lower, "select ") {
			name = strings.TrimPrefix(lower, "select ")
			if n := strings.Index(name, " "); n > 0 {
				name = name[:n]
			}
		} else if strings.HasPrefix(lower, "begin batch") {
			name = "batchmetadata"
		} else {
			name = lower
		}
		switch name {
		case "nometadata":
			respFrame.WriteHeader(0, frm.OpResult, head.Stream)
			respFrame.WriteInt(frm.ResultKindPrepared)
			// <id>
			respFrame.WriteShortBytes(binary.BigEndian.AppendUint64(nil, 1))
			writeResultMetadataID(1)
			// <metadata>
			respFrame.WriteInt(0) // <flags>
			respFrame.WriteInt(0) // <columns_count>
			if srv.protocol >= implProtocol.ProtoVersion4 {
				respFrame.WriteInt(0) // <pk_count>
			}
			// <result_metadata>
			respFrame.WriteInt(int32(frm.FlagNoMetaData)) // <flags>
			respFrame.WriteInt(0)
		case "metadata":
			respFrame.WriteHeader(0, frm.OpResult, head.Stream)
			respFrame.WriteInt(frm.ResultKindPrepared)
			// <id>
			respFrame.WriteShortBytes(binary.BigEndian.AppendUint64(nil, 2))
			writeResultMetadataID(2)
			// <metadata>
			respFrame.WriteInt(0) // <flags>
			respFrame.WriteInt(0) // <columns_count>
			if srv.protocol >= implProtocol.ProtoVersion4 {
				respFrame.WriteInt(0) // <pk_count>
			}
			// <result_metadata>
			respFrame.WriteInt(int32(frm.FlagGlobalTableSpec)) // <flags>
			respFrame.WriteInt(1)                              // <columns_count>
			// <global_table_spec>
			respFrame.WriteString("keyspace")
			respFrame.WriteString("table")
			// <col_spec_0>
			respFrame.WriteString("col0")                     // <name>
			respFrame.WriteShort(uint16(cqltype.TypeBoolean)) // <type>
		case "batchmetadata":
			respFrame.WriteHeader(0, frm.OpResult, head.Stream)
			respFrame.WriteInt(frm.ResultKindPrepared)
			// <id>
			respFrame.WriteShortBytes(binary.BigEndian.AppendUint64(nil, 3))
			writeResultMetadataID(3)
			// <metadata>
			respFrame.WriteInt(0) // <flags>
			respFrame.WriteInt(2) // <columns_count>
			if srv.protocol >= implProtocol.ProtoVersion4 {
				respFrame.WriteInt(0) // <pk_count>
			}
			// <col_spec_0>
			respFrame.WriteString("ks1")
			respFrame.WriteString("tbl1")
			respFrame.WriteString("col1")
			respFrame.WriteShort(uint16(cqltype.TypeInt))
			// <col_spec_1>
			respFrame.WriteString("ks2")
			respFrame.WriteString("tbl2")
			respFrame.WriteString("col2")
			respFrame.WriteShort(uint16(cqltype.TypeInt))
			// <result_metadata>
			respFrame.WriteInt(int32(frm.FlagNoMetaData))
			respFrame.WriteInt(0)
		case "metadatachangednocolumns":
			// Prepared with real result metadata and (under the extension) a metadata
			// id, so the driver caches columns and asks the server to skip metadata on
			// execute. Its EXECUTE reply, id 4 below, is then deliberately malformed.
			respFrame.WriteHeader(0, frm.OpResult, head.Stream)
			respFrame.WriteInt(frm.ResultKindPrepared)
			// <id>
			respFrame.WriteShortBytes(binary.BigEndian.AppendUint64(nil, 4))
			writeResultMetadataID(4)
			// <metadata>
			respFrame.WriteInt(0) // <flags>
			respFrame.WriteInt(0) // <columns_count>
			if srv.protocol >= implProtocol.ProtoVersion4 {
				respFrame.WriteInt(0) // <pk_count>
			}
			// <result_metadata>
			respFrame.WriteInt(int32(frm.FlagGlobalTableSpec)) // <flags>
			respFrame.WriteInt(1)                              // <columns_count>
			// <global_table_spec>
			respFrame.WriteString("keyspace")
			respFrame.WriteString("table")
			// <col_spec_0>
			respFrame.WriteString("col0")                     // <name>
			respFrame.WriteShort(uint16(cqltype.TypeBoolean)) // <type>
		default:
			respFrame.WriteHeader(0, frm.OpError, head.Stream)
			respFrame.WriteInt(0)
			respFrame.WriteString("unsupported query: " + name)
		}
	case frm.OpExecute:
		b := reqFrame.ReadShortBytesCopy()
		id := binary.BigEndian.Uint64(b)
		srv.mu.Lock()
		executeHook := srv.executeHook
		srv.mu.Unlock()
		if executeHook != nil && executeHook(id, head.Stream, respFrame) {
			break
		}
		if useMetadataID {
			// The driver writes the result metadata id between the prepared id and the
			// query parameters once the extension is negotiated. Consume it, or every
			// read below lands two bytes plus the id's length too early.
			reqFrame.ReadShortBytesCopy()
		}
		// <query_parameters>
		reqFrame.ReadConsistency() // <consistency>
		var flags uint32
		if srv.protocol > implProtocol.ProtoVersion4 {
			ui := reqFrame.ReadInt()
			flags = uint32(ui)
		} else {
			flags = uint32(reqFrame.ReadByteValue())
		}
		switch id {
		case 1:
			if flags&frm.FlagSkipMetaData != 0 {
				respFrame.WriteHeader(0, frm.OpError, head.Stream)
				respFrame.WriteInt(0)
				respFrame.WriteString("skip metadata unexpected")
			} else {
				respFrame.WriteHeader(0, frm.OpResult, head.Stream)
				respFrame.WriteInt(frm.ResultKindRows)
				// <metadata>
				respFrame.WriteInt(0) // <flags>
				respFrame.WriteInt(0) // <columns_count>
				// <rows_count>
				respFrame.WriteInt(0)
			}
		case 2:
			if flags&frm.FlagSkipMetaData != 0 {
				respFrame.WriteHeader(0, frm.OpResult, head.Stream)
				respFrame.WriteInt(frm.ResultKindRows)
				// <metadata>
				respFrame.WriteInt(0) // <flags>
				respFrame.WriteInt(0) // <columns_count>
				// <rows_count>
				respFrame.WriteInt(0)
			} else {
				respFrame.WriteHeader(0, frm.OpError, head.Stream)
				respFrame.WriteInt(0)
				respFrame.WriteString("skip metadata expected")
			}
		case 4:
			// The driver is expected to have asked to skip metadata here: the extension
			// is negotiated, this statement holds a metadata id, and its prepared
			// response carried columns. Refusing otherwise keeps that part of the
			// skip-metadata decision pinned on a live connection.
			if flags&frm.FlagSkipMetaData == 0 {
				respFrame.WriteHeader(0, frm.OpError, head.Stream)
				respFrame.WriteInt(0)
				respFrame.WriteString("skip metadata expected")
			} else {
				// METADATA_CHANGED promises new result metadata; NO_METADATA says none
				// was sent. A driver that adopted the new id would stop being sent
				// metadata altogether, and one that decoded these rows against its
				// cached columns would be decoding against the very columns the server
				// just declared stale. Neither is acceptable, so it must reject this.
				respFrame.WriteHeader(0, frm.OpResult, head.Stream)
				respFrame.WriteInt(frm.ResultKindRows)
				// <metadata>
				respFrame.WriteInt(int32(frm.FlagMetaDataChanged | frm.FlagNoMetaData)) // <flags>
				respFrame.WriteInt(0)                                                   // <columns_count>
				respFrame.WriteShortBytes(binary.BigEndian.AppendUint64(nil, 0xBEEF))   // <new_metadata_id>
				// <rows_count>
				respFrame.WriteInt(0)
			}
		default:
			respFrame.WriteHeader(0, frm.OpError, head.Stream)
			respFrame.WriteInt(int32(implProtocol.CodeUnprepared))
			respFrame.WriteString("unprepared")
			respFrame.WriteShortBytes(binary.BigEndian.AppendUint64(nil, id))
		}
	default:
		respFrame.WriteHeader(0, frm.OpError, head.Stream)
		respFrame.WriteInt(0)
		respFrame.WriteString("not supported")
	}

	respFrame.Buf[0] = srv.protocol | 0x80

	if err := respFrame.Finish(); err != nil {
		srv.errorLocked(err)
	}

	if err := respFrame.WriteFrameTo(conn); err != nil {
		if !errors.Is(err, net.ErrClosed) && !errors.Is(err, syscall.ECONNRESET) && !errors.Is(err, syscall.EPIPE) {
			srv.errorLocked(err)
		}
	}
}

func (srv *TestServer) readFrame(conn net.Conn) (*implProtocol.Framer, error) {
	buf := make([]byte, srv.headerSize)
	head, err := implProtocol.ReadHeader(conn, buf)
	if err != nil {
		return nil, err
	}
	framer := implProtocol.NewFramer(nil, srv.protocol)

	err = framer.ReadFrame(conn, &head)
	if err != nil {
		return nil, err
	}

	// should be a request frame
	if head.Version.Response() {
		return nil, fmt.Errorf("expected to read a request frame got version: %v", head.Version)
	} else if head.Version.Version() != srv.protocol {
		return nil, fmt.Errorf("expected to read protocol version 0x%x got 0x%x", srv.protocol, head.Version.Version())
	}

	return framer, nil
}

// newTestConnWithFramerPool creates a minimal driverConn with an initialized framer pool
// suitable for testing releaseFramer and EWMA logic.
func newTestConnWithFramerPool() *transport.Conn {
	c := &transport.Conn{}
	c.InternalFramers.InternalDefaults = transport.InternalFramerConfig{
		InternalProto: implProtocol.ProtoVersion4 & implProtocol.ProtoVersionMask,
	}
	c.InternalFramers.InternalInitPool(c)
	return c
}

func buildTestFrame(t *testing.T, f *implProtocol.Framer, req implProtocol.FrameBuilder, streamID int) ([]byte, frm.FrameHeader) {
	t.Helper()

	if err := req.BuildFrame(f, streamID); err != nil {
		t.Fatalf("BuildFrame failed: %v", err)
	}

	buf := append([]byte(nil), f.Buf...)
	header, err := implProtocol.ReadHeader(bytes.NewReader(buf), make([]byte, implProtocol.HeadSize))
	if err != nil {
		t.Fatalf("readHeader failed: %v", err)
	}

	return buf, header
}

const segmentTestTimeout = 10 * time.Second

// recvCallResp waits for a response to be delivered to call, failing the test on
// the test goroutine if it never arrives. Assertions must not run on a child
// goroutine: require's FailNow is only valid on the test goroutine, and a test
// that does not wait for its own assertions can return while they are still in
// flight — a genuine mismatch is then dropped or reported as a panic after the
// test completed.
func recvCallResp(t *testing.T, call *transport.InternalCallReq) transport.InternalCallResp {
	t.Helper()

	select {
	case resp := <-call.InternalResp:
		return resp
	case <-time.After(segmentTestTimeout):
		t.Fatalf("timed out waiting for a response on stream %d", call.InternalStreamID)
		return transport.InternalCallResp{}
	}
}

// deadlineRecordingConn is a net.Conn whose SetReadDeadline calls are recorded
// so tests can assert the exact sequence of deadline values passed.
//
// Read blocks until proceed is closed, then returns (0, io.EOF). A sync.Once
// guards the close of readEntered so that repeated Read calls (e.g. from
// bufio's internal retry logic) do not panic.
type deadlineRecordingConn struct {
	readEnteredOnce sync.Once
	readEntered     chan struct{} // closed on first Read entry
	proceed         chan struct{} // close to unblock Read

	mu        sync.Mutex
	deadlines []time.Time // all values passed to SetReadDeadline, in order
}

var _ net.Conn = (*deadlineRecordingConn)(nil)

func (c *deadlineRecordingConn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deadlines = append(c.deadlines, t)
	return nil
}

func (c *deadlineRecordingConn) Read(p []byte) (int, error) {
	c.readEnteredOnce.Do(func() { close(c.readEntered) })
	<-c.proceed
	return 0, io.EOF
}

func (c *deadlineRecordingConn) Write(p []byte) (int, error)        { return 0, io.ErrClosedPipe }
func (c *deadlineRecordingConn) Close() error                       { return nil }
func (c *deadlineRecordingConn) LocalAddr() net.Addr                { return nil }
func (c *deadlineRecordingConn) RemoteAddr() net.Addr               { return nil }
func (c *deadlineRecordingConn) SetDeadline(t time.Time) error      { return nil }
func (c *deadlineRecordingConn) SetWriteDeadline(t time.Time) error { return nil }

// nonBlockingDeadlineConn is a net.Conn that records SetReadDeadline calls and
// returns immediately from Read, so a single connReader.Read can be exercised
// synchronously. setDeadlineErr, when set, makes SetReadDeadline fail; reads
// counts the Read calls that actually reached the connection.
type nonBlockingDeadlineConn struct {
	setDeadlineErr error
	deadlines      []time.Time
	reads          int
}

var _ net.Conn = (*nonBlockingDeadlineConn)(nil)

func (c *nonBlockingDeadlineConn) SetReadDeadline(t time.Time) error {
	c.deadlines = append(c.deadlines, t)
	return c.setDeadlineErr
}
func (c *nonBlockingDeadlineConn) Read(p []byte) (int, error)         { c.reads++; return 0, io.EOF }
func (c *nonBlockingDeadlineConn) Write(p []byte) (int, error)        { return 0, io.ErrClosedPipe }
func (c *nonBlockingDeadlineConn) Close() error                       { return nil }
func (c *nonBlockingDeadlineConn) LocalAddr() net.Addr                { return nil }
func (c *nonBlockingDeadlineConn) RemoteAddr() net.Addr               { return nil }
func (c *nonBlockingDeadlineConn) SetDeadline(t time.Time) error      { return nil }
func (c *nonBlockingDeadlineConn) SetWriteDeadline(t time.Time) error { return nil }

// scriptedConn is a net.Conn whose reads come from a script and which records the
// read deadlines it was given, so tests can drive connReader.Read's retry loop.
type scriptedConn struct {
	steps     []scriptedRead
	step      int
	deadlines []time.Time
}

var _ net.Conn = (*scriptedConn)(nil)

func (c *scriptedConn) Read(p []byte) (int, error) {
	if c.step >= len(c.steps) {
		return 0, io.EOF
	}
	step := c.steps[c.step]
	c.step++
	return copy(p, step.data), step.err
}

func (c *scriptedConn) SetReadDeadline(t time.Time) error {
	c.deadlines = append(c.deadlines, t)
	return nil
}
func (c *scriptedConn) Write(p []byte) (int, error)      { return 0, io.ErrClosedPipe }
func (c *scriptedConn) Close() error                     { return nil }
func (c *scriptedConn) LocalAddr() net.Addr              { return nil }
func (c *scriptedConn) RemoteAddr() net.Addr             { return nil }
func (c *scriptedConn) SetDeadline(time.Time) error      { return nil }
func (c *scriptedConn) SetWriteDeadline(time.Time) error { return nil }

// scriptedReadSource is a connReadSource whose reads come from a script of
// (bytes, error) steps and which records every InternalSetDisarm transition. It lets tests
// drive partial reads, timing-out reads and the disarm/re-arm pairing without a
// socket, and — being a connReadSource — it can be installed as driverConn.r.
//
// A step means "the peer delivered these bytes, and then this error", not "one
// Read call": a step's bytes are handed out across as many reads as the caller
// takes to consume them, and the error surfaces on the read that empties it. So a
// script stays meaningful regardless of how the code under test sizes its reads —
// which matters because the header readers deliberately read the first byte on its
// own (headerReader).
type scriptedReadSource struct {
	steps    []scriptedRead
	step     int
	consumed int    // bytes of steps[step].data already delivered
	disarms  []bool // every value passed to InternalSetDisarm, in order
	arms     int    // reads started while not disarmed
	readLens []int  // len(p) of every read, in order
}

type scriptedRead struct {
	data []byte
	err  error
}

var _ transport.InternalConnReadSource = (*scriptedReadSource)(nil)

func (s *scriptedReadSource) Read(p []byte) (int, error) {
	if len(s.disarms) == 0 || !s.disarms[len(s.disarms)-1] {
		s.arms++
	}
	s.readLens = append(s.readLens, len(p))
	if s.step >= len(s.steps) {
		return 0, io.EOF
	}
	step := s.steps[s.step]
	n := copy(p, step.data[s.consumed:])
	s.consumed += n
	if s.consumed < len(step.data) {
		// More of this step's bytes to come; its error belongs to the read that
		// delivers the last of them.
		return n, nil
	}
	s.step++
	s.consumed = 0
	return n, step.err
}

func (s *scriptedReadSource) Close() error               { return nil }
func (s *scriptedReadSource) RemoteAddr() net.Addr       { return nil }
func (s *scriptedReadSource) SetTimeout(_ time.Duration) {}
func (s *scriptedReadSource) GetTimeout() time.Duration  { return 0 }
func (s *scriptedReadSource) InternalSetDisarm(v bool)   { s.disarms = append(s.disarms, v) }

// timeoutErr is a net.Error reporting a timeout, standing in for the
// os.ErrDeadlineExceeded a real socket returns once its read deadline expires.
type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

var _ net.Error = timeoutErr{}

// armFailingConn serves a fixed byte stream and fails SetReadDeadline from
// failFromArm onwards, with a plain error that is deliberately not a net.Error —
// net.Conn.SetReadDeadline is only conventionally a *net.OpError, and a connection
// from a user-supplied Dialer or HostDialer need not follow that.
// armFailingConn serves data and then fails every read-deadline arm, so the
// first read that must reach the socket for bytes beyond data cannot start.
// Failing on the data being used up rather than on an arm count keeps it
// independent of how many arms the header read takes, which buffering changes.
type armFailingConn struct {
	data []byte
	off  int
	err  error
}

var _ net.Conn = (*armFailingConn)(nil)

func (c *armFailingConn) SetReadDeadline(time.Time) error {
	if c.off >= len(c.data) {
		return c.err
	}
	return nil
}

func (c *armFailingConn) Read(p []byte) (int, error) {
	if c.off >= len(c.data) {
		return 0, io.EOF
	}
	n := copy(p, c.data[c.off:])
	c.off += n
	return n, nil
}

func (c *armFailingConn) Write(p []byte) (int, error)      { return 0, io.ErrClosedPipe }
func (c *armFailingConn) Close() error                     { return nil }
func (c *armFailingConn) LocalAddr() net.Addr              { return nil }
func (c *armFailingConn) RemoteAddr() net.Addr             { return nil }
func (c *armFailingConn) SetDeadline(time.Time) error      { return nil }
func (c *armFailingConn) SetWriteDeadline(time.Time) error { return nil }

// closeOnBodyRead serves header and then, on the read that would fetch the body,
// marks the connection closed before failing.
type closeOnBodyRead struct {
	c      *transport.Conn
	header []byte
	off    int
}

func (r *closeOnBodyRead) Read(p []byte) (int, error) {
	if r.off < len(r.header) {
		n := copy(p, r.header[r.off:])
		r.off += n
		return n, nil
	}
	// Only c.closed is set: closeWithError would also cancel c.ctx, and exec would
	// then race between the delivery and that cancellation, hiding the decision
	// this test is about.
	r.c.InternalMu.Lock()
	r.c.InternalClosed = true
	r.c.InternalMu.Unlock()
	return 0, io.ErrUnexpectedEOF
}

// applicationInfoFunc adapts a function to ApplicationInfo, so a test can supply
// options StaticApplicationInfo would never produce.
type applicationInfoFunc func(map[string]string)

func (f applicationInfoFunc) UpdateStartupOptions(opts map[string]string) { f(opts) }

// compressionInjectingAppInfo is an ApplicationInfo that writes the driver-owned
// COMPRESSION key, which the interface does not prevent: UpdateStartupOptions is handed
// the raw options map.
type compressionInjectingAppInfo struct{ algorithm string }

func (a compressionInjectingAppInfo) UpdateStartupOptions(opts map[string]string) {
	opts["COMPRESSION"] = a.algorithm
}

// parseStartupOptions decodes a STARTUP body: [short n] then n [string][string] pairs.
//
// Done by hand rather than through a framer: the framer's readers advance over the
// buffer the mock server still needs, and there is no readStringMap to borrow.
func parseStartupOptions(tb testing.TB, body []byte) map[string]string {
	tb.Helper()

	readShort := func() int {
		require.GreaterOrEqual(tb, len(body), 2, "truncated STARTUP body")
		v := int(binary.BigEndian.Uint16(body))
		body = body[2:]
		return v
	}
	readString := func() string {
		n := readShort()
		require.GreaterOrEqual(tb, len(body), n, "truncated STARTUP string")
		s := string(body[:n])
		body = body[n:]
		return s
	}

	opts := make(map[string]string)
	for i, n := 0, readShort(); i < n; i++ {
		k := readString()
		opts[k] = readString()
	}
	return opts
}

func newCallTable() *transport.InternalCallTable { return &transport.InternalCallTable{} }

func callTableOf(stream int, call *transport.InternalCallReq) *transport.InternalCallTable {
	t := newCallTable()
	t.Set(stream, call)
	return t
}

func TestConn(t *testing.T) {
	t.Run("ConnReader", func(t *testing.T) {
		// TestConn/ConnReader/ClearsDeadlineWhenTimeoutDisabled verifies that
		// connReader.Read arms a read deadline when a positive timeout is configured
		// and clears any previously-armed deadline when the timeout is disabled (0).
		// Without the clear, a deadline armed during connection setup (e.g.
		// ConnectTimeout) would persist and keep expiring on an idle connection.
		t.Run("ClearsDeadlineWhenTimeoutDisabled", func(t *testing.T) {
			t.Parallel()

			mock := &nonBlockingDeadlineConn{}
			cr := &transport.InternalConnReader{InternalConn: mock, InternalR: bufio.NewReader(mock)}

			buf := make([]byte, 1)

			// Positive timeout: Read arms a concrete (non-zero) deadline.
			cr.SetTimeout(5 * time.Second)
			_, _ = cr.Read(buf)
			require.Len(t, mock.deadlines, 1)
			require.False(t, mock.deadlines[0].IsZero(),
				"a positive timeout should arm a non-zero read deadline")

			// Disabled timeout: Read must clear the previously-armed deadline.
			cr.SetTimeout(0)
			_, _ = cr.Read(buf)
			require.Len(t, mock.deadlines, 2)
			require.True(t, mock.deadlines[1].IsZero(),
				"disabling the timeout should clear the read deadline (zero time)")
		})

		// TestConn/ConnReader/NilConnPositiveTimeoutNoPanic verifies
		// connReader.Read does not dereference a nil conn when a positive timeout is
		// configured. Only test constructions produce a nil conn, but the
		// deadline-arming branch must be guarded consistently with the other branches.
		t.Run("NilConnPositiveTimeoutNoPanic", func(t *testing.T) {
			t.Parallel()
			cr := &transport.InternalConnReader{InternalR: bufio.NewReader(bytes.NewReader([]byte{0x01}))}
			cr.SetTimeout(5 * time.Second)
			require.NotPanics(t, func() {
				buf := make([]byte, 1)
				_, _ = cr.Read(buf)
			})
		})

		// TestConn/ConnReader/RetriesWhileMakingProgress pins connReader.Read's retry policy.
		// ReadTimeout is a per-read budget, so a large body over a slow link can legitimately
		// need more than one; but it is also the driver's "identify faulty connections early"
		// mechanism (clusterSettings.ReadTimeout), so a peer that has actually stopped must not
		// get maxReadAttempts budgets before the connection is dropped. Progress is what
		// separates the two.
		t.Run("RetriesWhileMakingProgress", func(t *testing.T) {
			t.Parallel()

			// Every step is one read attempt; a step that carries data and a timeout is a
			// slow-but-progressing peer, a step with only a timeout is a stalled one.
			for _, tc := range []struct {
				name          string
				bufLen        int
				steps         []scriptedRead
				wantN         int
				wantErr       bool
				wantDeadlines int
			}{
				{
					// The fault-detection guarantee: nothing delivered means the peer stopped,
					// so this fails within one ReadTimeout even though data would follow.
					name:          "empty timeout is not resumed",
					bufLen:        4,
					steps:         []scriptedRead{{err: timeoutErr{}}, {data: []byte{1, 2, 3, 4}}},
					wantN:         0,
					wantErr:       true,
					wantDeadlines: 1,
				},
				{
					// The tolerance guarantee: bytes accumulate across attempts and no data is
					// dropped, so a body slower than one ReadTimeout still completes.
					name:   "accumulates across progressing attempts",
					bufLen: 4,
					steps: []scriptedRead{
						{data: []byte{1}, err: timeoutErr{}},
						{data: []byte{2}, err: timeoutErr{}},
						{data: []byte{3, 4}},
					},
					wantN:         4,
					wantDeadlines: 3,
				},
				{
					name:   "stops once progress stops",
					bufLen: 4,
					steps: []scriptedRead{
						{data: []byte{1}, err: timeoutErr{}},
						{err: timeoutErr{}},
						{data: []byte{2, 3, 4}},
					},
					wantN:         1,
					wantErr:       true,
					wantDeadlines: 2,
				},
				{
					// A non-timeout network error is not resumable, whatever Temporary() says
					// about it — the stream position can no longer be trusted.
					name:   "non-timeout net error is not retried",
					bufLen: 4,
					steps: []scriptedRead{
						{data: []byte{1}, err: &net.OpError{Op: "read", Err: errors.New("connection reset by peer")}},
						{data: []byte{2, 3, 4}},
					},
					wantN:         1,
					wantErr:       true,
					wantDeadlines: 1,
				},
				{
					// Bounded: a peer trickling one byte per ReadTimeout forever still fails,
					// after maxReadAttempts.
					name:   "gives up after maxReadAttempts",
					bufLen: 10,
					steps: []scriptedRead{
						{data: []byte{1}, err: timeoutErr{}},
						{data: []byte{2}, err: timeoutErr{}},
						{data: []byte{3}, err: timeoutErr{}},
						{data: []byte{4}, err: timeoutErr{}},
						{data: []byte{5}, err: timeoutErr{}},
						{data: []byte{6}, err: timeoutErr{}},
					},
					wantN:         transport.InternalMaxReadAttempts,
					wantErr:       true,
					wantDeadlines: transport.InternalMaxReadAttempts,
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					mock := &scriptedConn{steps: tc.steps}
					cr := &transport.InternalConnReader{InternalConn: mock, InternalR: bufio.NewReader(mock)}
					cr.SetTimeout(time.Second)

					n, err := cr.Read(make([]byte, tc.bufLen))

					require.Equal(t, tc.wantN, n)
					if tc.wantErr {
						require.Error(t, err)
					} else {
						require.NoError(t, err)
					}
					require.Len(t, mock.deadlines, tc.wantDeadlines,
						"one fresh read deadline per attempt")
				})
			}
		})

		// TestConn/ConnReader/ReturnsDeadlineError verifies that connReader.Read reports a
		// failing SetReadDeadline instead of reading with no deadline (or a stale one),
		// matching the write path (deadlineContextWriter.InternalWriteContext). All three
		// deadline branches are covered: disarmed, positive timeout, disabled timeout.
		t.Run("ReturnsDeadlineError", func(t *testing.T) {
			t.Parallel()

			setDeadlineErr := errors.New("set read deadline failed")

			for _, tc := range []struct {
				name    string
				timeout time.Duration
				disarm  bool
			}{
				{name: "disarmed", timeout: 5 * time.Second, disarm: true},
				{name: "positive timeout", timeout: 5 * time.Second},
				{name: "timeout disabled", timeout: 0},
			} {
				t.Run(tc.name, func(t *testing.T) {
					t.Parallel()

					mock := &nonBlockingDeadlineConn{setDeadlineErr: setDeadlineErr}
					cr := &transport.InternalConnReader{InternalConn: mock, InternalR: bufio.NewReader(mock)}
					cr.SetTimeout(tc.timeout)
					cr.InternalSetDisarm(tc.disarm)

					n, err := cr.Read(make([]byte, 1))
					require.ErrorIs(t, err, setDeadlineErr)
					require.Zero(t, n)
					require.Zero(t, mock.reads,
						"Read must not touch the connection once arming the deadline failed")
					// Marked as well as reported: the underlying error need not be a
					// net.Error, so "nothing was consumed" has to be recoverable by name for
					// bodyReadDesyncedConn to classify a body read that hits this as fatal.
					require.ErrorIs(t, err, transport.InternalErrArmReadDeadline)
					require.True(t, transport.InternalBodyReadDesyncedConn(err),
						"a body read that never reached the socket leaves the whole body queued")
				})
			}
		})
	})

	t.Run("Exec", func(t *testing.T) {
		t.Run("CloseWithError", func(t *testing.T) {
			t.Parallel()

			const execCloseTestTimeout = 10 * time.Second

			t.Run("BuildFrameErrorReleasesResources", func(t *testing.T) {
				c, server := newTestExecConn(t, testContextWriter{})
				defer server.Close()

				_, err := c.InternalExec(context.Background(), implProtocol.FrameWriterFunc(func(f *implProtocol.Framer, streamID int) error {
					return io.ErrUnexpectedEOF
				}), nil, 0)
				if !errors.Is(err, io.ErrUnexpectedEOF) {
					t.Fatalf("expected build error %v, got %v", io.ErrUnexpectedEOF, err)
				}

				c.InternalMu.Lock()
				defer c.InternalMu.Unlock()
				if c.InternalCalls.Len() != 0 {
					t.Fatalf("expected no in-flight calls after build error, got %d", c.InternalCalls.Len())
				}
			})

			t.Run("ContextCanceledBeforeWriteReleasesResources", func(t *testing.T) {
				writeEntered := make(chan struct{})
				c, server := newTestExecConn(t, contextWriterFunc(func(ctx context.Context, p []byte) (int, error) {
					close(writeEntered)
					<-ctx.Done()
					return 0, ctx.Err()
				}))
				defer server.Close()

				ctx, cancel := context.WithCancel(context.Background())
				errCh := make(chan error, 1)
				go func() {
					_, err := c.InternalExec(ctx, implProtocol.FrameWriterFunc(func(f *implProtocol.Framer, streamID int) error {
						f.Buf = append(f.Buf[:0], 'x')
						return nil
					}), nil, 0)
					errCh <- err
				}()

				select {
				case <-writeEntered:
				case <-time.After(execCloseTestTimeout):
					t.Fatal("exec never reached the write path")
				}
				cancel()

				select {
				case err := <-errCh:
					if !errors.Is(err, context.Canceled) {
						t.Fatalf("expected context cancel error %v, got %v", context.Canceled, err)
					}
				case <-time.After(execCloseTestTimeout):
					t.Fatal("exec deadlocked after context cancellation before write")
				}

				c.InternalMu.Lock()
				defer c.InternalMu.Unlock()
				if c.InternalCalls.Len() != 0 {
					t.Fatalf("expected no in-flight calls after canceled write, got %d", c.InternalCalls.Len())
				}
			})

			t.Run("ResponseErrorReleasesResources", func(t *testing.T) {
				c, server := newTestExecConn(t, testContextWriter{})
				defer server.Close()

				errCh := make(chan error, 1)
				go func() {
					_, err := c.InternalExec(context.Background(), implProtocol.FrameWriterFunc(func(f *implProtocol.Framer, streamID int) error {
						f.Buf = append(f.Buf[:0], 'x')
						return nil
					}), nil, 0)
					errCh <- err
				}()

				waitForSingleCall(t, c)
				call := detachSingleCall(t, c)
				call.InternalResp <- transport.InternalCallResp{InternalErr: io.EOF}

				select {
				case err := <-errCh:
					if !errors.Is(err, io.EOF) {
						t.Fatalf("expected response error %v, got %v", io.EOF, err)
					}
				case <-time.After(execCloseTestTimeout):
					t.Fatal("exec deadlocked after response error")
				}

				c.InternalMu.Lock()
				defer c.InternalMu.Unlock()
				if c.InternalCalls.Len() != 0 {
					t.Fatalf("expected no in-flight calls after response error, got %d", c.InternalCalls.Len())
				}
			})

			t.Run("PartialWriteDoesNotDeadlock", func(t *testing.T) {
				c, server := newTestExecConn(t, testContextWriter{
					n:   1,
					err: io.ErrUnexpectedEOF,
				})
				defer server.Close()

				errCh := make(chan error, 1)
				go func() {
					_, err := c.InternalExec(context.Background(), implProtocol.FrameWriterFunc(func(f *implProtocol.Framer, streamID int) error {
						f.Buf = append(f.Buf[:0], 'x')
						return nil
					}), nil, 0)
					errCh <- err
				}()

				select {
				case err := <-errCh:
					if !errors.Is(err, io.ErrUnexpectedEOF) {
						t.Fatalf("expected write error %v, got %v", io.ErrUnexpectedEOF, err)
					}
				case <-time.After(execCloseTestTimeout):
					t.Fatal("exec deadlocked after partial write failure")
				}
			})

			t.Run("ConnectionCloseErrorDoesNotDeadlock", func(t *testing.T) {
				writeStarted := make(chan struct{})
				var writeStartedOnce sync.Once
				c, server := newTestExecConn(t, testContextWriter{
					onWrite: func() {
						writeStartedOnce.Do(func() {
							close(writeStarted)
						})
					},
				})
				defer server.Close()

				closeDone := make(chan struct{})
				go func() {
					<-writeStarted
					c.InternalCloseWithError(io.EOF)
					close(closeDone)
				}()

				errCh := make(chan error, 1)
				go func() {
					_, err := c.InternalExec(context.Background(), implProtocol.FrameWriterFunc(func(f *implProtocol.Framer, streamID int) error {
						f.Buf = append(f.Buf[:0], 'x')
						return nil
					}), nil, 0)
					errCh <- err
				}()

				select {
				case err := <-errCh:
					if !errors.Is(err, io.EOF) {
						t.Fatalf("expected close error %v, got %v", io.EOF, err)
					}
				case <-time.After(execCloseTestTimeout):
					t.Fatal("exec deadlocked after closeWithError")
				}

				select {
				case <-closeDone:
				case <-time.After(execCloseTestTimeout):
					t.Fatal("closeWithError deadlocked waiting for exec to release the call")
				}
			})

			t.Run("TimeoutUnblocksAbandonRecvCall", func(t *testing.T) {
				c, server := newTestExecConn(t, testContextWriter{})
				defer server.Close()

				errCh := make(chan error, 1)
				go func() {
					_, err := c.InternalExec(context.Background(), implProtocol.FrameWriterFunc(func(f *implProtocol.Framer, streamID int) error {
						f.Buf = append(f.Buf[:0], 'x')
						return nil
					}), nil, time.Millisecond)
					errCh <- err
				}()

				call := waitForSingleCall(t, c)

				select {
				case err := <-errCh:
					if !errors.Is(err, runtimeerrors.ErrTimeoutNoResponse) {
						t.Fatalf("expected timeout error %v, got %v", runtimeerrors.ErrTimeoutNoResponse, err)
					}
				case <-time.After(execCloseTestTimeout):
					t.Fatal("exec deadlocked waiting for timeout")
				}

				if !c.InternalRemoveCallIfOpen(call) {
					t.Fatal("expected timed out call to still be registered")
				}

				done := make(chan struct{})
				go func() {
					c.InternalAbandonRecvCall(call, c.InternalGetReadFramer())
					close(done)
				}()

				select {
				case <-done:
				case <-time.After(execCloseTestTimeout):
					t.Fatal("abandonRecvCall deadlocked after timeout")
				}
			})

			t.Run("ContextCancelUnblocksAbandonRecvCall", func(t *testing.T) {
				c, server := newTestExecConn(t, testContextWriter{})
				defer server.Close()

				ctx, cancel := context.WithCancel(context.Background())
				errCh := make(chan error, 1)
				go func() {
					_, err := c.InternalExec(ctx, implProtocol.FrameWriterFunc(func(f *implProtocol.Framer, streamID int) error {
						f.Buf = append(f.Buf[:0], 'x')
						return nil
					}), nil, 0)
					errCh <- err
				}()

				call := waitForSingleCall(t, c)
				cancel()

				select {
				case err := <-errCh:
					if !errors.Is(err, context.Canceled) {
						t.Fatalf("expected context cancel error %v, got %v", context.Canceled, err)
					}
				case <-time.After(execCloseTestTimeout):
					t.Fatal("exec deadlocked waiting for context cancellation")
				}

				if !c.InternalRemoveCallIfOpen(call) {
					t.Fatal("expected canceled call to still be registered")
				}

				done := make(chan struct{})
				go func() {
					c.InternalAbandonRecvCall(call, c.InternalGetReadFramer())
					close(done)
				}()

				select {
				case <-done:
				case <-time.After(execCloseTestTimeout):
					t.Fatal("abandonRecvCall deadlocked after context cancellation")
				}
			})

			t.Run("ConnectionCloseAbandonsInflightStream", func(t *testing.T) {
				writeStarted := make(chan struct{})
				var writeStartedOnce sync.Once
				observerCtx := newTestStreamObserverContext()
				c, server := newTestExecConn(t, testContextWriter{
					onWrite: func() {
						writeStartedOnce.Do(func() {
							close(writeStarted)
						})
					},
				})
				c.InternalStreamObserver = &testStreamObserver{ctx: observerCtx}
				defer server.Close()

				errCh := make(chan error, 1)
				go func() {
					_, err := c.InternalExec(context.Background(), implProtocol.FrameWriterFunc(func(f *implProtocol.Framer, streamID int) error {
						f.Buf = append(f.Buf[:0], 'x')
						return nil
					}), nil, 0)
					errCh <- err
				}()

				select {
				case <-observerCtx.started:
				case <-time.After(execCloseTestTimeout):
					t.Fatal("stream observer did not observe the request start")
				}

				select {
				case <-writeStarted:
				case <-time.After(execCloseTestTimeout):
					t.Fatal("exec never reached the write path")
				}

				closeDone := make(chan struct{})
				go func() {
					c.Close()
					close(closeDone)
				}()

				select {
				case err := <-errCh:
					if !errors.Is(err, runtimeerrors.ErrConnectionClosed) {
						t.Fatalf("expected close error %v, got %v", runtimeerrors.ErrConnectionClosed, err)
					}
				case <-time.After(execCloseTestTimeout):
					t.Fatal("exec deadlocked after Close")
				}

				select {
				case <-observerCtx.abandoned:
				case <-time.After(execCloseTestTimeout):
					t.Fatal("Close did not abandon the in-flight stream")
				}

				select {
				case <-observerCtx.finished:
					t.Fatal("Close should not mark the in-flight stream as finished")
				default:
				}

				select {
				case <-closeDone:
				case <-time.After(execCloseTestTimeout):
					t.Fatal("Close did not wait for the in-flight exec cleanup")
				}

				c.InternalMu.Lock()
				defer c.InternalMu.Unlock()
				if c.InternalCalls != nil {
					t.Fatal("expected in-flight calls to be detached on Close")
				}
			})
		})

		// TestConn/Exec/StreamIDMismatchFailsConnection drives the same mismatch with a real
		// exec() waiting on call.resp, so it pins what actually resolves that call: the
		// connection close the error triggers, not a delivery from the mismatch arm.
		t.Run("StreamIDMismatchFailsConnection", func(t *testing.T) {
			t.Parallel()

			const testTimeout = 10 * time.Second

			// mismatchHeader builds a response frame header addressed to streamID, sized to
			// carry no body.
			mismatchHeader := func(streamID int) []byte {
				return []byte{
					implProtocol.ProtoVersion4 | implProtocol.ProtoDirectionMask, 0x00,
					byte(streamID >> 8), byte(streamID),
					byte(frm.OpResult),
					0x00, 0x00, 0x00, 0x00, // no body
				}
			}

			// remapWaitingCall starts an exec() and re-keys c.calls so the entry sits under a
			// different stream than call.streamID. Returns the result channel and the wrong
			// map key a mismatched response must be addressed to.
			remapWaitingCall := func(t *testing.T, c *transport.Conn, observerCtx *testStreamObserverContext) (<-chan error, int) {
				t.Helper()

				errCh := make(chan error, 1)
				go func() {
					_, err := c.InternalExec(context.Background(), implProtocol.FrameWriterFunc(func(f *implProtocol.Framer, streamID int) error {
						f.Buf = append(f.Buf[:0], 'x')
						return nil
					}), nil, 0)
					errCh <- err
				}()

				call := waitForSingleCall(t, c)
				select {
				case <-observerCtx.started:
				case <-time.After(testTimeout):
					t.Fatal("stream observer did not observe the request start")
				}

				wrongKey := call.InternalStreamID + 1
				if wrongKey > c.InternalStreams.NumStreams {
					wrongKey = 1
				}

				c.InternalMu.Lock()
				c.InternalCalls.Delete(call.InternalStreamID)
				c.InternalCalls.Set(wrongKey, call)
				c.InternalMu.Unlock()

				return errCh, wrongKey
			}

			// duplicateWaitingCall is remapWaitingCall without the delete: the live entry
			// stays under call.streamID and a stale one is added beside it, which is what a
			// recycled-and-re-issued callReq leaves behind. Returns the stale key.
			duplicateWaitingCall := func(t *testing.T, c *transport.Conn, observerCtx *testStreamObserverContext) (<-chan error, int) {
				t.Helper()

				errCh := make(chan error, 1)
				go func() {
					_, err := c.InternalExec(context.Background(), implProtocol.FrameWriterFunc(func(f *implProtocol.Framer, streamID int) error {
						f.Buf = append(f.Buf[:0], 'x')
						return nil
					}), nil, 0)
					errCh <- err
				}()

				call := waitForSingleCall(t, c)
				select {
				case <-observerCtx.started:
				case <-time.After(testTimeout):
					t.Fatal("stream observer did not observe the request start")
				}

				staleKey := call.InternalStreamID + 1
				if staleKey > c.InternalStreams.NumStreams {
					staleKey = 1
				}

				c.InternalMu.Lock()
				c.InternalCalls.Set(staleKey, call)
				c.InternalMu.Unlock()

				return errCh, staleKey
			}

			t.Run("TheCloseReleasesAndRecyclesTheStream", func(t *testing.T) {
				t.Parallel()

				observerCtx := newTestStreamObserverContext()
				c, server := newTestExecConn(t, testContextWriter{})
				c.InternalStreamObserver = &testStreamObserver{ctx: observerCtx}
				defer server.Close()

				errCh, wrongKey := remapWaitingCall(t, c, observerCtx)

				procErr := c.InternalProcessFrameSource(context.Background(), transport.InternalFrameSource{InternalR: bytes.NewReader(mismatchHeader(wrongKey))})
				require.Error(t, procErr)

				// The arm left the call where it was, so it is still exec's only way out.
				select {
				case err := <-errCh:
					t.Fatalf("exec returned %v before the connection was closed", err)
				case <-time.After(10 * time.Millisecond):
				}

				// serve() does exactly this with the error it was handed.
				c.InternalCloseWithError(procErr)

				select {
				case err := <-errCh:
					require.ErrorIs(t, err, procErr)
				case <-time.After(testTimeout):
					t.Fatal("exec did not return after the connection was closed")
				}

				select {
				case <-observerCtx.abandoned:
				case <-time.After(testTimeout):
					t.Fatal("stream was never marked abandoned")
				}
				select {
				case <-observerCtx.finished:
					t.Fatal("stream should not also be marked finished")
				default:
				}

				// The stream bit stays set: c.streams dies with the connection, and nothing
				// on the close path clears it for any in-flight call. What must not survive
				// is the call itself.
				c.InternalMu.Lock()
				remaining := c.InternalCalls.Len()
				c.InternalMu.Unlock()
				if remaining != 0 {
					t.Fatalf("expected the close to drain c.calls, got %d", remaining)
				}
			})

			// The shape a mismatch can actually take: addCall only ever keys by
			// call.streamID, so the key and the field can diverge only once the callReq has
			// been recycled and re-issued, leaving a stale entry beside the live one. Both
			// entries then point at one callReq, and the close has to survive meeting it
			// twice.
			t.Run("ADuplicateEntryDoesNotWedgeTheClose", func(t *testing.T) {
				t.Parallel()

				observerCtx := newTestStreamObserverContext()
				c, server := newTestExecConn(t, testContextWriter{})
				c.InternalStreamObserver = &testStreamObserver{ctx: observerCtx}
				defer server.Close()

				errCh, staleKey := duplicateWaitingCall(t, c, observerCtx)

				procErr := c.InternalProcessFrameSource(context.Background(), transport.InternalFrameSource{InternalR: bytes.NewReader(mismatchHeader(staleKey))})
				require.Error(t, procErr)

				closed := make(chan struct{})
				go func() {
					c.InternalCloseWithError(procErr)
					close(closed)
				}()
				select {
				case <-closed:
				case <-time.After(testTimeout):
					t.Fatal("closeWithError never returned: it met the recycled callReq a second time")
				}

				select {
				case err := <-errCh:
					require.ErrorIs(t, err, procErr)
				case <-time.After(testTimeout):
					t.Fatal("exec did not return after the connection was closed")
				}
			})

			t.Run("RacesWithConnectionClose", func(t *testing.T) {
				t.Parallel()

				for i := 0; i < 100; i++ {
					observerCtx := newTestStreamObserverContext()
					c, server := newTestExecConn(t, testContextWriter{})
					c.InternalStreamObserver = &testStreamObserver{ctx: observerCtx}

					errCh, wrongKey := remapWaitingCall(t, c, observerCtx)
					header := mismatchHeader(wrongKey)

					var wg sync.WaitGroup
					wg.Add(2)
					go func() {
						defer wg.Done()
						c.InternalProcessFrameSource(context.Background(), transport.InternalFrameSource{InternalR: bytes.NewReader(header)})
					}()
					go func() {
						defer wg.Done()
						c.Close()
					}()
					wg.Wait()
					server.Close()

					select {
					case <-errCh:
					case <-time.After(testTimeout):
						t.Fatal("exec never returned after the raced delivery/close")
					}

					finished, abandoned := false, false
					select {
					case <-observerCtx.finished:
						finished = true
					default:
					}
					select {
					case <-observerCtx.abandoned:
						abandoned = true
					default:
					}
					if finished == abandoned {
						t.Fatalf("iteration %d: expected exactly one terminal callback, finished=%v abandoned=%v", i, finished, abandoned)
					}
				}
			})
		})
	})

	t.Run("HeaderRead", func(t *testing.T) {
		// TestConn/HeaderRead/DisarmBoundedToFirstByte pins that only the idle wait for a
		// header's first byte runs without a read deadline.
		//
		// The disarm is per-read (connReader.armDeadline runs once per read attempt), so a
		// disarm held across the whole header read leaves every byte of it unbounded: a peer
		// that sends one byte and then stops holds the serve goroutine for as long as it
		// keeps the socket open, and ReadTimeout never drops the connection. That cannot be
		// asserted by waiting — the point of the bug is that nothing ever returns — so it is
		// pinned on the mechanism instead: the first read asks for a single byte, and the
		// deadline is re-armed before the second read starts.
		t.Run("DisarmBoundedToFirstByte", func(t *testing.T) {
			t.Parallel()

			// A peer that delivers one byte of the header and then stalls until the re-armed
			// deadline expires.
			steps := []scriptedRead{{data: []byte{0x84}}, {err: timeoutErr{}}}

			for _, tc := range []struct {
				name string
				read func(*transport.Conn) error
			}{
				{
					name: "frame header",
					read: func(c *transport.Conn) error { _, err := c.InternalReadFrameHeader(c.InternalR); return err },
				},
				{
					name: "segment header",
					read: func(c *transport.Conn) error { _, err := c.InternalReadFirstSegmentHeader(); return err },
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					t.Parallel()

					r := &scriptedReadSource{steps: steps}
					c := &transport.Conn{InternalR: r}

					err := tc.read(c)

					require.Error(t, err)
					// Not normalised: a timeout that already consumed part of a header leaves
					// the stream at an unknown offset, so serve() must close the connection
					// rather than resume and mis-frame everything after it.
					require.False(t, errors.Is(err, implProtocol.ErrReadHeaderTimeout),
						"a timeout partway through a header must stay fatal, got %q", err)

					require.GreaterOrEqual(t, len(r.readLens), 2,
						"the header must be read in more than one read for the deadline to be re-armed in between")
					require.Equal(t, 1, r.readLens[0],
						"the unbounded read must ask for a single byte, so only the idle wait is unbounded")
					require.Equal(t, 1, r.arms,
						"exactly one read must start with the deadline re-armed: the one after the first byte arrived")
				})
			}
		})

		// TestConn/HeaderRead/FirstSegmentDisarmsAndRearms pins that the idle wait for a segment
		// header runs with the read deadline disarmed and that the deadline is restored on
		// every exit path — including the error path, where a missed re-arm would leave the
		// connection reading with no deadline for the rest of its life.
		t.Run("FirstSegmentDisarmsAndRearms", func(t *testing.T) {
			t.Parallel()

			header := mustUncompressedSegment(t, []byte("hello"), true)

			for _, tc := range []struct {
				name  string
				steps []scriptedRead
			}{
				{"header read succeeds", []scriptedRead{{data: header}}},
				{"header read fails", []scriptedRead{{err: timeoutErr{}}}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					r := &scriptedReadSource{steps: tc.steps}
					c := &transport.Conn{InternalR: r}

					_, _ = c.InternalReadFirstSegmentHeader()

					// Asserted as a shape rather than an exact slice: the re-arm can come from
					// headerReader (as soon as a byte arrives) as well as from the deferred
					// clear, so a successful read legitimately clears twice. What must hold is
					// that the read starts disarmed, ends re-armed, and is never re-disarmed —
					// the last of those is what a redundant deferred clear must stay harmless
					// for.
					require.NotEmpty(t, r.disarms, "the header read must disarm the deadline")
					require.True(t, r.disarms[0], "the idle wait must start with the deadline disarmed")
					require.False(t, r.disarms[len(r.disarms)-1],
						"the deadline must be re-armed before readFirstSegmentHeader returns")
					for i, d := range r.disarms[1:] {
						require.False(t, d, "InternalSetDisarm(true) at index %d: the deadline must never be re-disarmed", i+1)
					}
				})
			}
		})

		// TestConn/HeaderRead/TimeoutBenignOnlyWhenNothingConsumed pins the line between
		// the two kinds of header-read timeout, for both header readers.
		//
		// Consumed nothing: the peer was simply idle, serve() logs ErrReadHeaderTimeout and
		// reads again. Consumed part of a header: the stream is now at an unknown offset, so
		// the error must NOT be normalised — serve() has to close the connection instead of
		// resuming and mis-framing every frame that follows.
		t.Run("TimeoutBenignOnlyWhenNothingConsumed", func(t *testing.T) {
			t.Parallel()

			for _, tc := range []struct {
				name      string
				steps     []scriptedRead
				wantIsErr bool
			}{
				{
					name:      "nothing consumed",
					steps:     []scriptedRead{{err: timeoutErr{}}},
					wantIsErr: true,
				},
				{
					name:      "partial header consumed",
					steps:     []scriptedRead{{data: []byte{0x84, 0x00, 0x00, 0x01}, err: timeoutErr{}}},
					wantIsErr: false,
				},
			} {
				t.Run("frame header/"+tc.name, func(t *testing.T) {
					c := &transport.Conn{InternalR: &scriptedReadSource{steps: tc.steps}}

					_, err := c.InternalReadFrameHeader(c.InternalR)

					require.Error(t, err)
					require.Equal(t, tc.wantIsErr, errors.Is(err, implProtocol.ErrReadHeaderTimeout),
						"errors.Is(ErrReadHeaderTimeout) on %q", err)
				})

				t.Run("segment header/"+tc.name, func(t *testing.T) {
					c := &transport.Conn{InternalR: &scriptedReadSource{steps: tc.steps}}

					_, err := c.InternalReadFirstSegmentHeader()

					require.Error(t, err)
					require.Equal(t, tc.wantIsErr, errors.Is(err, implProtocol.ErrReadHeaderTimeout),
						"errors.Is(ErrReadHeaderTimeout) on %q", err)
				})
			}
		})
	})

	t.Run("HeartBeat", func(t *testing.T) {
		t.Run("ClosesConnectionThatKeepsFailing", func(t *testing.T) {
			t.Parallel()

			const testTimeout = 30 * time.Second

			log := &unexpectedOptionsLogger{seen: make(chan struct{})}

			conn, server := newTestExecConn(t, testContextWriter{})
			conn.InternalVersion = implProtocol.ProtoVersion4
			conn.InternalCfg = &transport.ConnConfig{}
			conn.InternalLogger = slog.New(log)
			defer server.Close()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go conn.InternalHeartBeat(ctx)

			// An ERROR frame: [int error code][string message], the shortest reply a server
			// may answer an OPTIONS with that heartBeat has to classify as a failure.
			errorReply := func(streamID int) []byte {
				const msg = "scripted heartbeat failure"
				body := []byte{0x00, 0x00, 0x10, 0x01, byte(len(msg) >> 8), byte(len(msg))}
				body = append(body, msg...)
				return append([]byte{
					implProtocol.ProtoVersion4 | implProtocol.ProtoDirectionMask, 0x00,
					byte(streamID >> 8), byte(streamID),
					byte(frm.OpError),
					byte(len(body) >> 24), byte(len(body) >> 16), byte(len(body) >> 8), byte(len(body)),
				}, body...)
			}

			// The first beat gets the unexpected frame, so the interval is already at 1s.
			call := waitForSingleCallWithin(t, conn, testTimeout)
			ready := []byte{
				implProtocol.ProtoVersion4 | implProtocol.ProtoDirectionMask, 0x00,
				byte(call.InternalStreamID >> 8), byte(call.InternalStreamID),
				byte(frm.OpReady),
				0x00, 0x00, 0x00, 0x00, // no body
			}
			if err := conn.InternalProcessFrameSource(ctx, transport.InternalFrameSource{InternalR: bytes.NewReader(ready)}); err != nil {
				t.Fatalf("delivering the READY reply to the heartbeat: %v", err)
			}

			select {
			case <-log.seen:
			case <-time.After(testTimeout):
				t.Fatal("heartBeat never logged the unexpected OPTIONS reply")
			}

			// Answer every later beat with an ERROR. heartBeat closes the connection once
			// the failures pass its bound; without counting them it never would.
			deadline := time.Now().Add(testTimeout)
			for !conn.Closed() {
				if time.Now().After(deadline) {
					t.Fatal("a connection answering every heartbeat with an ERROR was never closed")
				}
				// Poll rather than wait: the beat that trips the bound is answered and then
				// no further call is ever registered, so a blocking wait would outlive the
				// close it is looking for.
				conn.InternalMu.Lock()
				var call *transport.InternalCallReq
				for _, inFlight := range conn.InternalCalls.All() {
					call = inFlight
					break
				}
				conn.InternalMu.Unlock()
				if call == nil {
					time.Sleep(time.Millisecond)
					continue
				}
				if err := conn.InternalProcessFrameSource(ctx, transport.InternalFrameSource{InternalR: bytes.NewReader(errorReply(call.InternalStreamID))}); err != nil {
					t.Fatalf("delivering the ERROR reply to the heartbeat: %v", err)
				}
			}
		})

		t.Run("ControlSurvivesUnexpectedOptionsResponse", func(t *testing.T) {
			t.Parallel()

			const testTimeout = 10 * time.Second

			log := &unexpectedOptionsLogger{seen: make(chan struct{})}

			conn, server := newTestExecConn(t, testContextWriter{})
			// execInternal checks the response's protocol version against c.version; an unset
			// one makes every frame a protocol error before heartBeat classifies it.
			conn.InternalVersion = implProtocol.ProtoVersion4
			defer server.Close()

			// Only the fields the failing reconnect reaches. With no hosts and no contact
			// points it fails in resolveInitialEndpoints, well before refreshRingNow.
			sess := func() *implCluster.Core {
				opaqueStorageValue := new(implCluster.Core)
				implCluster.SessionData(opaqueStorageValue).InternalLogger = slog.New(log)
				implCluster.SessionData(opaqueStorageValue).InternalConnCfg = &transport.ConnConfig{}
				return opaqueStorageValue
			}()
			implCluster.SessionData(sess).InternalHostSource = &implCluster.InternalRingDescriber{InternalCfg: &implCluster.SessionData(sess).InternalCfg, InternalLogger: slog.New(log)}

			// state is left at controlConnStarting so heartBeat's CAS lets it run.
			cc := &implCluster.InternalControlConn{InternalSession: sess, InternalQuit: make(chan struct{})}
			cc.InternalConn.Store(&implCluster.InternalConnHost{InternalConn: conn})

			go cc.InternalHeartBeat()
			defer close(cc.InternalQuit)

			// Catch the first beat's in-flight call, then answer OPTIONS with READY: a frame
			// parseFrame builds happily, but not one an OPTIONS may be answered with.
			call := waitForSingleCallWithin(t, conn, testTimeout)
			ready := []byte{
				implProtocol.ProtoVersion4 | implProtocol.ProtoDirectionMask, 0x00,
				byte(call.InternalStreamID >> 8), byte(call.InternalStreamID),
				byte(frm.OpReady),
				0x00, 0x00, 0x00, 0x00, // no body
			}
			if err := conn.InternalProcessFrameSource(context.Background(), transport.InternalFrameSource{InternalR: bytes.NewReader(ready)}); err != nil {
				t.Fatalf("delivering the READY reply to the heartbeat: %v", err)
			}

			select {
			case <-log.seen:
			case <-time.After(testTimeout):
				t.Fatal("the control heartbeat never logged the unexpected OPTIONS reply")
			}

			// attemptReconnect closes the old connection before trying any host, so this is
			// what separates a reconnect from a continue that trusts the connection anyway.
			deadline := time.Now().Add(testTimeout)
			for !conn.Closed() {
				if time.Now().After(deadline) {
					t.Fatal("the unexpected reply did not cost the control connection a reconnect")
				}
				time.Sleep(time.Millisecond)
			}
		})

		t.Run("KeepaliveDisabledAndBlockedWriteTimeout", func(t *testing.T) {
			c, server := typedTestConn(t, 4, nil)
			config := settings.DefaultConfig("localhost")
			config.KeepaliveInterval = 0
			c.InternalCfg = &transport.ConnConfig{TypedConfig: config.ConnectivityPointer()}
			done := make(chan struct{})
			go func() { c.InternalHeartBeat(c.InternalCtx); close(done) }()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("disabled heartbeat active")
			}
			server.Close()
			c, _ = typedTestConn(t, 4, nil)
			config.KeepaliveInterval = time.Millisecond
			config.KeepaliveTimeout = 10 * time.Millisecond
			c.InternalCfg = &transport.ConnConfig{TypedConfig: config.ConnectivityPointer()}
			done = make(chan struct{})
			go func() { c.InternalHeartBeat(c.InternalCtx); close(done) }()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("heartbeat timeout did not cover blocked write")
			}
			if !c.Closed() {
				t.Fatal("timed out writer left connection alive")
			}
		})

		t.Run("SurvivesUnexpectedOptionsResponse", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			srv := NewTestServer(t, defaultProto, ctx)
			defer srv.Stop()

			log := &unexpectedOptionsLogger{seen: make(chan struct{})}

			// Spoil every OPTIONS after startup's, so the reply is in place before the first
			// beat; losing that race pushes the next one 30s out.
			atomic.StoreInt32(&srv.readyOnOptions, 1)

			cluster := testCluster(defaultProto, srv.Address)
			cluster.NumConns = 1
			cluster.Logger = slog.New(log)
			db, err := cluster.Open()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()

			if err := waitForPoolSize(db, 1, 5*time.Second); err != nil {
				t.Fatal(err)
			}

			// Wait for the driver to parse the first beat rather than sleeping past it.
			select {
			case <-log.seen:
			case <-time.After(10 * time.Second):
				t.Fatal("heartBeat never logged the unexpected OPTIONS reply")
			}

			if err := execTestStatement(context.Background(), db, "void", CompiledOptions{}); err != nil {
				t.Fatalf("the session did not survive an unexpected heartbeat reply: %v", err)
			}
		})
	})

	t.Run("ProcessFrame", func(t *testing.T) {
		// TestConn/ProcessFrame/DesyncedBodyIsFatalAndWakesCaller pins the two halves of what
		// has to happen when a frame body cannot be read off the wire:
		//
		//   - processFrame returns the error, so serve() closes the connection. The
		//     unread remainder of the body is still queued on the socket, so reusing the
		//     connection would read it as the next frame header and mis-frame everything
		//     after it.
		//   - the waiting caller is handed the error first. head.Stream has already been
		//     removed from c.calls by this point, so closeWithError can no longer find the
		//     call: returning without delivering would leave exec() waiting out its full
		//     request timeout for a response that can never arrive.
		t.Run("DesyncedBodyIsFatalAndWakesCaller", func(t *testing.T) {
			t.Parallel()

			header := []byte{
				implProtocol.ProtoVersion4 | implProtocol.ProtoDirectionMask, 0x00, 0x00, 0x01, byte(frm.OpResult),
				0x00, 0x00, 0x00, 0x08, // body length 8
			}

			// The peer sends a complete header and then stalls partway through the body.
			r := &scriptedReadSource{steps: []scriptedRead{
				{data: header},
				{data: []byte{0x01, 0x02, 0x03}, err: timeoutErr{}},
			}}

			// Buffered so the delivery does not need a second goroutine.
			call := &transport.InternalCallReq{InternalStreamID: 1, InternalResp: make(chan transport.InternalCallResp, 1)}
			c := &transport.Conn{
				InternalR:       r,
				InternalCalls:   callTableOf(1, call),
				InternalVersion: implProtocol.ProtoVersion4,
				InternalStreams: streams.New(),
				InternalLogger:  logging.Nop(),
			}

			err := c.InternalProcessFrame(context.Background(), c.InternalR)

			require.Error(t, err, "a half-read body desyncs the connection and must be fatal")
			var netErr net.Error
			require.ErrorAs(t, err, &netErr)

			select {
			case resp := <-call.InternalResp:
				require.ErrorIs(t, resp.InternalErr, err, "the waiting caller must be handed the same failure")
			default:
				t.Fatal("the call was never woken: it would wait out its full request timeout")
			}
		})

		// TestConn/ProcessFrame/FailedDeadlineArmIsFatal pins that a body read which never
		// reached the socket is fatal to the connection, and that widening the rule that
		// far did not make every read failure fatal.
		//
		// A failed read-deadline arm is the worst case of a desynced body: connReader.Read
		// returns before touching the socket, so the entire body is still queued on a
		// connection that is otherwise healthy, and serve() reads it as the next frame
		// header. Nothing else surfaces it. Classifying it needs the error to say so —
		// SetReadDeadline's error need not be a net.Error, so the net.Error test alone let
		// this through as a per-request failure and kept the connection in the pool.
		//
		// The second case is the boundary: a short read out of a buffer that has already
		// arrived yields io.ErrUnexpectedEOF, which is not a net.Error either, and must
		// stay per-request. The stream is not desynced — there is no socket behind it — so
		// the connection survives and only the one request fails.
		t.Run("FailedDeadlineArmIsFatal", func(t *testing.T) {
			t.Parallel()

			header := []byte{
				implProtocol.ProtoVersion4 | implProtocol.ProtoDirectionMask, 0x00, 0x00, 0x01, byte(frm.OpResult),
				0x00, 0x00, 0x00, 0x08, // body length 8
			}
			armErr := errors.New("set read deadline failed")

			for _, tc := range []struct {
				name      string
				source    func() (transport.InternalFrameSource, func())
				wantFatal bool
			}{
				{
					name: "failed deadline arm",
					source: func() (transport.InternalFrameSource, func()) {
						// The mock serves the header, then fails the next arm, which is the
						// body's: the header is read in full first (checked below), and the
						// body is not buffered, so reading it must arm a deadline.
						mock := &armFailingConn{data: header, err: armErr}
						cr := &transport.InternalConnReader{InternalConn: mock, InternalR: bufio.NewReader(mock)}
						cr.SetTimeout(5 * time.Second)
						return transport.InternalFrameSource{InternalR: cr}, func() {
							require.Equal(t, len(header), mock.off,
								"the whole header must have been read before the body arm failed")
						}
					},
					wantFatal: true,
				},
				{
					name: "short read from an arrived buffer",
					source: func() (transport.InternalFrameSource, func()) {
						// A header promising 8 body bytes over a buffer holding 3.
						buf := append(append([]byte(nil), header...), 0x01, 0x02, 0x03)
						return transport.InternalFrameSource{InternalR: bytes.NewReader(buf)}, func() {}
					},
					wantFatal: false,
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					t.Parallel()

					src, checkSource := tc.source()

					// Buffered so the delivery does not need a second goroutine.
					call := &transport.InternalCallReq{InternalStreamID: 1, InternalResp: make(chan transport.InternalCallResp, 1)}
					c := &transport.Conn{
						InternalCalls:   callTableOf(1, call),
						InternalVersion: implProtocol.ProtoVersion4,
						InternalStreams: streams.New(),
						InternalLogger:  logging.Nop(),
					}

					err := c.InternalProcessFrameSource(context.Background(), src)
					checkSource()

					// The caller is always woken, fatal or not: head.Stream has already been
					// removed from c.calls, so closeWithError can no longer find the call.
					var delivered error
					select {
					case resp := <-call.InternalResp:
						delivered = resp.InternalErr
					default:
						t.Fatal("the call was never woken: it would wait out its full request timeout")
					}
					require.Error(t, delivered)

					if tc.wantFatal {
						require.ErrorIs(t, err, transport.InternalErrArmReadDeadline,
							"an unread body on a live connection must close it")
						require.ErrorIs(t, err, armErr, "the underlying failure must stay inspectable")
						require.ErrorIs(t, delivered, err, "the waiting caller must be handed the same failure")
						return
					}

					require.NoError(t, err,
						"a short read out of an arrived buffer leaves no unread bytes on a socket, so the connection survives")
					require.ErrorIs(t, delivered, io.ErrUnexpectedEOF)
				})
			}
		})

		// TestConn/ProcessFrame/HandsOffOwnershipOnClose pins the handoff
		// callResp.removedFromCalls exists for, in the window that used to get it wrong: the
		// call has already been removed from c.calls, so closeWithError's drain can never
		// reach it, and only exec can release its stream and recycle the callReq. Keying
		// that on c.closed -- as the connection closes underneath the delivery -- makes exec
		// stand down for a drain that will never come, and the stream stays allocated for
		// the rest of the connection's life.
		t.Run("HandsOffOwnershipOnClose", func(t *testing.T) {
			t.Parallel()

			const testTimeout = 10 * time.Second

			c, server := newTestExecConn(t, testContextWriter{})
			c.InternalVersion = implProtocol.ProtoVersion4
			defer server.Close()

			errCh := make(chan error, 1)
			go func() {
				_, err := c.InternalExec(context.Background(), implProtocol.FrameWriterFunc(func(f *implProtocol.Framer, streamID int) error {
					f.Buf = append(f.Buf[:0], 'x')
					return nil
				}), nil, 0)
				errCh <- err
			}()

			call := waitForSingleCall(t, c)
			header := []byte{
				implProtocol.ProtoVersion4 | implProtocol.ProtoDirectionMask, 0x00,
				byte(call.InternalStreamID >> 8), byte(call.InternalStreamID),
				byte(frm.OpResult),
				0x00, 0x00, 0x00, 0x08, // a body that never arrives
			}

			c.InternalProcessFrameSource(context.Background(), transport.InternalFrameSource{InternalR: &closeOnBodyRead{c: c, header: header}})

			select {
			case err := <-errCh:
				require.ErrorIs(t, err, io.ErrUnexpectedEOF)
			case <-time.After(testTimeout):
				t.Fatal("exec was never woken: it would wait out its full request timeout")
			}

			if inUse := c.InternalStreams.InUse(); inUse != 0 {
				t.Fatalf("exec left the stream to a drain that can never run, %d still in use", inUse)
			}
		})

		t.Run("Stream0Unsolicited", func(t *testing.T) {
			// Native stream 0 is valid; unsolicited responses follow the ordinary
			// discard path rather than being misclassified as a reserved stream.

			var buf bytes.Buffer
			f := implProtocol.NewFramer(nil, implProtocol.ProtoVersion4)
			f.WriteHeader(0, frm.OpResult, 0)
			f.WriteInt(frm.ResultKindVoid)
			f.Buf[0] |= 0x80
			if err := f.Finish(); err != nil {
				t.Fatal(err)
			}
			if err := f.WriteFrameTo(&buf); err != nil {
				t.Fatal(err)
			}

			conn := &transport.Conn{
				InternalR: &transport.InternalConnReader{
					InternalR: bufio.NewReader(&buf),
				},
				InternalStreams: streams.New(),
				InternalLogger:  slog.Default(),
				InternalCfg:     &transport.ConnConfig{},
			}

			err := conn.InternalRecv(context.Background(), false)
			if err != nil {
				t.Fatalf("valid nonnegative stream rejected: %v", err)
			}
		})

		t.Run("StreamIDMismatchIsAnError", func(t *testing.T) {
			t.Parallel()

			header := []byte{
				implProtocol.ProtoVersion4 | implProtocol.ProtoDirectionMask, 0x00, 0x00, 0x01, byte(frm.OpResult),
				0x00, 0x00, 0x00, 0x00, // no body
			}

			// Keyed under stream 1, but the call believes it is stream 2. resp is buffered so
			// the delivery does not need a second goroutine.
			call := &transport.InternalCallReq{InternalStreamID: 2, InternalResp: make(chan transport.InternalCallResp, 1)}
			c := &transport.Conn{
				InternalCalls:   callTableOf(1, call),
				InternalVersion: implProtocol.ProtoVersion4,
				InternalStreams: streams.New(),
				InternalLogger:  logging.Nop(),
			}

			err := c.InternalProcessFrameSource(context.Background(), transport.InternalFrameSource{InternalR: bytes.NewReader(header)})
			require.Error(t, err)
			require.Contains(t, err.Error(), "stream 1")
			require.Contains(t, err.Error(), "stream 2")

			// Nothing was delivered and nothing was removed. Waking the call would mean
			// trusting call.streamID, which is the half of the disagreement that may name a
			// stream some other call owns; closeWithError drains the map instead.
			select {
			case resp := <-call.InternalResp:
				t.Fatalf("the mismatched call was woken with %v, leaving closeWithError nothing to drain", resp.InternalErr)
			default:
			}

			c.InternalMu.Lock()
			remaining, entry := c.InternalCalls.Len(), c.InternalCalls.Get(1)
			c.InternalMu.Unlock()
			require.Equal(t, 1, remaining, "the disagreeing entry must be left for closeWithError")
			require.Same(t, call, entry, "the disagreeing entry must be left for closeWithError")
		})
	})

	t.Run("Read", func(t *testing.T) {
		// TestConn/Read/DelegatesToConnReader verifies that the exported (*driverConn).Read is
		// wired to the connection's connReader, so *driverConn keeps satisfying io.Reader for
		// external callers after the read-path refactor.
		t.Run("DelegatesToConnReader", func(t *testing.T) {
			t.Parallel()

			payload := []byte{0xDE, 0xAD, 0xBE, 0xEF}
			c := &transport.Conn{InternalR: &transport.InternalConnReader{InternalR: bufio.NewReader(bytes.NewReader(payload))}}

			buf := make([]byte, len(payload))
			n, err := c.Read(buf)
			require.NoError(t, err)
			require.Equal(t, len(payload), n)
			require.Equal(t, payload, buf)
		})
	})

	t.Run("RecvSegment", func(t *testing.T) {
		// TestConn/RecvSegment/DisarmsDeadlineOnIdle verifies that recvSegment
		// disarms the read deadline (SetReadDeadline(time.Time{})) on the underlying
		// connection while reading the first segment header, and re-arms it afterwards.
		//
		// The disarm is driven by the connReader.disarm flag rather than by zeroing the
		// operational timeout: the configured timeout value is left intact throughout,
		// so a concurrent finalizeConnection switching ConnectTimeout -> ReadTimeout is
		// never clobbered.
		//
		// This exercises the fix for the idle proto-v5 connection timeout regression:
		// without the disarm, connReader.Read re-arms the deadline on every call, so
		// a short ReadTimeout fires on a healthy idle connection and serve() drops it.
		t.Run("DisarmsDeadlineOnIdle", func(t *testing.T) {
			t.Parallel()

			const configuredTimeout = 5 * time.Millisecond

			mock := &deadlineRecordingConn{
				readEntered: make(chan struct{}),
				proceed:     make(chan struct{}),
			}

			cr := &transport.InternalConnReader{
				InternalConn: mock,
				InternalR:    bufio.NewReader(mock),
			}
			cr.SetTimeout(configuredTimeout)

			w := &transport.InternalDeadlineContextWriter{
				InternalW:         mock,
				InternalSemaphore: make(chan struct{}, 1),
				InternalQuit:      make(chan struct{}),
			}

			c := &transport.Conn{
				InternalR:       cr,
				InternalCalls:   newCallTable(),
				InternalVersion: implProtocol.ProtoVersion5,
				InternalStreams: streams.New(),
				InternalW:       w,
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			errCh := make(chan error, 1)
			go func() { errCh <- c.InternalRecvSegment(ctx) }()

			// Wait until connReader.Read has been entered — at this point the disarm
			// flag is set and SetReadDeadline(zero) has already been called, but the
			// re-arm has not yet happened.
			select {
			case <-mock.readEntered:
			case <-ctx.Done():
				t.Fatal("timed out waiting for recvSegment to enter Read")
			}

			// --- assertions valid while recvSegment is blocked inside Read ---

			// SetReadDeadline must have been called with the zero time to disarm the
			// deadline. connReader.Read clears the deadline whenever the disarm flag is
			// set, so every value seen before the segment read must be the zero time.
			mock.mu.Lock()
			deadlines := append([]time.Time(nil), mock.deadlines...)
			mock.mu.Unlock()

			require.NotEmpty(t, deadlines, "expected at least one SetReadDeadline call before the segment read")
			for i, d := range deadlines {
				require.True(t, d.IsZero(),
					"SetReadDeadline call %d should pass the zero time to disarm the deadline; got %v", i, d)
			}

			// The disarm flag must be set so connReader.Read does not re-arm the
			// deadline while blocking for segment data...
			require.True(t, cr.InternalDisarm.Load(),
				"connReader disarm flag should be set while recvSegment blocks on the initial segment read")
			// ...while the operational timeout value is left intact (not zeroed), so a
			// concurrent finalizeConnection cannot be clobbered by a restore.
			require.Equal(t, configuredTimeout, cr.GetTimeout(),
				"connReader timeout should be preserved (not zeroed) while recvSegment blocks on the initial segment read")

			// Unblock Read — mock returns (0, io.EOF), which terminates recvSegment.
			close(mock.proceed)

			select {
			case err := <-errCh:
				// io.EOF is the expected outcome: we fed no bytes so recvSegment has
				// nothing to parse and propagates the EOF from the mock.
				// The critical invariant is that the error is NOT ErrReadHeaderTimeout:
				// io.EOF is a clean close, not a timeout, so serve() must treat it as fatal.
				require.ErrorIs(t, err, io.EOF,
					"recvSegment should propagate io.EOF from a closed read, not misclassify it")
				require.False(t, errors.Is(err, implProtocol.ErrReadHeaderTimeout),
					"io.EOF must not be wrapped as ErrReadHeaderTimeout")
			case <-ctx.Done():
				t.Fatal("timed out waiting for recvSegment to return after Read was unblocked")
			}

			// --- assertions valid after recvSegment has returned ---

			// The disarm flag must be cleared so subsequent body reads are bounded by
			// the operational timeout again.
			require.False(t, cr.InternalDisarm.Load(),
				"connReader disarm flag should be cleared after recvSegment returns")
			// The configured timeout is unchanged throughout, regardless of the error path.
			require.Equal(t, configuredTimeout, cr.GetTimeout(),
				"connReader timeout should remain its configured value after recvSegment returns")
		})

		// TestConn/RecvSegment/PayloadReadIsBounded verifies that after the (idle-disarmed)
		// segment header is read, the read deadline is re-armed for the payload read,
		// so a peer that sends a segment header and then stalls does not hang the
		// receive loop indefinitely.
		t.Run("PayloadReadIsBounded", func(t *testing.T) {
			t.Parallel()

			server, client, err := tcpConnPair()
			require.NoError(t, err)
			defer server.Close()
			defer client.Close()

			cr := &transport.InternalConnReader{InternalConn: server, InternalR: bufio.NewReader(server)}
			// Use a generous timeout/watchdog margin (rather than something tighter
			// like 100ms/2s) so this doesn't flake under -race on a loaded CI runner,
			// where scheduling jitter alone can exceed a couple hundred milliseconds.
			const readTimeout = 500 * time.Millisecond
			cr.SetTimeout(readTimeout)

			c := &transport.Conn{
				InternalR:       cr,
				InternalCalls:   newCallTable(),
				InternalVersion: implProtocol.ProtoVersion5,
				InternalStreams: streams.New(),
			}

			// Build a valid self-contained segment but send ONLY its 6-byte header: the
			// header advertises a payload the peer never delivers (a mid-transfer stall).
			payload := make([]byte, 128)
			seg, err := newUncompressedSegment(payload, true)
			require.NoError(t, err)
			const uncompressedSegmentHeaderSize = 6

			go func() {
				_, _ = client.Write(seg[:uncompressedSegmentHeaderSize])
			}()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			errCh := make(chan error, 1)
			go func() { errCh <- c.InternalRecvSegment(ctx) }()

			select {
			case err := <-errCh:
				require.Error(t, err, "recvSegment should fail when the advertised payload never arrives")
				require.False(t, errors.Is(err, implProtocol.ErrReadHeaderTimeout),
					"a mid-transfer payload stall must not be normalised to the benign idle header timeout")
			case <-time.After(5 * time.Second):
				t.Fatal("recvSegment hung on the payload read; the read deadline was not re-armed after the segment header")
			}
		})

		t.Run("ProcessesAllFrames", func(t *testing.T) {
			server, client, err := tcpConnPair()
			require.NoError(t, err)
			defer server.Close()
			defer client.Close()

			w := &transport.InternalDeadlineContextWriter{
				InternalW:         server,
				InternalSemaphore: make(chan struct{}, 1),
				InternalQuit:      make(chan struct{}),
			}
			w.InternalTimeout.Store(int64(time.Second * 10))

			c := &transport.Conn{
				InternalR: &transport.InternalConnReader{
					InternalConn: server,
					InternalR:    bufio.NewReader(server),
				},
				InternalCalls:      newCallTable(),
				InternalVersion:    implProtocol.ProtoVersion5,
				InternalAddr:       server.RemoteAddr().String(),
				InternalStreams:    streams.New(),
				InternalIsSchemaV2: true,
				InternalLogger:     logging.Nop(),
				InternalW:          w,
			}
			c.InternalWriteTimeout.Store(int64(time.Second * 10))

			call1 := &transport.InternalCallReq{
				InternalStreamID: 1,
				InternalResp:     make(chan transport.InternalCallResp, 1),
			}

			call2 := &transport.InternalCallReq{
				InternalStreamID: 2,
				InternalResp:     make(chan transport.InternalCallResp, 1),
			}

			c.InternalCalls.Set(1, call1)
			c.InternalCalls.Set(2, call2)

			req := fixtureQueryFrame{
				statement: "SELECT * FROM system.local",
				params: fixtureQueryParams{
					consistency: cqltype.Quorum,
					keyspace:    "gocql_test",
				},
			}

			framer1 := implProtocol.NewFramer(nil, implProtocol.ProtoVersion5)
			err = req.BuildFrame(framer1, 1)
			require.NoError(t, err)

			framer2 := implProtocol.NewFramer(nil, implProtocol.ProtoVersion5)
			err = req.BuildFrame(framer2, 2)
			require.NoError(t, err)

			// Write from the test goroutine: this is a real loopback socket, and the two
			// frames plus the segment header and CRC are ~124 bytes, so the write completes
			// into the socket buffer without a reader.
			var buf []byte
			buf = append(buf, framer1.Buf...)
			buf = append(buf, framer2.Buf...)

			uncompressedSegment, err := newUncompressedSegment(buf, true)
			require.NoError(t, err)

			_, err = client.Write(uncompressedSegment)
			require.NoError(t, err)

			ctx, cancel := context.WithTimeout(context.Background(), segmentTestTimeout)
			defer cancel()

			errCh := make(chan error, 1)
			go func() {
				errCh <- c.InternalRecvSegment(ctx)
			}()

			// Receive in segment order: processAllFramesInSegment consumes the frames
			// sequentially and each send is on an unbuffered channel, so frame 1 is fully
			// delivered before frame 2 starts.
			//
			// The header is skipped because resp.framer has already parsed it; framer.buf
			// holds the body only.
			resp1 := recvCallResp(t, call1)
			require.Equal(t, framer1.Buf[9:], resp1.InternalFramer.Buf)

			resp2 := recvCallResp(t, call2)
			require.Equal(t, framer2.Buf[9:], resp2.InternalFramer.Buf)

			select {
			case err := <-errCh:
				require.NoError(t, err)
			case <-time.After(segmentTestTimeout):
				t.Fatal("recvSegment did not return after delivering both frames")
			}
		})

		// TestConn/RecvSegment/ReassemblesSplitHeader verifies that recvSegment
		// correctly reassembles a single CQL frame that is split across multiple
		// non-self-contained segments even when the 9-byte CQL frame header itself
		// spans two segments. The first segment intentionally carries only 4 payload
		// bytes, so the header must be accumulated across both segments before it can
		// be parsed.
		t.Run("ReassemblesSplitHeader", func(t *testing.T) {
			server, client, err := tcpConnPair()
			require.NoError(t, err)
			defer server.Close()
			defer client.Close()

			w := &transport.InternalDeadlineContextWriter{
				InternalW:         server,
				InternalSemaphore: make(chan struct{}, 1),
				InternalQuit:      make(chan struct{}),
			}
			w.InternalTimeout.Store(int64(time.Second * 10))

			c := &transport.Conn{
				InternalR: &transport.InternalConnReader{
					InternalConn: server,
					InternalR:    bufio.NewReader(server),
				},
				InternalCalls:      newCallTable(),
				InternalVersion:    implProtocol.ProtoVersion5,
				InternalAddr:       server.RemoteAddr().String(),
				InternalStreams:    streams.New(),
				InternalIsSchemaV2: true,
				InternalW:          w,
			}
			c.InternalWriteTimeout.Store(int64(time.Second * 10))

			call1 := &transport.InternalCallReq{InternalStreamID: 1, InternalResp: make(chan transport.InternalCallResp, 1)}
			c.InternalCalls.Set(1, call1)

			req := fixtureQueryFrame{
				statement: "SELECT * FROM system.local",
				params:    fixtureQueryParams{consistency: cqltype.Quorum, keyspace: "gocql_test"},
			}
			framer := implProtocol.NewFramer(nil, implProtocol.ProtoVersion5)
			require.NoError(t, req.BuildFrame(framer, 1))
			full := append([]byte(nil), framer.Buf...)

			// Split the frame into two non-self-contained segments. The first carries
			// only 4 bytes, fewer than the 9-byte CQL frame header, so the header is
			// split across both segments.
			const splitAt = 4
			seg1, err := newUncompressedSegment(full[:splitAt], false)
			require.NoError(t, err)
			seg2, err := newUncompressedSegment(full[splitAt:], false)
			require.NoError(t, err)

			// The helper goroutines only ferry results out; every assertion runs on the
			// test goroutine. A require failing inside a helper exits that goroutine via
			// runtime.Goexit, which here would starve recvSegment of its response
			// delivery and park the test until the suite timeout — and an assertion in
			// the response goroutine is not synchronised with test completion, so it
			// could be skipped outright when the test finishes first.
			writeErr := make(chan error, 1)
			go func() {
				buf := append(append([]byte(nil), seg1...), seg2...)
				_, err := client.Write(buf)
				writeErr <- err
			}()

			// A generous margin rather than something tight, so this does not flake
			// under -race on a loaded CI runner (see TestConn/RecvSegment/PayloadReadIsBounded),
			// while still failing this test rather than the whole suite on a hang.
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			errCh := make(chan error, 1)
			go func() { errCh <- c.InternalRecvSegment(ctx) }()

			respBuf := make(chan []byte, 1)
			go func() {
				resp := <-call1.InternalResp
				// resp.framer.buf holds the frame body (the header is already parsed).
				respBuf <- resp.InternalFramer.Buf
			}()

			select {
			case <-ctx.Done():
				t.Fatal("timed out waiting for reassembled frame")
			case err := <-errCh:
				require.NoError(t, err)
			}

			// Delivery to call1.resp happens-before recvSegment returns, so neither
			// receive can block for long once errCh has fired.
			require.NoError(t, <-writeErr, "writing the segmented stream")
			require.Equal(t, full[9:], <-respBuf)
		})

		// TestConn/RecvSegment/ReservedStreamFrame verifies that a reserved-stream
		// frame (streamID -1) packed inside a self-contained v5 segment has its body
		// consumed from the segment payload rather than the socket. A normal response
		// frame for a real call follows it in the same segment; if the reserved frame's
		// body were read from the socket (c.r) instead of the segment reader, the
		// segment reader would be left misaligned and the trailing frame would never be
		// delivered to its call (the test would time out).
		t.Run("ReservedStreamFrame", func(t *testing.T) {
			server, client, err := tcpConnPair()
			require.NoError(t, err)
			defer server.Close()
			defer client.Close()

			w := &transport.InternalDeadlineContextWriter{
				InternalW:         server,
				InternalSemaphore: make(chan struct{}, 1),
				InternalQuit:      make(chan struct{}),
			}
			w.InternalTimeout.Store(int64(time.Second * 10))

			c := &transport.Conn{
				InternalR: &transport.InternalConnReader{
					InternalConn: server,
					InternalR:    bufio.NewReader(server),
				},
				InternalCalls:      newCallTable(),
				InternalVersion:    implProtocol.ProtoVersion5,
				InternalAddr:       server.RemoteAddr().String(),
				InternalStreams:    streams.New(),
				InternalIsSchemaV2: true,
				InternalLogger:     logging.Nop(),
				InternalW:          w,
			}
			c.InternalWriteTimeout.Store(int64(time.Second * 10))

			// Only stream 2 has a registered call; the reserved stream -1 frame ahead of
			// it goes through the readFrameIntoFramer body-read path.
			call2 := &transport.InternalCallReq{InternalStreamID: 2, InternalResp: make(chan transport.InternalCallResp, 1)}
			c.InternalCalls.Set(2, call2)

			req := fixtureQueryFrame{
				statement: "SELECT * FROM system.local",
				params:    fixtureQueryParams{consistency: cqltype.Quorum, keyspace: "gocql_test"},
			}

			// A request-direction frame parses as an error on the response path; for
			// stream -1 that error is tolerated (logged) — all we need here is for its
			// body to be consumed from the segment reader.
			reservedFramer := implProtocol.NewFramer(nil, implProtocol.ProtoVersion5)
			require.NoError(t, req.BuildFrame(reservedFramer, -1))
			framer2 := implProtocol.NewFramer(nil, implProtocol.ProtoVersion5)
			require.NoError(t, req.BuildFrame(framer2, 2))

			// Written from the test goroutine; see TestConn/RecvSegment/ProcessesAllFrames.
			var buf []byte
			buf = append(buf, reservedFramer.Buf...)
			buf = append(buf, framer2.Buf...)

			seg, err := newUncompressedSegment(buf, true)
			require.NoError(t, err)
			_, err = client.Write(seg)
			require.NoError(t, err)

			ctx, cancel := context.WithTimeout(context.Background(), segmentTestTimeout)
			defer cancel()

			errCh := make(chan error, 1)
			go func() { errCh <- c.InternalRecvSegment(ctx) }()

			// This is the assertion the test exists for: if the reserved frame's body had
			// been read from the socket instead of the segment payload, the segment reader
			// would be misaligned and this response would never arrive (or arrive corrupt).
			resp2 := recvCallResp(t, call2)
			require.Equal(t, framer2.Buf[9:], resp2.InternalFramer.Buf)

			select {
			case err := <-errCh:
				require.NoError(t, err)
			case <-time.After(segmentTestTimeout):
				t.Fatal("recvSegment did not return; reserved-stream frame body was likely read from the socket, not the segment")
			}
		})
	})

	t.Run("ReleaseFramer", func(t *testing.T) {
		t.Parallel()

		t.Run("EWMAEquilibrium", func(t *testing.T) {
			c := newTestConnWithFramerPool()

			// Release framers with bufCap == avg. EWMA should not drift.
			for i := 0; i < 20; i++ {
				f := c.InternalGetReadFramer()
				// readBuffer is defaultBufSize (128), avg starts at defaultBufSize
				c.InternalReleaseReadFramer(f)
			}

			avg := c.InternalFramers.InternalReadPool.InternalBufAvgSize.Load()
			if avg != implProtocol.DefaultBufSize {
				t.Errorf("EWMA should stay at defaultBufSize=%d when all buffers equal, got %d", implProtocol.DefaultBufSize, avg)
			}
		})

		t.Run("DelegatesFramerReleaseToConn", func(t *testing.T) {
			c := newTestConnWithFramerPool()

			f := c.InternalGetReadFramer()
			f.ReadBuffer = make([]byte, 4096)

			f.Release()

			avgAfterFirstRelease := c.InternalFramers.InternalReadPool.InternalBufAvgSize.Load()
			if avgAfterFirstRelease <= implProtocol.DefaultBufSize {
				t.Fatalf("framer.Release() should route through driverConn.releaseFramer and update EWMA, got %d", avgAfterFirstRelease)
			}

			f.Release()

			if avgAfterSecondRelease := c.InternalFramers.InternalReadPool.InternalBufAvgSize.Load(); avgAfterSecondRelease != avgAfterFirstRelease {
				t.Fatalf("second framer.Release() should be a no-op: first avg=%d second avg=%d", avgAfterFirstRelease, avgAfterSecondRelease)
			}
		})

		t.Run("EWMAConvergesUpward", func(t *testing.T) {
			c := newTestConnWithFramerPool()

			// Release framers with a larger buffer; EWMA should converge toward it.
			const targetSize = 4096
			for i := 0; i < 100; i++ {
				f := c.InternalGetReadFramer()
				f.ReadBuffer = make([]byte, targetSize)
				c.InternalReleaseReadFramer(f)
			}

			avg := c.InternalFramers.InternalReadPool.InternalBufAvgSize.Load()
			// After 100 iterations with weight=8, avg should be very close to targetSize.
			// Allow 1% tolerance.
			if avg < targetSize*99/100 || avg > targetSize*101/100 {
				t.Errorf("EWMA should converge to ~%d, got %d", targetSize, avg)
			}
		})

		t.Run("EWMAConvergesDownward", func(t *testing.T) {
			c := newTestConnWithFramerPool()

			// First, push EWMA up.
			for i := 0; i < 100; i++ {
				f := c.InternalGetReadFramer()
				f.ReadBuffer = make([]byte, 4096)
				c.InternalReleaseReadFramer(f)
			}

			// Now release framers with small buffers; EWMA should converge back down.
			// Due to upward bias (+4 rounding), convergence downward is slower.
			const smallSize = 256
			for i := 0; i < 200; i++ {
				f := c.InternalGetReadFramer()
				f.ReadBuffer = make([]byte, smallSize)
				c.InternalReleaseReadFramer(f)
			}

			avg := c.InternalFramers.InternalReadPool.InternalBufAvgSize.Load()
			// Due to the upward-biased rounding (+framerBufEWMAWeight/2), the EWMA settles
			// slightly above the actual sample value when converging downward. The steady-state
			// offset is at most framerBufEWMAWeight/2 (i.e., 4) per step which compounds to
			// roughly framerBufEWMAWeight/2 above the target. Allow generous tolerance.
			if avg < smallSize || avg > smallSize+2*transport.InternalFramerBufEWMAWeight {
				t.Errorf("EWMA should converge toward ~%d (with upward bias), got %d", smallSize, avg)
			}
		})

		t.Run("ShrinkOversizedBuffer", func(t *testing.T) {
			c := newTestConnWithFramerPool()

			// EWMA starts at defaultBufSize (128). Release a very large framer.
			f := c.InternalGetReadFramer()
			f.ReadBuffer = make([]byte, 100000)
			origBuf := f.ReadBuffer
			c.InternalReleaseReadFramer(f)

			// Get the framer back from the pool and check that its buffer was shrunk.
			f2 := c.InternalGetReadFramer()
			if cap(f2.ReadBuffer) >= cap(origBuf) {
				t.Errorf("oversized buffer should have been shrunk: original cap=%d, new cap=%d",
					cap(origBuf), cap(f2.ReadBuffer))
			}
			// Shrink target should be at least defaultBufSize.
			if cap(f2.ReadBuffer) < implProtocol.DefaultBufSize {
				t.Errorf("shrunk buffer should be at least defaultBufSize=%d, got cap=%d",
					implProtocol.DefaultBufSize, cap(f2.ReadBuffer))
			}
			c.InternalReleaseReadFramer(f2)
		})

		t.Run("NoShrinkNormalBuffer", func(t *testing.T) {
			c := newTestConnWithFramerPool()

			// Release a few framers with identical buffers; none should be shrunk.
			for i := 0; i < 10; i++ {
				f := c.InternalGetReadFramer()
				origCap := cap(f.ReadBuffer)
				c.InternalReleaseReadFramer(f)
				f2 := c.InternalGetReadFramer()
				if cap(f2.ReadBuffer) != origCap {
					t.Errorf("iteration %d: normal-sized buffer should not be shrunk: orig cap=%d, new cap=%d",
						i, origCap, cap(f2.ReadBuffer))
				}
				c.InternalReleaseReadFramer(f2)
			}
		})

		t.Run("ShrinkFloorIsDefaultBufSize", func(t *testing.T) {
			c := newTestConnWithFramerPool()

			// Push EWMA down to a very small value by releasing tiny buffers.
			// The shrink target should never go below defaultBufSize.
			for i := 0; i < 100; i++ {
				f := c.InternalGetReadFramer()
				f.ReadBuffer = make([]byte, 1) // Tiny buffer
				c.InternalReleaseReadFramer(f)
			}

			// Now release a moderately large buffer that triggers shrink.
			f := c.InternalGetReadFramer()
			f.ReadBuffer = make([]byte, 10000)
			c.InternalReleaseReadFramer(f)

			f2 := c.InternalGetReadFramer()
			if cap(f2.ReadBuffer) < implProtocol.DefaultBufSize {
				t.Errorf("shrink target should respect defaultBufSize floor: got cap=%d, want >= %d",
					cap(f2.ReadBuffer), implProtocol.DefaultBufSize)
			}
			c.InternalReleaseReadFramer(f2)
		})

		t.Run("NilFramer", func(t *testing.T) {
			c := newTestConnWithFramerPool()
			// Should not panic.
			c.InternalReleaseReadFramer(nil)
		})

		t.Run("NoPool", func(t *testing.T) {
			c := &transport.Conn{} // No pool initialized.
			f := implProtocol.NewFramer(nil, implProtocol.ProtoVersion4)
			// Should not panic, framer is just dropped.
			c.InternalReleaseReadFramer(f)
		})

		t.Run("ReadAndWritePoolsAreSeparate", func(t *testing.T) {
			c := newTestConnWithFramerPool()

			readFramer := c.InternalGetReadFramer()
			readFramer.ReadBuffer = make([]byte, 100000)
			c.InternalReleaseReadFramer(readFramer)

			writeFramer := c.InternalGetWriteFramer()
			writeFramer.Buf = make([]byte, 0, 8192)
			c.InternalReleaseWriteFramer(writeFramer)

			if writeAvg := c.InternalFramers.InternalWritePool.InternalBufAvgSize.Load(); writeAvg <= implProtocol.DefaultBufSize {
				t.Fatalf("writer pool should track its own EWMA, got %d", writeAvg)
			}

			writeFramer = c.InternalGetWriteFramer()
			if cap(writeFramer.Buf) >= cap(readFramer.ReadBuffer) {
				t.Fatalf("writer framer should not inherit oversized reader buffer state, got writer cap=%d reader cap=%d", cap(writeFramer.Buf), cap(readFramer.ReadBuffer))
			}
			c.InternalReleaseWriteFramer(writeFramer)
		})

		t.Run("WriteFramerResetsCustomPayloadFlagBetweenUses", func(t *testing.T) {
			c := newTestConnWithFramerPool()
			const streamID = 7

			f := c.InternalGetWriteFramer()
			payloadReq := &fixtureQueryFrame{
				statement: "SELECT now() FROM system.local",
				customPayload: map[string][]byte{
					"k": []byte("v"),
				},
			}
			_, payloadHeader := buildTestFrame(t, f, payloadReq, streamID)
			if payloadHeader.Flags&frm.FlagCustomPayload == 0 {
				t.Fatalf("custom payload frame should set %v, got flags=%08b", frm.FlagCustomPayload, payloadHeader.Flags)
			}

			c.InternalReleaseWriteFramer(f)
			if got, want := f.Flags, c.InternalFramers.InternalDefaults.InternalFlags; got != want {
				t.Fatalf("releaseWriteFramer should restore default flags: got %08b want %08b", got, want)
			}

			f = c.InternalGetWriteFramer()
			plainReq := &fixtureQueryFrame{statement: "SELECT now() FROM system.local"}
			plainBuf, plainHeader := buildTestFrame(t, f, plainReq, streamID)
			if plainHeader.Flags != c.InternalFramers.InternalDefaults.InternalFlags {
				t.Fatalf("plain query should use default flags after pooled reuse: got %08b want %08b", plainHeader.Flags, c.InternalFramers.InternalDefaults.InternalFlags)
			}

			fresh := implProtocol.NewFramer(nil, implProtocol.ProtoVersion4)
			freshBuf, freshHeader := buildTestFrame(t, fresh, plainReq, streamID)
			if plainHeader.Flags != freshHeader.Flags {
				t.Fatalf("reused plain query flags do not match fresh framer: got %08b want %08b", plainHeader.Flags, freshHeader.Flags)
			}
			if !bytes.Equal(plainBuf, freshBuf) {
				t.Fatal("reused plain query frame does not match fresh framer output")
			}

			c.InternalReleaseWriteFramer(f)
		})

		t.Run("WriteFramerResetsTracingFlagBetweenUses", func(t *testing.T) {
			c := newTestConnWithFramerPool()
			const streamID = 9

			f := c.InternalGetWriteFramer()
			f.Trace()
			tracedReq := &fixtureQueryFrame{statement: "SELECT now() FROM system.local"}
			_, tracedHeader := buildTestFrame(t, f, tracedReq, streamID)
			if tracedHeader.Flags&frm.FlagTracing == 0 {
				t.Fatalf("traced query should set %v, got flags=%08b", frm.FlagTracing, tracedHeader.Flags)
			}

			c.InternalReleaseWriteFramer(f)
			if got, want := f.Flags, c.InternalFramers.InternalDefaults.InternalFlags; got != want {
				t.Fatalf("releaseWriteFramer should restore default flags: got %08b want %08b", got, want)
			}

			f = c.InternalGetWriteFramer()
			_, plainHeader := buildTestFrame(t, f, tracedReq, streamID)
			if plainHeader.Flags&frm.FlagTracing != 0 {
				t.Fatalf("plain query should not inherit tracing flag after pooled reuse: got %08b", plainHeader.Flags)
			}

			c.InternalReleaseWriteFramer(f)
		})
	})

	t.Run("WriteCoalescing", func(t *testing.T) {
		t.Run("Flush", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			server, client, err := tcpConnPair()
			if err != nil {
				t.Fatal(err)
			}

			done := make(chan struct{}, 1)
			var (
				buf      bytes.Buffer
				bufMutex sync.Mutex
			)
			go func() {
				defer close(done)
				defer server.Close()
				var err error
				b := make([]byte, 256)
				var n int
				for {
					if n, err = server.Read(b); err != nil {
						break
					}
					bufMutex.Lock()
					buf.Write(b[:n])
					bufMutex.Unlock()
				}
				if err != io.EOF {
					t.Errorf("unexpected read error: %v", err)
				}
			}()
			enqueued := make(chan struct{})
			resetTimer := make(chan struct{})
			w := &transport.InternalWriteCoalescer{
				InternalWriteCh: make(chan transport.InternalWriteRequest),
				InternalC:       client,
				InternalQuit:    ctx.Done(),
				InternalTestEnqueuedHook: func() {
					enqueued <- struct{}{}
				},
				InternalTestFlushedHook: func() {
					client.Close()
				},
			}
			w.InternalSetWriteTimeout(500 * time.Millisecond)
			timerC := make(chan time.Time, 1)
			go func() {
				w.InternalWriteFlusherImpl(timerC, func() { resetTimer <- struct{}{} })
			}()

			go func() {
				if _, err := w.InternalWriteContext(context.Background(), []byte("one")); err != nil {
					t.Error(err)
				}
			}()

			go func() {
				if _, err := w.InternalWriteContext(context.Background(), []byte("two")); err != nil {
					t.Error(err)
				}
			}()

			<-enqueued
			<-resetTimer
			<-enqueued

			// flush
			timerC <- time.Now()

			<-done

			if got := buf.String(); got != "onetwo" && got != "twoone" {
				t.Fatalf("expected to get %q got %q", "onetwo or twoone", got)
			}
		})

		t.Run("WriteAfterClose", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			var buf bytes.Buffer
			defer cancel()
			server, client, err := tcpConnPair()
			if err != nil {
				t.Fatal(err)
			}

			done := make(chan struct{}, 1)
			go func() {
				io.Copy(&buf, server)
				server.Close()
				close(done)
			}()
			w := transport.InternalNewWriteCoalescer(client, 0, 5*time.Millisecond, ctx.Done())

			// ensure 1 write works
			if _, err := w.InternalWriteContext(context.Background(), []byte("one")); err != nil {
				t.Fatal(err)
			}

			client.Close()
			<-done
			if v := buf.String(); v != "one" {
				t.Fatalf("expected buffer to be %q got %q", "one", v)
			}

			// now close and do a write, we should error
			cancel()
			client.Close() // close client conn too, since server won't see the answer anyway.

			if _, err := w.InternalWriteContext(context.Background(), []byte("two")); err == nil {
				t.Fatal("expected to get error for write after closing")
			} else if err != io.EOF {
				t.Fatalf("expected to get EOF got %v", err)
			}
		})
	})
}
