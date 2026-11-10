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

package cluster

import (
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/endpoint"
	implProtocol "github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/transport"

	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	frm "github.com/scylladb/gocql/v2/internal/frame"
)

func testCluster(proto frm.ProtoVersion, addresses ...string) *InternalClusterSettings {
	cluster := InternalNewClusterSettings(addresses...)
	cluster.ProtoVersion = int(proto)
	cluster.Data().InternalDisableControlConn = true
	cluster.PoolConfig.HostSelectionPolicy = RoundRobinHostPolicy()
	return cluster
}

// waitForPoolSize blocks until session's connection pool holds at least want
// connections, or timeout elapses.
//
// open only guarantees the first connection to each host: the rest of
// a pool is filled asynchronously, so a test making a claim about every
// connection of a session has to wait for them rather than assert on whichever
// ones it happened to observe.
func waitForPoolSize(session *Core, want int, timeout time.Duration) error {
	deadline := time.After(timeout)
	for {
		if got := SessionData(session).InternalPool.Size(); got >= want {
			return nil
		}

		select {
		case <-deadline:
			return fmt.Errorf("pool reached %d of %d connections in %s", SessionData(session).InternalPool.Size(), want, timeout)
		case <-time.After(time.Millisecond):
		}
	}
}

func createTestSslCluster(addr string, proto frm.ProtoVersion, useClientCert bool) *InternalClusterSettings {
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

func (srv *TestServer) session() (*Core, error) {
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
			respFrame.WriteInt(implProtocol.ErrCodeUnprepared)
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

func newCallTable() *transport.InternalCallTable { return &transport.InternalCallTable{} }
