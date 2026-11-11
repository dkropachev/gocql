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
 * Copyright (c) 2016, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package cluster

import (
	"context"
	"net"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	logging "github.com/scylladb/gocql/v2/internal/logging"
	"github.com/scylladb/gocql/v2/internal/metadata"
	"github.com/scylladb/gocql/v2/internal/protocol"
	runtimeerrors "github.com/scylladb/gocql/v2/internal/runtimeerrors"
	"github.com/scylladb/gocql/v2/internal/transport"

	crand "crypto/rand"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/scylladb/gocql/v2/internal/events"

	frm "github.com/scylladb/gocql/v2/internal/frame"
)

var (
	randr    *rand.Rand
	mutRandr sync.Mutex
)

func init() {
	b := make([]byte, 4)
	if _, err := crand.Read(b); err != nil {
		panic(fmt.Sprintf("unable to seed random number generator: %v", err))
	}

	randr = rand.New(rand.NewSource(int64(protocol.ReadInt(b))))
}

const (
	controlConnStarting = 0
	controlConnStarted  = 1
	controlConnClosing  = -1
)

type InternalControlConnection interface {
	InternalGetConn() *InternalConnHost
	InternalAwaitSchemaAgreement() error
	InternalConnect(hosts []*implEndpoint.HostInfo) error
	InternalClose()
	InternalGetSession() *Core
	InternalReconnect() error
}

type InternalControlConn struct {
	connections     map[*transport.Conn]struct{}
	InternalConn    atomic.Pointer[InternalConnHost]
	InternalSession *Core
	InternalQuit    chan struct{}
	workers         sync.WaitGroup
	closeOnce       sync.Once
	workerMu        sync.Mutex
	state           int32
	reconnecting    int32
}

func (c *InternalControlConn) InternalGetSession() *Core {
	return c.InternalSession
}

func createControlConn(session *Core) *InternalControlConn {

	control := &InternalControlConn{
		InternalSession: session,
		InternalQuit:    make(chan struct{}),
	}

	return control
}

func (c *InternalControlConn) InternalHeartBeat() {
	if !atomic.CompareAndSwapInt32(&c.state, controlConnStarting, controlConnStarted) {
		return
	}

	sleepTime := 1 * time.Second
	timer := time.NewTimer(sleepTime)
	defer timer.Stop()

	for {
		timer.Reset(sleepTime)

		select {
		case <-c.InternalQuit:
			return
		case <-timer.C:
		}

		resp, err := c.writeFrame(&protocol.WriteOptionsFrame{})
		if err != nil {
			goto reconn
		}

		switch resp.(type) {
		case *frm.SupportedFrame:
			// Everything ok
			sleepTime = 30 * time.Second
			continue
		case error:
			goto reconn
		default:

			// Reachable from the wire: parseFrame builds a frame for every opcode it
			// knows, and this goroutine has no recover above it. There is no failure
			// counter here as there is in Conn.heartBeat -- every non-SUPPORTED
			// outcome already reconnects, so this arm joins them rather than adding
			// a bound only it would respect.
			c.InternalSession.logger().Warn("control connection: unexpected frame in response to OPTIONS; reconnecting", "frame", fmt.Sprintf("%T", resp))
			goto reconn
		}

	reconn:
		// try to connect a bit faster
		sleepTime = 1 * time.Second
		c.InternalReconnect()
		continue
	}
}

func shuffleHosts(hosts []*implEndpoint.HostInfo) []*implEndpoint.HostInfo {
	shuffled := make([]*implEndpoint.HostInfo, len(hosts))
	copy(shuffled, hosts)

	mutRandr.Lock()
	randr.Shuffle(len(hosts), func(i, j int) {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	})
	mutRandr.Unlock()

	return shuffled
}

func hostAddress(host *implEndpoint.HostInfo) string {
	return net.JoinHostPort(host.ConnectAddress().String(), strconv.Itoa(host.Port()))
}

// controlConnConfig returns the connection config used for control connections.
// It is the only place where control-connection specific settings are applied,
// so that every path which (re)establishes the control connection agrees on them.
//
// It is declared here, rather than alongside the rest of Session, so it sits
// next to the paths whose agreement it exists to guarantee.
func sessionControlConnConfig(s *Core) *transport.ConnConfig {
	cfg := *s.privateStorage.InternalConnCfg
	cfg.InternalDisableCoalesce = true
	cfg.InternalIsControlConn = true
	return &cfg
}

func (c *InternalControlConn) InternalConnect(hosts []*implEndpoint.HostInfo) error {
	if len(hosts) == 0 {
		return errors.New("control: no endpoints specified")
	}

	// shuffle endpoints so not all drivers will connect to the same initial
	// node.
	hosts = shuffleHosts(hosts)

	cfg := sessionControlConnConfig(c.InternalSession)

	var conn *transport.Conn
	failure := &runtimeerrors.ConnectError{Stage: "control connection"}
	for _, host := range hosts {
		var err error
		conn, err = sessionDial(c.InternalSession, c.InternalSession.privateStorage.InternalCtx, host, cfg, c)
		// conn.finalizeConnection() to be called outside of this function, since initialization process is not completed yet
		if err != nil {
			c.InternalSession.logger().Warn("control connection: dial failed", logging.KeyHost, hostAddress(host), logging.Err(err))
			failure.Hosts = append(failure.Hosts, runtimeerrors.HostError{Address: hostAddress(host), Err: err})
			continue
		}
		err = c.setupConn(conn)
		if err == nil {
			break
		}
		c.InternalSession.logger().Warn("control connection: setup failed", logging.KeyHost, hostAddress(host), logging.Err(err))
		failure.Hosts = append(failure.Hosts, runtimeerrors.HostError{Address: hostAddress(host), Err: err})
		conn.Close()
		conn = nil
	}
	if conn == nil {
		return failure
	}

	// we could fetch the initial ring here and update initial host data. So that
	// when we return from here we have a ring topology ready to go.

	c.startWorker(c.InternalHeartBeat)

	return nil
}

type InternalConnHost struct {
	InternalConn transport.ConnInterface
	InternalHost *implEndpoint.HostInfo
}

func (c *InternalControlConn) setupConn(conn *transport.Conn) error {
	setupCtx, cancel := conn.InternalOpeningContext(conn.InternalCtx)
	defer cancel()
	stopAndJoin := conn.InternalSetupCancellation(setupCtx)
	defer stopAndJoin()
	// we need up-to-date host info for the filterHost call below
	hosts, err := readV2Topology(setupCtx, conn, transport.InternalQrySystemLocal, c.InternalSession.privateStorage.InternalCfg.Port)
	var host *implEndpoint.HostInfo
	if err == nil {
		if len(hosts) == 0 {
			err = cqltype.TypeValidation("control topology", "query returned 0 rows")
		} else if len(hosts) != 1 || implEndpoint.InternalHostInfoHostUUID(hosts[0]).IsEmpty() {
			err = cqltype.TypeValidation("control topology", "expected one local identity")
		} else {
			host = hosts[0]
		}
	}
	if err != nil {
		return err
	}

	if c.InternalSession.privateStorage.InternalCfg.FilterHost(host) {
		return fmt.Errorf("host was filtered: %v", host.ConnectAddress())
	}

	if err := c.registerEventsContext(setupCtx, conn); err != nil {
		return fmt.Errorf("register events: %w", err)
	}

	if err := conn.InternalValidateOpeningTarget(setupCtx); err != nil {
		return err
	}
	stopAndJoin()
	if err := setupCtx.Err(); err != nil {
		return err
	}
	implEndpoint.InternalHostInfoUpdate(conn.InternalHost, host)
	host = conn.InternalHost
	ch := &InternalConnHost{InternalConn: conn, InternalHost: host}
	c.workerMu.Lock()
	if atomic.LoadInt32(&c.state) == controlConnClosing {
		c.workerMu.Unlock()
		return runtimeerrors.ErrClosed
	}
	if c.connections == nil {
		c.connections = make(map[*transport.Conn]struct{})
	}
	c.connections[conn] = struct{}{}
	old := c.InternalConn.Swap(ch)
	c.workerMu.Unlock()
	var oldHost events.HostInfo
	if old != nil && old.InternalHost != nil {
		oldHost.HostID = old.InternalHost.HostID()
		oldHost.Host = old.InternalHost.ConnectAddress()
		oldHost.Port = old.InternalHost.Port()
	}
	c.InternalSession.PublishV2Metadata()
	sessionPublishEvent(c.InternalSession, &events.ControlConnectionRecreatedEvent{
		OldHost: oldHost,
		NewHost: events.HostInfo{
			HostID: host.HostID(),
			Host:   host.ConnectAddress(),
			Port:   host.Port(),
		},
	})
	if c.InternalSession.initialized() {
		c.InternalSession.Peripheral( // We connected to control conn, so add the connect the host in pool as well.
		// Notify session we can start trying to connect to the node.
		// We can't start the fill before the session is initialized, otherwise the fill would interfere
		// with the fill called by Session.init. Session.init needs to wait for its fill to finish and that
		// would return immediately if we started the fill here.
		// TODO(martin-sucha): Trigger pool refill for all hosts, like in reconnectDownedHosts?
		).InternalStartWorker(func() { c.InternalSession.startPoolFill(host) })
	}
	return nil
}
func (c *InternalControlConn) registerEventsContext(ctx context.Context, conn *transport.Conn) error {
	var events []string

	if !c.InternalSession.privateStorage.InternalCfg.Events.DisableTopologyEvents {
		events = append(events, "TOPOLOGY_CHANGE")
	}
	if !c.InternalSession.privateStorage.InternalCfg.Events.DisableNodeStatusEvents {
		events = append(events, "STATUS_CHANGE")
	}
	if !c.InternalSession.privateStorage.InternalCfg.Events.DisableSchemaEvents {
		events = append(events, "SCHEMA_CHANGE")
	}
	if c.InternalSession.privateStorage.InternalCfg.ClientRoutesConfig != nil && !c.InternalSession.privateStorage.InternalCfg.privateStorage.InternalDisableClientRouteEvents {
		events = append(events, "CLIENT_ROUTES_CHANGE")
	}

	if len(events) == 0 {
		return nil
	}

	framer, err := conn.InternalExec(ctx,
		&protocol.WriteRegisterFrame{
			Events: events,
		}, nil, conn.InternalCfg.ConnectTimeout)
	if err != nil {
		return err
	}
	defer framer.Release()

	frame, err := framer.ParseFrame()
	if err != nil {
		return err
	} else if _, ok := frame.(*frm.ReadyFrame); !ok {
		return fmt.Errorf("unexpected frame in response to register: got %T: %v\n", frame, frame)
	}

	return nil
}

func (c *InternalControlConn) InternalReconnect() error {
	if atomic.LoadInt32(&c.state) == controlConnClosing {
		return fmt.Errorf("control connection is closing")
	}
	if !atomic.CompareAndSwapInt32(&c.reconnecting, 0, 1) {
		return fmt.Errorf("control connection is reconnecting")
	}
	defer atomic.StoreInt32(&c.reconnecting, 0)

	err := c.attemptReconnect()
	if err != nil {
		err = fmt.Errorf("gocql: unable to reconnect control connection: %w", err)
		c.InternalSession.logger().Error("control connection: reconnect failed", logging.Err(err))
		return err
	}

	err = c.InternalSession.RefreshRingNow()
	if err != nil {
		c.InternalSession.logger().Warn("control connection: ring refresh failed", logging.Err(err))
	}

	if c.InternalSession.privateStorage.InternalMetadataDescriber == nil {
		return err
	}
	err = c.InternalSession.privateStorage.InternalMetadataDescriber.refreshAllSchema()
	if err != nil {
		c.InternalSession.logger().Warn("control connection: schema refresh failed", logging.Err(err))
	}
	return nil
}

func (c *InternalControlConn) attemptReconnect() error {
	hosts := c.InternalSession.privateStorage.InternalHostSource.InternalGetHostsList()
	hosts = shuffleHosts(hosts)

	// keep the old behavior of connecting to the old host first by moving it to
	// the front of the slice
	ch := c.InternalGetConn()
	if ch != nil {
		for i := range hosts {
			if hosts[i].Equal(ch.InternalHost) {
				hosts[0], hosts[i] = hosts[i], hosts[0]
				break
			}
		}
		ch.InternalConn.Close()
	}

	err := c.attemptReconnectToAnyOfHosts(hosts)
	if err == nil {
		return nil
	}
	c.InternalSession.logger().Warn("control connection: no ring node reachable; falling back to initial contact points", logging.Err(err))
	// Fallback to initial contact points, as it may be the case that all known initialHosts
	// changed their IPs while keeping the same hostname(s).
	initialHosts, resolvErr := implEndpoint.InternalResolveInitialEndpoints(c.InternalSession.privateStorage.InternalCfg.DNSResolver, c.InternalSession.privateStorage.InternalCfg.Hosts, c.InternalSession.privateStorage.InternalCfg.Port, c.InternalSession.privateStorage.InternalLogger)
	if resolvErr != nil {
		return fmt.Errorf("resolve contact points' hostnames: %w", resolvErr)
	}

	return c.attemptReconnectToAnyOfHosts(initialHosts)
}

func (c *InternalControlConn) attemptReconnectToAnyOfHosts(hosts []*implEndpoint.HostInfo) error {
	cfg := sessionControlConnConfig(c.InternalSession)
	for _, host := range hosts {
		conn, err := sessionDial(c.InternalSession, c.InternalSession.privateStorage.InternalCtx, host, cfg, c)
		if err != nil {
			snapshot, _ := snapshotV2Host(c.InternalSession, host)
			if c.InternalSession.privateStorage.InternalCfg.ConvictionPolicy.AddFailure(err, snapshot) {
				c.InternalSession.HandleNodeDown(host.ConnectAddress(), host.Port())
			}
			c.InternalSession.logger().Warn("control connection: dial failed", logging.KeyHost, hostAddress(host), logging.Err(err))
			continue
		}
		err = c.setupConn(conn)
		if err != nil {
			c.InternalSession.logger().Warn("control connection: setup failed", logging.KeyHost, hostAddress(host), logging.Err(err))
			conn.Close()
			continue
		}
		conn.InternalFinalizeConnection()
		sessionPublishEvent(c.InternalSession, &events.ControlConnectionRecreatedEvent{
			NewHost: events.HostInfo{
				Host:   host.ConnectAddress(),
				Port:   host.Port(),
				HostID: host.HostID(),
			},
		})
		return nil
	}
	return fmt.Errorf("unable to connect to any known node: %v", hosts)
}

func (c *InternalControlConn) HandleError(conn *transport.Conn, err error, closed bool) {
	if !closed {
		return
	}

	oldConn := c.InternalGetConn()

	// If connection has long gone, and not been attempted for awhile,
	// it's possible to have oldConn as nil here (#1297).
	if oldConn != nil && oldConn.InternalConn != conn {
		return
	}

	c.startWorker(func() { _ = c.InternalReconnect() })
}

func (c *InternalControlConn) InternalGetConn() *InternalConnHost {
	return c.InternalConn.Load()
}

// writeFrame sends frame w on the control connection and returns the parsed
// response frame.
//
// NOTE: The returned frame must not retain any byte-slice references to the
// framer's read buffer, because the framer is released back to the pool
// immediately after parseFrame returns (via defer). Frame types that use
// readBytesCopy (e.g. SupportedFrame, AuthChallengeFrame, AuthSuccessFrame)
// are safe; frame types that use readBytes and expose []byte fields would not
// be safe and must not be returned from this function.
func (c *InternalControlConn) writeFrame(w protocol.FrameBuilder) (protocol.Frame, error) {
	ch := c.InternalGetConn()
	if ch == nil {
		return nil, metadata.InternalErrNoControl
	}

	framer, err := ch.InternalConn.InternalExec(metadata.InternalV2SchemaContext(c.InternalSession.NewSchemaSource()), w, nil, c.InternalSession.privateStorage.InternalCfg.MetadataSchemaRequestTimeout)
	if err != nil {
		return nil, err
	}
	defer framer.Release()

	return framer.ParseFrame()
}

func (c *InternalControlConn) InternalAwaitSchemaAgreement() error {
	return c.InternalSession.awaitV2SchemaAgreement(metadata.InternalV2SchemaContext(c.InternalSession.NewSchemaSource()))
}

func (c *InternalControlConn) startWorker(fn func()) bool {
	c.workerMu.Lock()
	defer c.workerMu.Unlock()
	if atomic.LoadInt32(&c.state) == controlConnClosing {
		return false
	}
	c.workers.Add(1)
	go func() { defer c.workers.Done(); fn() }()
	return true
}
func (c *InternalControlConn) InternalClose() {
	c.closeOnce.Do(func() {
		c.workerMu.Lock()
		atomic.StoreInt32(&c.state, controlConnClosing)
		if c.InternalQuit != nil {
			close(c.InternalQuit)
		}
		c.workerMu.Unlock()
	})
	if current := c.InternalGetConn(); current != nil {
		current.InternalConn.Close()
	}
	c.workers.Wait()
	c.workerMu.Lock()
	connections := make([]*transport.Conn, 0, len(c.connections))
	for conn := range c.connections {
		connections = append(connections, conn)
	}
	c.workerMu.Unlock()
	for _, conn := range connections {
		conn.Close()
	}
	for _, conn := range connections {
		conn.InternalWaitEventDispatch()
	}
}
