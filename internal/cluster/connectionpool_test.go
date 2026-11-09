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
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/endpoint"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	implPool "github.com/scylladb/gocql/v2/internal/pool"
)

// errorConn is a mock net.Conn whose Close returns an error,
// triggering HandleError via driverConn.closeWithError.
type errorConn struct {
	net.Conn
}

func (e errorConn) Close() error {
	return errors.New("mock close error")
}

func (e errorConn) SetTimeout(_ time.Duration) {}
func (e errorConn) GetTimeout() time.Duration  { return 0 }

// InternalSetDisarm is required of any reader installed as driverConn.r (connReadSource). This
// mock is never read from, so there is nothing to disarm.
func (e errorConn) InternalSetDisarm(_ bool) {}

// newUndialedHostPool builds a hostConnPool for host without connecting it.
// newHostConnPool is a pure constructor -- it installs a nopConnPicker and
// leaves the pool unfilled -- so this needs no server, and every HostPoolInfo
// accessor the iteration exposes is safe on the result.
func newUndialedHostPool(t *testing.T, session *Core, ip string) *implPool.InternalHostConnPool {
	t.Helper()

	addr := net.ParseIP(ip)
	if addr == nil {
		t.Fatalf("newUndialedHostPool: %q is not an IP", ip)
	}
	host := func() *endpoint.HostInfo {
		opaqueStorageValuexxxx := new(endpoint.HostInfo)
		implEndpoint.HostInfoData(opaqueStorageValuexxxx).InternalHostId = cqltype.MustRandomUUID()
		implEndpoint.HostInfoData(opaqueStorageValuexxxx).InternalConnectAddress = addr
		implEndpoint.HostInfoData(opaqueStorageValuexxxx).InternalBroadcastAddress = addr
		implEndpoint.HostInfoData(opaqueStorageValuexxxx).InternalPort = 9042
		return opaqueStorageValuexxxx
	}()

	return InternalNewHostConnPool(session, host, 1, "")
}

// closeSignalPicker is a driverConnPicker that reports when it has been closed.
//
// Closing a retired pool is the half of policyConnPool.removeHost that dropping
// the map entry does not prove: hostConnPool.Close is what reaches
// connPicker.Close and hangs up the host's connections, and it runs on a new
// goroutine, so there is nothing synchronous to assert on. Signalling from the
// picker makes the wait deterministic rather than a poll, and embedding
// nopConnPicker keeps this to the one method that matters.
type closeSignalPicker struct {
	nopConnPicker

	once   sync.Once
	closed chan struct{}
}

func newCloseSignalPicker() *closeSignalPicker {
	return &closeSignalPicker{closed: make(chan struct{})}
}

// Close is idempotent. hostConnPool.Close already guards against closing twice,
// but a double close here would panic rather than fail, which is a bad way for
// a regression in that guard to surface.
func (p *closeSignalPicker) Close() {
	p.once.Do(func() { close(p.closed) })
}

// newSignallingHostPool builds an undialed pool for host whose Close is
// observable. The connPicker is replaced before the pool is published to
// hostConnPools, so nothing else can be reading it yet.
func newSignallingHostPool(session *Core, host *endpoint.HostInfo) (*implPool.InternalHostConnPool, *closeSignalPicker) {
	pool := InternalNewHostConnPool(session, host, 1, "")
	picker := newCloseSignalPicker()
	pool.InternalConnPicker = picker
	return pool, picker
}

func awaitPoolClosed(t *testing.T, picker *closeSignalPicker, what string) {
	t.Helper()
	select {
	case <-picker.closed:
	case <-time.After(2 * time.Second):
		t.Fatalf("%s: connection pool was never closed", what)
	}
}

func assertPoolNotClosed(t *testing.T, picker *closeSignalPicker, what string) {
	t.Helper()
	select {
	case <-picker.closed:
		t.Errorf("%s: connection pool was closed", what)
	default:
	}
}
