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
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/scylladb/gocql/v2/internal/connectivity"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/protocol"
)

// sessionIDStartupKey is the STARTUP option correlating the connections that
// belong to one session in system.clients. Unlike driverConfigStartupKey it is
// sent on every connection, since correlating them is the whole point.
const InternalSessionIDStartupKey = "SESSION_ID"

// ApplicationInfo adds the application's own STARTUP options, such as its
// name and version, to every connection.
type ApplicationInfo interface {
	UpdateStartupOptions(map[string]string)
}

// StaticApplicationInfo is an ApplicationInfo with fixed values.
type StaticApplicationInfo struct {
	applicationName    string
	applicationVersion string
	clientID           string
}

func NewStaticApplicationInfo(name, version, clientID string) *StaticApplicationInfo {
	return &StaticApplicationInfo{applicationName: name, applicationVersion: version, clientID: clientID}
}

func (i *StaticApplicationInfo) UpdateStartupOptions(opts map[string]string) {
	if i.applicationName != "" {
		opts["APPLICATION_NAME"] = i.applicationName
	}
	if i.applicationVersion != "" {
		opts["APPLICATION_VERSION"] = i.applicationVersion
	}
	if i.clientID != "" {
		opts["CLIENT_ID"] = i.clientID
	}
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
	s.conn.InternalScyllaSupported = InternalParseSupported(s.conn.InternalSupported, s.conn.InternalLogger)
	s.conn.recalculateSystemRequestTimeout()
	if _, poolOwned := s.conn.InternalErrorHandler.(poolOwner); !poolOwned || s.conn.InternalCfg.TypedConfig == nil {
		if current := s.conn.InternalHost.ScyllaFeatures(); current != s.conn.InternalScyllaSupported.ScyllaHostFeatures {
			implEndpoint.InternalHostInfoSetScyllaFeatures(s.conn.InternalHost, s.conn.InternalScyllaSupported.ScyllaHostFeatures)
		}
	}
	s.conn.InternalCqlProtoExts = InternalParseCQLProtocolExtensions(s.conn.InternalSupported, s.conn.InternalVersion, s.conn.InternalLogger)

	if s.conn.InternalCfg.TypedConfig != nil {
		s.conn.InternalCqlProtoExts = InternalTypedTabletExtensions(s.conn.InternalCqlProtoExts, s.conn.InternalSupported)
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
func InternalStartupOptions(cqlVersion, driverName, driverVersion string, info ApplicationInfo, driverConfig InternalStartupReporter, sessionID string, isScyllaConn bool) map[string]string {
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
