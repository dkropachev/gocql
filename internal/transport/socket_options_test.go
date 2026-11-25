// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package transport

import (
	"errors"
	"net"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/connectivity"
)

// tcpAddrWrapper reports a TCP local address but exposes no socket controls,
// like a connection from a metrics or rate-limiting dialer.
type tcpAddrWrapper struct{ net.Conn }

func (tcpAddrWrapper) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 40000}
}

// unwrappingConn hides its inner connection's controls but returns it from
// Unwrap.
type unwrappingConn struct{ inner net.Conn }

func (c unwrappingConn) Read(b []byte) (int, error)         { return c.inner.Read(b) }
func (c unwrappingConn) Write(b []byte) (int, error)        { return c.inner.Write(b) }
func (c unwrappingConn) Close() error                       { return c.inner.Close() }
func (c unwrappingConn) LocalAddr() net.Addr                { return c.inner.LocalAddr() }
func (c unwrappingConn) RemoteAddr() net.Addr               { return c.inner.RemoteAddr() }
func (c unwrappingConn) SetDeadline(t time.Time) error      { return c.inner.SetDeadline(t) }
func (c unwrappingConn) SetReadDeadline(t time.Time) error  { return c.inner.SetReadDeadline(t) }
func (c unwrappingConn) SetWriteDeadline(t time.Time) error { return c.inner.SetWriteDeadline(t) }
func (c unwrappingConn) Unwrap() net.Conn                   { return c.inner }

type recordingControls struct {
	net.Conn
	noDelay   []bool
	keepalive []bool
	period    time.Duration
}

func (c *recordingControls) SetNoDelay(v bool) error { c.noDelay = append(c.noDelay, v); return nil }
func (c *recordingControls) SetKeepAlive(v bool) error {
	c.keepalive = append(c.keepalive, v)
	return nil
}
func (c *recordingControls) SetKeepAlivePeriod(d time.Duration) error {
	c.period = d
	return nil
}

func TestSocketOptionsOnWrappedConnections(t *testing.T) {
	defaults := connectivity.Config{NoDelay: true}
	t.Run("DefaultsAreBestEffort", func(t *testing.T) {
		a, b := net.Pipe()
		defer a.Close()
		defer b.Close()
		if err := InternalApplyV2SocketOptions(tcpAddrWrapper{a}, defaults, "tcp"); err != nil {
			t.Fatal("a wrapper without controls failed on the defaults:", err)
		}
	})
	t.Run("ChosenOptionsFail", func(t *testing.T) {
		for _, tc := range []struct {
			option  connectivity.SocketOptions
			setting string
		}{{connectivity.SocketNoDelay, "WithNagle"}, {connectivity.SocketKeepalive, "WithTCPKeepalive"}} {
			a, b := net.Pipe()
			config := defaults
			config.ChosenSocketOptions = tc.option
			err := InternalApplyV2SocketOptions(tcpAddrWrapper{a}, config, "tcp")
			a.Close()
			b.Close()
			var unsupported *connectivity.UnsupportedSettingError
			if !errors.As(err, &unsupported) || unsupported.Setting != tc.setting {
				t.Fatalf("chosen %s on a wrapper without controls: %v", tc.setting, err)
			}
		}
	})
	t.Run("FoundThroughUnwrap", func(t *testing.T) {
		a, b := net.Pipe()
		defer a.Close()
		defer b.Close()
		controls := &recordingControls{Conn: a}
		config := defaults
		config.SocketKeepalive = 9 * time.Second
		config.ChosenSocketOptions = connectivity.SocketNoDelay | connectivity.SocketKeepalive
		if err := InternalApplyV2SocketOptions(unwrappingConn{unwrappingConn{controls}}, config, "tcp"); err != nil {
			t.Fatal(err)
		}
		if len(controls.noDelay) != 1 || !controls.noDelay[0] || len(controls.keepalive) != 1 || !controls.keepalive[0] || controls.period != 9*time.Second {
			t.Fatal("controls behind Unwrap were not applied", controls.noDelay, controls.keepalive, controls.period)
		}
	})
}
