// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"net"
	"time"
)

type socketOptions struct {
	*net.TCPConn
	read, write, linger                int
	readCalls, writeCalls, lingerCalls int
	period                             time.Duration
}

func (c *socketOptions) SetReadBuffer(n int) error {
	c.read = n
	c.readCalls++
	return c.TCPConn.SetReadBuffer(n)
}
func (c *socketOptions) SetWriteBuffer(n int) error {
	c.write = n
	c.writeCalls++
	return c.TCPConn.SetWriteBuffer(n)
}
func (c *socketOptions) SetLinger(n int) error {
	c.linger = n
	c.lingerCalls++
	return c.TCPConn.SetLinger(n)
}
func (c *socketOptions) SetKeepAlivePeriod(n time.Duration) error {
	c.period = n
	return c.TCPConn.SetKeepAlivePeriod(n)
}
