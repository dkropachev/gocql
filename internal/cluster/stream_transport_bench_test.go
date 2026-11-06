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

package cluster

import (
	"crypto/tls"
	"encoding/binary"
	"net"
	"sync/atomic"
	"testing"
)

type step04WireCounter struct {
	net.Conn
	reads, writes, readBytes, writeBytes, records atomic.Int64
}

func (c *step04WireCounter) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.reads.Add(1)
	c.readBytes.Add(int64(n))
	return n, err
}
func (c *step04WireCounter) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	c.writes.Add(1)
	c.writeBytes.Add(int64(n))
	// Count complete actual TLS application records independently of Write calls.
	for at := 0; at+5 <= n; {
		length := int(binary.BigEndian.Uint16(p[at+3 : at+5]))
		if at+5+length > n {
			break
		}
		if p[at] == 23 {
			c.records.Add(1)
		}
		at += 5 + length
	}
	return n, err
}
func (c *step04WireCounter) reset() {
	c.reads.Store(0)
	c.writes.Store(0)
	c.readBytes.Store(0)
	c.writeBytes.Store(0)
	c.records.Store(0)
}
func step04BenchTLS(b testing.TB) (*tls.Conn, *tls.Conn, *step04WireCounter, func()) {
	b.Helper()
	rawClient, rawServer := net.Pipe()
	wire := &step04WireCounter{Conn: rawClient}
	cert, config := step04TLS(b)
	client := tls.Client(wire, config)
	server := tls.Server(rawServer, &tls.Config{Certificates: []tls.Certificate{cert}, SessionTicketsDisabled: true})
	done := make(chan error, 1)
	go func() { done <- server.Handshake() }()
	if err := client.Handshake(); err != nil {
		b.Fatal(err)
	}
	if err := <-done; err != nil {
		b.Fatal(err)
	}
	return client, server, wire, func() { rawClient.Close(); rawServer.Close() }
}
