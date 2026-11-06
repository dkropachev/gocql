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
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"

	"github.com/scylladb/gocql/v2/internal/endpoint"
)

func validSupported() map[string][]string {
	return map[string][]string{"SCYLLA_SHARD": {"0"}, "SCYLLA_NR_SHARDS": {"12"}, "SCYLLA_PARTITIONER": {"org.apache.cassandra.dht.Murmur3Partitioner"}, "SCYLLA_SHARDING_ALGORITHM": {"biased-token-round-robin"}, "SCYLLA_SHARDING_IGNORE_MSB": {"0"}}
}

type boundedOpeningDialer struct {
	active, max, calls atomic.Int32
	entered            chan struct{}
	release            chan struct{}
}

func (d *boundedOpeningDialer) DialHost(ctx context.Context, host *endpoint.HostInfo) (*endpoint.DialedHost, error) {
	active := d.active.Add(1)
	defer d.active.Add(-1)
	d.calls.Add(1)
	for {
		old := d.max.Load()
		if old >= active || d.max.CompareAndSwap(old, active) {
			break
		}
	}
	d.entered <- struct{}{}
	select {
	case <-d.release:
		return nil, errors.New("injected opening failure")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type opaqueTCP struct{ net.Conn }
type forwardingTCP struct {
	*net.TCPConn
	mu        sync.Mutex
	noDelay   []bool
	keepalive []bool
}

func (c *forwardingTCP) SetNoDelay(value bool) error {
	c.mu.Lock()
	c.noDelay = append(c.noDelay, value)
	c.mu.Unlock()
	return c.TCPConn.SetNoDelay(value)
}
func (c *forwardingTCP) SetKeepAlive(value bool) error {
	c.mu.Lock()
	c.keepalive = append(c.keepalive, value)
	c.mu.Unlock()
	return c.TCPConn.SetKeepAlive(value)
}
