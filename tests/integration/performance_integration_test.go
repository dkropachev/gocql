//go:build integration

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package integration

import (
	"crypto/tls"
	"encoding/binary"
	"net"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"unsafe"

	gocql "github.com/scylladb/gocql/v2"

	"github.com/scylladb/gocql/v2/cql"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
)

// Keep native sockets unwrapped so net.Buffers retains the OS writev path.
// TCP_INFO counters are kernel-accounted bytes for exactly these client sockets.
// /proc/self/io syscr/syscw are whole-process syscall counts (including background
// control work and the counter read); they are labeled separately from Read calls.
type testIO struct {
	mu    sync.Mutex
	conns []*net.TCPConn
}

func (s *testIO) add(c net.Conn) {
	if tcp, ok := c.(*net.TCPConn); ok {
		s.mu.Lock()
		s.conns = append(s.conns, tcp)
		s.mu.Unlock()
	}
}
func (s *testIO) snapshot() (reads, writes, readBytes, writeBytes int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, conn := range s.conns {
		raw, e := conn.SyscallConn()
		if e != nil {
			continue
		}
		raw.Control(func(fd uintptr) {
			var info [256]byte
			length := uint32(len(info))
			_, _, err := syscall.Syscall6(syscall.SYS_GETSOCKOPT, fd, syscall.IPPROTO_TCP, syscall.TCP_INFO, uintptr(unsafe.Pointer(&info[0])), uintptr(unsafe.Pointer(&length)), 0)
			if err == 0 && length >= 208 {
				readBytes += int64(binary.NativeEndian.Uint64(info[128:136]))
				writeBytes += int64(binary.NativeEndian.Uint64(info[200:208]))
			}
		})
	}
	data, e := os.ReadFile("/proc/self/io")
	if e == nil {
		for _, line := range strings.Split(string(data), "\n") {
			parts := strings.Fields(line)
			if len(parts) == 2 {
				n, _ := strconv.ParseInt(parts[1], 10, 64)
				if parts[0] == "syscr:" {
					reads = n
				}
				if parts[0] == "syscw:" {
					writes = n
				}
			}
		}
	}
	return
}

type testInput struct {
	Cell, ID int32
	Payload  []byte
	Pieces   [][]byte
}
type testQuery struct{ Cell, Rows int32 }
type testBorrowed struct {
	ID      int32
	Payload cql.Bytes
	Pieces  cql.Sequence[cql.Bytes]
}
type testOutput struct {
	ID      int32
	Payload []byte
	Pieces  [][]byte
}
type testShape struct {
	Name                          string
	Rows, Cell, Page, Concurrency int
	Slow                          bool
}
type testResult struct {
	Shape                                                                             testShape
	API                                                                               string
	Operations                                                                        int
	Protocol                                                                          int
	TLS                                                                               bool
	Compression                                                                       string
	FirstNS, EndNS                                                                    [3]int64
	WallNS, CPUNS, AllocatedBytes, Allocations                                        int64
	ProcessReadSyscalls, ProcessWriteSyscalls, TCPReceivedBytes, TCPSentBytes         int64
	ApplicationCopyBytes, ApplicationStorageBytes                                     int64
	IdleHeapBytes, PeakSampledHeapAllocBytes, PeakPostGCHeapBytes, PostCloseHeapBytes uint64
	RuntimeHeapBeforeConnect, SessionHeapAfterConnect, HeapAfterSetup                 uint64
	WarmIdleDriverAndHarnessBytes, InclusivePeakDriverAndHarnessBytes                 int64
	PercentileSamples                                                                 int

	IdleReceiveCacheBytes, IdleReceiveCacheObjects int
	Negotiated                                     []map[string]interface{}

	DriverOwnedOutputCopyBytes int64 // zero: borrowed views or direct socket/transform final writes

	Sampling string
}

func quantiles(v []int64) [3]int64 {
	sort.Slice(v, func(i, j int) bool { return v[i] < v[j] })
	var r [3]int64
	for i, q := range []float64{.5, .95, .99} {
		r[i] = v[min(len(v)-1, int(float64(len(v)-1)*q))]
	}
	return r
}
func testCPU() int64 {
	var r syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &r)
	return r.Utime.Nano() + r.Stime.Nano()
}
func mem() runtime.MemStats { var m runtime.MemStats; runtime.ReadMemStats(&m); return m }

// Negotiated metadata is captured outside all normal measurement intervals.
func testNegotiated(s *gocql.Session) []map[string]interface{} {
	p := implCluster.SessionData(s).InternalPool
	p.InternalMu.RLock()
	defer p.InternalMu.RUnlock()
	result := []map[string]interface{}{}
	for host, pool := range p.InternalHostConnPools {
		pool.InternalMu.RLock()
		picker := pool.InternalConnPicker
		count, missing := picker.Size()
		var conns []*driverConn
		switch v := picker.(type) {
		case *typedConnPicker:
			v.InternalMu.RLock()
			for _, shard := range v.InternalConns {
				conns = append(conns, shard...)
			}
			v.InternalMu.RUnlock()
		case *defaultConnPicker:
			v.InternalMu.RLock()
			conns = append(conns, v.InternalConns...)
			v.InternalMu.RUnlock()
		}
		pool.InternalMu.RUnlock()
		for _, c := range conns {
			compression := "none"
			if c.InternalCompressor != nil {
				compression = c.InternalCompressor.Name()
			}
			entry := map[string]interface{}{"host_id": host.String(), "host_connections": count, "missing_connections": missing, "shards": c.InternalScyllaSupported.ShardsCount(), "shard": c.InternalScyllaSupported.Shard(), "protocol": c.InternalVersion, "compression": compression, "metadata_id": c.InternalTracksResultMetadataID(), "authentication_fixture": *flagRunAuthTest, "routing": "token-aware/shuffle-replicas/round-robin-fallback"}
			if reader, ok := c.InternalR.(*connReader); ok {
				entry["remote"] = reader.InternalConn.RemoteAddr().String()
				entry["local"] = reader.InternalConn.LocalAddr().String()
				if encrypted, ok := reader.InternalConn.(interface{ ConnectionState() tls.ConnectionState }); ok {
					state := encrypted.ConnectionState()
					entry["tls_version"] = state.Version
					entry["tls_cipher"] = state.CipherSuite
					entry["tls_handshake_complete"] = state.HandshakeComplete
				}
			}
			result = append(result, entry)
		}
	}
	return result
}
