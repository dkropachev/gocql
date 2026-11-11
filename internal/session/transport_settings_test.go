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

package session

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"io"
	"math/big"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/endpoint"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	implPool "github.com/scylladb/gocql/v2/internal/pool"
)

func testTLS(t testing.TB) (tls.Certificate, *tls.Config) {
	t.Helper()
	key := codecTestMust(ecdsa.GenerateKey(elliptic.P256(), rand.Reader))
	template := &x509.Certificate{SerialNumber: big.NewInt(42), DNSNames: []string{"transport.test"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der := codecTestMust(x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key))
	leaf := codecTestMust(x509.ParseCertificate(der))
	pool := x509.NewCertPool()
	pool.AddCert(leaf)
	cert := tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf, OCSPStaple: []byte("ocsp"), SignedCertificateTimestamps: [][]byte{[]byte("sct")}}
	return cert, &tls.Config{RootCAs: pool, ServerName: "transport.test", MinVersion: tls.VersionTLS13}
}
func testHost(t *testing.T, address net.Addr) *endpoint.HostInfo {
	t.Helper()
	host, port := codecTestMust2(net.SplitHostPort(address.String()))
	_ = port
	addr := address.(*net.TCPAddr)
	h := func() *endpoint.HostInfo {
		opaqueStorageValue := new(endpoint.HostInfo)
		implEndpoint.HostInfoData(opaqueStorageValue).InternalConnectAddress = net.ParseIP(host)
		implEndpoint.HostInfoData(opaqueStorageValue).InternalPort = addr.Port
		implEndpoint.HostInfoData(opaqueStorageValue).InternalHostId = cqltype.UUID{15: 1}
		return opaqueStorageValue
	}()
	implEndpoint.InternalHostInfoSetTranslatedConnectionInfo(h, endpoint.InternalTranslatedAddresses{CQL: endpoint.AddressPort{Address: net.ParseIP(host), Port: uint16(addr.Port)}, ShardAware: endpoint.AddressPort{Address: net.ParseIP(host), Port: uint16(addr.Port)}, ShardAwareTLS: endpoint.AddressPort{Address: net.ParseIP(host), Port: uint16(addr.Port)}})
	return h
}
func codecTestMust2(a, b string, err error) (string, string) {
	if err != nil {
		panic(err)
	}
	return a, b
}

type testDNS func(string) ([]net.IP, error)

func (f testDNS) LookupIP(name string) ([]net.IP, error) { return f(name) }

type testReconnect struct {
	calls chan int
	again bool
	pool  *implPool.InternalHostConnPool
}

func (p *testReconnect) Delay(attempt int) (time.Duration, bool) {
	if p.pool != nil {
		p.pool.InternalTransportState()
	}
	p.calls <- attempt
	return 5 * time.Millisecond, p.again
}

type longRepairPolicy struct {
	calls atomic.Int32
	again bool
}

func (p *longRepairPolicy) Delay(int) (time.Duration, bool) {
	p.calls.Add(1)
	return time.Hour, p.again
}

type countingHostDialer struct{ calls *atomic.Int32 }

func (d countingHostDialer) DialHost(context.Context, *endpoint.HostInfo) (*endpoint.DialedHost, error) {
	d.calls.Add(1)
	return nil, io.EOF
}
