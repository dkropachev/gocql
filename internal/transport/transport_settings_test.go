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

package transport

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"math/big"
	"net"
	"testing"
	"time"
)

func step04TLS(t testing.TB) (tls.Certificate, *tls.Config) {
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
