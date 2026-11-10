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

package cluster

import (
	"crypto/tls"
	"crypto/x509"
	"testing"

	"github.com/scylladb/gocql/v2/internal/logging"
	"github.com/scylladb/gocql/v2/internal/transport"
)

func TestTLSConfig(t *testing.T) {
	t.Run("ForAddr", func(t *testing.T) {
		for address, expected := range map[string]string{"[::1]:9042": "::1", "[fe80::1%lo]:9042": "fe80::1", "::1": "::1", "transport.test:9042": "transport.test", "127.0.0.1:9042": "127.0.0.1"} {
			original := &tls.Config{}
			selected := transport.InternalTlsConfigForAddr(original, address)
			if selected.ServerName != expected || original.ServerName != "" {
				t.Fatal("logical TLS identity", address, selected.ServerName, expected)
			}
		}
	})

	t.Run("Setup", func(t *testing.T) {
		t.Parallel()

		t.Run("InsecureSkipVerify", func(t *testing.T) {
			t.Parallel()

			tests := []struct {
				name                       string
				opts                       *transport.SslOptions
				expectedInsecureSkipVerify bool
			}{
				{
					name: "Config nil, EnableHostVerification false",
					opts: &transport.SslOptions{
						EnableHostVerification: false,
					},
					expectedInsecureSkipVerify: true,
				},
				{
					name: "Config nil, EnableHostVerification true",
					opts: &transport.SslOptions{
						EnableHostVerification: true,
					},
					expectedInsecureSkipVerify: false,
				},
				{
					name: "Config.InsecureSkipVerify false, EnableHostVerification false",
					opts: &transport.SslOptions{
						EnableHostVerification: false,
						Config: &tls.Config{
							InsecureSkipVerify: false,
						},
					},
					expectedInsecureSkipVerify: false,
				},
				{
					name: "Config.InsecureSkipVerify true, EnableHostVerification false",
					opts: &transport.SslOptions{
						EnableHostVerification: false,
						Config: &tls.Config{
							InsecureSkipVerify: true,
						},
					},
					expectedInsecureSkipVerify: true,
				},
				{
					name: "Config.InsecureSkipVerify false, EnableHostVerification true",
					opts: &transport.SslOptions{
						EnableHostVerification: true,
						Config: &tls.Config{
							InsecureSkipVerify: false,
						},
					},
					expectedInsecureSkipVerify: false,
				},
				{
					name: "Config.InsecureSkipVerify true, EnableHostVerification true",
					opts: &transport.SslOptions{
						EnableHostVerification: true,
						Config: &tls.Config{
							InsecureSkipVerify: true,
						},
					},
					expectedInsecureSkipVerify: false,
				},
			}
			for _, test := range tests {
				test := test
				t.Run(test.name, func(t *testing.T) {
					tlsConfig, err := setupTLSConfig(test.opts, &logging.InternalDefaultLogger{})
					if err != nil {
						t.Fatalf("unexpected error %q", err.Error())
					}
					if tlsConfig.InsecureSkipVerify != test.expectedInsecureSkipVerify {
						t.Fatalf("got %v, but expected %v", tlsConfig.InsecureSkipVerify,
							test.expectedInsecureSkipVerify)
					}

					// Verify that VerifyPeerCertificate is set when InsecureSkipVerify is false
					// and DisableStrictCertificateValidation is false (default)
					if !tlsConfig.InsecureSkipVerify && tlsConfig.VerifyPeerCertificate == nil {
						t.Fatal("VerifyPeerCertificate should be set when InsecureSkipVerify is false")
					}
					// Verify that VerifyPeerCertificate is not set when InsecureSkipVerify is true
					if tlsConfig.InsecureSkipVerify && tlsConfig.VerifyPeerCertificate != nil {
						t.Fatal("VerifyPeerCertificate should not be set when InsecureSkipVerify is true")
					}
				})
			}
		})

		t.Run("StrictValidation", func(t *testing.T) {
			t.Parallel()

			t.Run("VerifyPeerCertificate set when verification enabled", func(t *testing.T) {
				opts := &transport.SslOptions{
					EnableHostVerification: true,
				}

				tlsConfig, err := setupTLSConfig(opts, &logging.InternalDefaultLogger{})
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}

				if tlsConfig.InsecureSkipVerify {
					t.Error("expected InsecureSkipVerify to be false")
				}

				if tlsConfig.VerifyPeerCertificate == nil {
					t.Error("expected VerifyPeerCertificate to be set when verification is enabled")
				}
			})

			t.Run("VerifyPeerCertificate not set when verification disabled", func(t *testing.T) {
				opts := &transport.SslOptions{
					EnableHostVerification: false,
				}

				tlsConfig, err := setupTLSConfig(opts, &logging.InternalDefaultLogger{})
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}

				if !tlsConfig.InsecureSkipVerify {
					t.Error("expected InsecureSkipVerify to be true")
				}

				if tlsConfig.VerifyPeerCertificate != nil {
					t.Error("expected VerifyPeerCertificate to not be set when verification is disabled")
				}
			})

			t.Run("VerifyPeerCertificate set with custom tls.Config", func(t *testing.T) {
				opts := &transport.SslOptions{
					Config: &tls.Config{
						InsecureSkipVerify: false,
					},
				}

				tlsConfig, err := setupTLSConfig(opts, &logging.InternalDefaultLogger{})
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}

				if tlsConfig.InsecureSkipVerify {
					t.Error("expected InsecureSkipVerify to be false")
				}

				if tlsConfig.VerifyPeerCertificate == nil {
					t.Error("expected VerifyPeerCertificate to be set")
				}
			})

			t.Run("VerifyPeerCertificate not set when DisableStrictCertificateValidation is true", func(t *testing.T) {
				opts := &transport.SslOptions{
					EnableHostVerification:             true,
					DisableStrictCertificateValidation: true,
				}

				tlsConfig, err := setupTLSConfig(opts, &logging.InternalDefaultLogger{})
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}

				if tlsConfig.InsecureSkipVerify {
					t.Error("expected InsecureSkipVerify to be false")
				}

				if tlsConfig.VerifyPeerCertificate != nil {
					t.Error("expected VerifyPeerCertificate to not be set when DisableStrictCertificateValidation is true")
				}
			})
		})
	})

	t.Run("StrictVerifyPeerCertificate", func(t *testing.T) {
		t.Parallel()

		// Generate a valid certificate chain: Root CA -> Intermediate CA -> Leaf
		rootCA, rootKey, err := generateCertificate(true, nil, nil)
		if err != nil {
			t.Fatalf("failed to generate root CA: %v", err)
		}

		intermediateCA, intermediateKey, err := generateCertificate(true, rootCA, rootKey)
		if err != nil {
			t.Fatalf("failed to generate intermediate CA: %v", err)
		}

		leafCert, _, err := generateCertificate(false, intermediateCA, intermediateKey)
		if err != nil {
			t.Fatalf("failed to generate leaf certificate: %v", err)
		}

		// Create a trusted root pool with the root CA
		rootPool := x509.NewCertPool()
		rootPool.AddCert(rootCA)

		t.Run("valid certificate chain", func(t *testing.T) {
			verifyFunc := strictVerifyPeerCertificate(rootPool)

			// Prepare the certificate chain as it would be presented during TLS handshake
			rawCerts := [][]byte{
				leafCert.Raw,
				intermediateCA.Raw,
			}

			err := verifyFunc(rawCerts, nil)
			if err != nil {
				t.Errorf("expected valid chain to pass verification, got error: %v", err)
			}
		})

		t.Run("empty certificate chain", func(t *testing.T) {
			verifyFunc := strictVerifyPeerCertificate(rootPool)

			err := verifyFunc([][]byte{}, nil)
			if err == nil {
				t.Error("expected error for empty certificate chain")
			}
		})

		t.Run("chain with only intermediate CA in root pool", func(t *testing.T) {
			// Create a pool with only the intermediate CA (not the root)
			intermediatePool := x509.NewCertPool()
			intermediatePool.AddCert(intermediateCA)

			verifyFunc := strictVerifyPeerCertificate(intermediatePool)

			rawCerts := [][]byte{
				leafCert.Raw,
				intermediateCA.Raw,
			}

			// When an intermediate CA is placed in the root pool, our strict validation
			// should reject it because the intermediate is not self-signed (not a true root CA).
			// This prevents the security issue where trusting an intermediate as a root
			// allows that intermediate to issue certificates for any domain.
			err := verifyFunc(rawCerts, nil)
			if err == nil {
				t.Error("expected error when intermediate CA is in root pool (not a self-signed root)")
			}
		})

		t.Run("self-signed CA certificate", func(t *testing.T) {
			// Generate a self-signed CA certificate
			selfSigned, _, err := generateCertificate(true, nil, nil)
			if err != nil {
				t.Fatalf("failed to generate self-signed certificate: %v", err)
			}

			// Add it to the root pool
			selfSignedPool := x509.NewCertPool()
			selfSignedPool.AddCert(selfSigned)

			verifyFunc := strictVerifyPeerCertificate(selfSignedPool)

			rawCerts := [][]byte{
				selfSigned.Raw,
			}

			err = verifyFunc(rawCerts, nil)
			if err != nil {
				t.Errorf("expected self-signed CA certificate to pass verification when in root pool, got error: %v", err)
			}
		})

		t.Run("untrusted root", func(t *testing.T) {
			// Generate a different root CA that's not in our trust pool
			untrustedRoot, untrustedRootKey, err := generateCertificate(true, nil, nil)
			if err != nil {
				t.Fatalf("failed to generate untrusted root CA: %v", err)
			}

			untrustedIntermediate, untrustedIntermediateKey, err := generateCertificate(true, untrustedRoot, untrustedRootKey)
			if err != nil {
				t.Fatalf("failed to generate untrusted intermediate CA: %v", err)
			}

			untrustedLeaf, _, err := generateCertificate(false, untrustedIntermediate, untrustedIntermediateKey)
			if err != nil {
				t.Fatalf("failed to generate untrusted leaf certificate: %v", err)
			}

			verifyFunc := strictVerifyPeerCertificate(rootPool)

			rawCerts := [][]byte{
				untrustedLeaf.Raw,
				untrustedIntermediate.Raw,
			}

			err = verifyFunc(rawCerts, nil)
			if err == nil {
				t.Error("expected error for certificate chain with untrusted root")
			}
		})

		t.Run("malformed leaf certificate", func(t *testing.T) {
			verifyFunc := strictVerifyPeerCertificate(rootPool)

			rawCerts := [][]byte{
				{0x00, 0x01, 0x02}, // not a valid DER certificate
			}

			err := verifyFunc(rawCerts, nil)
			if err == nil {
				t.Error("expected error for malformed leaf certificate")
			}
		})

		t.Run("malformed intermediate certificate", func(t *testing.T) {
			verifyFunc := strictVerifyPeerCertificate(rootPool)

			rawCerts := [][]byte{
				leafCert.Raw,
				{0x00, 0x01, 0x02}, // not a valid DER certificate
			}

			err := verifyFunc(rawCerts, nil)
			if err == nil {
				t.Error("expected error for malformed intermediate certificate")
			}
		})

		t.Run("valid certificate chain with verifiedChains", func(t *testing.T) {
			verifyFunc := strictVerifyPeerCertificate(rootPool)

			verifiedChains := [][]*x509.Certificate{
				{leafCert, intermediateCA, rootCA},
			}
			rawCerts := [][]byte{
				leafCert.Raw,
				intermediateCA.Raw,
			}

			err := verifyFunc(rawCerts, verifiedChains)
			if err != nil {
				t.Errorf("expected valid chain with verifiedChains to pass, got error: %v", err)
			}
		})

		t.Run("intermediate CA in root pool with verifiedChains", func(t *testing.T) {
			intermediatePool := x509.NewCertPool()
			intermediatePool.AddCert(intermediateCA)

			verifyFunc := strictVerifyPeerCertificate(intermediatePool)

			verifiedChains := [][]*x509.Certificate{
				{leafCert, intermediateCA},
			}
			rawCerts := [][]byte{
				leafCert.Raw,
				intermediateCA.Raw,
			}

			err := verifyFunc(rawCerts, verifiedChains)
			if err == nil {
				t.Error("expected error when intermediate CA is in root pool with verifiedChains")
			}
		})
	})
}
