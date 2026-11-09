// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type tlsTestFiles struct {
	t     *testing.T
	dir   string
	pairs map[int64][2][]byte
	files TLSFiles
}

func newTLSTestFiles(t *testing.T) *tlsTestFiles {
	dir := t.TempDir()
	return &tlsTestFiles{t: t, dir: dir, pairs: map[int64][2][]byte{}, files: TLSFiles{CertFile: filepath.Join(dir, "client.crt"), KeyFile: filepath.Join(dir, "client.key"), CAFile: filepath.Join(dir, "ca.crt")}}
}

// pair returns a self-signed certificate with serial and its key.
func (f *tlsTestFiles) pair(serial int64) [2][]byte {
	f.t.Helper()
	if p, ok := f.pairs[serial]; ok {
		return p
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		f.t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(serial), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		f.t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		f.t.Fatal(err)
	}
	f.pairs[serial] = [2][]byte{pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})}
	return f.pairs[serial]
}

func (f *tlsTestFiles) write(path string, data []byte) {
	f.t.Helper()
	if err := os.WriteFile(path, data, 0o600); err != nil {
		f.t.Fatal(err)
	}
}

// set writes the client pair with serial and a CA file holding caSerial.
func (f *tlsTestFiles) set(serial, caSerial int64) {
	f.t.Helper()
	p := f.pair(serial)
	f.write(f.files.CertFile, p[0])
	f.write(f.files.KeyFile, p[1])
	f.write(f.files.CAFile, f.pair(caSerial)[0])
}

func (f *tlsTestFiles) source(base *tls.Config) *TLSFileSource {
	f.t.Helper()
	s, err := NewTLSFiles(base, f.files)
	if err != nil {
		f.t.Fatal(err)
	}
	return s
}

func (f *tlsTestFiles) serial(config *tls.Config) int64 {
	f.t.Helper()
	if len(config.Certificates) != 1 {
		f.t.Fatalf("certificates = %d, want 1", len(config.Certificates))
	}
	leaf, err := x509.ParseCertificate(config.Certificates[0].Certificate[0])
	if err != nil {
		f.t.Fatal(err)
	}
	return leaf.SerialNumber.Int64()
}

func (f *tlsTestFiles) trusts(config *tls.Config, serial int64) bool {
	f.t.Helper()
	block, _ := pem.Decode(f.pair(serial)[0])
	leaf, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		f.t.Fatal(err)
	}
	_, err = leaf.Verify(x509.VerifyOptions{Roots: config.RootCAs})
	return err == nil
}

func TestTLSSource(t *testing.T) {
	t.Run("Files/Arguments", func(t *testing.T) {
		pki := TLSFiles{CertFile: "testdata/pki/gocql.crt", KeyFile: "testdata/pki/gocql.key", CAFile: "testdata/pki/ca.crt"}
		getCert := &tls.Config{GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) { return nil, nil }}
		for name, tc := range map[string]struct {
			base  *tls.Config
			files TLSFiles
			want  string
		}{
			"CertWithoutKey":       {nil, TLSFiles{CertFile: pki.CertFile}, "set together"},
			"Empty":                {nil, TLSFiles{}, "pair or a CA file is required"},
			"NegativeInterval":     {nil, TLSFiles{CAFile: pki.CAFile, Interval: -time.Second}, "must not be negative"},
			"GetClientCertificate": {getCert, pki, "GetClientCertificate"},
			"MissingFile":          {nil, TLSFiles{CAFile: "testdata/pki/missing.crt"}, "missing.crt"},
			"MismatchedPair":       {nil, TLSFiles{CertFile: pki.CertFile, KeyFile: "testdata/pki/cassandra.key"}, "cert and key"},
			"NotPEM":               {nil, TLSFiles{CAFile: "testdata/pki/ca.cnf"}, "no PEM certificates"},
		} {
			t.Run(name, func(t *testing.T) {
				if _, err := NewTLSFiles(tc.base, tc.files); err == nil || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("NewTLSFiles = %v, want an error containing %q", err, tc.want)
				}
			})
		}
		base := &tls.Config{ServerName: "node", MinVersion: tls.VersionTLS13}
		s, err := NewTLSFiles(base, pki)
		if err != nil {
			t.Fatal(err)
		}
		if c := s.TLSConfig(); c.ServerName != "node" || c.MinVersion != tls.VersionTLS13 || len(c.Certificates) != 1 || c.RootCAs == nil {
			t.Fatal("files were not applied over the base")
		}
		if len(base.Certificates) != 0 || base.RootCAs != nil {
			t.Fatal("NewTLSFiles modified the base")
		}
		s, err = NewTLSFiles(nil, TLSFiles{CAFile: pki.CAFile})
		if err != nil {
			t.Fatal(err)
		}
		if c := s.TLSConfig(); c.MinVersion != tls.VersionTLS12 || c.RootCAs == nil || len(c.Certificates) != 0 {
			t.Fatal("CA-only files without a base did not build a TLS 1.2+ config")
		}
	})

	t.Run("Files/ReloadOnChange", func(t *testing.T) {
		f := newTLSTestFiles(t)
		f.set(1, 10)
		s := f.source(nil)
		before := s.TLSConfig()
		if err := s.Reload(); err != nil || s.TLSConfig() != before {
			t.Fatal("unchanged files replaced the config", err)
		}
		f.set(2, 10)
		if err := s.Reload(); err != nil || f.serial(s.TLSConfig()) != 2 {
			t.Fatal("new certificate not applied", err)
		}
		f.set(2, 20)
		if err := s.Reload(); err != nil || !f.trusts(s.TLSConfig(), 20) || f.trusts(s.TLSConfig(), 10) {
			t.Fatal("new CA not applied", err)
		}
	})

	t.Run("Files/IntervalGatesChecks", func(t *testing.T) {
		f := newTLSTestFiles(t)
		f.files.Interval = time.Hour
		f.set(1, 10)
		s := f.source(nil)
		f.set(2, 10)
		if f.serial(s.TLSConfig()) != 1 {
			t.Fatal("TLSConfig checked the files before Interval passed")
		}
		s.next.Store(0) // the interval has passed
		if f.serial(s.TLSConfig()) != 2 {
			t.Fatal("TLSConfig did not check the files after Interval")
		}
	})

	t.Run("Files/BrokenFilesKeepConfig", func(t *testing.T) {
		f := newTLSTestFiles(t)
		f.set(1, 10)
		s := f.source(nil)
		before := s.TLSConfig()
		f.write(f.files.KeyFile, f.pair(2)[1]) // key of another certificate
		if err := s.Reload(); err == nil || s.Err() == nil || s.TLSConfig() != before {
			t.Fatal("mismatched pair replaced the config or reported no error")
		}
		if err := os.Remove(f.files.CAFile); err != nil {
			t.Fatal(err)
		}
		if err := s.Reload(); err == nil || s.TLSConfig() != before {
			t.Fatal("missing CA file replaced the config")
		}
		f.set(2, 10)
		if err := s.Reload(); err != nil || s.Err() != nil || f.serial(s.TLSConfig()) != 2 {
			t.Fatal("valid files after a failure were not applied", err)
		}
	})

	t.Run("Files/SymlinkSwap", func(t *testing.T) {
		// Kubernetes secret volumes publish a new version by renaming a
		// ..data symlink; the visible names are symlinks through it.
		f := newTLSTestFiles(t)
		publish := func(version string, serial int64) {
			t.Helper()
			dir := filepath.Join(f.dir, version)
			if err := os.Mkdir(dir, 0o700); err != nil {
				t.Fatal(err)
			}
			p := f.pair(serial)
			f.write(filepath.Join(dir, "client.crt"), p[0])
			f.write(filepath.Join(dir, "client.key"), p[1])
			f.write(filepath.Join(dir, "ca.crt"), f.pair(10)[0])
			tmp := filepath.Join(f.dir, "..data_tmp")
			if err := os.Symlink(version, tmp); err != nil {
				t.Fatal(err)
			}
			if err := os.Rename(tmp, filepath.Join(f.dir, "..data")); err != nil {
				t.Fatal(err)
			}
		}
		publish("v1", 1)
		for _, name := range []string{"client.crt", "client.key", "ca.crt"} {
			if err := os.Symlink(filepath.Join("..data", name), filepath.Join(f.dir, name)); err != nil {
				t.Fatal(err)
			}
		}
		s := f.source(nil)
		publish("v2", 2)
		if err := s.Reload(); err != nil || f.serial(s.TLSConfig()) != 2 {
			t.Fatal("symlink swap not detected", err)
		}
	})

	t.Run("Files/IdentityChangeReplacesSessionCache", func(t *testing.T) {
		f := newTLSTestFiles(t)
		f.set(1, 10)
		cache := tls.NewLRUClientSessionCache(8)
		s := f.source(&tls.Config{ClientSessionCache: cache})
		reload := func() {
			t.Helper()
			if err := s.Reload(); err != nil {
				t.Fatal(err)
			}
		}
		if s.TLSConfig().ClientSessionCache != cache {
			t.Fatal("initial config lost the base's session cache")
		}
		f.set(1, 20)
		reload()
		if s.TLSConfig().ClientSessionCache != cache {
			t.Fatal("CA-only change replaced the session cache")
		}
		f.set(2, 20)
		reload()
		replaced := s.TLSConfig().ClientSessionCache
		if replaced == nil || replaced == cache {
			t.Fatal("identity change kept tickets of the old identity")
		}
		f.set(2, 10)
		reload()
		if s.TLSConfig().ClientSessionCache != replaced {
			t.Fatal("CA-only change after rotation replaced the cache again")
		}
	})

	t.Run("Files/ConcurrentDials", func(t *testing.T) {
		f := newTLSTestFiles(t)
		f.files.Interval = time.Nanosecond
		f.set(1, 10)
		s := f.source(nil)
		var wg sync.WaitGroup
		for range 16 {
			wg.Go(func() {
				for range 50 {
					if c := s.TLSConfig(); c == nil || len(c.Certificates) != 1 {
						t.Error("incomplete config")
					}
				}
			})
		}
		f.set(2, 10)
		wg.Wait()
	})
}
