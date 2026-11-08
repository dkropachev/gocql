// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	implSettings "github.com/scylladb/gocql/v2/internal/settings"
)

// tlsTestPEM returns a self-signed certificate with serial and its key.
func tlsTestPEM(t *testing.T, serial int64) (cert, key []byte) {
	t.Helper()
	private, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(serial), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &private.PublicKey, private)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(private)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
}

type tlsTestFiles struct {
	t                 *testing.T
	files             implSettings.TLSFiles
	certs             map[int64][2][]byte
	certPath, keyPath string
	caPath            string
}

func newTLSTestFiles(t *testing.T, dir string) *tlsTestFiles {
	f := &tlsTestFiles{t: t, certs: map[int64][2][]byte{}, certPath: filepath.Join(dir, "client.crt"), keyPath: filepath.Join(dir, "client.key"), caPath: filepath.Join(dir, "ca.crt")}
	f.files = implSettings.TLSFiles{CertFile: f.certPath, KeyFile: f.keyPath, CAFile: f.caPath}
	return f
}

func (f *tlsTestFiles) pair(serial int64) [2][]byte {
	if p, ok := f.certs[serial]; ok {
		return p
	}
	cert, key := tlsTestPEM(f.t, serial)
	f.certs[serial] = [2][]byte{cert, key}
	return f.certs[serial]
}

func (f *tlsTestFiles) write(path string, data []byte) {
	f.t.Helper()
	if err := os.WriteFile(path, data, 0o600); err != nil {
		f.t.Fatal(err)
	}
}

// set writes the client pair with serial and a CA bundle holding caSerial.
func (f *tlsTestFiles) set(serial, caSerial int64) {
	f.t.Helper()
	p := f.pair(serial)
	f.write(f.certPath, p[0])
	f.write(f.keyPath, p[1])
	f.write(f.caPath, f.pair(caSerial)[0])
}

// config builds the session Config the way ConfigBuilder.Build does.
func (f *tlsTestFiles) config(base *tls.Config) Config {
	f.t.Helper()
	content, err := implSettings.ReadTLSFiles(f.files)
	if err != nil {
		f.t.Fatal(err)
	}
	built, err := implSettings.BuildTLSFromFiles(base, content)
	if err != nil {
		f.t.Fatal(err)
	}
	files := f.files
	return Config{TLS: built, TLSFiles: &files, TLSFilesBase: base}
}

func tlsTestSerial(t *testing.T, config *tls.Config) int64 {
	t.Helper()
	if len(config.Certificates) != 1 {
		t.Fatalf("certificates = %d, want 1", len(config.Certificates))
	}
	leaf, err := x509.ParseCertificate(config.Certificates[0].Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	return leaf.SerialNumber.Int64()
}

func tlsTestTrusts(t *testing.T, config *tls.Config, f *tlsTestFiles, serial int64) bool {
	t.Helper()
	block, _ := pem.Decode(f.pair(serial)[0])
	leaf, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	_, err = leaf.Verify(x509.VerifyOptions{Roots: config.RootCAs})
	return err == nil
}

type tlsTestLogger struct {
	mu    sync.Mutex
	lines []string
}

func (l *tlsTestLogger) Print(v ...any) { l.Printf("%s", fmt.Sprint(v...)) }
func (l *tlsTestLogger) Printf(format string, v ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, fmt.Sprintf(format, v...))
}
func (l *tlsTestLogger) Println(v ...any) { l.Printf("%s", fmt.Sprintln(v...)) }
func (l *tlsTestLogger) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.lines)
}

func tlsTestSession(state *InternalSessionTLS) *Session {
	s := new(Session)
	s.privateStorage.tls = state
	return s
}

func TestSessionTLS(t *testing.T) {
	t.Run("UpdateTLS/Validation", func(t *testing.T) {
		plaintext := new(Session)
		if err := plaintext.UpdateTLS(&tls.Config{}); err == nil || !strings.Contains(err.Error(), "without TLS") {
			t.Fatalf("plaintext session: %v", err)
		}
		state := newSessionTLS(Config{TLS: &tls.Config{ServerName: "a"}, ClientRoutes: &implSettings.ClientRoutesOptions{ConnectionIDs: []string{"c"}, TLS: true}})
		s := tlsTestSession(state)
		if err := s.UpdateTLS(nil); err == nil {
			t.Fatal("nil config accepted")
		}
		if err := s.UpdateTLS(&tls.Config{InsecureSkipVerify: true}); err == nil || !strings.Contains(err.Error(), "client routes TLS") {
			t.Fatalf("unverified client-routes TLS: %v", err)
		}
		if got := state.current.Load().ServerName; got != "a" {
			t.Fatalf("rejected update changed the config: ServerName = %q", got)
		}
		InternalSessionMarkClosed(s)
		if err := s.UpdateTLS(&tls.Config{}); !errors.Is(err, ErrSessionClosed) {
			t.Fatalf("closed session: %v", err)
		}
	})

	t.Run("UpdateTLS/ReplacesWithCopy", func(t *testing.T) {
		state := newSessionTLS(Config{TLS: &tls.Config{ServerName: "old"}})
		s := tlsTestSession(state)
		next := &tls.Config{ServerName: "new", MinVersion: tls.VersionTLS13}
		if err := s.UpdateTLS(next); err != nil {
			t.Fatal(err)
		}
		next.ServerName = "mutated"
		if got := state.current.Load(); got.ServerName != "new" || got.MinVersion != tls.VersionTLS13 {
			t.Fatalf("current = %q/%x, want the copy taken at UpdateTLS", got.ServerName, got.MinVersion)
		}
	})

	t.Run("UpdateTLS/KeepsFileCertificate", func(t *testing.T) {
		f := newTLSTestFiles(t, t.TempDir())
		f.set(1, 10)
		state := newSessionTLS(f.config(nil))
		if err := tlsTestSession(state).UpdateTLS(&tls.Config{MinVersion: tls.VersionTLS13}); err != nil {
			t.Fatal(err)
		}
		current := state.current.Load()
		if current.MinVersion != tls.VersionTLS13 || tlsTestSerial(t, current) != 1 || !tlsTestTrusts(t, current, f, 10) {
			t.Fatal("update dropped the file certificate or CA, or ignored the new base")
		}
		f.set(2, 10)
		state.mu.Lock()
		changed, err := state.reloadLocked()
		state.mu.Unlock()
		if err != nil || !changed {
			t.Fatal(changed, err)
		}
		if current := state.current.Load(); current.MinVersion != tls.VersionTLS13 || tlsTestSerial(t, current) != 2 {
			t.Fatal("reload after UpdateTLS lost the updated base")
		}
		if err := tlsTestSession(state).UpdateTLS(&tls.Config{GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) { return nil, nil }}); err == nil {
			t.Fatal("GetClientCertificate accepted alongside a cert file")
		}
	})

	t.Run("Files/ReloadOnChange", func(t *testing.T) {
		f := newTLSTestFiles(t, t.TempDir())
		f.set(1, 10)
		state := newSessionTLS(f.config(nil))
		reload := func() bool {
			t.Helper()
			state.mu.Lock()
			defer state.mu.Unlock()
			changed, err := state.reloadLocked()
			if err != nil {
				t.Fatal(err)
			}
			return changed
		}
		if reload() {
			t.Fatal("unchanged files reported a change")
		}
		f.set(2, 10)
		if !reload() || tlsTestSerial(t, state.current.Load()) != 2 {
			t.Fatal("new certificate not applied")
		}
		f.set(2, 20)
		if !reload() || !tlsTestTrusts(t, state.current.Load(), f, 20) || tlsTestTrusts(t, state.current.Load(), f, 10) {
			t.Fatal("new CA not applied")
		}
	})

	t.Run("Files/BrokenFilesKeepConfig", func(t *testing.T) {
		f := newTLSTestFiles(t, t.TempDir())
		f.set(1, 10)
		state := newSessionTLS(f.config(nil))
		before := state.current.Load()
		f.write(f.keyPath, f.pair(2)[1]) // key of another certificate
		state.mu.Lock()
		_, err := state.reloadLocked()
		state.mu.Unlock()
		if err == nil || state.current.Load() != before {
			t.Fatalf("mismatched pair: err %v, config replaced %t", err, state.current.Load() != before)
		}
		if err := os.Remove(f.caPath); err != nil {
			t.Fatal(err)
		}
		state.mu.Lock()
		_, err = state.reloadLocked()
		state.mu.Unlock()
		if err == nil || state.current.Load() != before {
			t.Fatal("missing CA file replaced the config")
		}
		f.set(2, 10)
		state.mu.Lock()
		changed, err := state.reloadLocked()
		state.mu.Unlock()
		if err != nil || !changed || tlsTestSerial(t, state.current.Load()) != 2 {
			t.Fatal("valid files after a failure were not applied", err)
		}
	})

	t.Run("Files/SymlinkSwap", func(t *testing.T) {
		// Kubernetes secret volumes publish a new version by renaming a
		// ..data symlink; the visible names are symlinks through it.
		dir := t.TempDir()
		f := newTLSTestFiles(t, dir)
		publish := func(version string, serial, caSerial int64) {
			t.Helper()
			versionDir := filepath.Join(dir, version)
			if err := os.Mkdir(versionDir, 0o700); err != nil {
				t.Fatal(err)
			}
			p := f.pair(serial)
			f.write(filepath.Join(versionDir, "client.crt"), p[0])
			f.write(filepath.Join(versionDir, "client.key"), p[1])
			f.write(filepath.Join(versionDir, "ca.crt"), f.pair(caSerial)[0])
			tmp := filepath.Join(dir, "..data_tmp")
			if err := os.Symlink(version, tmp); err != nil {
				t.Fatal(err)
			}
			if err := os.Rename(tmp, filepath.Join(dir, "..data")); err != nil {
				t.Fatal(err)
			}
		}
		publish("v1", 1, 10)
		for _, name := range []string{"client.crt", "client.key", "ca.crt"} {
			if err := os.Symlink(filepath.Join("..data", name), filepath.Join(dir, name)); err != nil {
				t.Fatal(err)
			}
		}
		state := newSessionTLS(f.config(nil))
		publish("v2", 2, 10)
		state.mu.Lock()
		changed, err := state.reloadLocked()
		state.mu.Unlock()
		if err != nil || !changed || tlsTestSerial(t, state.current.Load()) != 2 {
			t.Fatal("symlink swap not detected", err)
		}
	})

	t.Run("Files/IdentityChangeReplacesSessionCache", func(t *testing.T) {
		f := newTLSTestFiles(t, t.TempDir())
		f.set(1, 10)
		cache := tls.NewLRUClientSessionCache(8)
		state := newSessionTLS(f.config(&tls.Config{ClientSessionCache: cache}))
		reload := func() {
			t.Helper()
			state.mu.Lock()
			defer state.mu.Unlock()
			if _, err := state.reloadLocked(); err != nil {
				t.Fatal(err)
			}
		}
		if state.current.Load().ClientSessionCache != cache {
			t.Fatal("initial config lost the caller's session cache")
		}
		f.set(1, 20)
		reload()
		if state.current.Load().ClientSessionCache != cache {
			t.Fatal("CA-only change replaced the session cache")
		}
		f.set(2, 20)
		reload()
		replaced := state.current.Load().ClientSessionCache
		if replaced == nil || replaced == cache {
			t.Fatal("identity change kept tickets of the old identity")
		}
		f.set(2, 10)
		reload()
		if state.current.Load().ClientSessionCache != replaced {
			t.Fatal("CA-only change after rotation replaced the cache again")
		}
	})

	t.Run("Files/Poll", func(t *testing.T) {
		f := newTLSTestFiles(t, t.TempDir())
		f.files.Interval = 5 * time.Millisecond
		f.set(1, 10)
		state := newSessionTLS(f.config(nil))
		logger := &tlsTestLogger{}
		done := make(chan struct{})
		exited := make(chan struct{})
		go func() { defer close(exited); state.poll(done, logger) }()
		waitFor := func(what string, ok func() bool) {
			t.Helper()
			deadline := time.Now().Add(5 * time.Second)
			for !ok() {
				if time.Now().After(deadline) {
					t.Fatal("timed out waiting for", what)
				}
				time.Sleep(time.Millisecond)
			}
		}
		f.set(2, 10)
		waitFor("the new certificate", func() bool { return tlsTestSerial(t, state.current.Load()) == 2 })
		f.write(f.keyPath, f.pair(3)[1])
		waitFor("a logged failure", func() bool { return logger.count() == 1 })
		time.Sleep(50 * time.Millisecond) // several more failing ticks
		if n := logger.count(); n != 1 {
			t.Fatalf("logged the same failure %d times", n)
		}
		f.set(3, 10)
		waitFor("recovery", func() bool { return tlsTestSerial(t, state.current.Load()) == 3 })
		close(done)
		select {
		case <-exited:
		case <-time.After(5 * time.Second):
			t.Fatal("poller did not stop")
		}
	})
}
