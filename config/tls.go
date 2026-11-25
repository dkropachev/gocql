// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package config

import (
	"crypto/sha256"
	"crypto/tls"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	implSettings "github.com/scylladb/gocql/v2/internal/settings"
)

// staticTLS is the source ConfigBuilder.WithTLS makes.
type staticTLS struct{ config *tls.Config }

func (s staticTLS) TLSConfig() *tls.Config { return s.config }

// TLSFiles names the PEM files of a TLSFileSource or of LoadTLSFiles.
//
// The zero TLSFiles names no files, and NewTLSFileSource and LoadTLSFiles
// reject it with a cql.ValidationError.
type TLSFiles struct {
	// CertFile and KeyFile hold the client certificate chain and its private
	// key. Set both or neither.
	CertFile, KeyFile string
	// CAFile holds the CA certificates that verify nodes. It replaces RootCAs.
	CAFile string
	// Interval is the least time between two checks of the files of a
	// TLSFileSource. Zero means one minute. LoadTLSFiles ignores it.
	Interval time.Duration
}

// TLSFileSource is an endpoint.TLSSource that loads the client certificate and key, and
// the CA certificates that replace RootCAs, from files over a base
// configuration, and reloads them when their content changes. It runs no
// goroutine: a dial checks the files when Interval has passed since the last
// check, so a change reaches the first connection dialed after it. A check
// that fails keeps the current files. It is safe for concurrent use.
//
// Start one with NewTLSFileSource. A zero TLSFileSource names no files: its
// TLSConfig returns nil, so ConfigBuilder.Build rejects it with a
// cql.ValidationError, and Reload returns one.
type TLSFileSource struct {
	current atomic.Pointer[tls.Config]

	// base, sessionCache, err and digest are guarded by mu; files
	// does not change after NewTLSFileSource.

	base *tls.Config
	// sessionCache replaces base's ClientSessionCache after the files changed,
	// so no connection resumes a session of the old identity or trust.
	sessionCache tls.ClientSessionCache
	err          error
	files        TLSFiles
	next         atomic.Int64 // UnixNano of the next due check
	mu           sync.Mutex
	digest       [sha256.Size]byte
}

// LoadTLSFiles loads files once over a copy of base, or over TLS 1.2+
// defaults when base is nil, and returns the result for
// ConfigBuilder.WithTLS: the certificate and key replace Certificates and
// the CA file replaces RootCAs. Verification is as for WithTLS: unless base
// names a ServerName, each connection verifies the node's IP address. It
// fails with a cql.ValidationError if the files cannot be loaded. Use
// NewTLSFileSource instead to reload files that change while sessions run.
func LoadTLSFiles(base *tls.Config, files TLSFiles) (*tls.Config, error) {
	if err := checkTLSFiles("LoadTLSFiles", base, files); err != nil {
		return nil, err
	}
	content, err := implSettings.ReadTLSFiles(files.CertFile, files.KeyFile, files.CAFile)
	if err != nil {
		return nil, err
	}
	return implSettings.BuildTLSFromFiles(base, content)
}

// checkTLSFiles rejects files that name no usable set, as a
// cql.ValidationError whose Field is function.
func checkTLSFiles(function string, base *tls.Config, files TLSFiles) error {
	invalid := func(reason string) error {
		return &cql.ValidationError{Field: function, Err: errors.New(reason)}
	}
	switch {
	case (files.CertFile == "") != (files.KeyFile == ""):
		return invalid("cert and key files must be set together")
	case files.CertFile == "" && files.CAFile == "":
		return invalid("a cert and key pair or a CA file is required")
	case files.Interval < 0:
		return invalid("interval must not be negative")
	case base != nil && files.CertFile != "" && base.GetClientCertificate != nil:
		return invalid("base sets GetClientCertificate, which would ignore CertFile")
	}
	return nil
}

// NewTLSFileSource loads files over a copy of base, or over TLS 1.2+ defaults when
// base is nil, and returns a source serving the result. It fails if the files
// cannot be loaded.
func NewTLSFileSource(base *tls.Config, files TLSFiles) (*TLSFileSource, error) {
	if err := checkTLSFiles("NewTLSFileSource", base, files); err != nil {
		return nil, err
	}
	if files.Interval == 0 {
		files.Interval = time.Minute
	}
	s := &TLSFileSource{files: files}
	if base != nil {
		s.base = implSettings.InternalCloneV2TLSConfig(base)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.reloadLocked(true); err != nil {
		return nil, err
	}
	return s, nil
}

// TLSConfig returns the current configuration, checking the files first when
// Interval has passed. Callers must not modify the result.
func (s *TLSFileSource) TLSConfig() *tls.Config {
	if time.Now().UnixNano() >= s.next.Load() && s.mu.TryLock() {
		_ = s.reloadLocked(false)
		s.mu.Unlock()
	}
	return s.current.Load()
}

// Reload checks the files now. It returns the error that kept the current
// files, or nil when they were applied or had not changed.
func (s *TLSFileSource) Reload() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reloadLocked(false)
}

// Err returns the error of the last check, or nil when it succeeded.
func (s *TLSFileSource) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

func (s *TLSFileSource) reloadLocked(first bool) error {
	if s.files == (TLSFiles{}) { // a zero source, which names no files
		s.err = &cql.ValidationError{Field: "TLSFileSource", Err: errors.New("zero source; start one with config.NewTLSFileSource")}
		return s.err
	}
	s.next.Store(time.Now().Add(s.files.Interval).UnixNano())
	s.err = s.loadLocked(first)
	return s.err
}

func (s *TLSFileSource) loadLocked(first bool) error {
	content, err := implSettings.ReadTLSFiles(s.files.CertFile, s.files.KeyFile, s.files.CAFile)
	if err != nil {
		return err
	}
	digest := content.Digest()
	if !first && digest == s.digest {
		return nil
	}
	config, err := implSettings.BuildTLSFromFiles(s.base, content)
	if err != nil {
		return err
	}
	if !first && s.base != nil && s.base.ClientSessionCache != nil {
		s.sessionCache = tls.NewLRUClientSessionCache(0)
	}
	if s.sessionCache != nil {
		config.ClientSessionCache = s.sessionCache
	}
	s.digest = digest
	s.current.Store(config)
	return nil
}
