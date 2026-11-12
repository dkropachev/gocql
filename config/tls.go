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

	"github.com/scylladb/gocql/v2/cqlerr"
	implSettings "github.com/scylladb/gocql/v2/internal/settings"
)

// TLSSource supplies the TLS configuration of each new connection, so the
// configuration can change while sessions run. The driver calls TLSConfig on
// every dial, from many goroutines, and copies the result; it must be fast and
// must not return nil. Connections already open keep the configuration they
// were dialed with. Pass a source to ConfigBuilder.WithTLSSource;
// TLSFileSource is a ready-made one. Use ConfigBuilder.WithTLS for a fixed
// configuration.
type TLSSource = implSettings.TLSSource

// staticTLS is the source ConfigBuilder.WithTLS makes.
type staticTLS struct{ config *tls.Config }

func (s staticTLS) TLSConfig() *tls.Config { return s.config }

// TLSFiles names the PEM files of a TLSFileSource.
type TLSFiles struct {
	// CertFile and KeyFile hold the client certificate chain and its private
	// key. Set both or neither.
	CertFile, KeyFile string
	// CAFile holds the CA certificates that verify nodes. It replaces RootCAs.
	CAFile string
	// Interval is the least time between two checks of the files. Zero means
	// one minute.
	Interval time.Duration
}

// TLSFileSource is a TLSSource that loads the client certificate and key, and
// the CA certificates that replace RootCAs, from files over a base
// configuration, and reloads them when their content changes. It runs no
// goroutine: a dial checks the files when Interval has passed since the last
// check, so a change reaches the first connection dialed after it. A check
// that fails keeps the current files. It is safe for concurrent use.
type TLSFileSource struct {
	current atomic.Pointer[tls.Config]

	// base, sessionCache, err and digest are guarded by mu; files
	// does not change after NewTLSFiles.

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

// NewTLSFiles loads files over a copy of base, or over TLS 1.2+ defaults when
// base is nil, and returns a source serving the result. It fails if the files
// cannot be loaded.
func NewTLSFiles(base *tls.Config, files TLSFiles) (*TLSFileSource, error) {
	invalid := func(reason string) error {
		return &cqlerr.ValidationError{Field: "NewTLSFiles", Err: errors.New(reason)}
	}
	switch {
	case (files.CertFile == "") != (files.KeyFile == ""):
		return nil, invalid("cert and key files must be set together")
	case files.CertFile == "" && files.CAFile == "":
		return nil, invalid("a cert and key pair or a CA file is required")
	case files.Interval < 0:
		return nil, invalid("interval must not be negative")
	case base != nil && files.CertFile != "" && base.GetClientCertificate != nil:
		return nil, invalid("base sets GetClientCertificate, which would ignore CertFile")
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
