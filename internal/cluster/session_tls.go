// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"crypto/sha256"
	"crypto/tls"
	"sync"
	"sync/atomic"
	"time"

	implSettings "github.com/scylladb/gocql/v2/internal/settings"
)

// InternalSessionTLS is a TLS session's current TLS config. The host dialer
// reads current on every dial; UpdateTLS and the file poller replace it.
type InternalSessionTLS struct {
	current atomic.Pointer[tls.Config]
	routes  *implSettings.ClientRoutesOptions
	files   *implSettings.TLSFiles

	// Fields below are guarded by mu, which also serializes replacements.

	// base is the config the files apply to.
	base *tls.Config
	// sessionCache replaces base's ClientSessionCache after the files changed
	// the client identity, so no connection resumes a session of the old one.
	sessionCache tls.ClientSessionCache
	lastErr      string
	// content is the last file content applied; digest and identity identify
	// all of it and its certificate and key.
	content          implSettings.TLSFilesContent
	mu               sync.Mutex
	digest, identity [sha256.Size]byte
	loaded           bool
}

// newSessionTLS returns the TLS state of a session built from config, or nil
// for a plaintext session. With TLS files it rereads them, so a session
// starts from the files as they are now rather than at Build.
func newSessionTLS(config Config) *InternalSessionTLS {
	if config.TLS == nil {
		return nil
	}
	t := &InternalSessionTLS{routes: config.ClientRoutes, files: config.TLSFiles, base: config.TLSFilesBase}
	t.current.Store(config.TLS)
	if t.files != nil {
		t.mu.Lock()
		_, _ = t.reloadLocked()
		t.mu.Unlock()
	}
	return t
}

// UpdateTLS replaces the TLS configuration of connections the session dials
// from now on. Open connections keep the configuration they were dialed with
// until they close, and reconnects, refills and new shards use the new one.
// config is copied. A session created without TLS cannot start using it, and
// TLS cannot be turned off. With ConfigBuilder.WithTLSFiles, config replaces the
// settings the files apply to, and the loaded certificate and CA still
// override its Certificates and RootCAs.
func (s *Session) UpdateTLS(config *tls.Config) error {
	if config == nil {
		return typeValidation("TLS update", "config is required")
	}
	t := s.privateStorage.tls
	if t == nil {
		return typeValidation("TLS update", "the session was created without TLS")
	}
	if s.Closed() {
		return ErrSessionClosed
	}
	return t.update(config)
}

func (t *InternalSessionTLS) update(config *tls.Config) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.files == nil {
		next := implSettings.InternalCloneV2TLSConfig(config)
		if err := implSettings.ValidateClientRoutesTLS(t.routes, next); err != nil {
			return err
		}
		t.current.Store(next)
		return nil
	}
	if err := implSettings.CheckTLSFilesBase(config, *t.files); err != nil {
		return err
	}
	base := implSettings.InternalCloneV2TLSConfig(config)
	next, err := implSettings.BuildTLSFromFiles(base, t.content)
	if err != nil {
		return err
	}
	if err := implSettings.ValidateClientRoutesTLS(t.routes, next); err != nil {
		return err
	}
	t.base = base
	t.sessionCache = nil
	t.current.Store(next)
	return nil
}

// reloadLocked rereads the files and applies them when their content changed.
// It reports whether the config changed. On error the config is unchanged.
func (t *InternalSessionTLS) reloadLocked() (bool, error) {
	content, err := implSettings.ReadTLSFiles(*t.files)
	if err != nil {
		return false, err
	}
	digest := content.Digest()
	if t.loaded && digest == t.digest {
		return false, nil
	}
	identity := content.IdentityDigest()
	next, err := implSettings.BuildTLSFromFiles(t.base, content)
	if err != nil {
		return false, err
	}
	if err := implSettings.ValidateClientRoutesTLS(t.routes, next); err != nil {
		return false, err
	}
	if t.loaded && identity != t.identity && t.base != nil && t.base.ClientSessionCache != nil {
		t.sessionCache = tls.NewLRUClientSessionCache(0)
	}
	if t.sessionCache != nil {
		next.ClientSessionCache = t.sessionCache
	}
	t.content, t.digest, t.identity, t.loaded = content, digest, identity, true
	t.current.Store(next)
	return true, nil
}

// poll reloads the files every interval until done closes. A failed reload is
// logged once per distinct error and retried on the next tick.
func (t *InternalSessionTLS) poll(done <-chan struct{}, logger StdLogger) {
	ticker := time.NewTicker(t.files.PollInterval())
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
		}
		t.mu.Lock()
		_, err := t.reloadLocked()
		message := ""
		if err != nil {
			message = err.Error()
		}
		report := message != "" && message != t.lastErr
		t.lastErr = message
		t.mu.Unlock()
		if report && logger != nil {
			logger.Printf("gocql: keeping the current TLS config: %v", err)
		}
	}
}

// InternalSessionCurrentTLS returns the TLS config the session's next dial
// uses, or nil for a plaintext session.
func InternalSessionCurrentTLS(s *Session) *tls.Config {
	if s.privateStorage.tls == nil {
		return nil
	}
	return s.privateStorage.tls.current.Load()
}
