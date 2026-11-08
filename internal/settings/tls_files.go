// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package settings

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"os"
	"time"
)

// TLSFiles names the PEM files a session loads its client certificate and CA
// from, and reloads when their content changes.
type TLSFiles struct {
	// CertFile and KeyFile hold the client certificate chain and its private
	// key. Set both or neither.
	CertFile, KeyFile string
	// CAFile holds the CA certificates that verify nodes. It replaces RootCAs.
	CAFile string
	// Interval is how often the files are checked. Zero means one minute.
	Interval time.Duration
}

// DefaultTLSFilesInterval is the poll period of a zero TLSFiles.Interval.
const DefaultTLSFilesInterval = time.Minute

// Check reports why files cannot be used, or "".
func (files TLSFiles) Check() string {
	switch {
	case (files.CertFile == "") != (files.KeyFile == ""):
		return "cert and key files must be set together"
	case files.CertFile == "" && files.CAFile == "":
		return "a cert and key pair or a CA file is required"
	case files.Interval < 0:
		return "interval must not be negative"
	}
	return ""
}

// PollInterval is the effective poll period.
func (files TLSFiles) PollInterval() time.Duration {
	if files.Interval == 0 {
		return DefaultTLSFilesInterval
	}
	return files.Interval
}

// TLSFilesContent is one read of a TLSFiles set.
type TLSFilesContent struct {
	Cert, Key, CA []byte
}

// ReadTLSFiles reads every named file.
func ReadTLSFiles(files TLSFiles) (TLSFilesContent, error) {
	var content TLSFilesContent
	read := func(path string, into *[]byte) error {
		if path == "" {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return typeValidation("TLS files", "%w", err)
		}
		*into = data
		return nil
	}
	if err := read(files.CertFile, &content.Cert); err != nil {
		return TLSFilesContent{}, err
	}
	if err := read(files.KeyFile, &content.Key); err != nil {
		return TLSFilesContent{}, err
	}
	if err := read(files.CAFile, &content.CA); err != nil {
		return TLSFilesContent{}, err
	}
	return content, nil
}

// Digest identifies the whole content.
func (c TLSFilesContent) Digest() [sha256.Size]byte {
	return digestParts(c.Cert, c.Key, c.CA)
}

// IdentityDigest identifies the client certificate and key only.
func (c TLSFilesContent) IdentityDigest() [sha256.Size]byte {
	return digestParts(c.Cert, c.Key)
}

func digestParts(parts ...[]byte) [sha256.Size]byte {
	h := sha256.New()
	var n [8]byte
	for _, part := range parts {
		binary.BigEndian.PutUint64(n[:], uint64(len(part)))
		h.Write(n[:])
		h.Write(part)
	}
	var sum [sha256.Size]byte
	h.Sum(sum[:0])
	return sum
}

// BuildTLSFromFiles returns a copy of base, or of TLS 1.2+ defaults when base
// is nil, with the certificate and CA from content. base is not modified.
func BuildTLSFromFiles(base *tls.Config, content TLSFilesContent) (*tls.Config, error) {
	var config *tls.Config
	if base == nil {
		config = &tls.Config{MinVersion: tls.VersionTLS12}
	} else {
		config = InternalCloneV2TLSConfig(base)
	}
	if content.Cert != nil {
		pair, err := tls.X509KeyPair(content.Cert, content.Key)
		if err != nil {
			return nil, typeValidation("TLS files", "cert and key: %w", err)
		}
		config.Certificates = []tls.Certificate{pair}
	}
	if content.CA != nil {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(content.CA) {
			return nil, typeValidation("TLS files", "CA file has no PEM certificates")
		}
		config.RootCAs = pool
	}
	return config, nil
}

// CheckTLSFilesBase reports a base TLS config that would override the
// certificate loaded from files.
func CheckTLSFilesBase(base *tls.Config, files TLSFiles) error {
	if base != nil && files.CertFile != "" && base.GetClientCertificate != nil {
		return typeValidation("TLS files", "TLS config sets GetClientCertificate, which would ignore CertFile")
	}
	return nil
}

// ValidateClientRoutesTLS reports a TLS config that client routes cannot use.
func ValidateClientRoutesTLS(routes *ClientRoutesOptions, config *tls.Config) error {
	if routes != nil && routes.TLS && (config == nil || config.InsecureSkipVerify && config.VerifyConnection == nil && config.VerifyPeerCertificate == nil) {
		return typeValidation("client routes TLS", "verified TLS configuration required")
	}
	return nil
}
