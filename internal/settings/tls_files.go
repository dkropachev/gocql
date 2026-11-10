// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package settings

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"os"

	"github.com/scylladb/gocql/v2/internal/cqltype"
)

// TLSFilesContent is one read of a certificate, key and CA file set.
type TLSFilesContent struct {
	Cert, Key, CA []byte
}

// ReadTLSFiles reads every named file; an empty name is skipped.
func ReadTLSFiles(certFile, keyFile, caFile string) (TLSFilesContent, error) {
	var content TLSFilesContent
	read := func(path string, into *[]byte) error {
		if path == "" {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return cqltype.TypeValidation("TLS files", "%w", err)
		}
		*into = data
		return nil
	}
	if err := read(certFile, &content.Cert); err != nil {
		return TLSFilesContent{}, err
	}
	if err := read(keyFile, &content.Key); err != nil {
		return TLSFilesContent{}, err
	}
	if err := read(caFile, &content.CA); err != nil {
		return TLSFilesContent{}, err
	}
	return content, nil
}

// Digest identifies the whole content.
func (c TLSFilesContent) Digest() [sha256.Size]byte {
	return digestParts(c.Cert, c.Key, c.CA)
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
			return nil, cqltype.TypeValidation("TLS files", "cert and key: %w", err)
		}
		config.Certificates = []tls.Certificate{pair}
	}
	if content.CA != nil {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(content.CA) {
			return nil, cqltype.TypeValidation("TLS files", "CA file has no PEM certificates")
		}
		config.RootCAs = pool
	}
	return config, nil
}

// ValidateClientRoutesTLS reports a TLS config that client routes cannot use.
func ValidateClientRoutesTLS(routes *ClientRoutesOptions, config *tls.Config) error {
	if routes != nil && routes.TLS && (config == nil || config.InsecureSkipVerify && config.VerifyConnection == nil && config.VerifyPeerCertificate == nil) {
		return cqltype.TypeValidation("client routes TLS", "verified TLS configuration required")
	}
	return nil
}
