// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package endpoint

import "crypto/tls"

// TLSIdentity is the identity one connection's TLS handshake sends and
// checks, over the session's single TLS configuration: the certificates,
// CAs and cipher settings never vary by node, only the names. Return one
// from the function given to config.ConfigBuilder.WithTLSIdentity.
//
// The zero TLSIdentity keeps the defaults: the SNI and the name the
// certificate must match are the tls.Config's ServerName when it names one,
// and otherwise the node's hostname or IP address.
type TLSIdentity struct {
	// Verify, when set, runs after the certificate chain and name checks
	// pass, with the handshake's state: VerifiedChains is filled unless the
	// tls.Config sets InsecureSkipVerify. Use it for checks a name cannot
	// express, such as pinning a key, requiring the node's host ID or IP
	// address in the certificate, or accepting only one datacenter's CA. An
	// error fails this connection only.
	Verify func(tls.ConnectionState) error
	// ServerName is the SNI the handshake sends, such as the name an SNI
	// proxy routes by. Empty keeps the default. An IP address sends no SNI,
	// as crypto/tls never does.
	ServerName string
	// VerifyName is the name the node's certificate must match, a DNS name
	// or an IP address, when it differs from ServerName: a node dialed
	// through a proxy or by address whose certificate names its DNS host.
	// Empty means ServerName, or the default name when ServerName is empty
	// too.
	VerifyName string
	// ChainOnly verifies the certificate chain against the tls.Config's
	// RootCAs but no name, for nodes dialed by an address their
	// certificates do not name. Trust only a private CA with it, or check
	// the node's identity in Verify. VerifyName is then unused.
	ChainOnly bool
}
