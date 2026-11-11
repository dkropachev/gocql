// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Package endpoint controls how hosts are addressed and dialed: address
// translation, DNS resolution and custom dialers.
package endpoint

import implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"

type (
	AddressPort        = implEndpoint.AddressPort
	Translator         = implEndpoint.AddressTranslator
	TranslatorFunc     = implEndpoint.AddressTranslatorFunc
	TranslatorHostInfo = implEndpoint.AddressTranslatorHostInfo
	Resolver           = implEndpoint.Resolver
	Dialer             = implEndpoint.Dialer
	DialerFunc         = implEndpoint.DialerFunc
	DefaultDialer      = implEndpoint.DefaultDialer
	DialTarget         = implEndpoint.DialTarget
)

// IdentityTranslator returns a translator that leaves addresses unchanged.
func IdentityTranslator() Translator { return implEndpoint.IdentityTranslator() }
