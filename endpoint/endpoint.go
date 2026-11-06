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
	TranslatorV2       = implEndpoint.AddressTranslatorV2
	TranslatorFuncV2   = implEndpoint.AddressTranslatorFuncV2
	TranslatorHostInfo = implEndpoint.AddressTranslatorHostInfo
	DNSResolver        = implEndpoint.DNSResolver
	SimpleDNSResolver  = implEndpoint.SimpleDNSResolver
	Dialer             = implEndpoint.Dialer
	DialerFunc         = implEndpoint.DialerFunc
	DefaultDialer      = implEndpoint.DefaultDialer
	DialTarget         = implEndpoint.DialTarget
)

var (
	ErrCannotFindHost    = implEndpoint.ErrCannotFindHost
	ErrHostAlreadyExists = implEndpoint.ErrHostAlreadyExists
)

// IdentityTranslator returns a translator that leaves addresses unchanged.
func IdentityTranslator() Translator { return implEndpoint.IdentityTranslator() }

// NewSimpleDNSResolver returns a resolver backed by the standard library.
func NewSimpleDNSResolver(preferIPv4 bool) *SimpleDNSResolver {
	return implEndpoint.NewSimpleDNSResolver(preferIPv4)
}
