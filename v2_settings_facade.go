// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	logging "github.com/scylladb/gocql/v2/internal/logging"
	implSettings "github.com/scylladb/gocql/v2/internal/settings"
)

type AddressPort = implEndpoint.AddressPort
type AddressTranslator = implEndpoint.AddressTranslator
type AddressTranslatorFunc = implEndpoint.AddressTranslatorFunc
type AddressTranslatorFuncV2 = implEndpoint.AddressTranslatorFuncV2
type AddressTranslatorHostInfo = implEndpoint.AddressTranslatorHostInfo
type AddressTranslatorV2 = implEndpoint.AddressTranslatorV2

type AuthExchange = implSettings.AuthExchange
type AuthProvider = implSettings.AuthProvider
type ClientRoutesOptions = implSettings.ClientRoutesOptions
type Config = implSettings.Config
type ConvictionPolicy = implSettings.ConvictionPolicy
type DNSResolver = implEndpoint.DNSResolver

func DefaultConfig(a0 ...string) Config { return implSettings.DefaultConfig(a0...) }

type DefaultDialer = implEndpoint.DefaultDialer
type DialTarget = implEndpoint.DialTarget

type Dialer = implEndpoint.Dialer
type DialerFunc = implEndpoint.DialerFunc

var ErrCannotFindHost = implEndpoint.ErrCannotFindHost
var ErrHostAlreadyExists = implEndpoint.ErrHostAlreadyExists

type IdentityOptions = implSettings.IdentityOptions

func IdentityTranslator() AddressTranslator { return implEndpoint.IdentityTranslator() }

type LifecycleEvent = implSettings.LifecycleEvent
type LifecycleObserver = implSettings.LifecycleObserver

const MetadataDisabled = implSettings.MetadataDisabled
const MetadataFull = implSettings.MetadataFull
const MetadataMinimal = implSettings.MetadataMinimal

type MetadataMode = implSettings.MetadataMode
type MetadataOptions = implSettings.MetadataOptions

func NewSimpleDNSResolver(a0 bool) *SimpleDNSResolver { return implEndpoint.NewSimpleDNSResolver(a0) }

type Observer = implSettings.Observer

func PasswordAuth(a0 string, a1 string) AuthProvider { return implSettings.PasswordAuth(a0, a1) }

type PoolOptions = implSettings.PoolOptions
type Profile = implSettings.Profile
type ProtocolLimits = implSettings.ProtocolLimits
type ReconnectPolicy = implSettings.ReconnectPolicy
type RequestEvent = implSettings.RequestEvent

type SimpleConvictionPolicy = implSettings.SimpleConvictionPolicy
type SimpleDNSResolver = implEndpoint.SimpleDNSResolver
type SpeculationPolicy = implSettings.SpeculationPolicy

type StdLogger = logging.StdLogger
type UnsupportedSettingError = implSettings.UnsupportedSettingError
