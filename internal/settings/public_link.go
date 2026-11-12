// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package settings

// Package config defines the public Config and Profile, and package gocql
// consumes them. config cannot export the conversions without making them
// public API, so it registers them here from its init; gocql imports config,
// so the registration runs before gocql uses them. The stored values are
// func(C) ... of config's concrete types: calls assert the func, never box C.
var (
	publicResolve    any
	publicProfileIn  any
	publicProfileOut any
)

// InternalLinkPublic registers package config's conversions. Only package
// config calls it.
func InternalLinkPublic[C, P any](resolve func(C) (Config, error), in func(P) Profile, out func(Profile) P) {
	publicResolve, publicProfileIn, publicProfileOut = resolve, in, out
}

// InternalResolvePublic returns the settings of a public config.Config.
func InternalResolvePublic[C any](c C) (Config, error) {
	return publicResolve.(func(C) (Config, error))(c)
}

// InternalProfileFromPublic converts a public config.Profile.
func InternalProfileFromPublic[P any](p P) Profile {
	return publicProfileIn.(func(P) Profile)(p)
}

// InternalProfileToPublic converts to a public config.Profile.
func InternalProfileToPublic[P any](p Profile) P {
	return publicProfileOut.(func(Profile) P)(p)
}
