// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package settings

import "github.com/scylladb/gocql/v2/internal/link"

// Package config defines the public Config and Profile, and package gocql
// consumes them. config cannot export the conversions without making them
// public API, so it registers them in internal/link from its init. The type
// parameters are config's types, which this package cannot import.

// InternalResolvePublic returns the settings of a public config.Config.
func InternalResolvePublic[C any](c C) (Config, error) {
	return link.Get[func(C) (Config, error)](link.ConfigResolve)(c)
}

// InternalProfileFromPublic converts a public config.Profile.
func InternalProfileFromPublic[P any](p P) Profile {
	return link.Get[func(P) Profile](link.ProfileToEngine)(p)
}

// InternalProfileToPublic converts to a public config.Profile.
func InternalProfileToPublic[P any](p Profile) P {
	return link.Get[func(Profile) P](link.ProfileFromEngine)(p)
}
