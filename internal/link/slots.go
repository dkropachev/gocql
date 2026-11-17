// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package link

// The slots, by the public package that fills them. Each comment names the
// function type the slot holds.
var (
	// ConfigResolve holds func(config.Config) (settings.Config, error).
	ConfigResolve = NewSlot("config.Config.resolve")
	// ProfileToEngine holds func(config.Profile) settings.Profile.
	ProfileToEngine = NewSlot("config.Profile.toEngine")
	// ProfileFromEngine holds func(settings.Profile) config.Profile.
	ProfileFromEngine = NewSlot("config.Profile.fromEngine")
)
