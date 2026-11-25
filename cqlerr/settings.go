// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cqlerr

// UnsupportedSettingError reports a setting that the request, the session or
// its server cannot honor. The driver returns it filled in; its zero value is
// an error without detail.
type UnsupportedSettingError struct {
	// Setting names what to change: the config.ConfigBuilder or
	// config.ProfileBuilder setter that chose the setting, such as
	// "WithCompression" ("New" for the dialect), or the exec.Options or
	// gocql.PrepareOptions field, such as "Keyspace".
	Setting string
	// Reason says why it cannot be honored.
	Reason string
}

// Error names the setting and the reason it cannot be honored.
func (e *UnsupportedSettingError) Error() string {
	return "gocql: unsupported " + e.Setting + ": " + e.Reason
}

// Kind returns KindInvalid.
func (e *UnsupportedSettingError) Kind() Kind { return KindInvalid }
