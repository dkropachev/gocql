// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cqlerr

// UnsupportedSettingError reports a setting that the request, the session or
// its server cannot honor. The driver returns it filled in; its zero value is
// an error without detail.
type UnsupportedSettingError struct{ Setting, Reason string }

func (e *UnsupportedSettingError) Error() string {
	return "gocql: unsupported " + e.Setting + ": " + e.Reason
}

// Kind returns KindInvalid.
func (e *UnsupportedSettingError) Kind() Kind { return KindInvalid }
