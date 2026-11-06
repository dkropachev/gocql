// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Package auth authenticates sessions to the cluster.
package auth

import implSettings "github.com/scylladb/gocql/v2/internal/settings"

type (
	Provider = implSettings.AuthProvider
	Exchange = implSettings.AuthExchange
)

// Password authenticates with PasswordAuthenticator credentials.
func Password(username, password string) Provider {
	return implSettings.PasswordAuth(username, password)
}
