// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Package endpoint controls how a session reaches hosts: address
// translation, DNS resolution, custom dialers and TLS, and the policies
// that decide when to reconnect to a node (ReconnectPolicy) and when its
// connection failures mark it down (ConvictionPolicy).
package endpoint
