// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Package structmap holds the type-agnostic rules for mapping Go struct fields
// to CQL columns: tag parsing, embedding and conflict resolution. Package cql
// and cmd/cqlgen share them.
package structmap
