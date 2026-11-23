// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

// StartPage returns the state that starts paging from the first page. It is
// the zero PageState.
func StartPage() PageState { return PageState{} }
