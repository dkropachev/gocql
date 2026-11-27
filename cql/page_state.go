// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cql

// StartPage returns the state that starts paging from the first page. It is
// the zero PageState.
func StartPage() PageState { return PageState{} }

// ParsePageState decodes a cursor PageState.MarshalText encoded, start and
// completion included. Empty text is the start state, so a request without a
// cursor starts paging. Any other input it cannot decode is rejected with a
// ValidationError. The cursor carries the server's paging state: verify it
// before parsing one an untrusted client sent (see PageState).
func ParsePageState(text string) (PageState, error) {
	var p PageState
	err := p.UnmarshalText([]byte(text))
	return p, err
}
