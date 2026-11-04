// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	model "github.com/scylladb/gocql/v2/internal/model"
	protocol "github.com/scylladb/gocql/v2/internal/protocol"
)

// optionTemplates are the parameters blocks of resolved options, encoded
// once, indexed by request shape and by whether the request sends a default
// timestamp. The shapes are protocol 4; protocol 5 QUERY, in the keyspace the
// options name or else the session's; and protocol 5 EXECUTE, which sends no
// keyspace. A QUERY in another keyspace, or a request on protocol 3, falls
// back to the template caches.
type optionTemplates [3][2]*model.ParamsTemplate

// newOptionTemplates encodes the parameters blocks of options resolved to
// profile and values, for requests in keyspace.
func newOptionTemplates(profile *Profile, values *OptionValues, keyspace string) *optionTemplates {
	t := new(optionTemplates)
	for shape := range t {
		version := byte(5)
		if shape == 0 {
			version = 4
		}
		for ts := range t[shape] {
			w := attemptRequest{PageSize: int(profile.PageSize), DefaultTimestamp: ts == 1}
			if shape == 1 {
				w.Keyspace = keyspace
			}
			if profile.SerialConsistency.Set {
				w.SerialConsistency = profile.SerialConsistency.Value
			}
			if values.NowInSeconds.Set {
				w.HasNowInSeconds, w.NowInSeconds = true, values.NowInSeconds.Value
			}
			// An error leaves the slot empty: the request encodes its
			// parameters field by field, which reports it.
			t[shape][ts], _ = protocol.NewParamsTemplate(&w, version)
		}
	}
	return t
}

// find returns the template for w on a connection of version, or nil.
func (t *optionTemplates) find(w *attemptRequest, version byte) *model.ParamsTemplate {
	if t == nil || version < 4 || version > 5 {
		return nil
	}
	shape := 0
	if version == 5 {
		shape = 1
		if len(w.PreparedID) > 0 {
			shape = 2
		}
	}
	ts := 0
	if w.DefaultTimestamp {
		ts = 1
	}
	if p := t[shape][ts]; p.Matches(w, version) {
		return p
	}
	return nil
}

// paramsTemplate returns resolved's parameters template for w on a
// connection of version, or nil to encode w field by field: for a batch, on
// protocol 3, for a QUERY outside the options' keyspace, and for options
// resolved for one execution, which encode no templates.
func paramsTemplate(resolved *InternalResolvedOptions, w *attemptRequest, version byte) *model.ParamsTemplate {
	if w.Batch != nil {
		return nil
	}
	return resolved.templates.find(w, version)
}
