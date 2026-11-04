// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"sync/atomic"

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

// sharedParamsTemplates is how many parameter templates a session shares.
// Queries without their own options resolve to the session profile's, so a
// few slots cover a session's protocol and timestamp combinations.
const sharedParamsTemplates = 8

// paramsTemplateCache is a session's shared parameter templates. Slots are
// replaced round-robin; a template is immutable, so a slot is a plain
// atomic pointer.
type paramsTemplateCache struct {
	slots [sharedParamsTemplates]atomic.Pointer[model.ParamsTemplate]
	next  atomic.Uint32
}

func (c *paramsTemplateCache) find(w *attemptRequest, version byte) *model.ParamsTemplate {
	for i := range c.slots {
		if t := c.slots[i].Load(); t.Matches(w, version) {
			return t
		}
	}
	return nil
}

func (c *paramsTemplateCache) add(t *model.ParamsTemplate) {
	c.slots[c.next.Add(1)%sharedParamsTemplates].Store(t)
}

// ownsParamsTemplate reports whether options set any option a parameters
// template encodes, other than the timestamp, whose value each attempt fills
// in. Such a query's template is its own; others share the session's.
func ownsParamsTemplate(o *OptionValues) bool {
	return o.PageSize.Set || o.SerialConsistency.Set || o.NowInSeconds.Set || o.Keyspace != "" || o.Profile != nil
}

// paramsTemplate returns the parameters template for w on a connection of
// version, or nil to encode w field by field. The resolved options' own
// templates come first, then a query's slot. Options without overrides share the session's templates, so a
// query of the session profile builds none of its own; a query with its own
// options keeps its template in its slot, out of the shared ones. An
// execution with its own options and no query has nowhere to keep one.
func (s *InternalSessionRuntime) paramsTemplate(query *queryState, resolved *InternalResolvedOptions, w *attemptRequest, version byte) *model.ParamsTemplate {
	if w.Batch != nil {
		return nil
	}
	if t := resolved.templates.find(w, version); t != nil {
		return t
	}
	opts := &resolved.InternalOptions
	if query != nil {
		if t := query.params.Load(); t.Matches(w, version) {
			return t
		}
	}
	owned := ownsParamsTemplate(opts)
	if owned && query == nil {
		return nil
	}
	var t *model.ParamsTemplate
	if !owned {
		t = s.paramsTemplates.find(w, version)
	}
	if t == nil {
		var err error
		if t, err = protocol.NewParamsTemplate(w, version); err != nil {
			return nil
		}
		if !owned {
			s.paramsTemplates.add(t)
		}
	}
	if query != nil {
		query.params.Store(t)
	}
	return t
}
