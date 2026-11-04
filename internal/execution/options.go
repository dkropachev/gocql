// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"sync/atomic"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
)

// Options is a frozen set of execution options, validated and resolved once
// by NewOptions. The zero Options executes with the session's default
// profile and no overrides. Options never change, so they are safe to share
// between goroutines, queries and sessions.
type Options struct{ p *frozenOptions }

type frozenOptions struct {
	// fixed is the resolution of options that name their own profile, which
	// does not depend on the session.
	fixed *InternalResolvedOptions
	// last is the resolution for the session that used the options last.
	// A resolution is immutable once published.
	last   atomic.Pointer[sessionOptions]
	values OptionValues
}

type sessionOptions struct {
	runtime *InternalSessionRuntime
	value   InternalResolvedOptions
}

// noOptionValues backs the values of the zero Options.
var noOptionValues OptionValues

// NewOptions validates values and freezes them. The custom payload and the
// profile are copied, so later changes to values do not reach the Options.
func NewOptions(values OptionValues) (Options, error) {
	values, err := cloneQueryOptions(values)
	if err != nil {
		return Options{}, err
	}
	if values.Profile != nil {
		profile := *values.Profile
		values.Profile = &profile
	}
	resolved, err := resolveOptionValues(nil, values)
	if err != nil {
		return Options{}, err
	}
	f := &frozenOptions{values: values}
	if values.Profile != nil {
		f.fixed = &resolved
	}
	return Options{p: f}, nil
}

// Values returns a copy of the values o was built from, to derive other
// Options.
func (o Options) Values() OptionValues {
	v := *o.values()
	v.CustomPayload = append([]PayloadEntry(nil), v.CustomPayload...)
	if v.Profile != nil {
		profile := *v.Profile
		v.Profile = &profile
	}
	return v
}

func (o Options) values() *OptionValues {
	if o.p == nil {
		return &noOptionValues
	}
	return &o.p.values
}

// resolve returns o resolved for rt. The zero Options and options with their
// own profile resolve without synchronization; others reuse the resolution
// of the last session that used them.
func (o Options) resolve(rt *InternalSessionRuntime) *InternalResolvedOptions {
	if o.p == nil {
		return &rt.defaultOptions
	}
	if o.p.fixed != nil {
		return o.p.fixed
	}
	if r := o.p.last.Load(); r != nil && r.runtime == rt {
		return &r.value
	}
	// NewOptions validated the values, and the session profile was
	// validated at Connect, so resolution cannot fail here.
	value, _ := resolveOptionValues(rt.InternalProfile, o.p.values)
	r := &sessionOptions{runtime: rt, value: value}
	o.p.last.Store(r)
	return &r.value
}

// InternalResolveFor returns o resolved for session s.
func InternalResolveFor(s *implCluster.Session, o Options) InternalResolvedOptions {
	return *o.resolve(InternalRuntimeFor(s))
}

// InternalSetProfile sets the session's default profile and the resolution
// of the zero Options.
func (rt *InternalSessionRuntime) InternalSetProfile(profile *Profile) {
	rt.InternalProfile = profile
	rt.defaultOptions, _ = resolveOptionValues(profile, OptionValues{})
}
