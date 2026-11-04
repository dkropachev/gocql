// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
)

// Options is a frozen set of execution options, validated and resolved once
// by NewOptions. The zero Options executes with the session's default
// profile and no overrides. Options never change, so they are safe to share
// between goroutines and queries.
type Options struct{ p *frozenOptions }

type frozenOptions struct {
	// runtime is the session resolved is for; nil when resolved does not
	// depend on the session, because the options name their own profile.
	runtime  *InternalSessionRuntime
	resolved InternalResolvedOptions
	values   OptionValues
	// bound reports that resolved is valid: the options name their own
	// profile or were built for a session.
	bound bool
}

// noOptionValues backs the values of the zero Options.
var noOptionValues OptionValues

// NewOptions validates values and freezes them, resolved for session, so
// executions on session use them as they are. Options built without a
// session, or used on another one, are resolved again on each execution,
// unless they name their own profile. The custom payload and the profile
// are copied, so later changes to values do not reach the Options.
func NewOptions(session *implCluster.Session, values OptionValues) (Options, error) {
	values, err := cloneQueryOptions(values)
	if err != nil {
		return Options{}, err
	}
	if values.Profile != nil {
		profile := *values.Profile
		values.Profile = &profile
	}
	var rt *InternalSessionRuntime
	var base *Profile
	if session != nil && values.Profile == nil {
		rt = InternalRuntimeFor(session)
		base = rt.InternalProfile
	}
	resolved, err := resolveOptionValues(base, values)
	if err != nil {
		return Options{}, err
	}
	bound := values.Profile != nil || rt != nil
	return Options{p: &frozenOptions{runtime: rt, bound: bound, resolved: resolved, values: values}}, nil
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

// resolve returns o resolved for rt: the zero Options, options with their
// own profile and options built for rt as they are, without
// synchronization. Others are resolved into scratch.
func (o Options) resolve(rt *InternalSessionRuntime, scratch *InternalResolvedOptions) *InternalResolvedOptions {
	if o.p == nil {
		return &rt.defaultOptions
	}
	if o.p.bound && (o.p.runtime == nil || o.p.runtime == rt) {
		return &o.p.resolved
	}
	// NewOptions validated the values, and the session profile was
	// validated at Connect, so resolution cannot fail here.
	*scratch, _ = resolveOptionValues(rt.InternalProfile, o.p.values)
	return scratch
}

// resolveFixed returns o resolved for rt in storage that stays valid, for a
// query that keeps its options' resolution.
func (o Options) resolveFixed(rt *InternalSessionRuntime) *InternalResolvedOptions {
	var scratch *InternalResolvedOptions
	if o.p != nil && !(o.p.bound && (o.p.runtime == nil || o.p.runtime == rt)) {
		scratch = new(InternalResolvedOptions)
	}
	return o.resolve(rt, scratch)
}

// InternalResolveFor returns o resolved for session s.
func InternalResolveFor(s *implCluster.Session, o Options) InternalResolvedOptions {
	var scratch InternalResolvedOptions
	return *o.resolve(InternalRuntimeFor(s), &scratch)
}

// InternalSetProfile sets the session's default profile and the resolution
// of the zero Options.
func (rt *InternalSessionRuntime) InternalSetProfile(profile *Profile) {
	rt.InternalProfile = profile
	rt.defaultOptions, _ = resolveOptionValues(profile, OptionValues{})
}
