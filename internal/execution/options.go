// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
)

// BoundOptions is a frozen set of execution options, validated, resolved and
// encoded once by NewBoundOptions for one session. The zero BoundOptions
// executes with the session's default profile and no overrides. BoundOptions
// never change, so they are safe to share between goroutines and queries.
//
// BoundOptions resolved for a session carry that session's default profile
// and keyspace. An execution on another session resolves them again for that
// session, on each execution, so they stay correct there but lose the saving.
type BoundOptions struct{ p *frozenOptions }

type frozenOptions struct {
	// rt is the runtime of the session the options were built for, nil when
	// they were built without one.
	rt *InternalSessionRuntime
	// resolved holds the profile merged with the values, the values
	// themselves and their encoded parameters blocks.
	resolved InternalResolvedOptions
	// bound reports that resolved is final: the options were built for a
	// session or name their own profile. Unbound options, built without a
	// session, are resolved again for each execution, and so are bound
	// options executed on a session other than rt.
	bound bool
}

// noOptionValues backs the values of the zero BoundOptions.
var noOptionValues Options

// NewBoundOptions validates values and freezes them, resolved for session, so
// executions use them as they are. BoundOptions built without a session are
// resolved again on each execution, unless they name their own profile. The
// custom payload and the profile are copied, so later changes to values do
// not reach the BoundOptions.
func NewBoundOptions(session *implCluster.Core, values Options) (BoundOptions, error) {
	values, err := cloneQueryOptions(values)
	if err != nil {
		return BoundOptions{}, err
	}
	if values.Profile != nil {
		profile := *values.Profile
		values.Profile = &profile
	}
	var owner *InternalSessionRuntime
	if session != nil {
		owner = InternalRuntimeFor(session)
	}
	// Without its own profile, o resolves against the session's.
	sessionProfile := owner != nil && values.Profile == nil
	var base *Profile
	if sessionProfile {
		base = owner.InternalProfile
	}
	resolved, err := resolveOptionValues(base, values)
	if err != nil {
		return BoundOptions{}, err
	}
	keyspace := values.Keyspace
	if keyspace == "" && owner != nil {
		keyspace = owner.InternalConfig.Keyspace
	}
	resolved.templates = newOptionTemplates(&resolved.InternalProfile, &resolved.InternalOptions, keyspace)
	bound := values.Profile != nil || sessionProfile
	return BoundOptions{p: &frozenOptions{rt: owner, bound: bound, resolved: resolved}}, nil
}

// Values returns a copy of the values o was built from, to derive other
// BoundOptions.
func (o BoundOptions) Values() Options {
	v := *o.values()
	v.CustomPayload = append([]PayloadEntry(nil), v.CustomPayload...)
	if v.Profile != nil {
		profile := *v.Profile
		v.Profile = &profile
	}
	return v
}

func (o BoundOptions) values() *Options {
	if o.p == nil {
		return &noOptionValues
	}
	return &o.p.resolved.InternalOptions
}

// final reports whether o's resolution holds for rt as it is: o was bound for
// rt's session, or names its own profile without having been bound to a
// session.
func (o BoundOptions) final(rt *InternalSessionRuntime) bool {
	return o.p.bound && (o.p.rt == rt || o.p.rt == nil)
}

// resolve returns o resolved for rt: the zero BoundOptions and options bound
// for rt as they are, without synchronization. Other options are resolved
// into scratch.
func (o BoundOptions) resolve(rt *InternalSessionRuntime, scratch *InternalResolvedOptions) *InternalResolvedOptions {
	if o.p == nil {
		return &rt.defaultOptions
	}
	if o.final(rt) {
		return &o.p.resolved
	}
	// NewBoundOptions validated the values, and the session profile was
	// validated at Connect, so resolution cannot fail here.
	*scratch, _ = resolveOptionValues(rt.InternalProfile, o.p.resolved.InternalOptions)
	return scratch
}

// resolveFixed returns o resolved for rt in storage that stays valid, for a
// query that keeps its options' resolution.
func (o BoundOptions) resolveFixed(rt *InternalSessionRuntime) *InternalResolvedOptions {
	if o.p == nil || o.final(rt) {
		return o.resolve(rt, nil)
	}
	r := o.resolve(rt, new(InternalResolvedOptions))
	keyspace := r.InternalOptions.Keyspace
	if keyspace == "" {
		keyspace = rt.InternalConfig.Keyspace
	}
	r.templates = newOptionTemplates(&r.InternalProfile, &r.InternalOptions, keyspace)
	return r
}

// InternalResolveFor returns o resolved for session s.
func InternalResolveFor(s *implCluster.Core, o BoundOptions) InternalResolvedOptions {
	var scratch InternalResolvedOptions
	return *o.resolve(InternalRuntimeFor(s), &scratch)
}

// InternalSetProfile sets the session's default profile and the resolution
// of the zero BoundOptions, with templates for the session's keyspace: set the
// configuration first.
func (rt *InternalSessionRuntime) InternalSetProfile(profile *Profile) {
	rt.InternalProfile = profile
	rt.defaultOptions, _ = resolveOptionValues(profile, Options{})
	rt.defaultOptions.templates = newOptionTemplates(&rt.defaultOptions.InternalProfile, &rt.defaultOptions.InternalOptions, rt.InternalConfig.Keyspace)
}
