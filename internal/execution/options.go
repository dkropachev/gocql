// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
)

// Options is a frozen set of execution options, validated, resolved and
// encoded once by NewOptions for one session. The zero Options executes with
// the session's default profile and no overrides. Options never change, so
// they are safe to share between goroutines and queries.
//
// Options resolved for a session carry that session's default profile and
// keyspace, and executions use them as they are without checking which
// session runs them: use them only on the session they were built for.
type Options struct{ p *frozenOptions }

type frozenOptions struct {
	// resolved holds the profile merged with the values, the values
	// themselves and their encoded parameters blocks.
	resolved InternalResolvedOptions
	// bound reports that resolved is final: the options were built for a
	// session or name their own profile. Unbound options, built without a
	// session, are resolved again for each execution.
	bound bool
}

// noOptionValues backs the values of the zero Options.
var noOptionValues OptionValues

// NewOptions validates values and freezes them, resolved for session, so
// executions use them as they are. Options built without a session are
// resolved again on each execution, unless they name their own profile. The
// custom payload and the profile are copied, so later changes to values do
// not reach the Options.
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
	keyspace := values.Keyspace
	if keyspace == "" && session != nil {
		keyspace = InternalRuntimeFor(session).InternalConfig.Keyspace
	}
	resolved.templates = newOptionTemplates(&resolved.InternalProfile, &resolved.InternalOptions, keyspace)
	bound := values.Profile != nil || rt != nil
	return Options{p: &frozenOptions{bound: bound, resolved: resolved}}, nil
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
	return &o.p.resolved.InternalOptions
}

// resolve returns o resolved for rt: the zero Options and bound options as
// they are, without synchronization. Unbound options are resolved into
// scratch.
func (o Options) resolve(rt *InternalSessionRuntime, scratch *InternalResolvedOptions) *InternalResolvedOptions {
	if o.p == nil {
		return &rt.defaultOptions
	}
	if o.p.bound {
		return &o.p.resolved
	}
	// NewOptions validated the values, and the session profile was
	// validated at Connect, so resolution cannot fail here.
	*scratch, _ = resolveOptionValues(rt.InternalProfile, o.p.resolved.InternalOptions)
	return scratch
}

// resolveFixed returns o resolved for rt in storage that stays valid, for a
// query that keeps its options' resolution.
func (o Options) resolveFixed(rt *InternalSessionRuntime) *InternalResolvedOptions {
	if o.p == nil || o.p.bound {
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
func InternalResolveFor(s *implCluster.Session, o Options) InternalResolvedOptions {
	var scratch InternalResolvedOptions
	return *o.resolve(InternalRuntimeFor(s), &scratch)
}

// InternalSetProfile sets the session's default profile and the resolution
// of the zero Options, with templates for the session's keyspace: set the
// configuration first.
func (rt *InternalSessionRuntime) InternalSetProfile(profile *Profile) {
	rt.InternalProfile = profile
	rt.defaultOptions, _ = resolveOptionValues(profile, OptionValues{})
	rt.defaultOptions.templates = newOptionTemplates(&rt.defaultOptions.InternalProfile, &rt.defaultOptions.InternalOptions, rt.InternalConfig.Keyspace)
}
