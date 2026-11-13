// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/settings"
)

// CompiledOptions is a frozen set of execution options, validated, resolved and
// encoded once by NewCompiledOptions for one session. The zero CompiledOptions
// executes with the session's default profile and no overrides. CompiledOptions
// never change, so they are safe to share between goroutines and queries.
//
// CompiledOptions resolved for a session carry that session's default profile
// and keyspace. An execution on another session resolves them again for that
// session, on each execution, so they stay correct there but lose the saving.
type CompiledOptions struct{ p *frozenOptions }

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

// noOptionValues backs the values of the zero CompiledOptions.
var noOptionValues Options

// NewCompiledOptions validates values and freezes them, resolved for session, so
// executions use them as they are. CompiledOptions built without a session are
// resolved again on each execution, unless they name their own profile. The
// custom payload is copied, including borrowed bytes, so later changes to it
// do not reach the CompiledOptions. values.Profile is kept, not copied: the
// caller passes a profile it no longer changes, as the public facade does
// when it converts the caller's Profile.
func NewCompiledOptions(session *implCluster.Core, values Options) (CompiledOptions, error) {
	values, err := cloneQueryOptions(values)
	if err != nil {
		return CompiledOptions{}, err
	}
	var owner *InternalSessionRuntime
	if session != nil {
		owner = InternalRuntimeFor(session)
	}
	// Without its own profile, o resolves against the session's.
	sessionProfile := owner != nil && values.Profile == nil
	var base *settings.Profile
	if sessionProfile {
		base = owner.InternalProfile
	}
	resolved, err := resolveOptionValues(base, values)
	if err != nil {
		return CompiledOptions{}, err
	}
	keyspace := values.Keyspace
	if keyspace == "" && owner != nil {
		keyspace = owner.config.Keyspace
	}
	resolved.templates = newOptionTemplates(&resolved.InternalProfile, &resolved.InternalOptions, keyspace)
	bound := values.Profile != nil || sessionProfile
	return CompiledOptions{p: &frozenOptions{rt: owner, bound: bound, resolved: resolved}}, nil
}

// Values returns a copy of the values o was built from, to derive other
// CompiledOptions.
func (o CompiledOptions) Values() Options {
	v := *o.values()
	v.CustomPayload = append([]model.PayloadEntry(nil), v.CustomPayload...)
	if v.Profile != nil {
		profile := *v.Profile
		v.Profile = &profile
	}
	return v
}

func (o CompiledOptions) values() *Options {
	if o.p == nil {
		return &noOptionValues
	}
	return &o.p.resolved.InternalOptions
}

// final reports whether o's resolution holds for rt as it is: o was bound for
// rt's session, or names its own profile without having been bound to a
// session.
func (o CompiledOptions) final(rt *InternalSessionRuntime) bool {
	return o.p.bound && (o.p.rt == rt || o.p.rt == nil)
}

// resolve returns o resolved for rt: the zero CompiledOptions and options bound
// for rt as they are, without synchronization. Other options are resolved
// into scratch.
func (o CompiledOptions) resolve(rt *InternalSessionRuntime, scratch *InternalResolvedOptions) *InternalResolvedOptions {
	if r := o.shared(rt); r != nil {
		return r
	}
	// NewCompiledOptions validated the values, and the session profile was
	// validated at Connect, so resolution cannot fail here.
	*scratch, _ = resolveOptionValues(rt.InternalProfile, o.p.resolved.InternalOptions)
	return scratch
}

// shared returns o resolved for rt when that resolution is shared and needs
// no work: for the zero CompiledOptions and options bound for rt. Otherwise
// it returns nil.
func (o CompiledOptions) shared(rt *InternalSessionRuntime) *InternalResolvedOptions {
	if o.p == nil {
		return &rt.defaultOptions
	}
	if o.final(rt) {
		return &o.p.resolved
	}
	return nil
}

// resolveFixed returns o resolved for rt in storage that stays valid, for a
// query that keeps its options' resolution.
func (o CompiledOptions) resolveFixed(rt *InternalSessionRuntime) *InternalResolvedOptions {
	if o.p == nil || o.final(rt) {
		return o.resolve(rt, nil)
	}
	r := o.resolve(rt, new(InternalResolvedOptions))
	keyspace := r.InternalOptions.Keyspace
	if keyspace == "" {
		keyspace = rt.config.Keyspace
	}
	r.templates = newOptionTemplates(&r.InternalProfile, &r.InternalOptions, keyspace)
	return r
}

// InternalResolveFor returns o resolved for session s.
func InternalResolveFor(s *implCluster.Core, o CompiledOptions) InternalResolvedOptions {
	var scratch InternalResolvedOptions
	return *o.resolve(InternalRuntimeFor(s), &scratch)
}

// InternalSetProfile sets the session's default profile and the resolution
// of the zero CompiledOptions, with templates for the session's keyspace: set the
// configuration first.
func (rt *InternalSessionRuntime) InternalSetProfile(profile *settings.Profile) {
	rt.InternalProfile = profile
	rt.defaultOptions, _ = resolveOptionValues(profile, Options{})
	rt.defaultOptions.templates = newOptionTemplates(&rt.defaultOptions.InternalProfile, &rt.defaultOptions.InternalOptions, rt.config.Keyspace)
}
