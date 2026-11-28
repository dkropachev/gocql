// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"sync/atomic"

	"github.com/scylladb/gocql/v2/cql"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/observe"
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
	// layered caches these options laid over the defaults of the view
	// they were last passed to, so that a call passing options to a view
	// with defaults allocates nothing once warm.
	layered atomic.Pointer[layeredOptions]
	// resolved holds the profile merged with the values, the values
	// themselves and their encoded parameters blocks.
	resolved InternalResolvedOptions
	// bound reports that resolved is final: the options were built for a
	// session or name their own profile. Unbound options, built without a
	// session, are resolved again for each execution, and so are bound
	// options executed on a session other than rt.
	bound bool
}

// layeredOptions is call options laid over the defaults of one view: the
// result, or the error the combination fails with.
type layeredOptions struct {
	defaults *frozenOptions
	options  CompiledOptions
	err      error
}

// layeredOver returns o laid over defaults field by field, as the options of a
// call are laid over the defaults of a view made by With: each field o sets
// replaces the one of defaults, and the fields it leaves unset keep those
// of defaults. Either one zero returns the other as it is. The result is
// cached on o for the latest defaults, so a call that passes the same
// options to the same view again allocates nothing. A combination the
// options do not allow, such as an unpaged page size from defaults with a
// prefetch from o, returns the cql.ValidationError compiling it would.
func (o CompiledOptions) layeredOver(defaults CompiledOptions) (CompiledOptions, error) {
	if o.p == nil {
		return defaults, nil
	}
	if defaults.p == nil {
		return o, nil
	}
	if l := o.p.layered.Load(); l != nil && l.defaults == defaults.p {
		return l.options, l.err
	}
	return o.layer(defaults)
}

// layer compiles o over defaults and caches the result on o. It stays out
// of line, off the cached path.
//
//go:noinline
func (o CompiledOptions) layer(defaults CompiledOptions) (CompiledOptions, error) {
	rt := defaults.p.rt
	if rt == nil {
		rt = o.p.rt
	}
	var core *implCluster.Core
	if rt != nil {
		core = rt.Core
	}
	// Both were validated and copied when compiled; the values are shared
	// with them, and NewCompiledOptions copies the payload again.
	layered, err := NewCompiledOptions(core, overlayOptions(defaults.p.resolved.InternalOptions, o.p.resolved.InternalOptions))
	// The call fails before anything is sent.
	err = notSent(err)
	o.p.layered.Store(&layeredOptions{defaults: defaults.p, options: layered, err: err})
	return layered, err
}

// overlayOptions returns over laid on base field by field: each field over
// sets replaces base's, as resolveOptionValues lays options over a profile.
// A cql.Option is set when its Set is true, a policy or keyspace when it is
// not nil or empty, a page size when it is not zero, and a payload when it
// has entries. Observers join, base's first, and the flags IncludeMetadata,
// CQLTrace and NoSessionObserver are set when either sets them. A HostID pin brings its
// ShardID and AnyShardFallback with it, so a pin to another host does not
// keep base's shard.
func overlayOptions(base, over Options) Options {
	out := base
	if over.Retry != nil {
		out.Retry = over.Retry
	}
	if over.Speculation != nil {
		out.Speculation = over.Speculation
	}
	if !over.Observer.IsZero() {
		out.Observer = observe.Join(base.Observer, over.Observer)
	}
	if over.Profile != nil {
		out.Profile = over.Profile
	}
	if over.privateStorage.InternalConnection != nil {
		out.privateStorage.InternalConnection = over.privateStorage.InternalConnection
	}
	out.privateStorage.InternalUntracked = base.privateStorage.InternalUntracked || over.privateStorage.InternalUntracked
	if over.Keyspace != "" {
		out.Keyspace = over.Keyspace
	}
	if len(over.CustomPayload) != 0 {
		out.CustomPayload = over.CustomPayload
	}
	overlayOption(&out.Timeout, over.Timeout)
	overlayOption(&out.PageTimeout, over.PageTimeout)
	overlayOption(&out.Prefetch, over.Prefetch)
	overlayOption(&out.Timestamp, over.Timestamp)
	overlayOption(&out.NowInSeconds, over.NowInSeconds)
	overlayOption(&out.Consistency, over.Consistency)
	overlayOption(&out.SerialConsistency, over.SerialConsistency)
	overlayOption(&out.Idempotent, over.Idempotent)
	if over.PageSize != 0 {
		out.PageSize = over.PageSize
	}
	if over.HostID.Set {
		out.HostID, out.ShardID, out.AnyShardFallback = over.HostID, over.ShardID, over.AnyShardFallback
	}
	out.IncludeMetadata = base.IncludeMetadata || over.IncludeMetadata
	out.CQLTrace = base.CQLTrace || over.CQLTrace
	out.NoSessionObserver = base.NoSessionObserver || over.NoSessionObserver
	return out
}

func overlayOption[T any](dst *cql.Option[T], over cql.Option[T]) {
	if over.Set {
		*dst = over
	}
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
	if owner != nil {
		if err := checkDialectOptions(&owner.config, &values); err != nil {
			return CompiledOptions{}, err
		}
	}
	resolved, err := resolveOptionValues(base, values)
	if err != nil {
		return CompiledOptions{}, err
	}
	// Options compiled for a session find their hooks for it now; others
	// find them for each execution's session.
	if owner != nil && overridesObserver(&values) {
		resolved.observerHooks = executionHooks(owner, &values)
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
	v.CustomPayload = append([]cql.PayloadEntry(nil), v.CustomPayload...)
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
	if o.p.rt == rt {
		scratch.observerHooks = o.p.resolved.observerHooks
	}
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
	if r.observerHooks == nil && overridesObserver(&r.InternalOptions) {
		r.observerHooks = executionHooks(rt, &r.InternalOptions)
	}
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

// checkDialectOptions rejects values that the dialect of a session with
// config cannot honor, before anything is sent: a shard, which only ScyllaDB
// has, and now_in_seconds, which only native protocol 5 carries. The error is
// an UnsupportedSettingError naming the option and the dialect.
func checkDialectOptions(config *settings.Config, values *Options) error {
	if values.ShardID.Set {
		if err := settings.ScyllaOnly(config, "ShardID"); err != nil {
			return err
		}
	}
	if values.NowInSeconds.Set {
		if err := settings.ProtocolV5Only(config, "NowInSeconds"); err != nil {
			return err
		}
	}
	return checkKeyspaceOption(config, values.Keyspace)
}

// checkKeyspaceOption rejects a per-request keyspace other than the
// session's on a protocol 4 dialect, which has no field to send it in.
func checkKeyspaceOption(config *settings.Config, keyspace string) error {
	if keyspace == "" || keyspace == config.Keyspace {
		return nil
	}
	return settings.ProtocolV5Only(config, "Keyspace")
}
