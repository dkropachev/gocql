// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package routing

import (
	"github.com/scylladb/gocql/v2/metadata"
)

// Filter returns a policy that routes as p does, without the routes keep
// reports false for: Pick's route when keep rejects it, which sends the
// request to Fallback, and the routes of Fallback's plan, read lazily as
// FilterPlan reads them. The policy implements Validator and Learner,
// forwarding to p when p implements them, so wrapping a *Builtin keeps its
// validation and latency tracking.
func Filter(p Policy, keep func(metadata.Route) bool) Policy {
	return &filterPolicy{combined: combine(p), keep: keep}
}

type filterPolicy struct {
	keep func(metadata.Route) bool
	combined
}

func (f *filterPolicy) Pick(info Info, m *metadata.Snapshot) (metadata.Route, bool) {
	r, ok := f.policies[0].Pick(info, m)
	if !ok || !f.keep(r) {
		return metadata.Route{}, false
	}
	return r, true
}

func (f *filterPolicy) Fallback(info Info, m *metadata.Snapshot) Plan {
	return FilterPlan(f.policies[0].Fallback(info, m), f.keep)
}

// Prefer returns a policy that routes as p does, but tries the routes prefer
// reports true for first, in p's order, and then p's other routes, in p's
// order. Pick returns p's pick only when prefer accepts it, and otherwise
// sends the request to Fallback. Fallback's plan reads p's plan lazily,
// holding back the routes prefer rejects until p's plan has no more. The
// policy implements Validator and Learner as Filter's does.
func Prefer(p Policy, prefer func(metadata.Route) bool) Policy {
	return &preferPolicy{combined: combine(p), prefer: prefer}
}

type preferPolicy struct {
	prefer func(metadata.Route) bool
	combined
}

func (f *preferPolicy) Pick(info Info, m *metadata.Snapshot) (metadata.Route, bool) {
	r, ok := f.policies[0].Pick(info, m)
	if !ok || !f.prefer(r) {
		return metadata.Route{}, false
	}
	return r, true
}

func (f *preferPolicy) Fallback(info Info, m *metadata.Snapshot) Plan {
	return &preferPlan{plan: f.policies[0].Fallback(info, m), prefer: f.prefer}
}

type preferPlan struct {
	plan   Plan
	prefer func(metadata.Route) bool
	// later holds the routes prefer rejected, which follow the plan's
	// preferred routes; done reports that the plan has no more.
	later []metadata.Route
	done  bool
}

func (p *preferPlan) Next() (metadata.Route, bool) {
	for !p.done {
		r, ok := p.plan.Next()
		if !ok {
			p.done = true
			break
		}
		if p.prefer(r) {
			return r, true
		}
		p.later = append(p.later, r)
	}
	if len(p.later) == 0 {
		return metadata.Route{}, false
	}
	r := p.later[0]
	p.later = p.later[1:]
	return r, true
}

// Concat returns a policy that tries the routes of each of policies in turn:
// its Pick is the first policy's, and its Fallback's plan reads the plan of
// each policy lazily, one after the other, skipping a route an earlier plan
// returned. Its Validate fails when any policy that implements Validator
// fails, and its Learn feeds every policy that implements Learner. Concat of
// no policies routes nowhere.
func Concat(policies ...Policy) Policy {
	return &concatPolicy{combined: combine(policies...)}
}

type concatPolicy struct{ combined }

func (c *concatPolicy) Pick(info Info, m *metadata.Snapshot) (metadata.Route, bool) {
	if len(c.policies) == 0 {
		return metadata.Route{}, false
	}
	return c.policies[0].Pick(info, m)
}

func (c *concatPolicy) Fallback(info Info, m *metadata.Snapshot) Plan {
	return &concatPlan{policies: c.policies, info: info, snapshot: m}
}

type concatPlan struct {
	snapshot *metadata.Snapshot
	plan     Plan
	policies []Policy
	seen     []metadata.Route
	info     Info
}

func (p *concatPlan) Next() (metadata.Route, bool) {
	for {
		if p.plan == nil {
			if len(p.policies) == 0 {
				return metadata.Route{}, false
			}
			p.plan = p.policies[0].Fallback(p.info, p.snapshot)
			p.policies = p.policies[1:]
			if p.plan == nil {
				continue
			}
		}
		r, ok := p.plan.Next()
		if !ok {
			p.plan = nil
			continue
		}
		if !p.repeated(r) {
			p.seen = append(p.seen, r)
			return r, true
		}
	}
}

// repeated reports whether the plan already returned r.
func (p *concatPlan) repeated(r metadata.Route) bool {
	for _, s := range p.seen {
		if s == r {
			return true
		}
	}
	return false
}

// combined is the policies a combined policy routes with, and implements
// Validator and Learner for it.
type combined struct{ policies []Policy }

func combine(policies ...Policy) combined {
	return combined{policies: append([]Policy(nil), policies...)}
}

// Validate validates the snapshot with every policy that implements
// Validator, and returns the first error.
func (c combined) Validate(m *metadata.Snapshot) error {
	for _, p := range c.policies {
		if v, ok := p.(Validator); ok {
			if err := v.Validate(m); err != nil {
				return err
			}
		}
	}
	return nil
}

// Learn feeds e to every policy that implements Learner.
func (c combined) Learn(e AttemptEvent) {
	for _, p := range c.policies {
		if l, ok := p.(Learner); ok {
			l.Learn(e)
		}
	}
}

var (
	_ Validator = (*filterPolicy)(nil)
	_ Learner   = (*preferPolicy)(nil)
	_ Validator = (*concatPolicy)(nil)
)
