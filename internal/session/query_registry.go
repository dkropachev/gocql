// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package session

import (
	"context"
	"log/slog"
	"runtime"
	"strconv"
	"strings"
	"weak"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
)

// queryEntry records an open reusable query for its session runtime. The
// runtime refers to the query weakly, through the map key, so a query its
// owner dropped without Close is collected; its cleanup then removes the
// entry. A query whose execution still drains stays reachable through the
// transport's references to its input owners, so it is never collected
// mid-drain.
type queryEntry struct {
	// stack is the creation stack, recorded when the session logs at Debug
	// to report a query collected without Close.
	stack   []uintptr
	cleanup runtime.Cleanup
	// pooled reports a pooled query held open by a Rows or Page its caller
	// owns. It counts as busy, so collecting it also ends that count.
	pooled bool
}

// queryRef is the cleanup argument of a registered query. It must not
// reference the query, or the query would never be collected.
type queryRef struct {
	runtime *InternalSessionRuntime
	key     weak.Pointer[queryState]
}

// registerQuery records state as open so that Session.Close stops it.
func (rt *InternalSessionRuntime) registerQuery(state *queryState) {
	rt.addQuery(state, false)
}

// registerHeld records a busy pooled query whose execution a handle holds
// open, so that Session.Close stops the handle and a dropped handle's query
// is accounted for when collected. idle unregisters it.
func (rt *InternalSessionRuntime) registerHeld(state *queryState) {
	rt.addQuery(state, true)
}

func (rt *InternalSessionRuntime) addQuery(state *queryState, pooled bool) {
	key := weak.Make(state)
	entry := &queryEntry{pooled: pooled}
	if rt.debugLogger() != nil {
		entry.stack = queryCreationStack()
	}
	entry.cleanup = runtime.AddCleanup(state, collectQuery, queryRef{runtime: rt, key: key})
	rt.InternalMu.Lock()
	if rt.queries == nil {
		rt.queries = make(map[weak.Pointer[queryState]]*queryEntry)
	}
	rt.queries[key] = entry
	collected := rt.takeCollected()
	rt.InternalMu.Unlock()
	rt.reportCollected(collected)
}

// unregisterQuery removes a closed query's entry and its cleanup.
func (rt *InternalSessionRuntime) unregisterQuery(state *queryState) {
	key := weak.Make(state)
	rt.InternalMu.Lock()
	entry := rt.queries[key]
	delete(rt.queries, key)
	rt.InternalMu.Unlock()
	if entry != nil {
		entry.cleanup.Stop()
	}
}

// openQueries returns the registered queries that are still reachable. The
// caller holds rt.InternalMu.
func (rt *InternalSessionRuntime) openQueries() []*queryState {
	queries := make([]*queryState, 0, len(rt.queries))
	for key := range rt.queries {
		if q := key.Value(); q != nil {
			queries = append(queries, q)
		}
	}
	return queries
}

// collectedQuery is a query collected without Close whose creation the
// session has yet to report.
type collectedQuery struct {
	stack  []uintptr
	pooled bool
}

// collectQuery removes the entry of a query collected without Close. A
// collected pooled query was busy, held by a handle dropped without Close; it
// will never become idle, so its collection ends its busy count instead.
//
// It runs on the runtime's cleanup goroutine, shared by the whole process, so
// it calls no application code: when the query's creation stack was recorded
// for a Debug report, it only queues the report. The next query registration,
// or Session.Close, writes it through the session's logger. Nothing registers
// after Session.Close, so a query collected after Close wrote the last
// reports goes unreported.
//
// A collected query is unreachable, so no Close can race with the cleanup:
// unregisterQuery would have stopped it first.
func collectQuery(ref queryRef) {
	rt := ref.runtime
	rt.InternalMu.Lock()
	entry := rt.queries[ref.key]
	delete(rt.queries, ref.key)
	if entry != nil && entry.stack != nil && !rt.collectedFinal {
		rt.collected = append(rt.collected, collectedQuery{stack: entry.stack, pooled: entry.pooled})
	}
	rt.InternalMu.Unlock()
	if entry != nil && entry.pooled {
		rt.pooledIdle()
	}
}

// takeCollected returns and clears the queued reports of collected queries.
// The caller holds rt.InternalMu.
func (rt *InternalSessionRuntime) takeCollected() []collectedQuery {
	collected := rt.collected
	rt.collected = nil
	return collected
}

// reportCollected writes the Debug reports of collected queries. The caller
// must not hold rt.InternalMu: the logger is application code.
func (rt *InternalSessionRuntime) reportCollected(collected []collectedQuery) {
	if len(collected) == 0 {
		return
	}
	logger := rt.debugLogger()
	if logger == nil {
		return
	}
	for _, c := range collected {
		what := "query"
		if c.pooled {
			what = "Rows or Page"
		}
		logger.Debug(what+" collected without Close", slog.String("created_at", formatQueryStack(c.stack)))
	}
}

// debugLogger returns the session's logger when it logs at Debug, or nil.
func (rt *InternalSessionRuntime) debugLogger() *slog.Logger {
	logger := implCluster.SessionData(rt.Core).InternalLogger
	if logger == nil || !logger.Enabled(context.Background(), slog.LevelDebug) {
		return nil
	}
	return logger
}

func queryCreationStack() []uintptr {
	pcs := make([]uintptr, 32)
	// Skip runtime.Callers, this function, addQuery and its caller
	// (registerQuery or registerHeld), and the caller's caller (newQueryState
	// or hold).
	return pcs[:runtime.Callers(5, pcs)]
}

func formatQueryStack(pcs []uintptr) string {
	var b strings.Builder
	frames := runtime.CallersFrames(pcs)
	for {
		frame, more := frames.Next()
		b.WriteString("\t")
		b.WriteString(frame.Function)
		b.WriteString("\n\t\t")
		b.WriteString(frame.File)
		b.WriteString(":")
		b.WriteString(strconv.Itoa(frame.Line))
		b.WriteString("\n")
		if !more {
			return b.String()
		}
	}
}
