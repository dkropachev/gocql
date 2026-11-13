package session

import (
	"maps"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/model"
)

// preparedView is an immutable snapshot of the per-host prepared entries and the
// result metadata they were reconciled against. It is replaced as a whole under
// InternalMu and read without a lock, so a reader always sees a consistent
// {hosts, result, resultHost} triple. The hosts map is never mutated after
// publication.
type preparedView struct {
	hosts      map[cqltype.UUID]*InternalHostPrepared
	result     *model.ResultSchemaIdentity
	resultHost cqltype.UUID
}

var emptyPreparedView = &preparedView{}

// loadView returns the current view; it is never nil.
func (s *InternalPreparedState) loadView() *preparedView {
	if v := s.view.Load(); v != nil {
		return v
	}
	return emptyPreparedView
}

// host returns the prepared entry for a host without taking InternalMu.
func (s *InternalPreparedState) host(id cqltype.UUID) *InternalHostPrepared {
	return s.loadView().hosts[id]
}

// publishHost replaces one host entry. With adopt it also makes result, reported
// by that host, the statement's result metadata. The caller holds InternalMu.
func (s *InternalPreparedState) publishHost(id cqltype.UUID, entry *InternalHostPrepared, adopt bool, result *model.ResultSchemaIdentity) {
	old := s.loadView()
	hosts := make(map[cqltype.UUID]*InternalHostPrepared, len(old.hosts)+1)
	maps.Copy(hosts, old.hosts)
	hosts[id] = entry
	v := &preparedView{hosts: hosts, result: old.result, resultHost: old.resultHost}
	if adopt {
		v.result, v.resultHost = result, id
	}
	s.view.Store(v)
}
