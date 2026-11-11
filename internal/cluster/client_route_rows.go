// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/netip"
	"slices"
	"strings"
	"time"

	"github.com/scylladb/gocql/v2/internal/binding"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/events"
	"github.com/scylladb/gocql/v2/internal/metadata"
	"github.com/scylladb/gocql/v2/internal/transport"
)

type v2RouteRow struct {
	connection, host, address string
	port, tlsPort             int
}
type v2RouteID struct{}

func (v2RouteID) CheckType(t cqltype.DataType) error {
	if t.Kind() == cqltype.TypeUUID {
		return codec.UUIDCodec.CheckType(t)
	}
	return (metadata.InternalV2MetadataText{}).CheckType(t)
}
func (v2RouteID) Decode(r codec.ValueReader) (string, error) {
	if r.Type().Kind() == cqltype.TypeUUID {
		return (metadata.InternalV2MetadataUUIDString{}).Decode(r)
	}
	return (metadata.InternalV2MetadataText{}).Decode(r)
}
func (v2RouteID) Clone(v string) string { return v }
func v2DecodeRoutes() codec.RowDecoder[v2RouteRow] {
	return metadata.InternalV2SchemaRows[v2RouteRow]{InternalFields: []metadata.InternalV2SchemaField[v2RouteRow]{
		metadata.InternalV2RequiredSchemaColumn("connection_id", v2RouteID{}, func(r *v2RouteRow) *string { return &r.connection }),
		metadata.InternalV2RequiredSchemaColumn("host_id", v2RouteID{}, func(r *v2RouteRow) *string { return &r.host }),
		metadata.InternalV2RequiredSchemaColumn("address", metadata.InternalV2MetadataText{}, func(r *v2RouteRow) *string { return &r.address }),
		metadata.InternalV2SchemaColumn("port", metadata.InternalV2MetadataInt{}, func(r *v2RouteRow) *int { return &r.port }),
		metadata.InternalV2SchemaColumn("tls_port", metadata.InternalV2MetadataInt{}, func(r *v2RouteRow) *int { return &r.tlsPort }),
	}}
}

type v2RouteParams struct{ columns cqltype.Columns }

func (v2RouteParams) Compile(c cqltype.Columns) (codec.ParamPlan[[][]string], error) {
	return v2RouteParams{c}, nil
}
func (p v2RouteParams) Encode(w *codec.ParamWriter, values [][]string) error {
	if len(values) != p.columns.Len() {
		return cqltype.TypeValidation("route query", "wrong argument count")
	}
	for i, vs := range values {
		c, _ := p.columns.At(i)
		element, _ := c.Type.Element()
		if element.Kind() == cqltype.TypeUUID {
			ids := make([]cqltype.UUID, len(vs))
			for j, v := range vs {
				id, err := cqltype.ParseUUID(v)
				if err != nil {
					return err
				}
				ids[j] = id
			}
			if err := w.WriteParam(codec.ListEncoder(codec.UUIDCodec), ids); err != nil {
				return err
			}
		} else if err := w.WriteParam(codec.ListEncoder(codec.StringCodec), vs); err != nil {
			return err
		}
	}
	return nil
}
func readV2ClientRoutes(ctx context.Context, c InternalControlConnection, table, stmt string, bounds [][]string, tls bool) ([]InternalClientRoute, error) {
	if c == nil {
		return nil, metadata.InternalErrNoControl
	}
	backend, ok := c.(v2SystemQueryer)
	if !ok {
		return nil, cqltype.TypeValidation("client routes", "control does not support typed queries")
	}
	columns := make([]cqltype.ColumnSpec, len(bounds))
	for i, vs := range bounds {
		kind := cqltype.TypeUUID
		for _, v := range vs {
			if _, err := cqltype.ParseUUID(v); err != nil {
				kind = cqltype.TypeVarchar
				break
			}
		}
		typ, _ := cqltype.ScalarType(kind)
		list, _ := cqltype.ListType(typ)
		columns[i] = cqltype.ColumnSpec{Type: list}
	}
	timeout := time.Minute
	if s := c.InternalGetSession(); s != nil && s.privateStorage.InternalCfg.MetadataSchemaRequestTimeout > 0 {
		timeout = s.privateStorage.InternalCfg.MetadataSchemaRequestTimeout
	}
	rows, err := readV2SystemRowsVia(ctx, 4, timeout, backend.InternalQueryV2System, binding.Statement[[][]string, v2RouteRow]{CQL: stmt, Params: v2RouteParams{}, Rows: v2DecodeRoutes()}, cqltype.NewColumns(columns...), bounds)
	if err != nil {
		return nil, fmt.Errorf("error reading %s table: %w", table, err)
	}
	result := make([]InternalClientRoute, 0, len(rows))
	seen := make(map[InternalPair]bool, len(rows))
	for _, r := range rows {
		key := InternalPair{r.connection, r.host}
		if r.connection == "" || r.host == "" || seen[key] || r.address == "" || r.port < 0 || r.port > 65535 || r.tlsPort < 0 || r.tlsPort > 65535 {
			return nil, cqltype.TypeValidation("client route", "invalid/duplicate record %s/%s", r.connection, r.host)
		}
		seen[key] = true
		port := r.port
		if tls {
			port = r.tlsPort
		}
		if port == 0 {
			return nil, cqltype.TypeValidation("client route", "record %s/%s missing required port", r.connection, r.host)
		}
		result = append(result, InternalClientRoute{r.connection, r.host, r.address, uint16(port)})
	}
	return result, nil
}
func (p *ClientRoutesHandler) refreshContext(ctx context.Context) error {
	return p.updateHostPortMappingContext(ctx, InternalUpdateTask{InternalConnectionIDs: p.privateStorage.cfg.Endpoints.GetAllConnectionIDs()})
}
func (p *ClientRoutesHandler) OrderedRoutes(hostID string) []InternalClientRoute {
	p.privateStorage.InternalRouteCache.mu.Lock()
	defer p.privateStorage.InternalRouteCache.mu.Unlock()
	entry, ok := p.privateStorage.InternalRouteCache.routes[hostID]
	if !ok {
		return nil
	}
	out := make([]InternalClientRoute, 0, len(entry.allRoutes))
	if entry.current != nil {
		if index := entry.routeIndex(entry.current.InternalConnectionID); index >= 0 {
			out = append(out, entry.allRoutes[index])
		}
	}
	ids := p.privateStorage.cfg.Endpoints.GetAllConnectionIDs()
	if len(ids) == 0 {
		for _, r := range entry.allRoutes {
			ids = append(ids, r.InternalConnectionID)
		}
	}
	for _, id := range ids {
		if len(out) > 0 && out[0].InternalConnectionID == id {
			continue
		}
		if index := entry.routeIndex(id); index >= 0 {
			out = append(out, entry.allRoutes[index])
		}
	}
	return out
}
func (p *ClientRoutesHandler) BindRoute(route InternalClientRoute) bool {
	p.privateStorage.InternalRouteCache.mu.Lock()
	defer p.privateStorage.InternalRouteCache.mu.Unlock()
	entry, ok := p.privateStorage.InternalRouteCache.routes[route.InternalHostID]
	if !ok {
		return false
	}
	i := entry.routeIndex(route.InternalConnectionID)
	if i < 0 || entry.allRoutes[i] != route {
		return false
	}
	entry.BindCurrent(route.InternalConnectionID)
	p.privateStorage.InternalRouteCache.routes[route.InternalHostID] = entry
	return true
}
func (p *ClientRoutesHandler) resolveRoute(ctx context.Context, route InternalClientRoute) ([]netip.AddrPort, error) {
	if route.InternalPort == 0 {
		return nil, cqltype.TypeValidation("client route", "missing required port")
	}
	name := route.InternalAddress
	if override := p.privateStorage.addrOverrides[route.InternalConnectionID]; override != "" {
		name = override
	}
	addrs, err := p.privateStorage.resolver.LookupNetIP(ctx, "ip", name)
	if err != nil {
		return nil, err
	}
	var out []netip.AddrPort
	for _, a := range addrs {
		if a = a.Unmap(); a.IsValid() && !a.IsUnspecified() {
			out = append(out, netip.AddrPortFrom(a, route.InternalPort))
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no addresses for route %s/%s (%s)", route.InternalHostID, route.InternalConnectionID, name)
	}
	return out, ctx.Err()
}

// Route read publication compares both the connection scope and exact pair
// epochs. Full reads preserve a pair invalidated by an event after read start.
type v2RouteEpochs struct {
	connections map[string]uint64
	pairs       map[InternalPair]uint64
}

func (p *ClientRoutesHandler) CaptureRouteEpochs() v2RouteEpochs {
	p.privateStorage.InternalRouteCache.mu.Lock()
	defer p.privateStorage.InternalRouteCache.mu.Unlock()
	return v2RouteEpochs{maps.Clone(p.privateStorage.connectionEpoch), maps.Clone(p.privateStorage.pairEpoch)}
}
func (p *ClientRoutesHandler) InvalidateRouteTask(task InternalUpdateTask) {
	p.privateStorage.InternalRouteCache.mu.Lock()
	defer p.privateStorage.InternalRouteCache.mu.Unlock()
	if p.privateStorage.connectionEpoch == nil {
		p.privateStorage.connectionEpoch = make(map[string]uint64)
	}
	if p.privateStorage.pairEpoch == nil {
		p.privateStorage.pairEpoch = make(map[InternalPair]uint64)
	}
	if task.InternalPairs != nil {
		for _, key := range task.InternalPairs {
			p.privateStorage.pairEpoch[key]++
		}
	} else {
		for _, id := range task.InternalConnectionIDs {
			p.privateStorage.connectionEpoch[id]++
		}
	}
	touched := make(map[string]bool)
	if task.InternalPairs != nil {
		for _, key := range task.InternalPairs {
			touched[key.InternalHostID] = true
		}
	} else {
		for host, entry := range p.privateStorage.InternalRouteCache.routes {
			for _, route := range entry.allRoutes {
				if slices.Contains(task.InternalConnectionIDs, route.InternalConnectionID) {
					touched[host] = true
					break
				}
			}
		}
	}
	p.advanceRouteHostEpochsLocked(touched)

}
func (p *ClientRoutesHandler) ApplyRouteRead(task InternalUpdateTask, epochs v2RouteEpochs, incoming []InternalClientRoute) bool {
	p.privateStorage.InternalRouteCache.mu.Lock()
	defer p.privateStorage.InternalRouteCache.mu.Unlock()
	valid := func(key InternalPair) bool {
		return p.privateStorage.connectionEpoch[key.InternalConnectionID] == epochs.connections[key.InternalConnectionID] && p.privateStorage.pairEpoch[key] == epochs.pairs[key]
	}
	current := p.privateStorage.InternalRouteCache.snapshotCurrentConnectionIDsLocked()
	keys := slices.Clone(task.InternalPairs)
	if task.InternalPairs == nil {
		for host, entry := range p.privateStorage.InternalRouteCache.routes {
			for _, r := range entry.allRoutes {
				if slices.Contains(task.InternalConnectionIDs, r.InternalConnectionID) {
					keys = append(keys, InternalPair{r.InternalConnectionID, host})
				}
			}
		}
	}
	for _, key := range keys {
		if valid(key) {
			p.privateStorage.InternalRouteCache.deleteByPairsLocked([]InternalPair{key})
		}
	}
	stale := false
	if task.InternalPairs == nil {
		for _, id := range task.InternalConnectionIDs {
			if p.privateStorage.connectionEpoch[id] != epochs.connections[id] {
				stale = true
			}
		}
		for key, revision := range p.privateStorage.pairEpoch {
			if slices.Contains(task.InternalConnectionIDs, key.InternalConnectionID) && revision != epochs.pairs[key] {
				stale = true
			}
		}
	}
	for _, r := range incoming {
		if valid(InternalPair{r.InternalConnectionID, r.InternalHostID}) {
			p.privateStorage.InternalRouteCache.upsertRecordsLocked([]InternalClientRoute{r})
		} else {
			stale = true
		}
	}
	for _, key := range keys {
		if !valid(key) {
			stale = true
		}
	}
	p.privateStorage.InternalRouteCache.rebindCurrentConnectionIDsLocked(current)
	if p.privateStorage.InternalSession != nil && p.privateStorage.InternalSession.privateStorage.InternalHostSource != nil {
		changed := make(map[string]bool)
		for _, r := range incoming {
			changed[r.InternalHostID] = true
		}
		for _, key := range keys {
			changed[key.InternalHostID] = true
		}
		for id := range changed {
			if host := p.privateStorage.InternalSession.privateStorage.InternalHostSource.InternalGetHost(id); host != nil {
				implEndpoint.HostInfoData(host).InternalMu.Lock()
				implEndpoint.HostInfoData(host).InternalTransportGeneration++
				implEndpoint.HostInfoData(host).InternalMu.Unlock()
			}
		}
	}
	return !stale
}
func (p *ClientRoutesHandler) updateHostPortMappingContext(ctx context.Context, task InternalUpdateTask) error {
	epochs := p.CaptureRouteEpochs()
	var rows []InternalClientRoute
	var err error
	switch {
	case task.InternalPairs != nil:
		rows, err = getV2RoutesPairs(ctx, p.privateStorage.c, p.privateStorage.cfg.TableName, task.InternalPairs, p.privateStorage.pickTLSPorts)
	case task.InternalConnectionIDs != nil:
		rows, err = getV2RoutesConnections(ctx, p.privateStorage.c, p.privateStorage.cfg.TableName, task.InternalConnectionIDs, p.privateStorage.pickTLSPorts)
	default:
		return fmt.Errorf("updateTask has neither pairs nor connectionIDs")
	}
	if err != nil {
		if p.privateStorage.InternalSession != nil {
			state := p.privateStorage.InternalSession.Peripheral()
			state.InternalMu.Lock()
			state.routeDiagnostic = fmt.Errorf("client routes retained stale data: %w", err)
			state.InternalMu.Unlock()
			p.privateStorage.InternalSession.PublishV2Metadata()
		}
		return &metadata.PartialRefreshError{Generation: p.routeGeneration(), ClientRoutes: err}
	}
	if !p.ApplyRouteRead(task, epochs, rows) {
		return &metadata.PartialRefreshError{Generation: p.routeGeneration(), ClientRoutes: fmt.Errorf("routes changed during refresh")}
	}
	if p.privateStorage.InternalSession != nil {
		state := p.privateStorage.InternalSession.Peripheral()
		state.InternalMu.Lock()
		state.routeDiagnostic = nil
		state.InternalMu.Unlock()
		if !p.privateStorage.InternalSession.PublishV2Metadata() {
			return p.privateStorage.InternalSession.v2MetadataPublicationError()
		}
		p.privateStorage.InternalSession.PublishV2LocalEvent(Event{Kind: ClientRouteEvent, Generation: p.privateStorage.InternalSession.Metadata().Generation(), Change: "REFRESHED"})
	}
	return nil
}
func (p *ClientRoutesHandler) routeGeneration() uint64 {
	if p.privateStorage.InternalSession != nil {
		return p.privateStorage.InternalSession.Metadata().Generation()
	}
	return 0
}
func getV2RoutesConnections(ctx context.Context, c InternalControlConnection, table string, ids []string, tls bool) ([]InternalClientRoute, error) {
	if len(ids) == 0 {
		return nil, cqltype.TypeValidation("routes", "connection IDs cannot be empty")
	}
	if table == "" {
		table = "system.client_routes"
	}
	if !validV2RouteTable(table) {
		return nil, cqltype.TypeValidation("routes table", "invalid qualified identifier")
	}
	stmt := fmt.Sprintf("select connection_id, host_id, address, port, tls_port from %s where connection_id in ?", table)
	return readV2ClientRoutes(ctx, c, table, stmt, [][]string{ids}, tls)
}
func getV2RoutesPairs(ctx context.Context, c InternalControlConnection, table string, pairs []InternalPair, tls bool) ([]InternalClientRoute, error) {
	if len(pairs) == 0 {
		return nil, cqltype.TypeValidation("routes", "pairs cannot be empty")
	}
	if table == "" {
		table = "system.client_routes"
	}
	if !validV2RouteTable(table) {
		return nil, cqltype.TypeValidation("routes table", "invalid qualified identifier")
	}
	ids, hosts := make([]string, len(pairs)), make([]string, len(pairs))
	for i, p := range pairs {
		ids[i], hosts[i] = p.InternalConnectionID, p.InternalHostID
	}
	stmt := fmt.Sprintf("select connection_id, host_id, address, port, tls_port from %s where connection_id in ? and host_id in ?", table)
	rows, err := readV2ClientRoutes(ctx, c, table, stmt, [][]string{ids, hosts}, tls)
	if err != nil {
		return nil, err
	}
	return slices.DeleteFunc(rows, func(r InternalClientRoute) bool {
		return !slices.Contains(pairs, InternalPair{r.InternalConnectionID, r.InternalHostID})
	}), nil
}
func validV2RouteTable(table string) bool {
	parts := strings.Split(table, ".")
	if len(parts) != 2 {
		return false
	}
	for _, part := range parts {
		if part == "" {
			return false
		}
		for i, c := range part {
			if !(c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || i > 0 && c >= '0' && c <= '9') {
				return false
			}
		}
	}
	return true
}

// Direct internal notification bypasses the best-effort legacy event bus.
func (p *ClientRoutesHandler) handleRouteEvent(event events.Event) {
	p.HandleRouteEventOwned(event, nil)
}
func (p *ClientRoutesHandler) HandleRouteEventOwned(event events.Event, release func()) bool {
	switch e := event.(type) {
	case *events.ControlConnectionRecreatedEvent:
		return p.UpdateHostPortMappingAsync(InternalUpdateTask{InternalConnectionIDs: p.privateStorage.cfg.Endpoints.GetAllConnectionIDs(), InternalRelease: release})
	case *events.ClientRoutesChangedEvent:
		ids := filterAllowedConnectionIDs(e.ConnectionIDs, p.privateStorage.cfg.Endpoints.GetAllConnectionIDs())
		if len(ids) == 0 {
			return false
		}
		task := InternalUpdateTask{InternalConnectionIDs: ids}
		if pairs := getPairsFromEvent(ids, e.HostIDs); len(pairs) > 0 {
			task = InternalUpdateTask{InternalPairs: pairs}
		}
		if e.ChangeType == "DELETED" || e.ChangeType == "DROPPED" {
			p.InvalidateRouteTask(task)
			p.privateStorage.InternalRouteCache.mu.Lock()
			current := p.privateStorage.InternalRouteCache.snapshotCurrentConnectionIDsLocked()
			if task.InternalPairs != nil {
				p.privateStorage.InternalRouteCache.deleteByPairsLocked(task.InternalPairs)
			} else {
				p.privateStorage.InternalRouteCache.deleteByConnectionIDsLocked(task.InternalConnectionIDs)
			}
			p.privateStorage.InternalRouteCache.rebindCurrentConnectionIDsLocked(current)
			p.privateStorage.InternalRouteCache.mu.Unlock()
			if p.privateStorage.InternalSession != nil {
				p.privateStorage.InternalSession.PublishV2Metadata()
				p.privateStorage.InternalSession.emitV2Event(e)
			}
		}
		task.InternalRelease = release
		return p.UpdateHostPortMappingAsync(task)
	}
	return false

}

func (p *ClientRoutesHandler) routeMatches(route InternalClientRoute, epochs v2RouteEpochs) bool {
	p.privateStorage.InternalRouteCache.mu.Lock()
	defer p.privateStorage.InternalRouteCache.mu.Unlock()
	if p.privateStorage.connectionEpoch[route.InternalConnectionID] != epochs.connections[route.InternalConnectionID] || p.privateStorage.pairEpoch[InternalPair{route.InternalConnectionID, route.InternalHostID}] != epochs.pairs[InternalPair{route.InternalConnectionID, route.InternalHostID}] {
		return false
	}
	entry, ok := p.privateStorage.InternalRouteCache.routes[route.InternalHostID]
	if !ok {
		return false
	}
	i := entry.routeIndex(route.InternalConnectionID)
	return i >= 0 && entry.allRoutes[i] == route
}
func (s *Core) dialV2Routes(ctx context.Context, host *implEndpoint.HostInfo, cfg *transport.ConnConfig, handler transport.ConnErrorHandler) (*transport.Conn, error) {
	p := s.privateStorage.InternalClientRoutesHandler
	var causes []error
	epochs := p.captureHostRouteEpochs(host.HostID())
	for _, route := range p.OrderedRoutes(host.HostID()) {
		endpoints, err := p.resolveRoute(ctx, route)
		if err != nil {
			causes = append(causes, err)
			continue
		}
		for _, endpoint := range endpoints {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if !p.routeMatches(route, epochs) {
				return nil, cqltype.TypeValidation("route generation", "route changed while opening")
			}
			implEndpoint.InternalHostInfoSetTranslatedConnectionInfo(host, implEndpoint.InternalTranslatedAddresses{CQL: endpoint})
			conn, err := sessionDialWithoutObserver(s, ctx, host, cfg, handler, 0, 0)
			if err != nil {
				causes = append(causes, err)
				continue
			}
			prior := conn.InternalValidateTarget
			conn.InternalValidateTarget = func(ctx context.Context) error {
				if !p.routeMatches(route, epochs) {
					return cqltype.TypeValidation("route generation", "route changed while opening")
				}
				if prior != nil {
					return prior(ctx)
				}
				return nil
			}
			if !p.routeMatches(route, epochs) || !p.BindRoute(route) {
				conn.Close()
				conn.InternalWaitEventDispatch()
				return nil, cqltype.TypeValidation("route generation", "route superseded during opening")
			}
			return conn, nil
		}
	}
	return nil, fmt.Errorf("no usable client route for host %s: %w", host.HostID(), errors.Join(causes...))
}

func (p *ClientRoutesHandler) captureHostRouteEpochs(hostID string) v2RouteEpochs {
	p.privateStorage.InternalRouteCache.mu.Lock()
	defer p.privateStorage.InternalRouteCache.mu.Unlock()
	out := v2RouteEpochs{connections: make(map[string]uint64), pairs: make(map[InternalPair]uint64)}
	for _, route := range p.privateStorage.InternalRouteCache.routes[hostID].allRoutes {
		out.connections[route.InternalConnectionID] = p.privateStorage.connectionEpoch[route.InternalConnectionID]
		key := InternalPair{route.InternalConnectionID, hostID}
		out.pairs[key] = p.privateStorage.pairEpoch[key]
	}
	return out
}

// Called under the route cache publication gate. The transport insertion gate
// checks the same host epoch under host.mu, closing the validation/store gap.
func (p *ClientRoutesHandler) advanceRouteHostEpochsLocked(hosts map[string]bool) {
	if p.privateStorage.InternalSession == nil || p.privateStorage.InternalSession.privateStorage.InternalHostSource == nil {
		return
	}
	for id := range hosts {
		if host := p.privateStorage.InternalSession.privateStorage.InternalHostSource.InternalGetHost(id); host != nil {
			implEndpoint.HostInfoData(host).InternalMu.Lock()
			implEndpoint.HostInfoData(host).InternalTransportGeneration++
			implEndpoint.HostInfoData(host).InternalMu.Unlock()
		}
	}
}
