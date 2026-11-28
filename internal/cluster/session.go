/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2012, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package cluster

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	mrand "math/rand/v2"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/scylladb/gocql/v2/cql"
	"github.com/scylladb/gocql/v2/cqlerr"
	endpoint2 "github.com/scylladb/gocql/v2/endpoint"
	"github.com/scylladb/gocql/v2/event"
	"github.com/scylladb/gocql/v2/internal/debounce"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/eventbus"
	"github.com/scylladb/gocql/v2/internal/events"
	logging "github.com/scylladb/gocql/v2/internal/logging"
	"github.com/scylladb/gocql/v2/internal/metadata"
	"github.com/scylladb/gocql/v2/internal/pool"
	"github.com/scylladb/gocql/v2/internal/protocol"
	implSettings "github.com/scylladb/gocql/v2/internal/settings"
	"github.com/scylladb/gocql/v2/internal/transport"
	"github.com/scylladb/gocql/v2/internal/wire"
	metadata2 "github.com/scylladb/gocql/v2/metadata"
)

// Core owns cluster metadata, pooled connections and typed executions. It
// is safe for concurrent use. The public Session in internal/session wraps
// it; Connect constructs validated sessions.
func (s *Core) logger() *slog.Logger { return logging.Or(s.privateStorage.InternalLogger) }

type Core struct {
	privateStorage SessionStorage

	// id is a globally unique identifier for this session, reported to
	// the cluster via the SESSION_ID STARTUP option so that all connections
	// belonging to the same session can be correlated in system.clients.

}

func newSessionCommon(cfg InternalClusterSettings) (*Core, error) {
	return newSessionCommonWithRuntime(cfg, nil, nil)
}

func newSessionCommonWithRuntime(cfg InternalClusterSettings, typedConfig *implSettings.Config, initialize func(*Core, implSettings.Config) *implSettings.Config) (*Core, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("gocql: unable to create session: cluster config validation failed: %w", err)
	}

	// The session context lives until Close, so it cannot derive from the
	// caller's connect context; Connect cancels it through context.AfterFunc
	// if that context ends during startup.
	ctx, cancel := context.WithCancel(context.Background())

	// Every record the session logs names it, so that the records of
	// several sessions in one process can be told apart.
	id := newSessionID(cfg.logger())
	if cfg.Logger != nil {
		cfg.Logger = cfg.Logger.With(logging.KeySession, id.String())
	}

	s := func() *Core {
		opaqueStorageValue := new(Core)
		opaqueStorageValue.privateStorage.InternalCfg = cfg
		opaqueStorageValue.privateStorage.connectObserver = cfg.ConnectObserver
		opaqueStorageValue.privateStorage.InternalCtx = ctx
		opaqueStorageValue.privateStorage.InternalCancel = cancel
		opaqueStorageValue.privateStorage.InternalLogger = cfg.logger()
		opaqueStorageValue.privateStorage.InternalAddressTranslator = cfg.AddressTranslator
		opaqueStorageValue.privateStorage.readyCh = make(chan struct{}, 1)
		return opaqueStorageValue
	}()

	if cfg.ClientRoutesConfig != nil {
		s.privateStorage.InternalClientRoutesHandler = NewClientRoutesAddressTranslator(*cfg.ClientRoutesConfig, s.privateStorage.InternalCfg.DNSResolver, s.privateStorage.InternalCfg.SslOpts != nil, s.privateStorage.InternalLogger)
		s.privateStorage.InternalAddressTranslator = s.privateStorage.InternalClientRoutesHandler
	}

	if !cfg.DisableDriverConfigReporting {
		s.privateStorage.driverConfigReporter = newDriverConfigReporter(s)
	}
	s.privateStorage.id = id

	// Close created resources on error otherwise they'll leak
	var err error
	defer func() {
		if err != nil {
			s.Close()
		}
	}()

	// Capture typed settings and attach execution before publishing pool services
	// or starting any coordinator worker. Every host pool receives this snapshot.
	var runtimeConfig *implSettings.Config
	if typedConfig != nil {
		runtimeConfig = initialize(s, *typedConfig)
	}
	//Check the TLS Config before trying to connect to anything external
	connCfg, err := connConfig(&s.privateStorage.InternalCfg)
	if err != nil {
		return nil, fmt.Errorf("gocql: unable to create session: %w", err)
	}
	s.privateStorage.InternalConnCfg = connCfg
	if runtimeConfig != nil {
		connCfg.InternalReceiveCache = new(wire.ReceiveCache)
		connCfg.InternalOrphans = s.OrphanCounter()
		connCfg.TypedConfig = (*runtimeConfig).ConnectivityPointer()
		connCfg.TypedAuth = runtimeConfig.Auth
		connCfg.TypedLimits = runtimeConfig.Limits
		s.ConfigureV2Peripheral(*runtimeConfig)
	}
	s.privateStorage.InternalMetadataDescriber = s.NewMetadataDescriber()
	s.privateStorage.eventBus = eventbus.New[events.Event](cfg.EventBusConfig, cfg.Logger)
	if err = s.privateStorage.eventBus.Start(); err != nil {
		return nil, fmt.Errorf("gocql: unable to create session: %w", err)
	}
	s.privateStorage.InternalNodeEvents = InternalNewEventDebouncer("NodeEvents", func(receiver *Core) func(a0 []protocol.Frame) {
		return func(a0 []protocol.Frame) {
			receiver.HandleNodeEvent(a0)
		}
	}(s), s.privateStorage.InternalLogger)
	s.privateStorage.InternalSchemaEvents = InternalNewEventDebouncer("SchemaEvents", s.handleSchemaEventAndPublish, s.privateStorage.InternalLogger)
	resync := func() {
		s.debounceRingRefresh()
		s.scheduleV2SchemaRetry()
		s.PublishV2LocalEvent(event.Event{Kind: event.KindOverflow, Generation: s.Metadata().Generation(), Change: "CONTROL_RESYNC"})
	}
	s.privateStorage.InternalSchemaEvents.InternalResync = resync
	s.privateStorage.InternalNodeEvents.InternalResync = resync
	s.privateStorage.InternalHostSource = &InternalRingDescriber{InternalCfg: &s.privateStorage.InternalCfg, InternalLogger: s.privateStorage.InternalLogger}
	s.privateStorage.InternalRingRefresher = debounce.NewRefreshDebouncer(debounce.RingRefreshDebounceTime, func() error {
		return s.refreshRing()
	})

	s.privateStorage.InternalPool = newPolicyConnPool(s)
	s.privateStorage.connectObserver = cfg.ConnectObserver
	s.privateStorage.frameObserver = cfg.FrameHeaderObserver
	s.privateStorage.streamObserver = cfg.StreamObserver

	return s, nil
}

// randomUUID returns a version 4 UUID, or the error of the random source.
func randomUUID() (cql.UUID, error) {
	var u cql.UUID
	if _, err := io.ReadFull(rand.Reader, u[:]); err != nil {
		return u, err
	}
	u[6] = u[6]&0x0F | 0x40 // version 4
	u[8] = u[8]&0x3F | 0x80 // IETF variant
	return u, nil
}

// newSessionID generates a globally unique identifier for a session.
//
// Every connection reports one, so this always returns an id rather than
// propagating a failure to generate it.
//
// The fallback is defensive rather than reachable: RandomUUID reads from
// crypto/rand, which since Go 1.24 terminates the process instead of
// returning an error when the system entropy source fails
// (golang/go#66821). Since RandomUUID's signature still admits an error,
// handling it costs little and keeps the id unconditional whatever a future
// implementation does.
func newSessionID(logger *slog.Logger) cql.UUID {
	logger = logging.Or(logger)
	id, err := randomUUID()
	if err != nil {
		logger.Warn("unable to generate a random session id; falling back to a pseudo-random one", logging.Err(err))
		id = pseudoRandomUUID()
	}
	return id
}

// pseudoRandomUUID builds a version 4 UUID from the runtime's pseudo-random
// source, for the case where crypto/rand reports a failure instead of
// aborting the process. A session id merely correlates the connections of one
// session in system.clients and is never a security token, so a weaker source
// of randomness is preferable to no id at all.
func pseudoRandomUUID() cql.UUID {
	var u cql.UUID
	binary.BigEndian.PutUint64(u[:8], mrand.Uint64())
	binary.BigEndian.PutUint64(u[8:], mrand.Uint64())
	u[6] = u[6]&0x0F | 0x40 // set version to 4 (random uuid)
	u[8] = u[8]&0x3F | 0x80 // set to IETF variant
	return u
}

// ID returns the globally unique identifier of this session.
//
// Every connection the session opens reports this value to the cluster as the
// SESSION_ID startup option, where it appears in the
// system.clients.client_options column. Logging it, recording it in a support
// bundle or attaching it as a metric label is what allows client-side
// observations to be matched against those rows, instead of correlating by
// address and port.
//
// The id is assigned when the session is created and never changes, so it is
// safe to call concurrently and safe to read after the session is closed.
func (s *Core) ID() cql.UUID {
	return s.privateStorage.id
}

// connectSettings wraps an existing Node.
func connectSettings(cfg InternalClusterSettings) (*Core, error) {
	s, err := newSessionCommon(cfg)
	if err != nil {
		return nil, err
	}

	if err = s.init(); err != nil {
		if err == ErrNoConnectionsStarted { //nolint:errorlint // the bare sentinel is returned as is for compatibility
			return nil, err
		}
		return nil, fmt.Errorf("gocql: unable to create session: %w", err)
	}
	s.privateStorage.readyCh <- struct{}{}
	close(s.privateStorage.readyCh)

	return s, nil
}

// SubscribeToEvents adds a new subscriber to the event bus.
// name: subscriber name
// queueSize: buffer size for the subscriber events, when buffer is overflowed events are dropped
// filter: optional filter function (can be nil to receive all events)
//
// Returns a Subscriber instance that provides access to events and a Stop method.
func sessionSubscribeToEvents(s *Core, name string, queueSize int, filter eventbus.FilterFunc[events.Event]) *eventbus.Subscriber[events.Event] {
	return s.privateStorage.eventBus.Subscribe(name, queueSize, filter)
}

// connectControlLegacy opens the control connection under the legacy
// InitialReconnectionPolicy.
func (s *Core) connectControlLegacy(hosts []*implEndpoint.HostInfo) error {
	reconnectionPolicy := s.privateStorage.InternalCfg.InitialReconnectionPolicy
	// Validate rejects a non-positive limit, but a custom policy can still
	// return one here; read it once so the loop runs at least once.
	maxRetries := reconnectionPolicy.GetMaxRetries()
	if maxRetries <= 0 {
		return fmt.Errorf("no connection attempted, %T.GetMaxRetries returned %d", reconnectionPolicy, maxRetries)
	}
	var err error
	for i := 0; i < maxRetries; i++ {
		if i != 0 {
			time.Sleep(reconnectionPolicy.GetInterval(i))
		}

		if err = s.privateStorage.InternalControl.InternalConnect(hosts); err != nil {
			s.logger().Debug("control connection attempt failed", "attempt", i, logging.Err(err))
			continue
		}
		break
	}
	return err
}

// connectControlRetrying opens the control connection, asking policy for
// the delay before each new attempt (config.ConfigBuilder.WithConnectRetry).
// An error no retry can fix, rejected credentials or a configuration the
// cluster refuses, ends the attempts at once.
func (s *Core) connectControlRetrying(hosts []*implEndpoint.HostInfo, policy endpoint2.ReconnectPolicy) error {
	for attempt := 1; ; attempt++ {
		err := s.privateStorage.InternalControl.InternalConnect(hosts)
		if err == nil {
			return nil
		}
		s.logger().Debug("control connection attempt failed", "attempt", attempt, logging.Err(err))
		if !connectRetryable(err) {
			return err
		}
		delay, again := policy.Delay(attempt)
		if !again || delay < 0 {
			return err
		}
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-s.privateStorage.InternalCtx.Done():
			timer.Stop()
			return err
		}
	}
}

// connectRetryable reports whether a failed connect may succeed when tried
// again: not when the cluster rejected the credentials, the request or the
// dialect, or the session is gone.
func connectRetryable(err error) bool {
	if _, ok := errors.AsType[*cqlerr.ServerMismatchError](err); ok {
		return false
	}
	switch cqlerr.KindOf(err) {
	case cqlerr.KindUnauthorized, cqlerr.KindInvalid, cqlerr.KindProtocol, cqlerr.KindClosed, cqlerr.KindCanceled:
		return false
	}
	return true
}

func (s *Core) init() error {
	if s.privateStorage.InternalCfg.privateStorage.disableInit {
		return nil
	}

	hosts, err := implEndpoint.InternalResolveInitialEndpoints(s.privateStorage.InternalCfg.DNSResolver, s.privateStorage.InternalCfg.Hosts, s.privateStorage.InternalCfg.Port, s.privateStorage.InternalLogger)
	if err != nil {
		return err
	}

	if !s.privateStorage.InternalCfg.privateStorage.InternalDisableControlConn {
		s.privateStorage.InternalControl = createControlConn(s)
		if retry := s.privateStorage.InternalCfg.ConnectRetry; retry != nil {
			err = s.connectControlRetrying(hosts, retry)
		} else {
			err = s.connectControlLegacy(hosts)
		}
		if err != nil {
			return err
		}

		conn := s.privateStorage.InternalControl.InternalGetConn().InternalConn.(*transport.Conn)
		if err := checkServer(s.privateStorage.InternalCfg.Server, conn); err != nil {
			return err
		}
		s.privateStorage.InternalHostSource.setControlConn(s.privateStorage.InternalControl)

		if s.privateStorage.InternalClientRoutesHandler != nil {
			if err = s.privateStorage.InternalClientRoutesHandler.Initialize(s); err != nil {
				return fmt.Errorf("unable to initialize client routes handler: %w", err)
			}
		}

		if !s.privateStorage.InternalCfg.DisableInitialHostLookup {
			var newHosts []*implEndpoint.HostInfo
			newHosts, _, err = s.privateStorage.InternalHostSource.GetHostsFromSystem()
			if err != nil {
				return err
			}

			hosts = newHosts // filtered nodes remain visible without pools
		} else {
			hosts, err = s.v2ContactIdentities(metadata.InternalV2SchemaContext(s.NewSchemaSource()), hosts)
			if err != nil {
				return err
			}
		}

		newer, _ := checkSystemSchema(s.privateStorage.InternalControl)
		s.privateStorage.InternalUseSystemSchema = newer
		defer conn.InternalFinalizeConnection()
	} else {
		// For testing purposes we populate host ids
		for _, host := range hosts {
			if implEndpoint.HostInfoData(host).InternalHostId.IsZero() {
				implEndpoint.HostInfoData(host).InternalHostId = cql.RandomUUID()
			}
		}
	}

	hostMap := make(map[string]*implEndpoint.HostInfo, len(hosts))
	for _, host := range hosts {
		hostMap[host.HostID()] = host
	}

	hosts = hosts[:0]
	// each host will increment left and decrement it after connecting and once
	// there's none left, we'll close hostCh
	var left int64
	// we will receive up to len(hostMap) of messages so create a buffer so we
	// don't end up stuck in a goroutine if we stopped listening
	connectedCh := make(chan struct{}, len(hostMap))
	// we add one here because we don't want to end up closing hostCh until we're
	// done looping and the decerement code might be reached before we've looped
	// again
	atomic.AddInt64(&left, 1)
	// poolErrs holds why each pool that opened no connection failed.
	var poolErrsMu sync.Mutex
	var poolErrs []cqlerr.HostError
	for _, host := range hostMap {
		host := s.privateStorage.InternalHostSource.InternalAddOrUpdate(host)
		if s.privateStorage.InternalCfg.FilterHost(host) {
			continue
		}

		atomic.AddInt64(&left, 1)
		go func() {
			if err := s.privateStorage.InternalPool.InternalAddHost(host); err != nil {
				poolErrsMu.Lock()
				poolErrs = append(poolErrs, cqlerr.HostError{Address: host.ConnectAddressAndPort(), Err: err})
				poolErrsMu.Unlock()
			}
			connectedCh <- struct{}{}

			// if there are no hosts left, then close the hostCh to unblock the loop
			// below if its still waiting
			if atomic.AddInt64(&left, -1) == 0 {
				close(connectedCh)
			}
		}()

		hosts = append(hosts, host)
	}
	// once we're done looping we subtract the one we initially added and check
	// to see if we should close
	if atomic.AddInt64(&left, -1) == 0 {
		close(connectedCh)
	}

	if s.privateStorage.InternalCfg.privateStorage.InternalDisableControlConn {
		version := s.privateStorage.InternalHostSource.InternalGetHostsList()[0].Version()
		s.privateStorage.InternalUseSystemSchema = version.AtLeast(3, 0, 0)
		s.privateStorage.InternalHasAggregatesAndFunctions = version.AtLeast(2, 2, 0)
	}

	// Wait until every host has tried to connect.
	for range connectedCh {
	}

	s.startReconnectingDownedHosts()

	// The control connection reached a node, but the session still needs a
	// pooled connection to run requests on. Report why each pool failed:
	// the control connection may have reached a node that no pool can, such
	// as a contact point whose certificate was verified by name where the
	// discovered nodes are verified by IP.
	if s.privateStorage.InternalPool.Size() == 0 {
		if len(poolErrs) > 0 {
			slices.SortFunc(poolErrs, func(a, b cqlerr.HostError) int { return strings.Compare(a.Address, b.Address) })
			return &cqlerr.ConnectError{Stage: "connection pools", Hosts: poolErrs}
		}
		return ErrNoConnectionsStarted
	}

	s.privateStorage.InternalSessionStateMu.Lock()
	s.privateStorage.isInitialized = true
	s.privateStorage.InternalSessionStateMu.Unlock()

	return nil
}

// AwaitSchemaAgreement will wait until schema versions across all nodes in the
// cluster are the same (as seen from the point of view of the control connection).
// The maximum amount of time this takes is governed
// by the MaxWaitSchemaAgreement setting in the configuration (default: 60s).
// AwaitSchemaAgreement returns an error in case schema versions are not the same
// after the timeout specified in MaxWaitSchemaAgreement elapses.
func (s *Core) AwaitSchemaAgreement(ctx context.Context) error {
	return s.awaitV2SchemaAgreement(ctx)
}

// SetConsistency sets the default consistency level for this session. This
// setting can also be changed on a per-query basis and the default value
// is Quorum.
func (s *Core) Close() error {
	s.privateStorage.InternalSessionStateMu.Lock()
	if s.privateStorage.InternalIsClosing {
		done := s.privateStorage.closeDone
		s.privateStorage.InternalSessionStateMu.Unlock()
		if done != nil {
			<-done
		}
		return nil
	}
	s.privateStorage.InternalIsClosing = true
	s.privateStorage.closedOrClosing.Store(true)
	s.privateStorage.closeDone = make(chan struct{})
	done := s.privateStorage.closeDone
	s.privateStorage.InternalSessionStateMu.Unlock()
	defer close(done)
	owner := s.CurrentExecution()
	var preparedDrain, activeDrain InternalExecutionWaiter
	if owner != nil {
		preparedDrain = owner.StopPrepared()
		activeDrain = owner.StopActive()
	}
	if s.privateStorage.InternalCancel != nil {
		s.privateStorage.InternalCancel()
	}
	s.CloseV2Peripheral()
	if s.privateStorage.InternalConnCfg != nil {
		s.privateStorage.InternalConnCfg.InternalReceiveCache.Close()
	}

	if s.privateStorage.InternalPool != nil {
		s.privateStorage.InternalPool.Close()
	}

	if s.privateStorage.InternalControl != nil {
		s.privateStorage.InternalControl.InternalClose()
	}

	if s.privateStorage.InternalNodeEvents != nil {
		s.privateStorage.InternalNodeEvents.InternalStop()
	}

	if s.privateStorage.InternalSchemaEvents != nil {
		s.privateStorage.InternalSchemaEvents.InternalStop()
	}

	if s.privateStorage.eventBus != nil {
		_ = s.privateStorage.eventBus.Stop()
	}

	if s.privateStorage.InternalRingRefresher != nil {
		s.privateStorage.InternalRingRefresher.Stop()
	}

	if s.privateStorage.InternalClientRoutesHandler != nil {
		s.privateStorage.InternalClientRoutesHandler.Stop()
	}
	s.privateStorage.InternalSessionStateMu.Lock()
	s.privateStorage.InternalIsClosed = true
	s.privateStorage.closedOrClosing.Store(true)
	s.privateStorage.InternalSessionStateMu.Unlock()
	s.Peripheral().workers.Wait()
	if s.privateStorage.InternalMetadataDescriber != nil && s.privateStorage.InternalMetadataDescriber.InternalMetadata != nil {
		s.privateStorage.InternalMetadataDescriber.InternalMetadata.privateStorage.InternalTabletsMetadata.Close()
	}
	if preparedDrain != nil {
		preparedDrain.Wait()
	}
	if activeDrain != nil {
		activeDrain.Wait()
	}
	return nil
}

func (s *Core) Closed() bool {
	return s.privateStorage.closedOrClosing.Load()
}

func (s *Core) initialized() bool {
	s.privateStorage.InternalSessionStateMu.RLock()
	initialized := s.privateStorage.isInitialized
	s.privateStorage.InternalSessionStateMu.RUnlock()
	return initialized
}

// errSessionNotReady reports use of a session that has not finished
// connecting. Connect returns a session only once it is ready.
var errSessionNotReady = errors.New("gocql: session is not ready yet")

func sessionReady(s *Core) error {
	s.privateStorage.InternalSessionStateMu.RLock()
	err := errSessionNotReady
	if s.privateStorage.isInitialized || s.privateStorage.initErr != nil {
		err = s.privateStorage.initErr
	}
	s.privateStorage.InternalSessionStateMu.RUnlock()
	return err
}

func sessionRemoveHost(s *Core, h *implEndpoint.HostInfo) {
	s.privateStorage.InternalPool.InternalRemoveHost(implEndpoint.InternalHostInfoHostUUID(h))
	s.privateStorage.InternalHostSource.InternalRemoveHost(h.HostID())
}

// TableMetadata returns the schema metadata for the specified table. Returns an error if the keyspace or table does not exist.
func sessionTableMetadata(s *Core, keyspace, table string) (*metadata2.Table, error) {
	if s.Closed() {
		return nil, cqlerr.ErrClosed
	} else if err := sessionReady(s); err != nil {
		return nil, err
	} else if keyspace == "" {
		return nil, cqlerr.ErrNoKeyspace
	} else if table == "" {
		return nil, cqlerr.ErrNoTable
	}

	return s.privateStorage.InternalMetadataDescriber.GetTable(keyspace, table)
}

func (s *Core) GetConn() *transport.Conn {
	hosts := s.privateStorage.InternalHostSource.InternalGetHostsList()
	for _, host := range hosts {
		if !host.IsUp() {
			continue
		}

		pool, ok := s.privateStorage.InternalPool.InternalGetPool(host)
		if !ok {
			continue
		} else if conn := pool.Pick(nil); conn != nil {
			return conn
		}
	}

	return nil
}

// routing key indexes LRU cache
// routingKeyInfoCacheKey avoids concatenating keyspace+stmt into a string on
// every cache lookup; lru.Cache already takes a comparable key type for
// exactly this reason.
func sessionGetHosts(s *Core) []*implEndpoint.HostInfo {
	return s.privateStorage.InternalHostSource.InternalGetHostsList()
}

// GetHostPoolByID returns the connection pool for hostID, or nil if the session
// holds no pool for it -- including when hostID is not a well-formed UUID.
//
// The explicit nil matters: returning the *hostConnPool unconditionally would
// box a nil pointer into a non-nil HostPoolInfo, so a caller's `if pool != nil`
// would pass and the first method call would panic.
func sessionGetHostPoolByID(s *Core, hostID string) pool.HostPoolInfo {
	hostPool, ok := s.privateStorage.InternalPool.InternalGetPoolByHostID(hostID)
	if !ok {
		return nil
	}
	return hostPool
}

type ObservedConnect struct {
	// Host is the information about the host about to connect
	Host *implEndpoint.HostInfo

	Start time.Time // time immediately before the dial is called
	End   time.Time // time immediately after the dial returned

	// Err is the connection error (if any)
	Err error
}

// ConnectObserver is the interface implemented by connect observers / stat collectors.
type ConnectObserver interface {
	// ObserveConnect gets called when a new connection to cassandra is made.
	ObserveConnect(ObservedConnect)
}

type Error struct {
	Message string
	Code    int
}

func (e Error) Error() string {
	return e.Message
}

// MaxBatchStatements is the most statements one BATCH request can carry: the
// native protocol counts them in an unsigned 16-bit field.
const MaxBatchStatements = 65535

// checkServer fails opening a session whose control connection reached a
// server other than the one its dialect names: the dialect fixes how the
// driver reads results that differ between Cassandra and ScyllaDB.
func checkServer(want implSettings.Server, conn *transport.Conn) error {
	if want == implSettings.ServerAny {
		return nil
	}
	got := implSettings.ServerCassandra
	if conn.InternalIsScyllaConn() {
		got = implSettings.ServerScylla
	}
	if got != want {
		return &cqlerr.ServerMismatchError{Want: want.String(), Got: got.String()}
	}
	return nil
}
