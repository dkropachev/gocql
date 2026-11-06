// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

// Facade aliases referenced only by tests.

import (
	"context"
	"crypto/tls"
	x509 "crypto/x509"
	"io"
	"net"
	"time"

	binding "github.com/scylladb/gocql/v2/internal/binding"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	codec "github.com/scylladb/gocql/v2/internal/codec"
	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	implExecution "github.com/scylladb/gocql/v2/internal/execution"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	logging "github.com/scylladb/gocql/v2/internal/logging"
	implMetadata "github.com/scylladb/gocql/v2/internal/metadata"
	model "github.com/scylladb/gocql/v2/internal/model"
	paging "github.com/scylladb/gocql/v2/internal/paging"
	pool "github.com/scylladb/gocql/v2/internal/pool"
	protocol "github.com/scylladb/gocql/v2/internal/protocol"
	routing "github.com/scylladb/gocql/v2/internal/routing"
	schemafence "github.com/scylladb/gocql/v2/internal/schemafence"
	"github.com/scylladb/gocql/v2/internal/segment"
	implSettings "github.com/scylladb/gocql/v2/internal/settings"
	implTransport "github.com/scylladb/gocql/v2/internal/transport"
	"github.com/scylladb/gocql/v2/internal/wire"
	"github.com/scylladb/gocql/v2/internal/wirecompress"
	"github.com/scylladb/gocql/v2/retry"
	"github.com/scylladb/gocql/v2/tablets"
)

func retiredAcceptAllFilter() retiredHostFilter { return implCluster.AcceptAllFilter() }

func retiredAvoidSlowReplicas(a0 int) func(policy *tokenAwareHostPolicy) {
	return implCluster.AvoidSlowReplicas(a0)
}

type retiredClientRoutesConfig = implCluster.ClientRoutesConfig

type retiredClientRoutesEndpoint = implCluster.ClientRoutesEndpoint

type retiredClientRoutesEndpointList = implCluster.ClientRoutesEndpointList

type retiredClientRoutesHandler = implCluster.ClientRoutesHandler

type retiredClientRoutesOption = implCluster.ClientRoutesOption

type retiredClusterOption = implCluster.ClusterOption

type retiredConnectObserver = implCluster.ConnectObserver

func retiredDCAwareRoundRobinPolicy(a0 string, a1 ...dcAwarePolicyOption) retiredHostSelectionPolicy {
	return implCluster.DCAwareRoundRobinPolicy(a0, a1...)
}

func retiredDataCenterHostFilter(a0 string) retiredHostFilter {
	return implCluster.DataCenterHostFilter(a0)
}

func retiredDataCentreHostFilter(a0 string) retiredHostFilter {
	return implCluster.DataCentreHostFilter(a0)
}

func retiredDenyAllFilter() retiredHostFilter { return implCluster.DenyAllFilter() }

func retiredDontShuffleReplicas() func(*tokenAwareHostPolicy) {
	return implCluster.DontShuffleReplicas()
}

type retiredHostFilter = implCluster.HostFilter

type retiredHostFilterFunc = implCluster.HostFilterFunc

func retiredHostPolicyOptionDisableDCFailover(a0 dcFailoverDisabledPolicy) {
	implCluster.HostPolicyOptionDisableDCFailover(a0)
}

type retiredHostSelectionPolicy = implCluster.HostSelectionPolicy

type retiredHostStateNotifier = implCluster.HostStateNotifier

type retiredHostTierer = implCluster.HostTierer

type retiredKeyspaceUpdateEvent = implCluster.KeyspaceUpdateEvent

var retiredMAX_IN_FLIGHT_THRESHOLD = implCluster.MAX_IN_FLIGHT_THRESHOLD

type retiredMetadata = implCluster.Metadata

func retiredNewClientRoutesAddressTranslator(a0 retiredClientRoutesConfig, a1 DNSResolver, a2 bool, a3 StdLogger) *retiredClientRoutesHandler {
	return implCluster.NewClientRoutesAddressTranslator(a0, a1, a2, a3)
}

func retiredNewTraceWriter(a0 *Session, a1 io.Writer) *retiredTraceWriter {
	return implCluster.NewTraceWriter(a0, a1)
}

func retiredNewTracer(a0 *Session) *retiredTracerEnhanced { return implCluster.NewTracer(a0) }

type retiredNextHost = implCluster.NextHost

func retiredNonLocalReplicasFallback() func(policy *tokenAwareHostPolicy) {
	return implCluster.NonLocalReplicasFallback()
}

type retiredObservedConnect = implCluster.ObservedConnect

type retiredPoolConfig = implCluster.PoolConfig

func retiredRackAwareRoundRobinPolicy(a0 string, a1 string, a2 ...dcAwarePolicyOption) retiredHostSelectionPolicy {
	return implCluster.RackAwareRoundRobinPolicy(a0, a1, a2...)
}

type retiredReadyPolicy = implCluster.ReadyPolicy

func retiredRoundRobinHostPolicy() retiredHostSelectionPolicy {
	return implCluster.RoundRobinHostPolicy()
}

func retiredShuffleReplicas() func(*tokenAwareHostPolicy) { return implCluster.ShuffleReplicas() }

func retiredSingleHostReadyPolicy(a0 retiredHostSelectionPolicy) *singleHostReadyPolicy {
	return implCluster.SingleHostReadyPolicy(a0)
}

func retiredTokenAwareHostPolicy(a0 retiredHostSelectionPolicy, a1 ...func(*tokenAwareHostPolicy)) retiredHostSelectionPolicy {
	return implCluster.TokenAwareHostPolicy(a0, a1...)
}

type retiredTraceWriter = implCluster.TraceWriter

type retiredTracerEnhanced = implCluster.TracerEnhanced

func retiredWhiteListHostFilter(a0 ...string) retiredHostFilter {
	return implCluster.WhiteListHostFilter(a0...)
}

func retiredWithClientRoutes(a0 ...retiredClientRoutesOption) func(*clusterSettings) {
	return implCluster.WithClientRoutes(a0...)
}

func retiredWithEndpoints(a0 ...retiredClientRoutesEndpoint) func(*retiredClientRoutesConfig) {
	return implCluster.WithEndpoints(a0...)
}

func retiredWithMaxResolverConcurrency(a0 int) func(*retiredClientRoutesConfig) {
	return implCluster.WithMaxResolverConcurrency(a0)
}

func retiredWithResolveHealthyEndpointPeriod(a0 time.Duration) func(*retiredClientRoutesConfig) {
	return implCluster.WithResolveHealthyEndpointPeriod(a0)
}

func retiredWithShardAwareness(a0 bool) func(*retiredClientRoutesConfig) {
	return implCluster.WithShardAwareness(a0)
}

func retiredWithTable(a0 string) func(*retiredClientRoutesConfig) { return implCluster.WithTable(a0) }

type adaptiveOrderingReport = implCluster.InternalAdaptiveOrderingReport

func admitExecution[R any](a0 context.Context, a1 *Session, a2 *implExecution.Bound[R], a3 Options) (context.Context, context.CancelFunc, *executionRequest, error) {
	return implExecution.InternalAdmitExecution[R](a0, a1, a2, a3)
}

var asVectorType = implCluster.InternalAsVectorType

func attemptBytesWritten(a0 error) bool { return implExecution.InternalAttemptBytesWritten(a0) }

func buildConnectionReport(a0 *clusterSettings) connectionReport {
	return implCluster.InternalBuildConnectionReport(a0)
}

func buildControlPlaneReport(a0 *clusterSettings, a1 bool) controlPlaneReport {
	return implCluster.InternalBuildControlPlaneReport(a0, a1)
}

func buildLoadBalancingPolicyReport(a0 retiredHostSelectionPolicy) any {
	return implCluster.InternalBuildLoadBalancingPolicyReport(a0)
}

func buildLoadBalancingReport(a0 retiredHostSelectionPolicy) queryLoadBalancingReport {
	return implCluster.InternalBuildLoadBalancingReport(a0)
}

func buildNodeLocationPreferenceReport(a0 retiredHostSelectionPolicy) any {
	return implCluster.InternalBuildNodeLocationPreferenceReport(a0)
}

func buildQueryDefaultsReport(a0 *clusterSettings) queryDefaultsReport {
	return implCluster.InternalBuildQueryDefaultsReport(a0)
}

func buildQueryReport(a0 *Session) queryReport { return implCluster.InternalBuildQueryReport(a0) }

func buildReconnectionPolicyReport(a0 retiredReconnectionPolicy) any {
	return implCluster.InternalBuildReconnectionPolicyReport(a0)
}

func buildRequestsReport(a0 *clusterSettings) requestsReport {
	return implCluster.InternalBuildRequestsReport(a0)
}

func buildRetryPolicyReport(a0 retry.Policy) any {
	return implCluster.InternalBuildRetryPolicyReport(a0)
}

func buildSocketReport() socketReport { return implCluster.InternalBuildSocketReport() }

func buildTLSReport(a0 *clusterSettings) *tlsReport { return implCluster.InternalBuildTLSReport(a0) }

func buildV2VnodeRanges(a0 *tokenRing, a1 *KeyspaceMetadata) ([]TokenRange, error) {
	return implCluster.InternalBuildV2VnodeRanges(a0, a1)
}

func checkSystemSchema(a0 controlConnection) (bool, error) {
	return implCluster.InternalCheckSystemSchema(a0)
}

type clientRoute = implCluster.InternalClientRoute

type clientRouteCache = implCluster.InternalClientRouteCache

type clientRouteCacheEntry = implCluster.InternalClientRouteCacheEntry

type clusterSettings = implCluster.InternalClusterSettings

type connHost = implCluster.InternalConnHost

func connectSettings(a0 clusterSettings) (*Session, error) {
	return implCluster.InternalConnectSettings(a0)
}

type connectionReport = implCluster.InternalConnectionReport

func consistencyName(a0 Consistency) string { return implCluster.InternalConsistencyName(a0) }

type controlConn = implCluster.InternalControlConn

type controlConnection = implCluster.InternalControlConnection

type controlPlaneQueriesReport = implCluster.InternalControlPlaneQueriesReport

type controlPlaneReport = implCluster.InternalControlPlaneReport

type controlPlaneSchemaReport = implCluster.InternalControlPlaneSchemaReport

type cowHostList = implCluster.InternalCowHostList

func customPolicyName(a0 any) string { return implCluster.InternalCustomPolicyName(a0) }

type dcAwarePolicyOption = implCluster.InternalDcAwarePolicyOption

type dcAwareRR = implCluster.InternalDcAwareRR

type dcFailoverDisabledPolicy = implCluster.InternalDcFailoverDisabledPolicy

const defaultCQLPort = implCluster.InternalDefaultCQLPort

const defaultMaxPreparedStmts = implCluster.InternalDefaultMaxPreparedStmts

type driverConfigReport = implCluster.InternalDriverConfigReport

type driverConfigReporter = implCluster.InternalDriverConfigReporter

const driverConfigStartupKey = implCluster.InternalDriverConfigStartupKey

const driverConfigVersion = implCluster.InternalDriverConfigVersion

func effectiveInFlightMax(a0 int) int { return implCluster.InternalEffectiveInFlightMax(a0) }

const eventBufferSize = implCluster.InternalEventBufferSize

type eventDebouncer = implCluster.InternalEventDebouncer

type executionRequest = implExecution.InternalExecutionRequest

var getApacheCassandraType = implCluster.InternalGetApacheCassandraType

var getCassandraLongType = implCluster.InternalGetCassandraLongType

func getHostPortMappingForConnectionIDs(a0 controlConnection, a1 string, a2 []string, a3 bool) ([]clientRoute, error) {
	return implCluster.InternalGetHostPortMappingForConnectionIDs(a0, a1, a2, a3)
}

func getHostPortMappingForPairs(a0 controlConnection, a1 string, a2 []pair, a3 bool) ([]clientRoute, error) {
	return implCluster.InternalGetHostPortMappingForPairs(a0, a1, a2, a3)
}

func getKeyspaceMetadata(a0 *Session, a1 string) (*KeyspaceMetadata, error) {
	return implCluster.InternalGetKeyspaceMetadata(a0, a1)
}

func getPeersFromQuerySystemPeers(a0 []map[string]any, a1 int, a2 StdLogger) ([]*retiredHostInfo, error) {
	return implCluster.InternalGetPeersFromQuerySystemPeers(a0, a1, a2)
}

func hostInfoFromMap(a0 map[string]any, a1 int) (*retiredHostInfo, error) {
	return implCluster.InternalHostInfoFromMap(a0, a1)
}

type hostInfoList = implCluster.InternalHostInfoList

const hostInfoListMapThreshold = implCluster.InternalHostInfoListMapThreshold

type hostPrepared = implExecution.InternalHostPrepared

type hostSet = implCluster.InternalHostSet

type hostTokens = implCluster.InternalHostTokens

type int64SelectedHost = implCluster.InternalInt64SelectedHost

func isValidPeer(a0 *retiredHostInfo) bool { return implCluster.InternalIsValidPeer(a0) }

func isZeroToken(a0 *retiredHostInfo) bool { return implCluster.InternalIsZeroToken(a0) }

type loadBalancingCustomReport = implCluster.InternalLoadBalancingCustomReport

type loadBalancingTokenAwareReport = implCluster.InternalLoadBalancingTokenAwareReport

const maxPreparedRefreshScopes = implCluster.InternalMaxPreparedRefreshScopes

const maxReportableInFlight = implCluster.InternalMaxReportableInFlight

type metadataDescriber = implCluster.InternalMetadataDescriber

type networkTopology = implCluster.InternalNetworkTopology

func newClientRouteCache() clientRouteCache { return implCluster.InternalNewClientRouteCache() }

func newClusterSettings(a0 ...string) *clusterSettings {
	return implCluster.InternalNewClusterSettings(a0...)
}

func newConnectionHooks(a0 *Session) implTransport.InternalConnectionHooks {
	return implCluster.InternalNewConnectionHooks(a0)
}

func newDriverConfigReporter(a0 *Session) *driverConfigReporter {
	return implCluster.InternalNewDriverConfigReporter(a0)
}

func newEventDebouncer(a0 string, a1 func([]frame), a2 StdLogger) *eventDebouncer {
	return implCluster.InternalNewEventDebouncer(a0, a1, a2)
}

func newHostConnPool(a0 *Session, a1 *retiredHostInfo, a2 int, a3 string) *hostConnPool {
	return implCluster.InternalNewHostConnPool(a0, a1, a2, a3)
}

func newHostInfoList(a0 []*retiredHostInfo) *hostInfoList {
	return implCluster.InternalNewHostInfoList(a0)
}

func newMetadataDescriber(a0 *Session) *metadataDescriber {
	return implCluster.InternalNewMetadataDescriber(a0)
}

func newPage[R any](a0 *responsePage, a1 RowDecoder[R], a2 func()) (*Page[R], error) {
	return implExecution.InternalNewPage[R](a0, a1, a2)
}

func newPageWithPlans[R any](a0 *responsePage, a1 RowDecoder[R], a2 *resultPlanCache[R], a3 func()) (*Page[R], error) {
	return implExecution.InternalNewPageWithPlans[R](a0, a1, a2, a3)
}

func newPoolServices(a0 *Session) poolServices { return implCluster.InternalNewPoolServices(a0) }

func newSchemaSource(a0 *Session) implMetadata.Source { return implCluster.InternalNewSchemaSource(a0) }

func newSessionCommon(a0 clusterSettings) (*Session, error) {
	return implCluster.InternalNewSessionCommon(a0)
}

func newSessionID(a0 StdLogger) string { return implCluster.InternalNewSessionID(a0) }

type nodeLocationDCReport = implCluster.InternalNodeLocationDCReport

type nodeLocationRackReport = implCluster.InternalNodeLocationRackReport

type pair = implCluster.InternalPair

func parseProtocolFromError(a0 error) int { return implCluster.InternalParseProtocolFromError(a0) }

func partitionHealthy(a0 []*retiredHostInfo, a1 *Session) {
	implCluster.InternalPartitionHealthy(a0, a1)
}

func positiveMillis(a0 time.Duration) *int64 { return implCluster.InternalPositiveMillis(a0) }

type prepareCacheKey = implExecution.InternalPrepareCacheKey

type preparedState = implExecution.InternalPreparedState

func pseudoRandomUUID() UUID { return implCluster.InternalPseudoRandomUUID() }

func qualifiedForKeyspace(a0 string, a1 string) bool {
	return implExecution.InternalQualifiedForKeyspace(a0, a1)
}

type queryDefaultsReport = implCluster.InternalQueryDefaultsReport

type queryLoadBalancingReport = implCluster.InternalQueryLoadBalancingReport

type queryReport = implCluster.InternalQueryReport

func readV2SystemRows[P any, R any](a0 context.Context, a1 *driverConn, a2 Statement[P, R], a3 Columns, a4 P) ([]R, error) {
	return implCluster.InternalReadV2SystemRows[P, R](a0, a1, a2, a3, a4)
}

func readV2SystemRowsVia[P any, R any](a0 context.Context, a1 byte, a2 time.Duration, a3 func(context.Context, *attemptRequest) (*responsePage, error), a4 Statement[P, R], a5 Columns, a6 P) ([]R, error) {
	return implCluster.InternalReadV2SystemRowsVia[P, R](a0, a1, a2, a3, a4, a5, a6)
}

type reconnectionConstantReport = implCluster.InternalReconnectionConstantReport

type reconnectionCustomReport = implCluster.InternalReconnectionCustomReport

type reconnectionExponentialReport = implCluster.InternalReconnectionExponentialReport

type requestsReport = implCluster.InternalRequestsReport

func resolveOptions(a0 *Session, a1 Options) (resolvedOptions, error) {
	return implExecution.InternalResolveFor(a0, a1), nil
}

type resolvedOptions = implExecution.InternalResolvedOptions

type resultPlanCache[R any] = implExecution.InternalResultPlanCache[R]

type retryPolicyCustomReport = implCluster.InternalRetryPolicyCustomReport

type retryPolicySimpleReport = implCluster.InternalRetryPolicySimpleReport

type ringDescriber = implCluster.InternalRingDescriber

type roundRobinHostPolicy = implCluster.InternalRoundRobinHostPolicy

func runtimeFor(a0 *Session) *sessionRuntime { return implExecution.InternalRuntimeFor(a0) }

type schemaAgreementReport = implCluster.InternalSchemaAgreementReport

func scyllaIsCdcTable(a0 *Session, a1 string, a2 string) (bool, error) {
	return implCluster.InternalScyllaIsCdcTable(a0, a1, a2)
}

func selectPreparedResponse(a0 *preparedState, a1 UUID, a2 *hostPrepared, a3 []byte, a4 bool, a5 *responsePage, a6 ...uint64) error {
	return implExecution.InternalSelectPreparedResponse(a0, a1, a2, a3, a4, a5, a6...)
}

type selectedHost = implCluster.InternalSelectedHost

type sessionRuntime = implExecution.InternalSessionRuntime

func setupTLSConfig(a0 *retiredSslOptions, a1 StdLogger) (*tls.Config, error) {
	return implCluster.InternalSetupTLSConfig(a0, a1)
}

func shardAwareEnabled(a0 *clusterSettings) bool { return implCluster.InternalShardAwareEnabled(a0) }

type simpleStrategy = implCluster.InternalSimpleStrategy

type singleHostReadyPolicy = implCluster.InternalSingleHostReadyPolicy

type socketReport = implCluster.InternalSocketReport

func strictVerifyPeerCertificate(a0 *x509.CertPool) func([][]byte, [][]*x509.Certificate) error {
	return implCluster.InternalStrictVerifyPeerCertificate(a0)
}

type systemQueriesReport = implCluster.InternalSystemQueriesReport

type systemQueriesTimeoutReport = implCluster.InternalSystemQueriesTimeoutReport

type tlsReport = implCluster.InternalTlsReport

type tokenAwareHostPolicy = implCluster.InternalTokenAwareHostPolicy

type tokenRingReplicas = implCluster.InternalTokenRingReplicas

type updateTask = implCluster.InternalUpdateTask

type v2PendingTabletFeedback = implCluster.InternalV2PendingTabletFeedback

type v2PeripheralState = implCluster.InternalV2PeripheralState

type v2SafeLogger = logging.InternalV2SafeLogger

type v2TabletFeedbackKey = implCluster.InternalV2TabletFeedbackKey

const columnMetadataColumns = implMetadata.InternalColumnMetadataColumns

func compileMetadata(a0 *KeyspaceMetadata, a1 []TableMetadata, a2 []ColumnMetadata, a3 []FunctionMetadata, a4 []AggregateMetadata, a5 []TypeMetadata, a6 []IndexMetadata, a7 []ViewMetadata, a8 []byte) {
	implMetadata.InternalCompileMetadata(a0, a1, a2, a3, a4, a5, a6, a7, a8)
}

var errNoControl = implMetadata.InternalErrNoControl

func getV2CreateStatements(a0 context.Context, a1 implMetadata.Source, a2 string) ([]byte, error) {
	return implMetadata.InternalGetV2CreateStatements(a0, a1, a2)
}

func v2DecodeTopology() RowDecoder[v2TopologyRow] { return implMetadata.InternalV2DecodeTopology() }

type v2TopologyRow = implMetadata.InternalV2TopologyRow

func validateV2CQLType(a0 string, a1 string, a2 map[string]bool) ([]string, error) {
	return implMetadata.InternalValidateV2CQLType(a0, a1, a2)
}

func validateV2SchemaParts(a0 *KeyspaceMetadata, a1 []TableMetadata, a2 []ColumnMetadata, a3 []TypeMetadata, a4 []FunctionMetadata, a5 []AggregateMetadata, a6 []IndexMetadata, a7 []ViewMetadata, a8 ...int) error {
	return implMetadata.InternalValidateV2SchemaParts(a0, a1, a2, a3, a4, a5, a6, a7, a8...)
}

type retiredAuthenticator = implTransport.Authenticator

type driverConn = implTransport.Conn

type driverConnConfig = implTransport.ConnConfig

type retiredFrameHeaderObserver = implTransport.FrameHeaderObserver

type retiredObservedFrameHeader = implTransport.ObservedFrameHeader

type retiredObservedStream = implTransport.ObservedStream

type retiredPasswordAuthenticator = implTransport.PasswordAuthenticator

type retiredSslOptions = implTransport.SslOptions

type retiredStreamObserver = implTransport.StreamObserver

type retiredStreamObserverContext = implTransport.StreamObserverContext

type retiredTracer = implTransport.Tracer

func approve(a0 string, a1 []string) bool { return implTransport.InternalApprove(a0, a1) }

func bodyReadDesyncedConn(a0 error) bool { return implTransport.InternalBodyReadDesyncedConn(a0) }

type callReq = implTransport.InternalCallReq

type callTable = implTransport.InternalCallTable

type callResp = implTransport.InternalCallResp

func checkedV2PoolBudget(a0 PoolOptions, a1 int) (int, error) {
	return implTransport.InternalCheckedV2PoolBudget(a0, a1)
}

type connErrorHandlerFn = implTransport.InternalConnErrorHandlerFn

func decodeStreamRow[R any](a0 *streamResponse, a1 StreamRowPlan[R], a2 *R) (bool, error) {
	return implTransport.InternalDecodeStreamRow[R](a0, a1, a2)
}

type exclusiveConn = implTransport.InternalExclusiveConn

const framerBufEWMAWeight = implTransport.InternalFramerBufEWMAWeight

type framerConfig = implTransport.InternalFramerConfig

func getSchemaAgreement(a0 string, a1 []schemaAgreementHost, a2 StdLogger) error {
	return implTransport.InternalGetSchemaAgreement(a0, a1, a2)
}

type inputOwner = implExecution.InternalInputOwner

func newInputOwner() *inputOwner { return implExecution.InternalNewInputOwner() }

func parseTypedTabletHint(a0 Bytes, a1 string, a2 string) (tablets.TabletInfo, error) {
	return implTransport.InternalParseTypedTabletHint(a0, a1, a2)
}

func parseTypedTabletPayload(a0 Bytes, a1 string, a2 string, a3 bool) (tablets.TabletInfo, error) {
	return implTransport.InternalParseTypedTabletPayload(a0, a1, a2, a3)
}

type preparedDependencyKey = schemafence.InternalPreparedDependencyKey

type protocolError = implTransport.InternalProtocolError

const qrySystemLocal = implTransport.InternalQrySystemLocal

const qrySystemPeers = implTransport.InternalQrySystemPeers

const qrySystemPeersCassandra = implTransport.InternalQrySystemPeersCassandra

const qrySystemPeersV2 = implTransport.InternalQrySystemPeersV2

type responsePage = implTransport.InternalResponsePage

type schemaAgreementHost = implTransport.InternalSchemaAgreementHost

const sessionIDStartupKey = implTransport.InternalSessionIDStartupKey

func startupOptions(a0 string, a1 string, a2 string, a3 retiredApplicationInfo, a4 startupReporter, a5 string, a6 bool) map[string]string {
	return implTransport.InternalStartupOptions(a0, a1, a2, a3, a4, a5, a6)
}

type startupReporter = implTransport.InternalStartupReporter

type streamResponse = implTransport.InternalStreamResponse

type systemRequestState = implTransport.InternalSystemRequestState

func translateAddressPort(a0 AddressTranslator, a1 *retiredHostInfo, a2 AddressPort, a3 StdLogger) (AddressPort, error) {
	return implEndpoint.InternalTranslateAddressPort(a0, a1, a2, a3)
}

func translateHostAddresses(a0 AddressTranslator, a1 *retiredHostInfo, a2 StdLogger) (translatedAddresses, error) {
	return implTransport.InternalTranslateHostAddresses(a0, a1, a2)
}

type typedResultMetadata = model.ResultSchemaIdentity

func typedTabletExtensions(a0 []cqlProtocolExtension, a1 map[string][]string) []cqlProtocolExtension {
	return implEndpoint.InternalTypedTabletExtensions(a0, a1)
}

func useKeyspaceStmt(a0 string) string { return implTransport.InternalUseKeyspaceStmt(a0) }

func validateV2Supported(a0 map[string][]string, a1 PoolOptions) error {
	return implTransport.InternalValidateV2Supported(a0, a1)
}

type verifiedStreamUnits = implTransport.InternalVerifiedStreamUnits

func retiredNewCollectionType(a0 NativeType, a1 TypeInfo, a2 TypeInfo) CollectionType {
	return cqltype.NewCollectionType(a0, a1, a2)
}

func retiredNewCustomType(a0 byte, a1 Type, a2 string) NativeType {
	return cqltype.NewCustomType(a0, a1, a2)
}

func retiredNewNativeType(a0 byte, a1 Type) NativeType { return cqltype.NewNativeType(a0, a1) }

func retiredNewTupleType(a0 NativeType, a1 ...TypeInfo) TupleTypeInfo {
	return cqltype.NewTupleType(a0, a1...)
}

func retiredNewUDTType(a0 byte, a1 string, a2 string, a3 ...UDTField) UDTTypeInfo {
	return cqltype.NewUDTType(a0, a1, a2, a3...)
}

type attemptRequest = model.AttemptRequest

func columnsFromInfo(a0 []ColumnInfo) (Columns, error) { return cqltype.ColumnsFromInfo(a0) }

func dataTypeFromInfo(a0 TypeInfo) (DataType, error) { return cqltype.DataTypeFromInfo(a0) }

type encodedCell = model.EncodedCell

type encodedValues = model.EncodedValues

func finishValueWriter(a0 *ValueWriter) error { return codec.FinishValueWriter(a0) }

func newStreamRowReader(a0 Columns, a1 *wire.Stream) (*StreamRowReader, error) {
	return codec.NewStreamRowReader(a0, a1)
}

func newValueReader(a0 DataType, a1 CellState, a2 wire.Chain, a3 *wire.Lease) ValueReader {
	return codec.NewValueReader(a0, a1, a2, a3)
}

func newValueWriter(a0 *wire.Builder, a1 DataType, a2 bool) *ValueWriter {
	return codec.NewValueWriter(a0, a1, a2)
}

func readValue(a0 *wire.Cursor, a1 DataType, a2 *wire.Lease, a3 bool) (ValueReader, error) {
	return codec.ReadValue(a0, a1, a2, a3)
}

func typeValidation(a0 string, a1 string, a2 ...any) error {
	return cqltype.TypeValidation(a0, a1, a2...)
}

type typedBatchItem = model.TypedBatchItem

type typedBatchRequest = model.TypedBatchRequest

type pagingHistory = paging.InternalPagingHistory

type pagingHistoryEntry = paging.InternalPagingHistoryEntry

type driverConnPicker = pool.ConnPicker

type retiredConstantReconnectionPolicy = pool.ConstantReconnectionPolicy

type retiredExponentialReconnectionPolicy = pool.ExponentialReconnectionPolicy

type retiredHostInformation = implEndpoint.HostInformation

type retiredHostPoolInfo = pool.HostPoolInfo

type retiredNoReconnectionPolicy = pool.NoReconnectionPolicy

type retiredPartitioner = implEndpoint.Partitioner

type retiredReconnectionPolicy = pool.ReconnectionPolicy

type retiredSelectedHost = implEndpoint.SelectedHost

type retiredSetHosts = pool.SetHosts

type retiredSetPartitioner = pool.SetPartitioner

type retiredSetTablets = pool.SetTablets

type retiredToken = implEndpoint.Token

func retiredWrapTLS(a0 context.Context, a1 net.Conn, a2 string, a3 *tls.Config) (*retiredDialedHost, error) {
	return implTransport.WrapTLS(a0, a1, a2, a3)
}

func applyV2SocketOptions(a0 net.Conn, a1 Config, a2 string) error {
	return implTransport.InternalApplyV2SocketOptions(a0, implSettings.InternalConnectivityConfig(a1), a2)
}

type defaultConnPicker = pool.InternalDefaultConnPicker

type hostConnPool = pool.InternalHostConnPool

type hostToken = implEndpoint.InternalHostToken

type int64Hasher = implEndpoint.InternalInt64Hasher

type int64Token = implEndpoint.InternalInt64Token

type murmur3Partitioner = implEndpoint.InternalMurmur3Partitioner

func newDefaultConnPicker(a0 int) *defaultConnPicker { return pool.InternalNewDefaultConnPicker(a0) }

func newScyllaPortIterator(a0 int, a1 int) *scyllaPortIterator {
	return implTransport.InternalNewScyllaPortIterator(a0, a1)
}

func newTokenRing(a0 string, a1 []*retiredHostInfo) (*tokenRing, error) {
	return implEndpoint.InternalNewTokenRing(a0, a1)
}

func newTypedConnPicker(a0 *driverConn, a1 PoolOptions) (*typedConnPicker, error) {
	return pool.InternalNewTypedConnPicker(a0, a1)
}

type nopConnPicker = pool.InternalNopConnPicker

type orderedPartitioner = implEndpoint.InternalOrderedPartitioner

type orderedToken = implEndpoint.InternalOrderedToken

func parseInt64Token(a0 string) int64Token { return implEndpoint.InternalParseInt64Token(a0) }

type policyConnPool = pool.InternalPolicyConnPool

const poolClosed = pool.InternalPoolClosed

const poolReady = pool.InternalPoolReady

type poolServices = pool.InternalPoolServices

type randomPartitioner = implEndpoint.InternalRandomPartitioner

type randomToken = implEndpoint.InternalRandomToken

const scyllaCDCMaxSupportedVersion = implEndpoint.InternalScyllaCDCMaxSupportedVersion

const scyllaCDCMinSupportedVersion = implEndpoint.InternalScyllaCDCMinSupportedVersion

const scyllaCDCMinToken = implEndpoint.InternalScyllaCDCMinToken

type scyllaCDCPartitioner = implEndpoint.InternalScyllaCDCPartitioner

type scyllaConnPicker = pool.InternalScyllaConnPicker

const scyllaPortBasedBalancingMax = implTransport.InternalScyllaPortBasedBalancingMax

const scyllaPortBasedBalancingMin = implTransport.InternalScyllaPortBasedBalancingMin

type scyllaPortIterator = implTransport.InternalScyllaPortIterator

func scyllaShardForSourcePort(a0 uint16, a1 int) int {
	return implTransport.InternalScyllaShardForSourcePort(a0, a1)
}

func tlsConfigForAddr(a0 *tls.Config, a1 string) *tls.Config {
	return implTransport.InternalTlsConfigForAddr(a0, a1)
}

type tokenRing = implEndpoint.InternalTokenRing

type typedConnPicker = pool.InternalTypedConnPicker

type v2TLSHostDialer = implTransport.InternalV2TLSHostDialer

type connReadSource = implTransport.InternalConnReadSource

type connReader = implTransport.InternalConnReader

type contextWriter = implTransport.InternalContextWriter

type deadlineContextWriter = implTransport.InternalDeadlineContextWriter

type deadlineWriter = implTransport.InternalDeadlineWriter

var errArmReadDeadline = implTransport.InternalErrArmReadDeadline

type frameSource = implTransport.InternalFrameSource

const maxReadAttempts = implTransport.InternalMaxReadAttempts

func newWriteCoalescer(a0 deadlineWriter, a1 time.Duration, a2 time.Duration, a3 <-chan struct{}) *writeCoalescer {
	return implTransport.InternalNewWriteCoalescer(a0, a1, a2, a3)
}

type writeCoalescer = implTransport.InternalWriteCoalescer

type writeRequest = implTransport.InternalWriteRequest

type writeResult = implTransport.InternalWriteResult

func firstCQLWord(a0 string) string { return binding.InternalFirstCQLWord(a0) }

type retiredCompressor = protocol.Compressor

type retiredSegmentCompressor = protocol.SegmentCompressor

const apacheCassandraTypePrefix = cqltype.ApacheCassandraTypePrefix

func asSegmentCompressor(a0 retiredCompressor) (segment.Compressor, error) {
	return protocol.AsSegmentCompressor(a0)
}

const defaultBufSize = protocol.DefaultBufSize

func defaultFramerFlags(a0 retiredCompressor, a1 byte) byte {
	return protocol.DefaultFramerFlags(a0, a1)
}

type frame = protocol.Frame

type frameBuilder = protocol.FrameBuilder

type frameWriterFunc = protocol.FrameWriterFunc

type framer = protocol.Framer

type framerInterface = protocol.FramerInterface

const headSize = protocol.HeadSize

func newFramer(a0 retiredCompressor, a1 byte) *framer { return protocol.NewFramer(a0, a1) }

const protoDirectionMask = protocol.ProtoDirectionMask

const protoVersion2 = protocol.ProtoVersion2

const protoVersion3 = protocol.ProtoVersion3

const protoVersion4 = protocol.ProtoVersion4

const protoVersion5 = protocol.ProtoVersion5

const protoVersionMask = protocol.ProtoVersionMask

func readHeader(a0 io.Reader, a1 []byte) (frm.FrameHeader, error) {
	return protocol.ReadHeader(a0, a1)
}

type resultMetadata = protocol.ResultMetadata

func validateShortBytes(a0 string, a1 []byte) error { return protocol.ValidateShortBytes(a0, a1) }

type streamMetadataBudget = protocol.StreamMetadataBudget

type streamMetadataCursor = protocol.StreamMetadataCursor

type retiredApplicationInfo = implEndpoint.ApplicationInfo

type retiredDialedHost = implEndpoint.DialedHost

type retiredHostDialer = implEndpoint.HostDialer

type retiredHostInfo = implEndpoint.HostInfo

type retiredHostInfoBuilder = implEndpoint.HostInfoBuilder

func retiredNewStaticApplicationInfo(a0 string, a1 string, a2 string) *retiredStaticApplicationInfo {
	return implEndpoint.NewStaticApplicationInfo(a0, a1, a2)
}

const retiredNodeDown = implEndpoint.NodeDown

const retiredNodeUp = implEndpoint.NodeUp

type retiredScyllaConnectionFeatures = implEndpoint.ScyllaConnectionFeatures

func retiredScyllaGetSourcePort(a0 context.Context) uint16 {
	return implEndpoint.ScyllaGetSourcePort(a0)
}

type retiredScyllaHostFeatures = implEndpoint.ScyllaHostFeatures

type retiredScyllaShardAwareDialer = implEndpoint.ScyllaShardAwareDialer

type retiredShardDialer = implEndpoint.ShardDialer

type retiredStaticApplicationInfo = implEndpoint.StaticApplicationInfo

type cassVersion = implEndpoint.InternalCassVersion

func cloneV2TLSConfig(a0 *tls.Config) *tls.Config { return implSettings.InternalCloneV2TLSConfig(a0) }

type contextBox = implEndpoint.InternalContextBox

type cqlProtocolExtension = implEndpoint.InternalCqlProtocolExtension

type defaultLogger = logging.InternalDefaultLogger

func defaultProfile() Profile { return implSettings.InternalDefaultProfile() }

func findCQLProtoExtByName(a0 []cqlProtocolExtension, a1 string) cqlProtocolExtension {
	return implEndpoint.InternalFindCQLProtoExtByName(a0, a1)
}

type legacyDialer = implEndpoint.InternalLegacyDialer

type lwtAddMetadataMarkExt = implEndpoint.InternalLwtAddMetadataMarkExt

const lwtAddMetadataMarkKey = implEndpoint.InternalLwtAddMetadataMarkKey

func newLwtAddMetaMarkExt(a0 map[string][]string, a1 StdLogger) *lwtAddMetadataMarkExt {
	return implEndpoint.InternalNewLwtAddMetaMarkExt(a0, a1)
}

func newScyllaUseMetadataIDExt(a0 map[string][]string, a1 byte) *scyllaUseMetadataIDExt {
	return implEndpoint.InternalNewScyllaUseMetadataIDExt(a0, a1)
}

func newTimestampGenerator() func() int64 { return implSettings.InternalNewTimestampGenerator() }

type nopLogger = logging.InternalNopLogger

func parseCQLProtocolExtensions(a0 map[string][]string, a1 byte, a2 StdLogger) []cqlProtocolExtension {
	return implEndpoint.InternalParseCQLProtocolExtensions(a0, a1, a2)
}

func parseSupported(a0 map[string][]string, a1 StdLogger) retiredScyllaConnectionFeatures {
	return implEndpoint.InternalParseSupported(a0, a1)
}

const rateLimitError = implEndpoint.InternalRateLimitError

type rateLimitExt = implEndpoint.InternalRateLimitExt

func resolveInitialEndpoint(a0 DNSResolver, a1 string, a2 int) ([]*retiredHostInfo, error) {
	return implEndpoint.InternalResolveInitialEndpoint(a0, a1, a2)
}

type scyllaSourcePortCtx = implEndpoint.InternalScyllaSourcePortCtx

const scyllaUseMetadataID = implEndpoint.InternalScyllaUseMetadataID

type scyllaUseMetadataIDExt = implEndpoint.InternalScyllaUseMetadataIDExt

func snapshotConfig(a0 Config) Config { return implSettings.InternalSnapshotConfig(a0) }

const tabletsRoutingV1 = implEndpoint.InternalTabletsRoutingV1

const tabletsRoutingV2 = implEndpoint.InternalTabletsRoutingV2

type testLogger = logging.InternalTestLogger

type translatedAddresses = implEndpoint.InternalTranslatedAddresses

type v2DNSResolver = implEndpoint.InternalV2DNSResolver

func validateConfig(a0 Config) error { return implSettings.InternalValidateConfig(a0) }

type retiredSnappyCompressor = wirecompress.SnappyCompressor

func completePageState() PageState { return model.CompletePageState() }

func newTypedEnvelope() *typedEnvelope { return protocol.NewTypedEnvelope() }

type responseData = model.ResponseData

type typedEnvelope = protocol.TypedEnvelope

type typedParser = protocol.TypedParser

func buildRoutingHostIndex(a0 []HostSnapshot) *routingHostIndex {
	return routing.InternalBuildRoutingHostIndex(a0)
}

func clonePlacementSnapshot(a0 *MetadataSnapshot) *MetadataSnapshot {
	return routing.InternalClonePlacementSnapshot(a0)
}

func cloneV2Keyspace(a0 *KeyspaceMetadata) *KeyspaceMetadata {
	return implMetadata.InternalCloneV2Keyspace(a0)
}

func comparePlacementTable(a0 metadataTable, a1 metadataTable) int {
	return routing.InternalComparePlacementTable(a0, a1)
}

var cqlHelpers = implMetadata.InternalCqlHelpers

func cqlTypeIdentifiers(a0 string) []cqlTypeRef { return implMetadata.InternalCqlTypeIdentifiers(a0) }

type cqlTypeRef = implMetadata.InternalCqlTypeRef

type lazyRoutingPlan = routing.InternalLazyRoutingPlan

type metadataTable = routing.InternalMetadataTable

func newPartitionKey(a0 []encodedCell, a1 []int) (PartitionKey, bool, error) {
	return routing.InternalNewPartitionKey(a0, a1)
}

func observeTabletRanges(a0 *tabletRangeTree, a1 int64, a2 int64) tabletRangeObservation {
	return routing.InternalObserveTabletRanges(a0, a1, a2)
}

func placementPut[K any, V any](a0 *placementTree[K, V], a1 K, a2 V, a3 func(K, K) int) *placementTree[K, V] {
	return routing.InternalPlacementPut[K, V](a0, a1, a2, a3)
}

type routingDecisions = routing.InternalRoutingDecisions

type routingLatency = routing.InternalRoutingLatency

func routingLatencySample(a0 error) bool { return routing.InternalRoutingLatencySample(a0) }

type routingPolicy = routing.InternalRoutingPolicy

func sameTabletObservation(a0 *tabletRange, a1 tablets.TabletInfo) bool {
	return routing.InternalSameTabletObservation(a0, a1)
}

type scyllaEncryptionOptions = implMetadata.InternalScyllaEncryptionOptions

type tabletPlacement = routing.InternalTabletPlacement

type tabletRange = routing.InternalTabletRange

func tabletRangeFor(a0 *tabletRangeTree, a1 int64) *tabletRangeTree {
	return routing.InternalTabletRangeFor(a0, a1)
}

type tabletRangeObservation = routing.InternalTabletRangeObservation

type tabletRangeTree = routing.InternalTabletRangeTree

type targetPlan = routing.InternalTargetPlan

func updateTabletRange(a0 *tabletRangeTree, a1 tablets.TabletInfo) (*tabletRangeTree, bool) {
	return routing.InternalUpdateTabletRange(a0, a1)
}

type routingHostIndex = routing.InternalRoutingHostIndex

type placementTree[K, V any] = routing.InternalPlacementTree[K, V]

func comparePlacementToken(a, b int64) int { return routing.InternalComparePlacementToken(a, b) }
