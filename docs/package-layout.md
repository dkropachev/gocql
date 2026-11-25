# Package layout and boundaries

What each package of `github.com/scylladb/gocql/v2` may and may not contain, and
the import rules between them. Apply it whenever you add a package, add a type
or function to a public package, or move code between packages.
`docs/api-surface.md` governs the shape of the execution API;
this document governs where code lives.

## Goals

- A user reads only public packages and sees everything they can call, with
  its documentation. No public name is an alias of an internal type.
- The public API is chosen on purpose. Nothing becomes public because the
  driver's own code needed it.
- Internals can be rewritten without touching a public signature.
- Future features have an obvious home, so the layout does not drift.

## Layers

Packages fall into four layers. A package imports only from its own layer
(without cycles) or from layers below it.

| Layer | Packages | Holds |
|---|---|---|
| 0. Primitives (internal) | `internal/wire`, `internal/frame`, `internal/segment`, `internal/wirecompress`, `internal/crc`, `internal/murmur`, `internal/syncx`, `internal/logging`, `internal/link`, `internal/structmap`, `internal/cqlproto`, `internal/debounce`, `internal/eventbus`, `internal/streams`, `internal/cqlperf` | Byte views, framing, hashing, sync wrappers, logging adapters, struct-mapping helpers, the link registry, and measurement-build counters. Knows nothing about CQL types or the public API. |
| 1. Vocabulary (public) | `cql`, `cqlerr`, `retry`, `metadata`, `routing`, `endpoint`, `auth`, `event`, `observe`, `metrics` | Types, interfaces, constructors and pure logic that users name and call. |
| 2. Engine (internal) | every other `internal/` package, such as `internal/protocol`, `internal/transport`, `internal/pool`, `internal/cluster`, `internal/routing`, `internal/metadata` and `internal/session` | I/O, goroutines, pools, caches, protocol encoding, execution. Implements layer-1 interfaces and produces layer-1 values. |
| 3. Surface (public) | `gocql` (root), `config`, `exec`, `cqltrace`, `routing/routingtest`, future dialect packages | `Connect`, `Session` and its execution handles, configuration and per-execution options that the engine consumes, and dialect-only functions. Connects layer 1 to layer 2. |

Layer-1 packages depend on each other in this order, lowest first: `cql`;
`cqlerr`; `retry`; `metadata`; `routing`;
`endpoint`, `auth` and `event`; `observe`. `metrics` depends on nothing.
A new public package is layer 1 unless it is added to the layer-3 list in
`layout_test.go`.

Code outside the library is not layered: commands (`cmd/`), tests
(`tests/`, `internal/testkit`, `internal/tests`) and CI tools may import
anything. Examples (`examples/`) are user code and import only layers 1
and 3.

Rules that follow from the table:

1. **Layer 1 never imports layer 2 or 3.** A vocabulary package that needs
   engine behavior declares an interface and the engine implements it. The
   one internal package layer 1 may import besides layer 0 is
   `internal/link` (see "Engine access to public types" below).
2. **Layer 0 never imports layer 1.** A primitive that would need a `cql` type
   either takes a plainer input (bytes, `reflect.Value`, an integer) or
   belongs in layer 1.
3. **Layer 2 imports layer 1 freely.** That direction already works for
   `retry` and `metrics`, which `internal/cluster` and `internal/session`
   import today.
4. **Layer 3 is the only public layer that imports the engine.** Each layer-3
   type is a defined type over an engine type with explicit forwarding
   methods, as `Session`, `Prepared` and `Batch` are today. Each forwarding
   method is a decision, with its doc comment in the public package.
5. **No internal type appears in a public signature.** This covers parameters,
   results, fields, method receivers and the methods of aliased types.
6. **No type aliases from a public package to an internal one.** Aliases
   between public packages are allowed only for a deliberate rename with a
   deprecation path. That does not happen before v2.0.0.

## Engine access to public types

Public types keep their engine-only state unexported, and the engine still
has to build and read it. Go has no "friend" visibility, so the driver uses
two mechanisms, both in the dependency-free `internal/link`:

- **Hooks, for behavior.** A public package registers unexported functions
  (constructors, accessors, plan compilers) in `init`, and the engine calls
  them through `internal/link`. A hook is looked up when it is called, never
  during a user's package initialization, and a public function that needs
  one returns an empty result or an error, never panics, when the program
  links no session.
- **Layout mirrors, for hot representations.** Value types read on every
  request (`cql.Bytes`, `Columns`, `DataType`, `PageState`,
  `metadata.Route`, `routing.PartitionKey`, and the state of some
  `cqlerr` and `metadata` types) have an internal mirror struct with an identical
  layout. The engine converts a pointer to the public type into a pointer to
  its mirror with `unsafe`, at no run-time cost. A test compares the field
  offsets, sizes and types of each pair, so a layout change fails that test
  instead of corrupting memory.

Neither mechanism exports anything: a user sees only the public type and its
documented methods.

## Rules for public packages

- **Contents.** Types, interfaces, constructors, constants, sentinel errors,
  and pure logic: parsing, formatting, validation, comparison, codecs.
- **No runtime machinery.** No goroutines, no network or file I/O, no
  background timers, no global mutable state. `config.TLSFileSource` reads
  files only when the caller calls it, never in the background.
- **No driver-only members.** No exported setter, field or method that exists
  because an engine package needs it. The engine builds values through
  exported fields, public constructors, or the mechanisms of "Engine access
  to public types".
- **Results are plain data.** Values the driver returns (`Result`, errors,
  events, metadata structs, host snapshots) are structs with exported fields
  or getters. Users cannot change the driver's state through them.
- **Handles hide their implementation.** A type that stands for something
  living in the engine (`metadata.Snapshot`, `event.Subscription`) is a
  public struct around an unexported interface value, so methods can be
  added later. The engine supplies the implementation through a constructor
  that only layer 3 calls. Do not make such handles public interfaces: users
  would implement them, and no method could be added afterwards.
- **Interfaces users implement stay small and never grow.** `retry.Policy`,
  `routing.Policy`, `routing.TokenPartitioner`, `auth.Provider`,
  `endpoint.Dialer`, `endpoint.Resolver`, `endpoint.Translator`, the
  observer interfaces, and the reconnect, conviction and speculation
  policies. A later capability is a new optional interface the driver
  detects with a type assertion, never a new method on the existing one.
- **Function adapters accompany single-method interfaces**, for example
  `retry.PolicyFunc`, `endpoint.DialerFunc` and `endpoint.TranslatorFunc`.
- **Errors.** An error type lives in the lowest public package that
  produces it, as `strconv.NumError` lives in `strconv`. `cql` holds
  `ValidationError` and `CodecError`, which its own functions return;
  `cqlerr` holds every server, connection and execution error. `cqlerr`
  classifies all of them (`KindOf`, `ExecutionStatusOf`) through small
  interfaces the error types implement, never through internal imports.
- **Enum-like types** keep the protocol's values when the protocol numbers
  them (the zero `cql.Consistency` is ANY, the zero `cql.BatchKind` is
  LOGGED); the others start at a safe zero (`retry.Stop`,
  `cqlerr.MaybeExecuted`, `event.KindUnknown`, `cqlerr.WriteTypeUnknown`,
  `metadata.ColumnOrderNone`, an invalid `config.Dialect`). A
  setting that may be left unset is a `cql.Option`, never a zero enum. Each
  integer enum has a `String` method that prints `Type(n)` for a value it
  does not define; `cqlerr.ErrorCode` is an error instead, and its `Error`
  prints an unknown code in hexadecimal. `metadata.IndexKind` is a string
  and keeps the server's text. Each documents that later releases may add
  values, so users must not rely on exhaustive switches. Only
  `cql.Consistency` has a parser.
- **Zero values.** Every exported struct documents its zero value: usable,
  and what it means (`gocql.Batch`, `exec.Options`, `cql.PageState`), or
  rejected with a `cql.ValidationError` (`config.Config`, `cql.DataType`;
  likewise the zero `config.Dialect`). `TestStructZeroValueDocs` checks that
  the docs mention the zero value.
- **Third-party dependencies.** The main module depends only on compression
  libraries and `golang.org/x/sync`. An integration that needs another
  module (OpenTelemetry, Prometheus, YAML, cloud SDKs) is a separate Go
  module under `contrib/` with its own `go.mod`.

## Rules for internal packages

- **One owner per responsibility.** Connection pools live in `internal/pool`,
  the control connection and topology in `internal/cluster`, host selection
  in `internal/routing`, execution in `internal/session`. A file name that
  repeats across `internal/cluster`, `internal/pool`, `internal/endpoint` and
  `internal/transport` names each package's own part of a feature: `dial.go`
  (the session's per-shard dial in `internal/cluster`, socket dialing in
  `internal/transport`) and `scylla.go` (the shard-aware connection picker in
  `internal/pool`, the shard-aware dialer in `internal/transport`).
- **Name packages after what they do, not after a v1 file.** The `opaque_*.go`
  files and the `runtime_types.go` grab-bags each move to the package that
  owns the concept.
- **Internal seams use interfaces declared by the consumer.** When
  `internal/routing` needs something from `internal/pool`, `internal/routing`
  declares the interface it needs.
- **`internal/model` shrinks.** It holds shared engine vocabulary that is not
  public. A type in it that users see belongs in its public package, as
  `Result`, `HostSnapshot`, `RoutingInfo`, `Target`, `PageState` and
  `PayloadEntry` did.
- **Delete unused packages** rather than keep them for later.
- **Test helpers** live in `internal/testkit/...` and `internal/tests/...`
  and are never imported by production code.

## Package by package

### `gocql` (root, layer 3)

- **Contains:** `Connect`, `Session`, and the execution
  handles (`Prepared`, `Query`, `PreparedStream`, `StreamQuery`, `Batch`,
  `Rows`, `Page`, `RowStream`, `PageLease`), plus `PreparedInfo` and
  `StreamInfo` as plain data. `Result` lives in `cql`, because `cqlerr`
  needs it and the root package imports `cqlerr`.
- **Must not contain:** codecs, policies, configuration builders, error types,
  or anything only one dialect supports.
- **Future:** new execution handles.

### `config` (layer 3)

- **Contains:** `ConfigBuilder`, `Config`, `Dialect` and its values,
  `ProfileBuilder`, `Profile`, `ClientRoutes` and `TLSFileSource`. The dialect
  is a setting: `New` takes it, and `Build` validates the other settings
  against it. It is layer 3
  because the engine never imports it: `Connect` turns a `Config` into engine
  settings.
- **Must not contain:** policy or observer interfaces, event payloads, or
  aliases. `config` refers to them; their own packages define them
  (`observe`, `retry`, `endpoint`).
- **Future:** new settings and dialect values (`ScyllaV5`, a later Cassandra
  protocol). Loading configuration from files or a cloud
  connection bundle lives in a `contrib/` module that drives the builder.

### `exec` (layer 3)

- **Contains:** per-execution settings: `Options`, `PrepareOptions`,
  `StreamOptions`, `StreamLimits`, `SerialOverride`, and `CompiledOptions`,
  the frozen form `Session.CompileOptions` returns. It is layer 3 because
  `CompiledOptions` wraps an engine object.
- **Must not contain:** aliases, or settings that belong to the whole
  session (those are `config`).
- **Future:** per-request rate-limit and priority hints, and a request-level
  routing override.

### `cql` (layer 1)

- **Contains:** the CQL data vocabulary: `Statement`, `DataType`, `Columns`,
  `Consistency`, `BatchKind`, value types (`UUID`, `TimeUUID`, `Duration`,
  `Date`, `Timestamp`, `Decimal`, `Varint`, `Bytes`, `Text`, `Nullable`,
  `Option`), `PageState`, `PayloadEntry`, `Result` and `CQLTraceRef`,
  `ValidationError` and `CodecError`, codec interfaces and built-in codecs,
  the struct mapping (`ParamsOf`, `RowOf`), the dynamic values (`Value`,
  `DynamicRows`) and the streaming decoders (`StreamRowOf`,
  `StreamStatement`, ...). The codec implementations live here;
  type-agnostic reflection helpers live in `internal/structmap`. Dynamic and
  streaming codecs share the core codecs' readers, writers and validation
  plans, so they are not separate packages.
- **Must not contain:** anything that talks to a server, server error types
  (those go in `cqlerr`), or routing.
- **Future:** `encoding` interfaces on value types (#433), new CQL types,
  value compression (#218).

### `cqlerr` (layer 1)

- **Contains:** every server, connection and execution error type and
  sentinel, `Kind`, `ExecutionStatus`, and the classification functions,
  which also classify `cql.ValidationError` and `cql.CodecError`.
- **Must not contain:** construction logic tied to wire decoding. The engine
  decodes an error frame and fills in the public struct.
- **Future:** new server error codes as new types. `OtherServerError` covers
  codes not modeled yet.

### `metadata` (layer 1)

- **Contains:** schema structs (`Keyspace`, `Table`, `Column`, `View`,
  `Index`, `Type`, `Function`, `Aggregate`), `Mode`, the cluster placement
  types (`HostSnapshot`, `Target`, `TokenRange`, `Route`), and the
  `Snapshot` handle with its read methods. The placement types live here,
  not in `routing`, because `Snapshot` returns them and `routing` imports
  `metadata`. `Keyspace.ToCQL` is pure logic and stays.
- **Must not contain:** loaders, refresh logic or system-table queries
  (those belong in `internal/metadata` and `internal/cluster`).
- **Future:** tablet and strong-consistency attributes of keyspaces and
  tables, and per-table shard and replica information (#200).

### `routing` (layer 1)

- **Contains:** the `Policy`, `Plan` and `TokenPartitioner` interfaces, the
  optional `Validator` and `AttemptObserver` interfaces of a policy,
  `TokenPartitionerFunc`, `Options`, `LatencyOptions`, `Info`, `PartitionKey`
  and `AttemptEvent` (which `AttemptObserver.Observe` consumes), the built-in policy
  constructors (`NewPolicy`, `SingleTarget`) and the `*Builtin` they return,
  and the functions a custom policy picks with (`PlanOf`, `HostRoutes`,
  `ReplicaRoutes`). The built-in policies are the session's: `NewPolicy`
  validates its options and returns a `*Builtin` that builds the session's
  policy on first use.
- **Must not contain:** pool handles or connection state.
- **Future:** vector-search or other workload-specific placement as further
  `Options` or policies.

### `retry` (layer 1)

- **Contains:** the retry `Policy` and its built-ins, and
  `SpeculationPolicy`. Both decide whether another
  attempt is made.
- **Must not contain:** timers or attempt execution.

### `endpoint` (layer 1)

- **Contains:** how nodes are addressed, dialed and kept connected:
  `Dialer`, `Resolver`, `Translator` and their adapters, `DefaultDialer`,
  `DialTarget`, `Peer`, `TLSSource`, and the reconnect and conviction
  policies.
- **Must not contain:** pool management or shard selection logic.
- **Future:** proxy and private-link dialers, shard-aware port fallback
  policy (#123).

### `observe` (layer 1)

- **Contains:** `Observer`, any value implementing one or more of the hook
  interfaces `RequestObserver` and `AttemptObserver`; their function
  adapters `RequestObserverFunc` and `AttemptObserverFunc`; `Join`, which
  combines observers; and their event payloads (`RequestStart`,
  `RequestEvent`, `AttemptStart`). `AttemptObserver.StartAttempt` ends with
  `routing.AttemptEvent`, which routing policies also observe, and
  `endpoint.ConvictionEvent` stays with the conviction policy.
- **Must not contain:** exporters. OpenTelemetry and Prometheus integrations
  are `contrib/` modules that implement these interfaces.
- **Future:** new hooks as new hook interfaces, never as methods added to an
  existing one.

### `event` (layer 1)

- **Contains:** cluster events delivered by `Session.Subscribe`: `Event`,
  `Kind`, `Filter`, and the `Subscription` handle.
- **Must not contain:** the event bus or debouncing, which stay in the engine.

### `auth` (layer 1)

- **Contains:** `Provider`, `Exchange`, and built-in mechanisms that need only
  the standard library (`Password`).
- **Future:** SASL mechanisms. AWS SigV4, Kerberos or LDAP live in `contrib/`
  modules when they need a third-party dependency.

### `metrics` (layer 1)

- **Contains:** `Snapshot`, a plain struct of counters, which counts failed
  executions by `cqlerr.Kind`.
- **Must not contain:** exporters or registries.
- **Future:** per-host and per-shard snapshots, and latency histograms as new
  fields or new snapshot types. Adding a field to `Snapshot` is not a
  breaking change, so callers must not construct it positionally.

### Dialect packages (layer 3)

- **Contains:** functions that only one database supports, taking a
  `*gocql.Session` and returning an error when `Session.Dialect` is another
  database, in a package named after the database (`scylla`, `cassandra`).
  None exists today.
- **Must not contain:** types or settings that every dialect shares.
- **Future:** tablet introspection, ScyllaDB-only protocol extensions.

### `cqltrace` (layer 3)

- **Contains:** reading CQL trace sessions through a `Session`.
- **Must not contain:** tracing of the client itself, which is `observe`.

### `routing/routingtest` (layer 3)

- **Contains:** `NewSnapshot`, which builds a `metadata.Snapshot` from hosts,
  schema, token ranges and tablets for the tests of a routing policy. It is
  layer 3 because it builds the engine's snapshot, and importing it links the
  engine, so the built-in policies route over its snapshots.
- **Must not contain:** a session or anything that dials.

### Engine packages that share the node and connection work

Each package's `doc.go` says in full what it contains and must not contain.
In short:

- **`internal/endpoint`:** one node: `HostInfo` and its state, addresses and
  their resolution (`resolve.go`) and translation (`translate.go`), ScyllaDB
  node features, and the `Token` interface with its int64 form
  (`token_type.go`), which the connection pickers in `internal/pool` take.
  Nothing that dials.
- **`internal/transport`:** one connection: dialing, TLS, the shard-aware
  source port, connection startup (SUPPORTED parsing, protocol extensions,
  STARTUP options), and the request/response transport. Nothing that spans
  connections.
- **`internal/pool`:** the connections kept open to each node: host pools,
  connection pickers, a pool's own dial retries. Not which hosts exist.
- **`internal/cluster`:** the cluster view and the session core: control
  connection, system-table discovery (`peers.go`), events, metadata,
  partitioners and the token ring, replica placement, downed-host
  reconnection under the session's reconnect policy (`reconnect.go`), and the
  wiring of pools to the session. No host selection: that is
  `internal/routing`.

### `cmd/cqlgen`

- **Contains:** the code generator for struct mappings used by `cql.ParamsOf`
  and `cql.RowOf`.
- **Must not contain:** a query builder or object mapper. Those belong in
  separate projects.

### `contrib/...` (new, separate modules)

Integrations that need third-party modules, each with its own `go.mod`:
`contrib/otel` (OpenTelemetry tracing and metrics through `observe`),
`contrib/prometheus` (through `metrics` and `observe`), and `contrib/configfile`
(YAML or JSON configuration matching `docs/driver-config-schema.json`, and
cloud connection bundles).

## Outside the driver

These features do not belong in this module: CDC readers (a separate
library), query builders and object mappers, schema migration tools, and a
fake server for user tests. A user-facing fake server, if ever offered, is its
own module and does not reuse `internal/testkit`.

## Enforcement

- `TestPackageLayers` (`layout_test.go`) runs `go list` and fails when a
  package's non-test imports leave its layer. Every module package has a
  layer, so a new package is checked without being listed, and there is no
  list of allowed exceptions.
- `TestPublicAPI` (`api_golden_test.go`) writes every exported name of every
  public package to `testdata/api.txt`: constants with their values, and
  types with their exported fields (promoted ones included) and method sets.
  Any difference fails the test, so API changes show up as reviewed diffs;
  run `go test -run TestPublicAPI -update-api .` after a deliberate change.
  It fails outright when an internal type appears anywhere in the public
  API. It also builds with each of the library's build tags (`gocql_perf`,
  `gocql_debug`, `integration`) and fails when a tag changes the public API;
  `TestPackageLayers` checks the layers under the same tags.
- `TestBuildTagsCost` (`build_tags_test.go`) builds `examples/streaming`
  with and without `gocql_perf` and `gocql_debug` and fails when a build
  without tags links the perf counters or the debug assertions.
  `make test-unit-tags` (the `unit-tags` CI job) runs the unit tests
  without `-race`, which the allocation gates need, and once per tag.
- The coverage job runs `ci/public-coverage.sh` (`make coverage-public`) on
  the merged unit and integration profile: it reports statement coverage per
  public package and fails when an exported function or method of a public
  package never ran.
- `TestRuntimeDependencies` (`module_deps_test.go`) fails when a library
  package or a command, under the default build or any of those tags,
  imports a package from a module other than `klauspost/compress`,
  `pierrec/lz4/v4` and `golang.org/x/sync`. `TestConsumerModuleRequirements`
  builds a module importing every public package and fails when `go mod
  tidy` gives it any other requirement or its build needs another module.
- `make check-api-compat` (`ci/apidiff.sh`), which PR CI runs, fails on an
  incompatible change to a public package against the `apidiff` baseline in
  `testdata/apidiff`; `RELEASING.md` says when the baseline is rewritten.

## Where the code stands today

Every public package follows the rules above: no public name aliases an
internal type, no public package imports an engine package, and
`TestPackageLayers` allows no exception. Two parts of the internals still
need work, tracked in the release review:

- Some files still carry v1 names (`internal/endpoint/host_source.go`, the
  `opaque_*.go` files, `internal/session/runtime_types.go`), though the
  overlapping files the v1 package split left in `internal/cluster`,
  `internal/pool`, `internal/endpoint` and `internal/transport` are merged
  into their owners (section 11 of the v2 release review, #1188).
- Several public types still lack doc comments of their own; the API
  golden file lists every exported name to review.
