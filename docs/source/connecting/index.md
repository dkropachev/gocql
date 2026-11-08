# Connecting

A `Config` is built, never written by hand. `NewConfig(contactPoints...)`
returns a `ConfigBuilder`, and each method sets one setting; settings you do
not choose keep the driver's behavior, and when a setting is chosen again the
last call wins. Features that are on by default have a `No*` method to turn
them off; features that are off by default, such as TLS, have none. `Build`
validates everything and returns the immutable `Config`, or one error listing
every invalid call:

```go
config, err := gocql.NewConfig("10.0.0.1", "10.0.0.2").
    WithKeyspace("app").
    WithConnectionsPerShard(2).
    WithDefaultProfile(func(p *gocql.ProfileBuilder) {
        p.WithConsistency(cql.LocalQuorum).WithTimeout(5 * time.Second)
    }).
    Build()
```

Contact points may include native ports; `WithPort` supplies the port when one is
absent. A single absolute path, optionally prefixed with `unix:`, connects to a
Unix socket: protocol 3 and no host discovery are then filled in unless
chosen. `WithProtocolVersion` selects 3, 4 or 5; automatic negotiation caps at
4, so select `Protocol5` explicitly for a server that supports it.

Features that are on by default are turned off by `No*` methods, such as
`NoHeartbeat`, `NoAdvancedShardAwareness`, `NoMetadataRefresh` and `NoPreparedCache`.
Values passed to the builder, such as a `tls.Config`, are copied
when the method is called. Callbacks and provider objects must support
concurrent use.

Use `WithConnectionsPerHost` for a host override or `WithConnectionsPerShard` for
shard sizing; the later call replaces the earlier one. An explicit incremental stream occupies one configured connection through
idle time and pages. Default page-buffered streams use normal multiplexing. Cancellation interrupts work;
Session.Close rejects new work, cancels requests and joins writers/workers.
Queries copy parameters during admission; Wait joins transport cleanup before
executing the same query again.

`endpoint.Dialer` receives a typed `endpoint.DialTarget` and customizes host
setup. Hooks honor context cancellation and the selected endpoint/source.
`endpoint.DNSResolver`, `endpoint.Translator`, `HostFilter` and `ReconnectPolicy` are
independent extension points. Synchronous callbacks must be prompt and must
not reenter blocking Close/Wait.

```{toctree}
:maxdepth: 1

authentication
compression
tls
```

`ConvictionPolicy.AddFailure(error, HostSnapshot)` returns whether a failed host
should be marked down; `Reset(HostSnapshot)` clears application policy state.
The host snapshot is detached read-only metadata that can be retained. Custom
policies must support concurrent callbacks. The default SimpleConvictionPolicy
always convicts and its Reset is a no-op.
