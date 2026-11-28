# TLS

For a fixed configuration, pass `config.ConfigBuilder.WithTLS` a
`crypto/tls.Config` with trusted `RootCAs`, optional client certificates and a
minimum TLS version; it copies it:

<!-- snippet
var roots *x509.CertPool
-->
```go
cfg, err := config.New(config.ScyllaV4, "10.0.0.1").
	WithTLS(&tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS13}).
	Build()
```

`config.LoadTLSFiles` builds such a config from PEM files once, over a base
config or TLS 1.2+ defaults:

```go
tlsConfig, err := config.LoadTLSFiles(&tls.Config{MinVersion: tls.VersionTLS13}, config.TLSFiles{
	CertFile: "/etc/scylla-client/tls.crt",
	KeyFile:  "/etc/scylla-client/tls.key",
	CAFile:   "/etc/scylla-client/ca.crt",
})
cfg, err := config.New(config.ScyllaV4, "10.0.0.1").WithTLS(tlsConfig).Build()
```

Standard verification remains enabled: unless the config sets `ServerName` or
skips verification, each connection verifies the name it dials by. One
configuration serves every node; customize checks with `VerifyConnection`.

## Contact points by name, nodes by IP

A contact point is dialed by the name it is configured with, and its
certificate is verified against that name. The nodes the driver then
discovers are dialed by the IP address the cluster reports, and verified
against that IP address. With certificates that name only DNS names, the
contact point passes and every discovered node fails, the contact node's own
pool included, so `Connect` fails with a `cqlerr.ConnectError` for the
"connection pools" stage. Each host's error is an `x509.HostnameError` that
says to set `ServerName` or use `WithTLSIdentity`. Choose one:

- Set the `tls.Config`'s `ServerName` to a name every node's certificate
  carries, such as a wildcard's.
- Return a `VerifyName` per node from `WithTLSIdentity` (see below), or
  `ChainOnly` with a check of your own.
- Translate each node to a DNS name its certificate carries with
  `WithAddressTranslator`: a host name an `endpoint.Address` returns is
  resolved on every dial and is the name the connection verifies.

The address translator's name, like a contact point's, replaces only the
default: a `ServerName` in the config or an identity from `WithTLSIdentity`
takes precedence.
Use `ClientSessionCache` for ticket resumption; the driver keys it by node,
so nodes that share an SNI never resume each other's sessions. TLS failure
never downgrades to plaintext unless `WithTLSFallback` allows it (see below).

## Per-node identity

Certificates, CAs and cipher settings never vary by node, but the names a
handshake uses can. `WithTLSIdentity` takes a function the driver calls on
every dial with the `endpoint.DialTarget`, and returns the
`endpoint.TLSIdentity` of that connection:

- `ServerName` is the SNI sent, such as the name an SNI proxy routes by.
- `VerifyName` is the name the certificate must match when it differs from
  the SNI, such as a node dialed by IP whose certificate names its DNS host.
- `ChainOnly` verifies the chain against `RootCAs` but no name, for nodes
  dialed by addresses their certificates do not carry. Trust only a private
  CA with it, or check the identity in `Verify`.
- `Verify` runs after the chain and name checks with the
  `tls.ConnectionState`, whose `VerifiedChains` is filled: pin a key, require
  the node's host ID or IP in the certificate, or accept only one
  datacenter's CA. An error fails that connection only.

`Target.Host.HostID` is zero for a contact point the driver dials before it
learns the node's identity, so return an identity that needs only its address
then.

<!-- snippet
var roots *x509.CertPool
-->
```go
cfg, err := config.New(config.ScyllaV4, "10.0.0.1").
	WithTLS(&tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS13}).
	WithTLSIdentity(func(target endpoint.DialTarget) endpoint.TLSIdentity {
		if target.Host.HostID.IsZero() {
			return endpoint.TLSIdentity{ChainOnly: true} // a contact point
		}
		return endpoint.TLSIdentity{
			ServerName: target.Host.HostID.String() + ".nodes.example.com",
			VerifyName: "*.nodes.example.com",
		}
	}).
	Build()
```

## Enabling TLS on a running cluster

Nodes switch to TLS one at a time, so for a while some answer only
plaintext. `WithTLSPort` dials TLS on a port of its own on every node, the
`native_transport_port_ssl` that serves TLS beside the plaintext port;
shard-aware TLS connections keep the port each node advertises.
`WithTLSFallback(true)` makes each dial try TLS first and, when the node does
not speak TLS there (it answers something other than TLS, closes the
connection during the handshake, or refuses the TLS port), dial plaintext on
its CQL port. A certificate that fails verification never falls back.

The fallback is one-way: once a node has answered TLS, it is never dialed in
plaintext again, and once every node has, the session stops falling back.
Plaintext connections opened earlier stay until they close. Someone on the
network can still force plaintext to a node that has not yet answered TLS, so
turn the fallback off once the rollout is done.

```go
cfg, err := config.New(config.ScyllaV4, "10.0.0.1").
	WithTLS(&tls.Config{MinVersion: tls.VersionTLS13}).
	WithTLSPort(9142).
	WithTLSFallback(true).
	Build()
```

Raw dialing receives the selected native/shard endpoint before TLS wrapping.
Per-host hooks honor cancellation and preserve shard/source selection. Custom
URI identity verification must validate both the CA chain and expected identity;
the standalone streaming command has an explicit `-tls-uri` example.

## Changing TLS while sessions run

`WithTLSSource` takes an `endpoint.TLSSource` instead. The driver calls its
`TLSConfig` method on every dial and uses the result for
that connection: reconnects, pool refills, new shards and the control
connection all pick up the current config. Connections that are already open
keep the config they were dialed with until they close; nothing closes them
early. `Build` validates the config the source returns at that time. A source
must never return nil; a dial that gets nil fails
instead of falling back to plaintext.

`config.TLSFileSource` reloads the client certificate, key and CA from PEM files:

```go
files, err := config.NewTLSFileSource(&tls.Config{MinVersion: tls.VersionTLS13}, config.TLSFiles{
	CertFile: "/etc/scylla-client/tls.crt",
	KeyFile:  "/etc/scylla-client/tls.key",
	CAFile:   "/etc/scylla-client/ca.crt",
}, time.Minute)
cfg, err := config.New(config.ScyllaV4, "10.0.0.1").WithTLSSource(files).Build()
```

The files replace `Certificates` and `RootCAs` of the base config, or of TLS
1.2+ defaults when the base is nil. `NewTLSFileSource` fails if they cannot be
loaded. The source runs no goroutine: a dial checks the files when the
interval, its last argument (zero means one minute), has passed since the last check, and compares their
content, so Kubernetes secret volumes that swap a symlink are seen. A check that
fails, such as a key that does not match its certificate yet, keeps the current
files; `Err` reports it and the next due check retries. `Reload` checks
immediately.

A resumed connection does not send its client certificate again, and Go checks
only the expiry and host name of the cached server certificate, not the current
`RootCAs`. So when the files change and the base has a `ClientSessionCache`, the
source switches to a new cache: no connection resumes a session of the old
identity or one verified under the old CA. Each node then needs one full
handshake before tickets are reused.

Any type with a `TLSConfig() *tls.Config` method is a source, for example one
fed by Vault or cert-manager:

<!-- snippet
var initial, next *tls.Config
-->
```go
type rotatingTLS struct{ current atomic.Pointer[tls.Config] }

func (r *rotatingTLS) TLSConfig() *tls.Config { return r.current.Load() }

source := &rotatingTLS{}
source.current.Store(initial)
cfg, err := config.New(config.ScyllaV4, "10.0.0.1").WithTLSSource(source).Build()
// later: new dials use the new config
source.current.Store(next)
```

Do not modify a config after storing it; store a new one. When the client
certificate or the trusted CAs change, give the new config a new
`ClientSessionCache`; changes to versions, cipher suites or `ServerName` need
none. `GetClientCertificate` in a fixed config still
rotates only the client certificate on every handshake; it cannot be combined
with `TLSFiles.CertFile`.
