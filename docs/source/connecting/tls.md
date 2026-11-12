# TLS

For a fixed configuration, pass `config.ConfigBuilder.WithTLS` a
`crypto/tls.Config` with trusted `RootCAs`, optional client certificates and a
minimum TLS version; it copies it:

```go
cfg, err := config.New("10.0.0.1").
	WithTLS(&tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS13}).
	Build()
```

Standard verification remains enabled: unless the config sets `ServerName` or
skips verification, each connection verifies the node's IP address. One
configuration serves every node; customize checks with `VerifyConnection`.
Use `ClientSessionCache` for ticket resumption. TLS failure never downgrades to
plaintext.

Raw dialing receives the selected native/shard endpoint before TLS wrapping.
Per-host hooks honor cancellation and preserve shard/source selection. Custom
URI identity verification must validate both the CA chain and expected identity;
the standalone streaming command has an explicit `-tls-uri` example.

## Changing TLS while sessions run

`WithTLSSource` takes a `config.TLSSource` instead. The driver calls its
`TLSConfig` method on every dial and uses the result for
that connection: reconnects, pool refills, new shards and the control
connection all pick up the current config. Connections that are already open
keep the config they were dialed with until they close; nothing closes them
early. `Build` validates the config the source returns at that time. A source
must never return nil; a dial that gets nil fails
instead of falling back to plaintext.

`config.TLSFileSource` reloads the client certificate, key and CA from PEM files:

```go
files, err := config.NewTLSFiles(&tls.Config{MinVersion: tls.VersionTLS13}, config.TLSFiles{
	CertFile: "/etc/scylla-client/tls.crt",
	KeyFile:  "/etc/scylla-client/tls.key",
	CAFile:   "/etc/scylla-client/ca.crt",
})
cfg, err := config.New("10.0.0.1").WithTLSSource(files).Build()
```

The files replace `Certificates` and `RootCAs` of the base config, or of TLS
1.2+ defaults when the base is nil. `NewTLSFiles` fails if they cannot be
loaded. The source runs no goroutine: a dial checks the files when `Interval`
(one minute by default) has passed since the last check, and compares their
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

```go
type rotatingTLS struct{ current atomic.Pointer[tls.Config] }

func (r *rotatingTLS) TLSConfig() *tls.Config { return r.current.Load() }

source := &rotatingTLS{}
source.current.Store(initial)
cfg, err := config.New("10.0.0.1").WithTLSSource(source).Build()
// later: new dials use the new config
source.current.Store(next)
```

Do not modify a config after storing it; store a new one. When the client
certificate or the trusted CAs change, give the new config a new
`ClientSessionCache`; changes to versions, cipher suites or `ServerName` need
none. `GetClientCertificate` in a fixed config still
rotates only the client certificate on every handshake; it cannot be combined
with `TLSFiles.CertFile`.
