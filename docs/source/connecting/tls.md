# TLS

`ConfigBuilder.WithTLS` takes a `TLSSource`. For a fixed configuration, wrap a
`crypto/tls.Config` with trusted `RootCAs`, optional client certificates and a
minimum TLS version in `gocql.StaticTLS`, which copies it:

```go
config, err := gocql.NewConfig("10.0.0.1").
	WithTLS(gocql.StaticTLS(&tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS13})).
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

The driver calls its `TLSConfig` method on every dial and uses the result for
that connection: reconnects, pool refills, new shards and the control
connection all pick up the current config. Connections that are already open
keep the config they were dialed with until they close; nothing closes them
early. `Build` validates the config the source returns at that time. A source
must never return nil; a dial that gets nil fails
instead of falling back to plaintext.

`TLSHolder` is a source you update by hand. One holder can serve several
sessions:

```go
holder := gocql.NewTLSHolder(&tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots})
config, err := gocql.NewConfig("10.0.0.1").WithTLS(holder).Build()
// later, for example after a rotation event:
err = holder.Update(newConfig) // copied; new dials use it
```

`TLSFileSource` reloads the client certificate, key and CA from PEM files:

```go
files, err := gocql.NewTLSFiles(&tls.Config{MinVersion: tls.VersionTLS13}, gocql.TLSFiles{
	CertFile: "/etc/scylla-client/tls.crt",
	KeyFile:  "/etc/scylla-client/tls.key",
	CAFile:   "/etc/scylla-client/ca.crt",
})
config, err := gocql.NewConfig("10.0.0.1").WithTLS(files).Build()
```

The files replace `Certificates` and `RootCAs` of the base config, or of TLS
1.2+ defaults when the base is nil. `NewTLSFiles` fails if they cannot be
loaded. The source runs no goroutine: a dial checks the files when `Interval`
(one minute by default) has passed since the last check, and compares their
content, so Kubernetes secret volumes that swap a symlink are seen. A check that
fails, such as a key that does not match its certificate yet, keeps the current
files; `Err` reports it and the next due check retries. `Reload` checks
immediately.

A resumed TLS 1.3 connection does not send its client certificate again. When
the file certificate changes and the base has a `ClientSessionCache`, the
source switches to a new cache so no connection resumes the old identity. When
you give a holder a new identity, give it a new `ClientSessionCache` too.

Any type with a `TLSConfig() *tls.Config` method is a source, for example one
fed by Vault or cert-manager. `GetClientCertificate` in a fixed config still
rotates only the client certificate on every handshake; it cannot be combined
with `TLSFiles.CertFile`.
