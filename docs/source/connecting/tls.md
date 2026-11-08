# TLS

Pass a `crypto/tls.Config` with trusted `RootCAs`, optional client certificates
and a minimum TLS version to `ConfigBuilder.WithTLS`; it is copied when called.
Standard verification remains enabled: unless the config sets `ServerName` or
skips verification, each connection verifies the node's IP address. One
configuration serves every node; customize checks with `VerifyConnection`.
Use `ClientSessionCache` for ticket resumption. TLS failure never downgrades to
plaintext.

Raw dialing receives the selected native/shard endpoint before TLS wrapping.
Per-host hooks honor cancellation and preserve shard/source selection. Custom
URI identity verification must validate both the CA chain and expected identity;
the standalone streaming command has an explicit `-tls-uri` example.

## Changing TLS on a running session

`Session.UpdateTLS` replaces the whole TLS configuration: certificates, CAs,
versions, cipher suites and callbacks. It is copied, checked like
`ConfigBuilder.WithTLS`, and used by every connection dialed afterwards:
reconnects, pool refills, new shards and the control connection. Connections
that are already open keep the configuration they were dialed with until they
close; nothing closes them early. A session created without TLS cannot start
using it, and TLS cannot be turned off.

To reload certificates from disk, pass their paths to `ConfigBuilder.WithTLSFiles`:

```go
config, err := gocql.NewConfig("10.0.0.1").
	WithTLS(&tls.Config{MinVersion: tls.VersionTLS13}).
	WithTLSFiles(gocql.TLSFiles{
		CertFile: "/etc/scylla-client/tls.crt",
		KeyFile:  "/etc/scylla-client/tls.key",
		CAFile:   "/etc/scylla-client/ca.crt",
	}).
	Build()
```

The files replace `Certificates` and `RootCAs` of the `TLS` config, or of TLS
1.2+ defaults without it. `Build` fails if they cannot be loaded. The session
then checks their content every `Interval` (one minute by default), including
Kubernetes secret volumes that swap a symlink, and applies a change to new
connections. A failed reload, such as a key that does not match its
certificate yet, keeps the current files, is logged once, and is retried on the
next check. A config passed to `UpdateTLS` later replaces the settings the files
apply to; the files still supply the certificate and CA.

A resumed TLS 1.3 connection does not send its client certificate again. When
the file certificate changes and the config has a `ClientSessionCache`, the
session switches to a new cache so no connection resumes the old identity.
After `UpdateTLS` with a new identity, pass a new `ClientSessionCache` too.

`GetClientCertificate` still works for rotating only the client certificate on
every handshake; it cannot be combined with `TLSFiles.CertFile`.
