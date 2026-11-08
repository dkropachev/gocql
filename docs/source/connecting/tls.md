# TLS

Pass a `crypto/tls.Config` with trusted `RootCAs`, optional client certificates
and a minimum TLS version to `ConfigBuilder.TLS`; it is copied when called.
Standard verification remains enabled: unless the config sets `ServerName` or
skips verification, each connection verifies the node's IP address. One
configuration serves every node; rotate certificates with
`GetClientCertificate` and customize checks with `VerifyConnection`.
Use `ClientSessionCache` for ticket resumption. TLS failure never downgrades to
plaintext.

Raw dialing receives the selected native/shard endpoint before TLS wrapping.
Per-host hooks honor cancellation and preserve shard/source selection. Custom
URI identity verification must validate both the CA chain and expected identity;
the standalone streaming command has an explicit `-tls-uri` example.
