# TLS

Set `Config.TLS` to a `crypto/tls.Config` with trusted `RootCAs`, optional client
certificates and a minimum TLS version. Standard hostname verification remains
enabled. `TLSForHost(ctx, HostSnapshot)` selects host-specific TLS settings;
a nil result inherits configured TLS and cannot disable required encryption.
Use `ClientSessionCache` for ticket resumption. TLS failure never downgrades to
plaintext.

Raw dialing receives the selected native/shard endpoint before TLS wrapping.
Per-host hooks honor cancellation and preserve shard/source selection. Custom
URI identity verification must validate both the CA chain and expected identity;
the standalone streaming command has an explicit `-tls-uri` example.
