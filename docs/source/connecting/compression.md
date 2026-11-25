# Compression

Call `WithCompression` on the `config.ConfigBuilder` with `cql.NoCompression`,
`cql.LZ4Compression` or `cql.SnappyCompression`. Implementations belong to the single v2 module; no
standalone compressor module or `replace` directive is needed. Native protocol
5 supports LZ4 segment compression. The outbound data path compresses encoded
spans directly and sends original spans when a v5 segment is incompressible.
`cql.ParseCompression` reads the names `none`, `lz4` and `snappy` in any case,
and `cql.Compression` implements `encoding.TextMarshaler` and
`TextUnmarshaler`, so it can be a flag (`flag.TextVar`) or a configuration
file field.

Explicit incremental receive supports no compression on raw/TLS transports and protocol 5 LZ4
segments. Protocol 4 frame compression with LZ4 or Snappy produces an explicit
`StreamCapabilityError` before streaming submission. Incremental receive reports its capability error explicitly. Default
page-buffered streaming and ordinary buffered execution support protocol 4
frame compression.

See [streaming](../statements/streaming.md) for transform staging and lifetime.
