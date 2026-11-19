// This go.mod makes feature-specs/ a separate, code-free module so the
// specifications and their review ledger stay in the repository without
// shipping in the github.com/scylladb/gocql/v2 module zip: the Go toolchain
// leaves every directory that holds its own go.mod out of the parent module.
module gocql-feature-specs

go 1.27.0
