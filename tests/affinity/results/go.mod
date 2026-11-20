// This go.mod makes tests/affinity/results/ a separate, code-free module so
// the raw benchmark results stay in the repository, next to the harness and
// INVESTIGATION.md that cite them, without shipping in the
// github.com/scylladb/gocql/v2 module zip: the Go toolchain leaves every
// directory that holds its own go.mod out of the parent module.
module gocql-affinity-results

go 1.27.0
