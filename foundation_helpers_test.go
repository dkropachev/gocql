package gocql

import "github.com/scylladb/gocql/v2/exec"

func codecTestMust[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

// bindOptions binds v to session, failing loudly on values a test meant to
// be valid.
func bindOptions(session *testSession, v exec.Options) exec.CompiledOptions {
	o, err := session.CompileOptions(v)
	if err != nil {
		panic(err)
	}
	return o
}

// testSession is the session type of the package's unit tests, which build
// sessions directly instead of connecting.
type testSession = Session[ScyllaV4]
