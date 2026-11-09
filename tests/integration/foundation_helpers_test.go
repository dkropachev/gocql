//go:build integration

package integration

import gocql "github.com/scylladb/gocql/v2"

func codecTestMust[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

// bindOptions binds v to session, failing loudly on values a test meant to
// be valid.
func bindOptions(session *gocql.Session, v gocql.Options) gocql.SessionOptions {
	o, err := gocql.BindOptions(session, v)
	if err != nil {
		panic(err)
	}
	return o
}
