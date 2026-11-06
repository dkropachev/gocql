//go:build integration

package integration

import gocql "github.com/scylladb/gocql/v2"

func codecTestMust[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

// profileOf returns a pointer to a copy of p, so later changes to p do not
// reach options that already hold it.
func profileOf(p gocql.Profile) *gocql.Profile { return &p }

// testOptions freezes v, failing loudly on values a test meant to be valid.
func testOptions(v gocql.OptionValues) gocql.Options {
	o, err := gocql.NewOptions(nil, v)
	if err != nil {
		panic(err)
	}
	return o
}
