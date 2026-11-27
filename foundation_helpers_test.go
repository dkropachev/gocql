package gocql

func codecTestMust[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

// testSession is the session type of the package's unit tests, which build
// sessions directly instead of connecting.
type testSession = Session
