package gocql

func codecTestMust[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

// bindOptions binds v to session, failing loudly on values a test meant to
// be valid.
func bindOptions(session *Session, v Options) CompiledOptions {
	o, err := session.CompileOptions(v)
	if err != nil {
		panic(err)
	}
	return o
}
