package gocql

func codecTestMust[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

// profileOf returns a pointer to a copy of p, so later changes to p do not
// reach options that already hold it.
func profileOf(p Profile) *Profile { return &p }

// testOptions freezes v, failing loudly on values a test meant to be valid.
func testOptions(v OptionValues) Options {
	o, err := NewOptions(nil, v)
	if err != nil {
		panic(err)
	}
	return o
}
