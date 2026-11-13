package tests

import "testing"

func AssertTrue(t *testing.T, description string, value bool) {
	t.Helper()
	if !value {
		t.Fatalf("expected %s to be true", description)
	}
}

func AssertEqual(t *testing.T, description string, expected, actual any) {
	t.Helper()
	if expected != actual {
		t.Fatalf("expected %s to be (%+v) but was (%+v) instead", description, expected, actual)
	}
}

func AssertNil(t *testing.T, description string, actual any) {
	t.Helper()
	if actual != nil {
		t.Fatalf("expected %s to be (nil) but was (%+v) instead", description, actual)
	}
}
