//go:build unit
// +build unit

package serialization

import "testing"

func Test1Pointers(t *testing.T) {
	t.Parallel()

	val1 := new(int16)
	*val1 = int16(0)
	testPtr := getPointers(val1)

	// the first pointer has not been changed - it must be not error.
	if err := testPtr.Valid(val1); err != nil {
		t.Error("valid function not should return error")
	}

	val2 := new(int16)
	// the first pointer has been changed - it must be an error.
	if err := testPtr.Valid(val2); err == nil {
		t.Error("valid function should return error")
	}
}

func Test2Pointers(t *testing.T) {
	t.Parallel()

	val1 := new(*int16)
	*val1 = new(int16)
	testPtr := getPointers(val1)
	// the first pointer has not been changed - it must be not error,
	// but the second pointer has not been changed too - it must be an error.
	if err := testPtr.Valid(val1); err == nil {
		t.Error("valid function should return error")
	}

	*val1 = new(int16)
	// the first pointer has not been changed - it must be not error,
	// the second pointer has been changed - it must be not error.
	if err := testPtr.Valid(val1); err != nil {
		t.Error("valid function not should return error")
	}
}
