package frame

import "testing"

func TestOpString(t *testing.T) {
	t.Parallel()

	if got := OpAuthSuccess.String(); got != "AUTH_SUCCESS" {
		t.Fatalf("OpAuthSuccess.String() = %q", got)
	}
	// An opcode outside the protocol must still print something identifying
	// rather than collide with a known one.
	if got := Op(0x42).String(); got != "UNKNOWN_OP_66" {
		t.Fatalf("Op(0x42).String() = %q, want UNKNOWN_OP_66", got)
	}
}
