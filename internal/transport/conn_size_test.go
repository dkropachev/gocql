package transport

import (
	"testing"
	"unsafe"
)

// Conn must stay within the 1024 B allocation size class.
func TestConnSize(t *testing.T) {
	if n := unsafe.Sizeof(Conn{}); n > 1024 {
		t.Fatalf("Conn is %d bytes, want <= 1024", n)
	}
}
