package outer

import "testing"

func solo() int { return 0 }

func TestSolo(t *testing.T) { _ = solo() + InternalSolo() }
