package wirerecord

import (
	"testing"
)

// collect feeds the splitter and returns the frames it emitted, copied because the
// emitted slice aliases the splitter's buffer.
func collect(t *testing.T, s *FrameSplitter, chunks ...[]byte) [][]byte {
	t.Helper()

	var got [][]byte
	for _, chunk := range chunks {
		if err := s.Feed(chunk, func(frame []byte) error {
			got = append(got, append([]byte(nil), frame...))
			return nil
		}); err != nil {
			t.Fatalf("Feed: %v", err)
		}
	}
	return got
}

// mustNotEmit returns an emit that fails the test if anything reaches it.
func mustNotEmit(t *testing.T) func([]byte) error {
	t.Helper()
	return func([]byte) error {
		t.Error("emit called; no frame should have come out")
		return nil
	}
}
