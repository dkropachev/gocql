package outer

type State struct {
	InternalCount  int
	InternalTagged int `json:"t"`
}

func InternalHelper(s *State) int { return s.InternalCount }

func (s *State) InternalBump() { s.InternalCount++ }

func InternalExported() int { return 1 }

func Run() int {
	s := &State{}
	s.InternalBump()
	return InternalHelper(s) + s.InternalTagged
}

// InternalSolo must stay: a test file declares solo.
func InternalSolo() int { return Run() }

// InternalReport is used from consumer; as a method it would be exported on a
// public type, so methodize must keep it a function.
func InternalReport(s *State) int { return s.InternalCount }
