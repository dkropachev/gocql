package outer

// Use shadows the package name inner, so unfacade must pick another alias.
func Use() int {
	inner := 3
	c := NewCodec[int](inner)
	var cc Codec[int] = c
	return Plain(Default, "x") + inner + cc.V
}

// Page embeds the alias, so its field is named PageData and the alias must stay.
type Page struct{ PageData }

func (p Page) Count() int { return p.PageData.N }

func newState() *State { return &State{} }

// Local uses same-package aliases, which unfacade resolves without a qualifier.
func Local() *localState { return newLocal() }

// Wrap embeds an alias named like its target, which unfacade may qualify.
type Wrap struct{ Plainish }

func (w Wrap) Get() int { return w.Plainish.M }
