package inner

type Codec[T any] struct{ V T }

func NewCodec[T any](v T) Codec[T] { return Codec[T]{V: v} }

var Default = 1

func Plain(a int, b ...string) int { return a + len(b) }

type Data struct{ N int }

type Plainish struct{ M int }
