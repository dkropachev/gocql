package outer

import inner "example.com/fx/inner"

type Codec[T any] = inner.Codec[T]

func NewCodec[T any](a0 T) Codec[T] { return inner.NewCodec[T](a0) }

var Default = inner.Default

func Plain(a0 int, a1 ...string) int { return inner.Plain(a0, a1...) }

type PageData = inner.Data

type localState = State

func newLocal() *localState { return newState() }

type Plainish = inner.Plainish
