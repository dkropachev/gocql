package consumer

import "example.com/fx/outer"

func Call() int {
	return outer.InternalExported() + outer.Plain(1) + outer.InternalReport(&outer.State{})
}
