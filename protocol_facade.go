package gocql

import (
	model "github.com/scylladb/gocql/v2/internal/model"
	protocol "github.com/scylladb/gocql/v2/internal/protocol"
)

var ErrFrameTooBig = protocol.ErrFrameTooBig

type PageState = model.PageState

func RestorePageState(a0 []byte) PageState { return model.RestorePageState(a0) }

type Result = model.Result

func StartPage() PageState { return model.StartPage() }
