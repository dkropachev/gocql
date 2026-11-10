package gocql

import (
	model "github.com/scylladb/gocql/v2/internal/model"
)

type PageState = model.PageState

func RestorePageState(a0 []byte) PageState { return model.RestorePageState(a0) }

type Result = model.Result

// StartPage is the state that starts paging from the first page. It is the
// zero PageState.
var StartPage PageState
