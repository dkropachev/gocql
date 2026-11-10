package gocql

import (
	model "github.com/scylladb/gocql/v2/internal/model"
)

// PageState distinguishes the initial page, a continuation (whose token may be
// empty), and completion. Its zero value starts paging.
type PageState model.PageState

// More reports whether this state contains a continuation token.
func (p PageState) More() bool { return model.PageState(p).More() }

// Complete reports whether the server has finished paging.
func (p PageState) Complete() bool { return model.PageState(p).Complete() }

// Token returns an owned copy of a continuation token, including a non-nil
// empty slice for an empty continuation. Initial and complete states return nil;
// use More and Complete to distinguish them. The token is not full-state serialization.
func (p PageState) Token() []byte { return model.PageState(p).Token() }

// PageStateFromToken copies token into a continuation state. Nil and empty
// inputs both represent an empty continuation. Use StartPage to begin paging;
// use PageState.More and PageState.Complete to distinguish continuation and completion.
func PageStateFromToken(token []byte) PageState { return PageState(model.PageStateFromToken(token)) }

type Result = model.Result

// StartPage is the state that starts paging from the first page. It is the
// zero PageState.
var StartPage PageState
