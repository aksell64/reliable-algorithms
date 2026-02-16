package uniform

import "reliable/types"

const (
	ReadMsgName   = "read"
	StateMsgName  = "state"
	WriteMsgName  = "write"
	AcceptMsgName = "accept"
	DecideMsgName = "decide"
)

type ReadMsg struct {
	types.Message
	Ts int
}

type StateMsg struct {
	types.Message
	Ts  int
	Val *types.Value
}

type WriteMsg struct {
	types.Message
	Val *types.Value
	Ts  int
}

type AcceptMsg struct {
	types.Message
	Ts int
}

type DecidedMsg struct {
	types.Message
	Val types.Value
	Ts  int
}
