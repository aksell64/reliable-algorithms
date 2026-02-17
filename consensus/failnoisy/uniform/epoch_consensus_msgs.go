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
	Epoch int
}

type StateMsg struct {
	types.Message
	Ts    int
	Val   *types.Value
	Epoch int
}

type WriteMsg struct {
	types.Message
	Val   *types.Value
	Epoch int
}

type AcceptMsg struct {
	types.Message
	Epoch int
}

type DecidedMsg struct {
	types.Message
	Val   types.Value
	Epoch int
}
