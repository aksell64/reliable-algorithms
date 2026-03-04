package commitreveal

import (
	"reliable/messages"
)

const (
	CommitMsgName = "commit"
	RevealMsgName = "reveal"
	SchemeMsgName = "cr_scheme"
)

type CommitMsg struct {
	messages.BaseMsg
	Commit []byte
}

func (msg CommitMsg) Type() string {
	return CommitMsgName
}

type RevealMsg struct {
	messages.BaseMsg
	ValueRaw messages.RawMsg
	Salt     []byte
}

func (msg RevealMsg) Type() string {
	return RevealMsgName
}

type SchemeMsg struct {
	messages.BaseMsg
	messages.RawMsg
	Ts int
}

func (msg SchemeMsg) Type() string {
	return SchemeMsgName
}
