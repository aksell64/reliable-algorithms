package commitreveal

import (
	"reliable/messages"
	"reliable/types"
)

type CommitMsg struct {
	messages.BaseMsg
	Commit []byte
}

type RevealMsg struct {
	messages.BaseMsg
	Value types.Value
	Salt  []byte
}

type SchemeMsg struct {
	messages.BaseMsg
	Inner types.Message
	Ts    int
}
