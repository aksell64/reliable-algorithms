package election

import (
	"reliable/messages"
	"time"
)

const (
	ComplainMsgName = "complain"
)

type HeartbeatMessage struct {
	messages.BaseMsg
	Epoch  int
	SentAt time.Time
}

func (msg HeartbeatMessage) Type() string {
	return "hb"
}

type ComplainMsg struct {
	messages.BaseMsg
	Round int
}

func (msg ComplainMsg) Type() string {
	return ComplainMsgName
}
