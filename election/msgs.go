package election

import (
	"reliable/messages"
	"time"
)

type HeartbeatMessage struct {
	messages.BaseMsg
	Epoch  int
	SentAt time.Time
}

func (msg HeartbeatMessage) Type() string {
	return "hb"
}
