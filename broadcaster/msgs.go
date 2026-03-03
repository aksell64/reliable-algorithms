package broadcaster

import (
	"reliable/messages"
)

const (
	ReliableBroadcastMessageName = "reliable_broadcast"
)

type ReliableBroadcastMessage struct {
	messages.BaseMsg
	messages.RawMsg
}

func (msg ReliableBroadcastMessage) Type() string {
	return ReliableBroadcastMessageName
}
