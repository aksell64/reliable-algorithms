package broadcaster

import (
	"reliable/messages"
	"reliable/types"
)

const (
	ReliableBroadcastMessageName        = "reliable_broadcast"
	ByzConsistenceMessageName           = "byz_consistence"
	ByzConsistenceEchoMessageName       = "byz_consistence_echo"
	ByzConsistenceSignedEchoMessageName = "byz_consistence_signed_echo"
	ByzConsistenceFinalMessageName      = "byz_consistence_final"
)

type ReliableBroadcastMessage struct {
	messages.BaseMsg
	messages.RawMsg
}

func (msg ReliableBroadcastMessage) Type() string {
	return ReliableBroadcastMessageName
}

type ByzConsistenceMessage struct {
	messages.BaseMsg
	messages.RawMsg
}

type ByzConsistenceEchoMessage struct {
	messages.BaseMsg
	Inner messages.RawMsg
}

type ByzConsistenceSignedEchoMessage struct {
	messages.BaseMsg
	Inner messages.RawMsg
	Sign  []byte
}

type ByzConsistenceFinalMessage struct {
	messages.BaseMsg
	Inner messages.RawMsg
	Signs []ByzConsistenceFinalMessageSign
}

type ByzConsistenceFinalMessageSign struct {
	ProcessID types.ProcessID
	Sign      []byte
}
