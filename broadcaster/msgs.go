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
	ByzReliableMessageName              = "byz_reliable"
	ByzReliableEchoMessageName          = "byz_reliable_echo"
	ByzReliableReadyMessageName         = "byz_reliable_ready"
	ByzChannelMessageName               = "byz_channel"
	ByzChannelDomainMessageName         = "byz_channel_domain"
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

type ByzReliableMessage struct {
	messages.BaseMsg
	messages.RawMsg
}

type ByzReliableEchoMessage struct {
	messages.BaseMsg
	Inner messages.RawMsg
}

type ByzReliableReadyMessage struct {
	messages.BaseMsg
	Inner messages.RawMsg
}

type ByzChannelMessage struct {
	messages.BaseMsg
	Inner  messages.RawMsg
	Sender types.ProcessID
	Number int
}

func (msg ByzChannelMessage) Type() string {
	return ByzChannelMessageName
}

type ByzChannelDomainMessage struct {
	messages.BaseMsg
	Inner messages.RawMsg
	N     int
}

func (msg ByzChannelDomainMessage) Type() string {
	return ByzChannelDomainMessageName
}
