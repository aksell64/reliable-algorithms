package byzantine

import (
	"reliable/messages"
	"reliable/types"

	"github.com/google/uuid"
)

const (
	EpochMsgName       = "epoch"
	CCSignedMsgName    = "cc_signed"
	CCCollectedMsgName = "cc_collected"
)

type EpochMsg struct {
	messages.BaseMsg
	Epoch int
}

func (msg EpochMsg) Type() string {
	return EpochMsgName
}

func NewEpochMsg(from types.ProcessID, epoch int) types.Message {
	return EpochMsg{
		BaseMsg: messages.NewBase(uuid.New(), from, EpochMsgName),
		Epoch:   epoch,
	}
}

type CCSendMsg struct {
	messages.BaseMsg
	Signed []byte
	Msg    []byte
}

func (msg CCSendMsg) Type() string {
	return CCSignedMsgName
}

func NewCCSignedMsg(from types.ProcessID, sign, msg []byte) types.Message {
	return CCSendMsg{
		BaseMsg: messages.NewBase(uuid.New(), from, CCSignedMsgName),
		Signed:  sign,
		Msg:     msg,
	}
}

type CCCollectInner struct {
	Sender types.ProcessID
	Msg    []byte
	Sign   []byte
}

type CCCollectedMsg struct {
	messages.BaseMsg
	Inner []CCCollectInner
}

func (msg CCCollectedMsg) Type() string {
	return CCCollectedMsgName
}

func NewCCCollectedMsg(from types.ProcessID) CCCollectedMsg {
	return CCCollectedMsg{
		BaseMsg: messages.NewBase(uuid.New(), from, CCCollectedMsgName),
		Inner:   make([]CCCollectInner, 0),
	}
}

func (msg CCCollectedMsg) AddMsg(inner CCCollectInner) CCCollectedMsg {
	msg.Inner = append(msg.Inner, inner)
	return msg
}
