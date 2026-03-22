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
	RWStateMsgName     = "rw_state"
	RWReadMsgName      = "rw_read"
	RWWriteMsgName     = "rw_write"
	RWAcceptMsgName    = "rw_accept"
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

type RWReadMsg struct {
	messages.BaseMsg
}

func NewRwReadMsg(from types.ProcessID) types.Message {
	return RWReadMsg{
		BaseMsg: messages.NewBase(uuid.New(), from, RWReadMsgName),
	}
}

type RWStateMsg struct {
	messages.BaseMsg
	ValueEpoch  int
	ValueRaw    messages.RawMsg
	WriteSetRaw []RWSnapshot
}

type RWSnapshot struct {
	Epoch    int
	ValueRaw messages.RawMsg
}

type RWWriteMsg struct {
	messages.BaseMsg
	ValueRaw messages.RawMsg
}

func NewRWWriteMsg(from types.ProcessID, val messages.RawMsg) RWWriteMsg {
	return RWWriteMsg{
		BaseMsg:  messages.NewBase(uuid.New(), from, RWWriteMsgName),
		ValueRaw: val,
	}
}

type RWAcceptMsg struct {
	messages.BaseMsg
	ValueRaw messages.RawMsg
}
