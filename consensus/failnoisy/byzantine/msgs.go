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
	Ts     int
}

func (msg CCSendMsg) Type() string {
	return CCSignedMsgName
}

func NewCCSignedMsg(from types.ProcessID, sign, msg []byte, ts int) types.Message {
	return CCSendMsg{
		BaseMsg: messages.NewBase(uuid.New(), from, CCSignedMsgName),
		Signed:  sign,
		Msg:     msg,
		Ts:      ts,
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
	Ts    int
}

func (msg CCCollectedMsg) Type() string {
	return CCCollectedMsgName
}

func NewCCCollectedMsg(from types.ProcessID, ts int) CCCollectedMsg {
	return CCCollectedMsg{
		BaseMsg: messages.NewBase(uuid.New(), from, CCCollectedMsgName),
		Inner:   make([]CCCollectInner, 0),
		Ts:      ts,
	}
}

func (msg CCCollectedMsg) AddMsg(inner CCCollectInner) CCCollectedMsg {
	msg.Inner = append(msg.Inner, inner)
	return msg
}

type RWReadMsg struct {
	messages.BaseMsg
	Ts int
}

func NewRwReadMsg(from types.ProcessID, ts int) types.Message {
	return RWReadMsg{
		BaseMsg: messages.NewBase(uuid.New(), from, RWReadMsgName),
		Ts:      ts,
	}
}

type RWStateMsg struct {
	messages.BaseMsg
	ValueEpoch  int
	ValueRaw    messages.RawMsg
	WriteSetRaw []RWSnapshot
	Ts          int
}

type RWSnapshot struct {
	Epoch    int
	ValueRaw messages.RawMsg
}

type RWWriteMsg struct {
	messages.BaseMsg
	ValueRaw messages.RawMsg
	Ts       int
}

func NewRWWriteMsg(from types.ProcessID, val messages.RawMsg, ts int) RWWriteMsg {
	return RWWriteMsg{
		BaseMsg:  messages.NewBase(uuid.New(), from, RWWriteMsgName),
		ValueRaw: val,
		Ts:       ts,
	}
}

type RWAcceptMsg struct {
	messages.BaseMsg
	ValueRaw messages.RawMsg
	Ts       int
}
