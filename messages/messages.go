package messages

import (
	"reliable/types"

	"github.com/google/uuid"
)

const (
	ProposalMessageName          = "proposal"
	DecidedMessageName           = "decided"
	CrashMessageName             = "crash"
	ReliableBroadcastMessageName = "reliable_broadcast"
)

type ProposalMessage struct {
	Id        uuid.UUID
	Proposals []types.Value
	PID       types.ProcessID
	Round     int
}

func (msg ProposalMessage) ID() uuid.UUID {
	return msg.Id
}

func (msg ProposalMessage) Name() string {
	return ProposalMessageName
}

func (msg ProposalMessage) From() types.ProcessID {
	return msg.PID
}

type DecidedMessage struct {
	Id       uuid.UUID
	Decision types.Value
	PID      types.ProcessID
}

func (msg DecidedMessage) ID() uuid.UUID {
	return msg.Id
}

func (msg DecidedMessage) Name() string {
	return DecidedMessageName
}

func (msg DecidedMessage) From() types.ProcessID {
	return msg.PID
}

type CrashMessage struct {
	Id  uuid.UUID
	PID types.ProcessID
}

func (msg CrashMessage) ID() uuid.UUID {
	return msg.Id
}

func (msg CrashMessage) Name() string {
	return CrashMessageName
}

func (msg CrashMessage) From() types.ProcessID {
	return msg.PID
}

type ReliableBroadcastMessage struct {
	types.Message
	Id     uuid.UUID
	Inner  types.Message
	Sender types.ProcessID
}

func (msg ReliableBroadcastMessage) Name() string {
	return ReliableBroadcastMessageName
}

func (msg ReliableBroadcastMessage) From() types.ProcessID {
	return msg.Sender
}

func (msg ReliableBroadcastMessage) ID() uuid.UUID {
	return msg.Id
}

type Ack struct {
	types.Message
}

type BaseMsg struct {
	Id      uuid.UUID
	FromPID types.ProcessID
	MsgName string
}

func (msg BaseMsg) ID() uuid.UUID {
	return msg.Id
}
func (msg BaseMsg) Name() string {
	return msg.MsgName
}

func (msg BaseMsg) From() types.ProcessID {
	return msg.FromPID
}
func (msg BaseMsg) Type() string { return msg.Name() }

func NewBase(id uuid.UUID, from types.ProcessID, name string) BaseMsg {
	return BaseMsg{
		Id:      id,
		FromPID: from,
		MsgName: name,
	}
}
