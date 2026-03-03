package flooding

import (
	"reliable/types"

	"github.com/google/uuid"
)

const (
	ProposalMessageName = "proposal"
	DecidedMessageName  = "decided"
	CrashMessageName    = "crash"
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
