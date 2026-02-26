package failure

import (
	"reliable/types"

	"github.com/google/uuid"
)

type HeartbeatRequestMessage struct {
	types.Message
	id   uuid.UUID
	from types.ProcessID
}

func (msg HeartbeatRequestMessage) Name() string {
	return "HeartbeatRequestMessage"
}

func (msg HeartbeatRequestMessage) ID() uuid.UUID {
	return msg.id
}

func (msg HeartbeatRequestMessage) From() types.ProcessID {
	return msg.from
}

type HeartbeatResponseMessage struct {
	types.Message
	id    uuid.UUID
	reqId uuid.UUID
	from  types.ProcessID
}

func (msg HeartbeatResponseMessage) Name() string {
	return "HeartbeatResponseMessage"
}

func (msg HeartbeatResponseMessage) ID() uuid.UUID {
	return msg.id
}

func (msg HeartbeatResponseMessage) From() types.ProcessID {
	return msg.from
}
