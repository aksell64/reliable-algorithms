package election

import (
	"reliable/types"
	"time"

	"github.com/google/uuid"
)

type HeartbeatMessage struct {
	id     uuid.UUID
	epoch  int
	from   types.ProcessID
	sentAt time.Time
}

func (msg HeartbeatMessage) Name() string {
	return "HeartbeatRequestMessage"
}

func (msg HeartbeatMessage) ID() uuid.UUID {
	return msg.id
}

func (msg HeartbeatMessage) From() types.ProcessID {
	return msg.from
}
