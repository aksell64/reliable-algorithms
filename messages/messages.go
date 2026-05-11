package messages

import (
	"bytes"
	"reliable/types"

	"github.com/google/uuid"
)

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

func (msg BaseMsg) Equal(other BaseMsg) bool {
	return msg.ID() == other.ID() &&
		msg.Type() == other.Type() &&
		msg.From() == other.From() &&
		msg.Name() == other.Name()
}

func NewBase(id uuid.UUID, from types.ProcessID, name string) BaseMsg {
	return BaseMsg{
		Id:      id,
		FromPID: from,
		MsgName: name,
	}
}

type RawMsg struct {
	Raw     []byte
	RawType string
}

func NewRaw(raw []byte, typ string) RawMsg {
	return RawMsg{
		Raw:     raw,
		RawType: typ,
	}
}

func (msg RawMsg) Equal(other RawMsg) bool {
	return bytes.Equal(msg.Raw, other.Raw) && msg.RawType == other.RawType
}
