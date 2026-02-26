package network

import (
	"reliable/types"

	"github.com/google/uuid"
)

func init() {
	SetGlobal(NewInMemory())
}

func Global() Network {
	return globalNetwork
}

func SetGlobal(net Network) {
	globalNetwork = net
}

var globalNetwork Network

func Send(from, to types.ProcessID, msg types.Message) {
	globalNetwork.Send(from, to, msg)
}

func Connect(deliverer types.Deliverer) {
	globalNetwork.Connect(deliverer)
}

func Disconnect(id uuid.UUID) {}

type Network interface {
	Send(from, to types.ProcessID, msg types.Message)
	Connect(deliverer types.Deliverer)
	Boostrap() error
}
