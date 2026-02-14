package mocks

import (
	"reliable/types"
)

type MockUnBufChanDeliverer struct {
	types.Deliverer
	unbuffered chan types.Message
}

func NewMockUnBufChanDeliverer(pid types.ProcessID) *MockUnBufChanDeliverer {
	return &MockUnBufChanDeliverer{
		Deliverer:  types.NewUnaryDeliverer(pid),
		unbuffered: make(chan types.Message, 1),
	}
}

func (d *MockUnBufChanDeliverer) Deliver(msg types.Message) {
	d.unbuffered <- msg
}

func (d *MockUnBufChanDeliverer) Ch() chan types.Message {
	return d.unbuffered
}

func (d *MockUnBufChanDeliverer) Empty() bool {
	select {
	case <-d.unbuffered:
		return false
	default:
		return true
	}
}

func (d *MockUnBufChanDeliverer) Lose() {
	select {
	case <-d.unbuffered:
	default:
	}
}
