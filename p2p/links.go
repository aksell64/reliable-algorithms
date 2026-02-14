package p2p

import (
	"reliable/network"
	"reliable/types"
	"reliable/utils"
	"time"
)

type Link interface {
	types.Layer
	Send(to types.ProcessID, msg types.Message)
}

type LinkOpt func(link *BaseLink)

func WithSendSleep(min, max time.Duration) LinkOpt {
	return func(link *BaseLink) {
		f := func() {
			utils.RandomSleep(min, max)
		}
		link.sendSleepFunc = &f
	}
}

func WithDeliverSleep(min, max time.Duration) LinkOpt {
	return func(link *BaseLink) {
		f := func() {
			utils.RandomSleep(min, max)
		}
		link.deliverSleepFunc = &f
	}
}

func WithNetwork(net network.Network) LinkOpt {
	return func(link *BaseLink) {
		link.net = net
	}
}

type BaseLink struct {
	types.Deliverer
	self             types.ProcessID
	sendSleepFunc    *func()
	deliverSleepFunc *func()
	net              network.Network
	once             *types.WorkerOnce
}

func NewBaseLink(id types.ProcessID, opts ...LinkOpt) Link {
	l := &BaseLink{
		self:      id,
		Deliverer: types.NewUnaryDeliverer(id),
		once:      types.NewWorkerOnce(),
		net:       network.Global(),
	}

	for _, opt := range opts {
		opt(l)
	}
	return l
}

func (b *BaseLink) Init() {
	b.once.Init(func() {
		b.net.Connect(b)
	})
}

func (b *BaseLink) Start() {}

func (b *BaseLink) Stop() {
	b.once.Stop(func() {
	})
}

func (b *BaseLink) Send(to types.ProcessID, msg types.Message) {
	if b.sendSleepFunc != nil {
		f := *b.sendSleepFunc
		f()
	}

	if to == b.ProcessID() {
		b.Deliver(msg)
		return
	}

	b.net.Send(b.ProcessID(), to, msg)
}

func (b *BaseLink) Deliver(msg types.Message) {
	if b.deliverSleepFunc != nil {
		f := *b.deliverSleepFunc
		f()
	}
	b.Deliverer.Deliver(msg)
}

func (b *BaseLink) Instance() string {
	return "bl"
}
